import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from mobility_ingest.adapters import FeedAdapter, UpstreamError
from mobility_ingest.contract import NormalizedEvent, NormalizeResult, Quarantined, format_observed_at
from mobility_ingest.lease import acquire, assert_fenced


def record_fetch(conn: Connection, feed: str, fetched_at: datetime, status: int,
                 body: bytes) -> tuple[bool, int | None]:
    payload_hash = hashlib.sha256(body).hexdigest()
    conn.execute(
        text("INSERT INTO feed_state (feed, last_polled_at, last_success_at) "
             "VALUES (:feed, :at, NULL) ON DUPLICATE KEY UPDATE last_polled_at = VALUES(last_polled_at)"),
        {"feed": feed, "at": fetched_at.replace(tzinfo=None)})
    if status == 200:
        conn.execute(text("UPDATE feed_state SET last_success_at = :at, consecutive_failures = 0 "
                          "WHERE feed = :feed"),
                     {"feed": feed, "at": fetched_at.replace(tzinfo=None)})
    last_hash = conn.execute(
        text("SELECT payload_hash FROM raw_fetch WHERE feed = :feed ORDER BY id DESC LIMIT 1"),
        {"feed": feed}).scalar()
    if last_hash == payload_hash:
        return False, None
    result = conn.execute(
        text("INSERT INTO raw_fetch (feed, fetched_at, http_status, payload_hash, body) "
             "VALUES (:feed, :at, :status, :hash, :body)"),
        {"feed": feed, "at": fetched_at.replace(tzinfo=None), "status": status,
         "hash": payload_hash, "body": body})
    return True, result.lastrowid


TOPIC = "events.normalized"
SINKS = ("kafka", "opensearch")
EVENT_TYPE = "carpark_availability"


@dataclass(frozen=True)
class SaveResult:
    inserted: int
    skipped_unchanged: int
    skipped_duplicate: int
    quarantined: int


def _utc_naive(moment: datetime) -> datetime:
    return moment.astimezone(timezone.utc).replace(tzinfo=None)


def _envelope(event: NormalizedEvent) -> dict:
    return {
        "event_id": event.event_id,
        "schema_version": event.schema_version,
        "type": EVENT_TYPE,
        "feed": event.feed,
        "entity_id": event.entity_id,
        "observed_at": format_observed_at(event.observed_at),
        "trace_id": event.trace_id,
        "payload": event.payload,
        "lat": None if event.lat is None else str(event.lat),
        "lon": None if event.lon is None else str(event.lon),
        "state_hash": event.state_hash,
    }


def save_batch(conn: Connection, *, feed: str, fencing_token: int, raw_fetch_id: int | None,
               events: list[NormalizedEvent], quarantined: list[Quarantined]) -> SaveResult:
    assert_fenced(conn, feed, fencing_token)
    inserted = skipped_unchanged = skipped_duplicate = 0
    for event in events:
        latest = conn.execute(
            text("SELECT state_hash FROM normalized_events WHERE entity_id = :entity "
                 "ORDER BY observed_at DESC LIMIT 1"),
            {"entity": event.entity_id}).scalar()
        if latest == event.state_hash:
            skipped_unchanged += 1
            continue
        # Explicit check, not ON DUPLICATE KEY: PyMySQL reports matched rows, so a no-op
        # upsert looks like an insert. The feed lease (assert_fenced) serializes writers.
        already = conn.execute(
            text("SELECT 1 FROM normalized_events WHERE event_id = :event_id AND observed_at = :observed_at"),
            {"event_id": event.event_id, "observed_at": _utc_naive(event.observed_at)}).scalar()
        if already:
            skipped_duplicate += 1
            continue
        conn.execute(
            text("INSERT INTO normalized_events (event_id, feed, entity_id, observed_at, lat, lon, "
                 "payload, state_hash, raw_fetch_id, fencing_token, trace_id, schema_version) "
                 "VALUES (:event_id, :feed, :entity, :observed_at, :lat, :lon, :payload, :state_hash, "
                 ":raw_fetch_id, :token, :trace_id, :schema_version)"),
            {"event_id": event.event_id, "feed": event.feed, "entity": event.entity_id,
             "observed_at": _utc_naive(event.observed_at), "lat": event.lat, "lon": event.lon,
             "payload": json.dumps(event.payload), "state_hash": event.state_hash,
             "raw_fetch_id": raw_fetch_id, "token": fencing_token, "trace_id": event.trace_id,
             "schema_version": event.schema_version})
        conn.execute(
            text("INSERT INTO outbox (event_id, topic, partition_key, payload) "
                 "VALUES (:event_id, :topic, :key, :payload)"),
            {"event_id": event.event_id, "topic": TOPIC, "key": event.entity_id,
             "payload": json.dumps(_envelope(event))})
        outbox_id = conn.execute(
            text("SELECT id FROM outbox WHERE event_id = :event_id"),
            {"event_id": event.event_id}).scalar()
        for sink in SINKS:
            conn.execute(
                text("INSERT INTO outbox_delivery (outbox_id, sink) VALUES (:outbox_id, :sink)"),
                {"outbox_id": outbox_id, "sink": sink})
        inserted += 1
    now = _utc_naive(datetime.now(timezone.utc))
    for item in quarantined:
        conn.execute(
            text("INSERT INTO quarantine (feed, reason, content, raw_fetch_id, created_at) "
                 "VALUES (:feed, :reason, :content, :raw_fetch_id, :created_at)"),
            {"feed": feed, "reason": item.reason, "content": json.dumps(item.content, ensure_ascii=False),
             "raw_fetch_id": raw_fetch_id, "created_at": now})
    return SaveResult(inserted=inserted, skipped_unchanged=skipped_unchanged,
                      skipped_duplicate=skipped_duplicate, quarantined=len(quarantined))


@dataclass(frozen=True)
class PollResult:
    status: str
    inserted: int
    skipped_unchanged: int
    quarantined: int


def _upstream_failure(conn: Connection, feed: str) -> "PollResult":
    conn.execute(
        text("INSERT INTO feed_state (feed, consecutive_failures) VALUES (:feed, 1) "
             "ON DUPLICATE KEY UPDATE consecutive_failures = consecutive_failures + 1"),
        {"feed": feed})
    return PollResult(status="upstream_error", inserted=0, skipped_unchanged=0, quarantined=0)


def poll_once(engine: Engine, *, adapter: FeedAdapter,
              normalize: Callable[[bytes, datetime, str], NormalizeResult],
              holder: str, lease_ttl_seconds: int, now: datetime, trace_id: str) -> PollResult:
    with engine.begin() as conn:
        token = acquire(conn, adapter.name, holder, lease_ttl_seconds, now)
        if token is None:
            return PollResult(status="skipped_lease", inserted=0, skipped_unchanged=0, quarantined=0)
        try:
            fetched = adapter.fetch()
        except UpstreamError:
            return _upstream_failure(conn, adapter.name)
        if not 200 <= fetched.status < 300:
            # An error page is not feed data: do not store it as a fetch or normalize it.
            return _upstream_failure(conn, adapter.name)
        changed, raw_fetch_id = record_fetch(conn, adapter.name, now, fetched.status, fetched.body)
        if not changed:
            return PollResult(status="skipped_unchanged", inserted=0, skipped_unchanged=0, quarantined=0)
        result = normalize(fetched.body, now, trace_id)
        saved = save_batch(conn, feed=adapter.name, fencing_token=token, raw_fetch_id=raw_fetch_id,
                           events=result.events, quarantined=result.quarantined)
        return PollResult(status="ok", inserted=saved.inserted,
                          skipped_unchanged=saved.skipped_unchanged, quarantined=saved.quarantined)
