import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class OutboxRow:
    outbox_id: int
    topic: str
    partition_key: str
    payload: dict
    headers: dict[str, str]


class PartialPublish(Exception):
    """A sink delivered some rows of a batch and failed the rest.

    `acked` rows are marked sent; every other row of the batch is recorded as a failed attempt.
    """

    def __init__(self, acked: list[int], error: Exception | str):
        super().__init__(str(error))
        self.acked = acked
        self.error = error if isinstance(error, Exception) else RuntimeError(error)


class Sink(Protocol):
    name: str

    def publish(self, rows: list[OutboxRow]) -> list[int]: ...


def _utc_naive(moment: datetime) -> datetime:
    return moment.astimezone(timezone.utc).replace(tzinfo=None)


def _record_failure(engine: Engine, sink_name: str, outbox_ids: list[int], error: Exception) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE outbox_delivery SET attempts = attempts + 1, last_error = LEFT(:err, 255) "
                 "WHERE outbox_id = :id AND sink = :sink"),
            [{"err": f"{type(error).__name__}: {error}", "sink": sink_name, "id": oid}
             for oid in outbox_ids])


def relay_once(engine: Engine, sink: Sink, batch_size: int = 100) -> int:
    attempted: list[int] = []
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text("SELECT o.id, o.topic, o.partition_key, o.payload FROM outbox_delivery od "
                     "JOIN outbox o ON o.id = od.outbox_id "
                     "WHERE od.sink = :sink AND od.sent_at IS NULL "
                     "ORDER BY od.outbox_id LIMIT :n FOR UPDATE OF od SKIP LOCKED"),
                {"sink": sink.name, "n": batch_size}).all()
            if not rows:
                return 0
            batch = []
            for row in rows:
                payload = json.loads(row.payload)
                batch.append(OutboxRow(outbox_id=row.id, topic=row.topic, partition_key=row.partition_key,
                                       payload=payload,
                                       headers={"schema_version": str(payload["schema_version"])}))
            attempted = [r.outbox_id for r in batch]
            partial: PartialPublish | None = None
            try:
                acked = sink.publish(batch)
            except PartialPublish as exc:
                acked, partial = exc.acked, exc
            if acked:
                # Point updates on the primary key (outbox_id, sink): an `outbox_id IN (...)` range
                # would also lock the other sinks' rows for the same outbox ids and serialize relays.
                conn.execute(
                    text("UPDATE outbox_delivery SET sent_at = now(3) "
                         "WHERE outbox_id = :id AND sink = :sink"),
                    [{"id": oid, "sink": sink.name} for oid in acked])
        if partial is not None:
            # Committed above: the delivered rows stay sent; only the undelivered ones count an attempt.
            acked_ids = set(acked)
            _record_failure(engine, sink.name, [i for i in attempted if i not in acked_ids], partial.error)
        return len(acked)
    except Exception as exc:
        # Recorded after the transaction above has rolled back and released its locks.
        if attempted:
            _record_failure(engine, sink.name, attempted, exc)
        raise


def purge_outbox(engine: Engine, older_than_days: int = 3, batch_size: int = 1000) -> int:
    cutoff = _utc_naive(datetime.now(timezone.utc) - timedelta(days=older_than_days))
    total = 0
    while True:
        with engine.begin() as conn:
            ids = conn.execute(
                text("SELECT o.id FROM outbox o WHERE o.created_at < :cutoff AND NOT EXISTS ("
                     "SELECT 1 FROM outbox_delivery od WHERE od.outbox_id = o.id AND od.sent_at IS NULL) "
                     "ORDER BY o.id LIMIT :n"),
                {"cutoff": cutoff, "n": batch_size}).scalars().all()
            if not ids:
                return total
            conn.execute(text("DELETE FROM outbox_delivery WHERE outbox_id IN :ids")
                         .bindparams(bindparam("ids", expanding=True)), {"ids": ids})
            conn.execute(text("DELETE FROM outbox WHERE id IN :ids")
                         .bindparams(bindparam("ids", expanding=True)), {"ids": ids})
            total += len(ids)
        if len(ids) < batch_size:
            return total
