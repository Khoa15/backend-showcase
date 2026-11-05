from datetime import datetime, timezone
import pytest
from sqlalchemy import text
from mobility_ingest.db import make_engine
from mobility_ingest.contract import compute_event_id, compute_state_hash, NormalizedEvent, Quarantined
from mobility_ingest.ingest import save_batch
from mobility_ingest.lease import acquire, FencingError

pytestmark = pytest.mark.integration
T = datetime(2026, 11, 3, 12, 0, 0, tzinfo=timezone.utc)
TRACE = "b" * 32

def ev(entity, lots, t=T):
    payload = {"CarParkID": entity, "Area": None, "Development": None, "LotType": "C",
               "Agency": "HDB", "AvailableLots": lots}
    sh = compute_state_hash(payload)
    return NormalizedEvent(compute_event_id("save_test", entity, t, sh), "save_test", entity,
                           t, None, None, payload, sh, 1, TRACE)

def fenced_conn(url, feed):
    eng = make_engine(url)
    c = eng.connect()
    tx = c.begin()
    token = acquire(c, feed, "node-1", 30, T)
    return c, tx, token

def test_inserts_event_outbox_and_two_delivery_rows(migrated_url):
    c, tx, tok = fenced_conn(migrated_url, "save_a")
    r = save_batch(c, feed="save_a", fencing_token=tok, raw_fetch_id=None,
                   events=[ev("E1", 5)], quarantined=[])
    assert r.inserted == 1
    sinks = c.execute(text("SELECT sink FROM outbox_delivery od JOIN outbox o ON o.id=od.outbox_id "
                           "WHERE o.partition_key='E1' ORDER BY sink")).scalars().all()
    assert sinks == ["kafka", "opensearch"]
    tx.rollback(); c.close()

def test_unchanged_value_is_skipped(migrated_url):
    c, tx, tok = fenced_conn(migrated_url, "save_b")
    save_batch(c, feed="save_b", fencing_token=tok, raw_fetch_id=None, events=[ev("E2", 5)], quarantined=[])
    r = save_batch(c, feed="save_b", fencing_token=tok, raw_fetch_id=None,
                   events=[ev("E2", 5, datetime(2026, 11, 3, 12, 1, 0, tzinfo=timezone.utc))], quarantined=[])
    assert r.inserted == 0 and r.skipped_unchanged == 1
    tx.rollback(); c.close()

def test_changed_value_is_inserted(migrated_url):
    c, tx, tok = fenced_conn(migrated_url, "save_c")
    save_batch(c, feed="save_c", fencing_token=tok, raw_fetch_id=None, events=[ev("E3", 5)], quarantined=[])
    r = save_batch(c, feed="save_c", fencing_token=tok, raw_fetch_id=None,
                   events=[ev("E3", 6, datetime(2026, 11, 3, 12, 1, 0, tzinfo=timezone.utc))], quarantined=[])
    assert r.inserted == 1
    tx.rollback(); c.close()

def test_stale_fencing_token_writes_nothing(migrated_url):
    c, tx, tok = fenced_conn(migrated_url, "save_d")
    with pytest.raises(FencingError):
        save_batch(c, feed="save_d", fencing_token=tok + 1, raw_fetch_id=None,
                   events=[ev("E4", 5)], quarantined=[])
    n = c.execute(text("SELECT COUNT(*) FROM normalized_events WHERE feed='save_d'")).scalar()
    assert n == 0
    tx.rollback(); c.close()

def test_quarantined_items_are_stored_not_dropped(migrated_url):
    c, tx, tok = fenced_conn(migrated_url, "save_e")
    r = save_batch(c, feed="save_e", fencing_token=tok, raw_fetch_id=None, events=[],
                   quarantined=[Quarantined("parse_error", "<html>")])
    assert r.quarantined == 1
    assert c.execute(text("SELECT COUNT(*) FROM quarantine WHERE feed='save_e'")).scalar() == 1
    tx.rollback(); c.close()

def test_replayed_older_event_is_skipped_as_duplicate(migrated_url):
    c, tx, tok = fenced_conn(migrated_url, "save_f")
    save_batch(c, feed="save_f", fencing_token=tok, raw_fetch_id=None, events=[ev("E5", 5)], quarantined=[])
    save_batch(c, feed="save_f", fencing_token=tok, raw_fetch_id=None,
               events=[ev("E5", 6, datetime(2026, 11, 3, 12, 1, 0, tzinfo=timezone.utc))], quarantined=[])
    r = save_batch(c, feed="save_f", fencing_token=tok, raw_fetch_id=None, events=[ev("E5", 5)], quarantined=[])
    assert r.inserted == 0 and r.skipped_duplicate == 1
    tx.rollback(); c.close()
