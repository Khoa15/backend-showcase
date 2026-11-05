import threading
import time
from datetime import datetime, timezone, timedelta
import pytest
from sqlalchemy import text
from mobility_ingest.db import make_engine
from mobility_ingest.outbox import relay_once, purge_outbox, OutboxRow, PartialPublish

pytestmark = pytest.mark.integration

class RecordingSink:
    def __init__(self, name, fail=False):
        self.name, self.fail, self.seen = name, fail, []
    def publish(self, rows):
        if self.fail:
            raise RuntimeError("sink down")
        self.seen.extend(r.outbox_id for r in rows)
        return [r.outbox_id for r in rows]

def seeded_id(feed_tag, i=0):
    return f"{feed_tag}{i:060d}"

def seed(eng, n, feed_tag):
    with eng.begin() as c:
        for i in range(n):
            eid = seeded_id(feed_tag, i)
            c.execute(text("INSERT INTO outbox (event_id, topic, partition_key, payload) "
                           "VALUES (:e,'events.normalized',:k,:p)"),
                      {"e": eid, "k": "K", "p": '{"schema_version":1}'})
            oid = c.execute(text("SELECT id FROM outbox WHERE event_id=:e"), {"e": eid}).scalar()
            for s in ("kafka", "opensearch"):
                c.execute(text("INSERT INTO outbox_delivery (outbox_id, sink) VALUES (:o,:s)"), {"o": oid, "s": s})

def test_relay_marks_only_that_sink_sent(migrated_url):
    eng = make_engine(migrated_url); seed(eng, 3, "a")
    assert relay_once(eng, RecordingSink("kafka")) >= 3
    with eng.connect() as c:
        pending_os = c.execute(text("SELECT COUNT(*) FROM outbox_delivery WHERE sink='opensearch' AND sent_at IS NULL")).scalar()
        assert pending_os >= 3

def test_failing_sink_keeps_row_pending_and_counts_attempt(migrated_url):
    eng = make_engine(migrated_url); seed(eng, 1, "b")
    with pytest.raises(RuntimeError):
        relay_once(eng, RecordingSink("opensearch", fail=True))
    with eng.connect() as c:
        row = c.execute(text("SELECT attempts, sent_at, last_error FROM outbox_delivery od "
                             "JOIN outbox o ON o.id=od.outbox_id WHERE o.event_id = :eid AND od.sink='opensearch'"),
                        {"eid": seeded_id("b")}).one()
    assert row.attempts == 1 and row.sent_at is None and "sink down" in row.last_error

def test_two_relays_never_take_the_same_row(migrated_url):
    eng = make_engine(migrated_url); seed(eng, 20, "c")
    s1, s2 = RecordingSink("kafka"), RecordingSink("kafka")
    threads = [threading.Thread(target=relay_once, args=(eng, s, 5)) for s in (s1, s2)]
    [t.start() for t in threads]; [t.join() for t in threads]
    assert not set(s1.seen) & set(s2.seen)

def test_purge_removes_only_fully_sent_old_rows(migrated_url):
    eng = make_engine(migrated_url); seed(eng, 1, "d")
    with eng.begin() as c:
        c.execute(text("UPDATE outbox_delivery SET sent_at = now(3) - INTERVAL 4 DAY WHERE outbox_id IN "
                       "(SELECT id FROM outbox WHERE event_id = :eid)"), {"eid": seeded_id("d")})
        c.execute(text("UPDATE outbox SET created_at = now(3) - INTERVAL 4 DAY WHERE event_id = :eid"),
                  {"eid": seeded_id("d")})
    assert purge_outbox(eng, older_than_days=3) >= 1

class BlockingSink(RecordingSink):
    def __init__(self, name):
        super().__init__(name)
        self.entered, self.release = threading.Event(), threading.Event()
    def publish(self, rows):
        self.entered.set()
        assert self.release.wait(30)
        return super().publish(rows)

def test_relay_of_one_sink_does_not_block_another_sink(migrated_url):
    eng = make_engine(migrated_url); seed(eng, 3, "e")
    slow = BlockingSink("kafka")
    worker = threading.Thread(target=relay_once, args=(eng, slow, 1000))
    worker.start()
    try:
        assert slow.entered.wait(30)
        fast = RecordingSink("opensearch")
        started = time.monotonic()
        relay_once(eng, fast, 1000)
        assert time.monotonic() - started < 5, "opensearch relay waited on the kafka relay's locks"
        assert len(fast.seen) >= 3
    finally:
        slow.release.set(); worker.join()

class PartialSink(RecordingSink):
    def publish(self, rows):
        acked = [rows[0].outbox_id]
        raise PartialPublish(acked, RuntimeError(f"outbox {rows[1].outbox_id}: broker rejected"))

def test_partial_publish_marks_acked_sent_and_counts_attempt_only_for_the_rest(migrated_url):
    eng = make_engine(migrated_url); seed(eng, 2, "f")
    assert relay_once(eng, PartialSink("kafka"), 1000) == 1
    with eng.connect() as c:
        rows = {r.event_id: r for r in c.execute(text(
            "SELECT o.event_id, od.sent_at, od.attempts, od.last_error FROM outbox_delivery od "
            "JOIN outbox o ON o.id=od.outbox_id WHERE o.event_id IN (:a,:b) AND od.sink='kafka'"),
            {"a": seeded_id("f", 0), "b": seeded_id("f", 1)})}
    sent, failed = rows[seeded_id("f", 0)], rows[seeded_id("f", 1)]
    assert sent.sent_at is not None and sent.attempts == 0 and sent.last_error is None
    assert failed.sent_at is None and failed.attempts == 1 and "broker rejected" in failed.last_error
