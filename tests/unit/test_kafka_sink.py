import pytest
from mobility_ingest.outbox import OutboxRow, PartialPublish
from mobility_ingest.sinks.kafka import KafkaSink


class FakeError:
    def __str__(self):
        return "boom"


class FakeProducer:
    def __init__(self, fail_keys=(), buffer_full=False, stuck=False):
        self.fail_keys, self.buffer_full, self.stuck = set(fail_keys), buffer_full, stuck
        self.queue, self.purged = [], False

    def produce(self, topic, key, value, headers, on_delivery):
        if self.buffer_full:
            raise BufferError("queue full")
        self.queue.append((key.decode(), on_delivery))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        if self.stuck and not self.purged:
            return len(self.queue)
        for key, cb in self.queue:
            cb(FakeError() if self.purged or key in self.fail_keys else None, None)
        self.queue = []
        return 0

    def purge(self):
        self.purged = True


def rows(*keys):
    return [OutboxRow(outbox_id=i + 1, topic="t", partition_key=k, payload={"a": 1}, headers={"schema_version": "1"})
            for i, k in enumerate(keys)]


def sink(producer, **kw):
    return KafkaSink("unused", producer=producer, **kw)


def test_all_delivered_returns_every_outbox_id():
    assert sink(FakeProducer()).publish(rows("a", "b", "c")) == [1, 2, 3]


def test_partial_failure_reports_the_delivered_ids():
    with pytest.raises(PartialPublish) as exc:
        sink(FakeProducer(fail_keys={"b"})).publish(rows("a", "b", "c"))
    assert exc.value.acked == [1, 3] and "outbox 2" in str(exc.value)


def test_total_failure_raises_plain_error():
    with pytest.raises(RuntimeError) as exc:
        sink(FakeProducer(fail_keys={"a", "b"})).publish(rows("a", "b"))
    assert not isinstance(exc.value, PartialPublish)


def test_flush_timeout_purges_the_producer_queue():
    producer = FakeProducer(stuck=True)
    with pytest.raises(RuntimeError):
        sink(producer).publish(rows("a"))
    assert producer.purged and producer.queue == []


def test_full_producer_queue_gives_up_after_the_deadline():
    with pytest.raises(RuntimeError, match="queue full"):
        sink(FakeProducer(buffer_full=True), enqueue_timeout=0.2).publish(rows("a"))
