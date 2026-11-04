import json
import time

from confluent_kafka import KafkaError, Message, Producer

from mobility_ingest.outbox import OutboxRow, PartialPublish


class KafkaSink:
    name = "kafka"

    def __init__(self, bootstrap_servers: str, *, producer=None,
                 enqueue_timeout: float = 30.0, flush_timeout: float = 30.0):
        self._producer = producer or Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 5,
        })
        self._enqueue_timeout = enqueue_timeout
        self._flush_timeout = flush_timeout

    def publish(self, rows: list[OutboxRow]) -> list[int]:
        delivered: list[int] = []
        failures: list[str] = []

        def on_delivery(outbox_id: int, err: KafkaError | None, _msg: Message | None) -> None:
            if err is None:
                delivered.append(outbox_id)
            else:
                failures.append(f"outbox {outbox_id}: {err}")

        deadline = time.monotonic() + self._enqueue_timeout
        for index, row in enumerate(rows):
            if not self._enqueue(row, on_delivery, deadline):
                failures.append(f"producer queue full for {self._enqueue_timeout}s; "
                                f"{len(rows) - index} rows not enqueued")
                break
        remaining = self._producer.flush(self._flush_timeout)
        if remaining:
            # Drop what is still queued so it cannot be delivered after the DB has recorded
            # it as unsent. Messages already in flight may still reach the broker; consumers
            # dedupe on event_id.
            self._producer.purge()
            self._producer.flush(5)
            failures.append(f"{remaining} messages not delivered within {self._flush_timeout}s")
        if failures:
            message = "; ".join(failures)
            if delivered:
                raise PartialPublish(delivered, message)
            raise RuntimeError(message)
        return delivered

    def _enqueue(self, row: OutboxRow, on_delivery, deadline: float) -> bool:
        while True:
            try:
                self._producer.produce(
                    topic=row.topic,
                    key=row.partition_key.encode("utf-8"),
                    value=json.dumps(row.payload).encode("utf-8"),
                    headers=[(k, v.encode("utf-8")) for k, v in row.headers.items()],
                    on_delivery=lambda err, msg, oid=row.outbox_id: on_delivery(oid, err, msg))
                return True
            except BufferError:
                if time.monotonic() >= deadline:
                    return False
                self._producer.poll(0.1)
