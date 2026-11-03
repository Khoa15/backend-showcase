import json

from confluent_kafka import KafkaError, Message, Producer

from mobility_ingest.outbox import OutboxRow


class KafkaSink:
    name = "kafka"

    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 5,
        })

    def publish(self, rows: list[OutboxRow]) -> list[int]:
        delivered: list[int] = []
        failures: list[str] = []

        def on_delivery(outbox_id: int, err: KafkaError | None, _msg: Message) -> None:
            if err is None:
                delivered.append(outbox_id)
            else:
                failures.append(f"outbox {outbox_id}: {err}")

        for row in rows:
            while True:
                try:
                    self._producer.produce(
                        topic=row.topic,
                        key=row.partition_key.encode("utf-8"),
                        value=json.dumps(row.payload).encode("utf-8"),
                        headers=[(k, v.encode("utf-8")) for k, v in row.headers.items()],
                        on_delivery=lambda err, msg, oid=row.outbox_id: on_delivery(oid, err, msg))
                    break
                except BufferError:
                    self._producer.poll(0.1)
        remaining = self._producer.flush(30)
        if remaining:
            failures.append(f"{remaining} messages not delivered within 30s")
        if failures:
            raise RuntimeError("; ".join(failures))
        return delivered
