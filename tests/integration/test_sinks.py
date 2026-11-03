import json
import pytest
from mobility_ingest.outbox import OutboxRow
from mobility_ingest.sinks.kafka import KafkaSink
from mobility_ingest.sinks.opensearch import OpenSearchSink

pytestmark = pytest.mark.integration

PAYLOAD = {"schema_version": 1, "event_id": "e" * 64, "observed_at": "2026-11-03T12:00:00.000Z",
           "entity_id": "HE12", "feed": "carpark_availability"}
ROW = OutboxRow(outbox_id=1, topic="events.normalized", partition_key="HE12",
                payload=PAYLOAD, headers={"schema_version": "1"})

def test_kafka_sink_acknowledges_row(kafka_bootstrap):
    assert KafkaSink(kafka_bootstrap).publish([ROW]) == [1]

def test_opensearch_sink_indexes_by_event_id_and_is_idempotent(opensearch_client):
    sink = OpenSearchSink(opensearch_client)
    assert sink.publish([ROW]) == [1]
    assert sink.publish([ROW]) == [1]  # replay overwrites, does not duplicate
    opensearch_client.indices.refresh(index="events-2026.11.03")
    hits = opensearch_client.count(index="events-2026.11.03")["count"]
    assert hits == 1
