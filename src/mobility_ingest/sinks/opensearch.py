from datetime import datetime

from opensearchpy import helpers

from mobility_ingest.outbox import OutboxRow


def index_name(observed_at: str) -> str:
    stamp = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
    return f"events-{stamp:%Y.%m.%d}"


class OpenSearchSink:
    name = "opensearch"

    def __init__(self, client):
        self._client = client

    def publish(self, rows: list[OutboxRow]) -> list[int]:
        actions = [{
            "_op_type": "index",
            "_index": index_name(row.payload["observed_at"]),
            "_id": row.payload["event_id"],
            "_source": row.payload,
        } for row in rows]
        _, errors = helpers.bulk(self._client, actions, raise_on_error=False)
        failed_ids = {item["index"]["_id"] for item in errors}
        return [row.outbox_id for row in rows if row.payload["event_id"] not in failed_ids]
