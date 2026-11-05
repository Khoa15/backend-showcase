import json
from datetime import datetime
from decimal import Decimal, InvalidOperation

from mobility_ingest.contract import (
    NormalizedEvent, NormalizeResult, Quarantined, compute_event_id, compute_state_hash,
)
from mobility_ingest.schemas import SchemaInvalid, validate_payload

FEED = "carpark_availability"
SCHEMA = "carpark_availability.v1"
SCHEMA_VERSION = 1
MAPPED_FIELDS = ("CarParkID", "Area", "Development", "LotType", "Agency", "AvailableLots")


def _coordinates(location) -> tuple[Decimal | None, Decimal | None]:
    if not isinstance(location, str):
        return None, None
    parts = location.split(" ")
    if len(parts) != 2:
        return None, None
    try:
        lat, lon = Decimal(parts[0]), Decimal(parts[1])
    except InvalidOperation:
        return None, None
    if not (lat.is_finite() and lon.is_finite()):
        return None, None
    return lat, lon


def _content(record) -> dict | str:
    return record if isinstance(record, dict) else str(record)


def normalize_carparks(body: bytes, fetched_at: datetime, trace_id: str) -> NormalizeResult:
    try:
        doc = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        doc = None
    if not isinstance(doc, dict) or not isinstance(doc.get("value"), list):
        return NormalizeResult(events=[], quarantined=[
            Quarantined("parse_error", body.decode("utf-8", "replace")[:2000])])

    observed_at = fetched_at.replace(microsecond=fetched_at.microsecond // 1000 * 1000)
    quarantined: list[Quarantined] = []
    groups: dict[tuple, list[tuple[dict, tuple]]] = {}

    for record in doc["value"]:
        try:
            if not isinstance(record, dict):
                raise SchemaInvalid(["<root>: record is not an object"])
            validate_payload(SCHEMA, record)
        except SchemaInvalid:
            quarantined.append(Quarantined("schema_invalid", _content(record)))
            continue
        payload = {k: record.get(k) for k in MAPPED_FIELDS}
        lat, lon = _coordinates(record.get("Location"))
        groups.setdefault((payload["CarParkID"], payload["LotType"]), []).append(
            (record, (payload, lat, lon)))

    events: list[NormalizedEvent] = []
    for (car_park_id, lot_type), items in groups.items():
        # One entity per lot type: change detection and Kafka ordering are per (car park, lot type).
        entity_id = f"{car_park_id}:{lot_type}"
        first = items[0][1]
        if any(candidate != first for _, candidate in items[1:]):
            quarantined.extend(Quarantined("schema_invalid", record) for record, _ in items)
            continue
        payload, lat, lon = first
        # Coordinates are part of the state: a move with an unchanged lot count is a change.
        state_hash = compute_state_hash({**payload, "lat": None if lat is None else str(lat),
                                         "lon": None if lon is None else str(lon)})
        events.append(NormalizedEvent(
            event_id=compute_event_id(FEED, entity_id, observed_at, state_hash),
            feed=FEED, entity_id=entity_id, observed_at=observed_at, lat=lat, lon=lon,
            payload=payload, state_hash=state_hash, schema_version=SCHEMA_VERSION,
            trace_id=trace_id))
    return NormalizeResult(events=events, quarantined=quarantined)
