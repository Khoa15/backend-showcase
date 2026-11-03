import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal


def _canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def compute_state_hash(payload: dict) -> str:
    return _sha256_hex(_canonical(payload))


def _format_observed_at(observed_at: datetime) -> str:
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware UTC")
    utc = observed_at.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def compute_event_id(feed: str, entity_id: str, observed_at: datetime, state_hash: str) -> str:
    stamp = _format_observed_at(observed_at)
    return _sha256_hex(_canonical([feed, entity_id, stamp, state_hash]))


@dataclass(frozen=True)
class NormalizedEvent:
    event_id: str
    feed: str
    entity_id: str
    observed_at: datetime
    lat: Decimal | None
    lon: Decimal | None
    payload: dict
    state_hash: str
    schema_version: int
    trace_id: str


@dataclass(frozen=True)
class Quarantined:
    reason: Literal["schema_invalid", "parse_error"]
    content: dict | str


@dataclass(frozen=True)
class NormalizeResult:
    events: list[NormalizedEvent]
    quarantined: list[Quarantined]
