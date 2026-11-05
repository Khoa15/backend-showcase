from datetime import datetime, timezone
import re
from mobility_ingest.contract import compute_event_id, compute_state_hash

T = datetime(2026, 11, 3, 12, 0, 0, 123000, tzinfo=timezone.utc)

def test_event_id_is_64_lowercase_hex():
    eid = compute_event_id("carpark_availability", "HE12", T, compute_state_hash({"AvailableLots": 5}))
    assert re.fullmatch(r"[0-9a-f]{64}", eid)

def test_same_inputs_give_same_event_id():
    h = compute_state_hash({"AvailableLots": 5})
    assert compute_event_id("carpark_availability", "HE12", T, h) == compute_event_id("carpark_availability", "HE12", T, h)

def test_changed_value_changes_event_id():
    a = compute_state_hash({"AvailableLots": 5})
    b = compute_state_hash({"AvailableLots": 6})
    assert compute_event_id("carpark_availability", "HE12", T, a) != compute_event_id("carpark_availability", "HE12", T, b)

def test_state_hash_ignores_key_order():
    assert compute_state_hash({"a": 1, "b": 2}) == compute_state_hash({"b": 2, "a": 1})

def test_observed_at_must_be_utc_aware():
    import pytest
    with pytest.raises(ValueError):
        compute_event_id("carpark_availability", "HE12", datetime(2026, 11, 3), "0" * 64)
