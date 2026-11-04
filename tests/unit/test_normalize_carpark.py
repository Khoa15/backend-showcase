import json
from datetime import datetime, timezone
from pathlib import Path
from mobility_ingest.normalize.carpark import normalize_carparks

FIX = Path(__file__).parent.parent / "fixtures"
T = datetime(2026, 11, 3, 12, 0, 0, 123456, tzinfo=timezone.utc)
TRACE = "a" * 32

def rec(**over):
    base = {"CarParkID": "HE12", "Area": "Bishan", "Development": "Blk 1",
            "Location": "1.35 103.84", "AvailableLots": 12, "LotType": "C", "Agency": "HDB"}
    return {**base, **over}

def body(*records):
    return json.dumps({"value": list(records)}).encode()

def test_ok_fixture_produces_events_and_no_quarantine():
    res = normalize_carparks((FIX / "carpark_availability_ok.json").read_bytes(), T, TRACE)
    assert res.events and res.quarantined == []

def test_non_json_body_is_quarantined_not_empty():
    res = normalize_carparks(b"<html>503 Service Unavailable</html>", T, TRACE)
    assert res.events == []
    assert [q.reason for q in res.quarantined] == ["parse_error"]

def test_identical_duplicate_records_collapse_to_one_event():
    res = normalize_carparks(body(rec(), rec()), T, TRACE)
    assert len(res.events) == 1 and res.quarantined == []

def test_conflicting_duplicate_records_are_quarantined():
    res = normalize_carparks(body(rec(AvailableLots=12), rec(AvailableLots=3)), T, TRACE)
    assert res.events == []
    assert [q.reason for q in res.quarantined] == ["schema_invalid", "schema_invalid"]

def test_missing_location_yields_null_coordinates():
    rec_no_loc = rec(); del rec_no_loc["Location"]
    res = normalize_carparks(body(rec_no_loc), T, TRACE)
    assert res.events[0].lat is None and res.events[0].lon is None

def test_unknown_upstream_fields_are_ignored_and_do_not_change_identity():
    a = normalize_carparks(body(rec()), T, TRACE).events[0]
    b = normalize_carparks(body(rec(NewField="x")), T, TRACE).events[0]
    assert "NewField" not in b.payload
    assert a.event_id == b.event_id and a.state_hash == b.state_hash

def test_non_numeric_or_negative_lots_are_quarantined():
    for bad in ("twelve", -1):
        res = normalize_carparks(body(rec(AvailableLots=bad)), T, TRACE)
        assert res.events == []
        assert [q.reason for q in res.quarantined] == ["schema_invalid"]

def test_observed_at_is_truncated_to_milliseconds():
    e = normalize_carparks(body(rec()), T, TRACE).events[0]
    assert e.observed_at == datetime(2026, 11, 3, 12, 0, 0, 123000, tzinfo=timezone.utc)

def test_coordinate_change_alone_produces_a_new_event_identity():
    before = normalize_carparks(body(rec(Location="1.35 103.84")), T, TRACE).events[0]
    after = normalize_carparks(body(rec(Location="1.36 103.84")), T, TRACE).events[0]
    assert before.payload == after.payload
    assert before.state_hash != after.state_hash
    assert before.event_id != after.event_id

def test_each_lot_type_of_a_car_park_is_its_own_entity():
    res = normalize_carparks(body(rec(LotType="C", AvailableLots=5), rec(LotType="Y", AvailableLots=9)), T, TRACE)
    assert sorted(e.entity_id for e in res.events) == ["HE12:C", "HE12:Y"]
    assert {e.payload["CarParkID"] for e in res.events} == {"HE12"}
