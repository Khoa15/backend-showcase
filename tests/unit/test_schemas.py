import pytest
from mobility_ingest.schemas import SchemaInvalid, validate_payload

GOOD = {
    "CarParkID": "HE12", "Area": "Bishan", "Development": "Blk 1",
    "Location": "1.35 103.84", "AvailableLots": 12, "LotType": "C", "Agency": "HDB",
}

def test_valid_payload_passes():
    validate_payload("carpark_availability.v1", GOOD)

def test_missing_required_field_fails():
    bad = {k: v for k, v in GOOD.items() if k != "AvailableLots"}
    with pytest.raises(SchemaInvalid) as e:
        validate_payload("carpark_availability.v1", bad)
    assert any("AvailableLots" in err for err in e.value.errors)

def test_unknown_extra_field_is_allowed():
    validate_payload("carpark_availability.v1", {**GOOD, "NewField": "x"})
