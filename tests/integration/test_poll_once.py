from datetime import datetime, timezone, timedelta
from pathlib import Path
import pytest
from sqlalchemy import text
from mobility_ingest.db import make_engine
from mobility_ingest.adapters import FixtureAdapter
from mobility_ingest.ingest import poll_once
from mobility_ingest.normalize.carpark import normalize_carparks

pytestmark = pytest.mark.integration
FIX = Path(__file__).parent.parent / "fixtures" / "carpark_availability_ok.json"
T0 = datetime(2026, 11, 3, 12, 0, 0, tzinfo=timezone.utc)
TRACE = "c" * 32

def run(url, adapter, now, holder="node-1"):
    return poll_once(make_engine(url), adapter=adapter, normalize=normalize_carparks,
                     holder=holder, lease_ttl_seconds=30, now=now, trace_id=TRACE)

def test_first_poll_saves_events(migrated_url):
    adapter = FixtureAdapter("carpark_availability", FIX)
    r = run(migrated_url, adapter, T0)
    assert r.status == "ok" and r.inserted > 0

def test_second_poll_of_same_body_is_skipped(migrated_url):
    adapter = FixtureAdapter("carpark_availability", FIX)
    run(migrated_url, adapter, T0)
    r = run(migrated_url, adapter, T0 + timedelta(minutes=1))
    assert r.status == "skipped_unchanged" and r.inserted == 0

def test_second_node_while_lease_held_does_nothing(migrated_url):
    adapter = FixtureAdapter("carpark_availability", FIX)
    run(migrated_url, adapter, T0, holder="node-1")
    r = run(migrated_url, adapter, T0 + timedelta(seconds=5), holder="node-2")
    assert r.status == "skipped_lease"

def test_upstream_error_records_failure_and_creates_no_events(migrated_url):
    from mobility_ingest.adapters import UpstreamError
    class Broken:
        name = "carpark_availability_broken"
        def fetch(self):
            raise UpstreamError("timeout", retryable=True)
    r = run(migrated_url, Broken(), T0)
    assert r.status == "upstream_error"
    eng = make_engine(migrated_url)
    with eng.connect() as c:
        assert c.execute(text("SELECT consecutive_failures FROM feed_state WHERE feed=:f"),
                         {"f": "carpark_availability_broken"}).scalar() == 1
