from datetime import datetime, timezone, timedelta
import pytest
from sqlalchemy import text
from mobility_ingest.db import make_engine
from mobility_ingest.ingest import record_fetch

pytestmark = pytest.mark.integration
T0 = datetime(2026, 11, 3, 12, 0, 0, tzinfo=timezone.utc)

def count_raw(c, feed):
    return c.execute(text("SELECT COUNT(*) FROM raw_fetch WHERE feed=:f"), {"f": feed}).scalar()

def test_first_body_is_stored(migrated_url):
    with make_engine(migrated_url).begin() as c:
        changed, rid = record_fetch(c, "cf_a", T0, 200, b'{"value":[1]}')
        assert changed is True and rid is not None
        assert count_raw(c, "cf_a") == 1

def test_identical_body_stores_nothing_but_updates_last_success(migrated_url):
    with make_engine(migrated_url).begin() as c:
        record_fetch(c, "cf_b", T0, 200, b'{"value":[1]}')
        changed, rid = record_fetch(c, "cf_b", T0 + timedelta(minutes=1), 200, b'{"value":[1]}')
        assert changed is False and rid is None
        assert count_raw(c, "cf_b") == 1
        last = c.execute(text("SELECT last_success_at FROM feed_state WHERE feed='cf_b'")).scalar()
        assert last.replace(tzinfo=timezone.utc) == T0 + timedelta(minutes=1)

def test_changed_body_is_stored(migrated_url):
    with make_engine(migrated_url).begin() as c:
        record_fetch(c, "cf_c", T0, 200, b'{"value":[1]}')
        changed, _ = record_fetch(c, "cf_c", T0 + timedelta(minutes=1), 200, b'{"value":[2]}')
        assert changed is True
        assert count_raw(c, "cf_c") == 2
