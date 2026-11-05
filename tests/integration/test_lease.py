from datetime import datetime, timedelta, timezone
import pytest
from mobility_ingest.db import make_engine
from mobility_ingest.lease import acquire, assert_fenced, FencingError

pytestmark = pytest.mark.integration
T0 = datetime(2026, 11, 3, 12, 0, 0, tzinfo=timezone.utc)

def test_first_acquire_returns_token_one(migrated_url):
    with make_engine(migrated_url).begin() as c:
        assert acquire(c, "lease_test_a", "node-1", 30, T0) == 1

def test_second_holder_is_refused_while_lease_is_valid(migrated_url):
    with make_engine(migrated_url).begin() as c:
        acquire(c, "lease_test_b", "node-1", 30, T0)
        assert acquire(c, "lease_test_b", "node-2", 30, T0 + timedelta(seconds=5)) is None

def test_takeover_after_expiry_increments_token(migrated_url):
    with make_engine(migrated_url).begin() as c:
        acquire(c, "lease_test_c", "node-1", 30, T0)
        assert acquire(c, "lease_test_c", "node-2", 30, T0 + timedelta(seconds=60)) == 2

def test_stale_token_is_fenced_off(migrated_url):
    with make_engine(migrated_url).begin() as c:
        acquire(c, "lease_test_d", "node-1", 30, T0)
        acquire(c, "lease_test_d", "node-2", 30, T0 + timedelta(seconds=60))
        with pytest.raises(FencingError):
            assert_fenced(c, "lease_test_d", 1)
        assert_fenced(c, "lease_test_d", 2)
