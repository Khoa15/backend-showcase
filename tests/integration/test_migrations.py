import pytest
from sqlalchemy import inspect
from mobility_ingest.db import make_engine

pytestmark = pytest.mark.integration

EXPECTED = {"feed_state", "feed_lease", "raw_fetch", "normalized_events",
            "quarantine", "outbox", "outbox_delivery"}

def test_all_tables_exist_after_upgrade(migrated_url):
    tables = set(inspect(make_engine(migrated_url)).get_table_names())
    assert EXPECTED <= tables

def test_normalized_events_is_partitioned_and_has_state_hash(migrated_url):
    eng = make_engine(migrated_url)
    with eng.connect() as c:
        cols = {r[0] for r in c.exec_driver_sql(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_NAME='normalized_events' AND TABLE_SCHEMA=DATABASE()")}
        parts = c.exec_driver_sql(
            "SELECT COUNT(*) FROM information_schema.PARTITIONS "
            "WHERE TABLE_NAME='normalized_events' AND TABLE_SCHEMA=DATABASE() AND PARTITION_NAME IS NOT NULL").scalar()
    assert "state_hash" in cols and parts >= 2

def test_outbox_event_id_is_unique(migrated_url):
    import sqlalchemy as sa
    eng = make_engine(migrated_url)
    with eng.begin() as c:
        c.exec_driver_sql("INSERT INTO outbox (event_id, topic, partition_key, payload) VALUES (%s,'t','k','{}')", ("a" * 64,))
        with pytest.raises(sa.exc.IntegrityError):
            c.exec_driver_sql("INSERT INTO outbox (event_id, topic, partition_key, payload) VALUES (%s,'t','k','{}')", ("a" * 64,))
