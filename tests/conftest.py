import time

import pymysql
import pytest

MYSQL_HOST, MYSQL_PORT, MYSQL_PASSWORD = "127.0.0.1", 3307, "test"
TEST_DB = "ingest_test"


@pytest.fixture(scope="session")
def mysql_url():
    deadline = time.monotonic() + 60
    while True:
        try:
            conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user="root",
                                   password=MYSQL_PASSWORD, autocommit=True)
            break
        except pymysql.err.OperationalError as exc:
            if time.monotonic() > deadline:
                pytest.skip(f"MySQL not reachable on {MYSQL_HOST}:{MYSQL_PORT}: {exc}")
            time.sleep(1)
    with conn.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB}")
        cur.execute(f"CREATE DATABASE {TEST_DB} CHARACTER SET utf8mb4")
    conn.close()
    yield f"mysql+pymysql://root:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{TEST_DB}"


@pytest.fixture(scope="session")
def migrated_url(mysql_url):
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    root = Path(__file__).resolve().parents[1]
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "migrations"))
    cfg.set_main_option("sqlalchemy.url", mysql_url)
    command.upgrade(cfg, "head")
    yield mysql_url


KAFKA_BOOTSTRAP = "localhost:9092"
OPENSEARCH_HOST, OPENSEARCH_PORT = "localhost", 9200


def _wait(check, what: str, seconds: int = 120):
    deadline = time.monotonic() + seconds
    while True:
        try:
            return check()
        except Exception as exc:
            if time.monotonic() > deadline:
                pytest.skip(f"{what} not reachable: {exc}")
            time.sleep(2)


@pytest.fixture(scope="session")
def kafka_bootstrap():
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    _wait(lambda: admin.list_topics(timeout=5), "Kafka")
    futures = admin.create_topics([NewTopic(
        "events.normalized", num_partitions=12, replication_factor=1,
        config={"retention.ms": str(7 * 24 * 3600 * 1000)})])
    for future in futures.values():
        try:
            future.result()
        except Exception as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc) and "already exists" not in str(exc):
                raise
    yield KAFKA_BOOTSTRAP


@pytest.fixture(scope="session")
def opensearch_client():
    from opensearchpy import OpenSearch

    client = OpenSearch(hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}], use_ssl=False)
    _wait(lambda: client.cluster.health(wait_for_status="yellow", request_timeout=10), "OpenSearch")
    client.indices.delete(index="events-2026.11.03", ignore=[404])
    yield client
