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
