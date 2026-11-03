import hashlib
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.engine import Connection


def record_fetch(conn: Connection, feed: str, fetched_at: datetime, status: int,
                 body: bytes) -> tuple[bool, int | None]:
    payload_hash = hashlib.sha256(body).hexdigest()
    conn.execute(
        text("INSERT INTO feed_state (feed, last_polled_at, last_success_at) "
             "VALUES (:feed, :at, NULL) ON DUPLICATE KEY UPDATE last_polled_at = VALUES(last_polled_at)"),
        {"feed": feed, "at": fetched_at.replace(tzinfo=None)})
    if status == 200:
        conn.execute(text("UPDATE feed_state SET last_success_at = :at, consecutive_failures = 0 "
                          "WHERE feed = :feed"),
                     {"feed": feed, "at": fetched_at.replace(tzinfo=None)})
    last_hash = conn.execute(
        text("SELECT payload_hash FROM raw_fetch WHERE feed = :feed ORDER BY id DESC LIMIT 1"),
        {"feed": feed}).scalar()
    if last_hash == payload_hash:
        return False, None
    result = conn.execute(
        text("INSERT INTO raw_fetch (feed, fetched_at, http_status, payload_hash, body) "
             "VALUES (:feed, :at, :status, :hash, :body)"),
        {"feed": feed, "at": fetched_at.replace(tzinfo=None), "status": status,
         "hash": payload_hash, "body": body})
    return True, result.lastrowid
