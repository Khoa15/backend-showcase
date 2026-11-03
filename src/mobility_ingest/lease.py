from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.engine import Connection

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class FencingError(Exception):
    pass


def _utc_naive(moment: datetime) -> datetime:
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("lease times must be timezone-aware")
    return moment.astimezone(timezone.utc).replace(tzinfo=None)


def acquire(conn: Connection, feed: str, holder: str, ttl_seconds: int, now: datetime) -> int | None:
    conn.execute(
        text("INSERT INTO feed_lease (feed, holder, fencing_token, expires_at) "
             "VALUES (:feed, '', 0, :epoch) ON DUPLICATE KEY UPDATE feed = feed"),
        {"feed": feed, "epoch": _utc_naive(EPOCH)})
    row = conn.execute(
        text("SELECT holder, fencing_token, expires_at FROM feed_lease WHERE feed = :feed FOR UPDATE"),
        {"feed": feed}).one()
    now_naive = _utc_naive(now)
    if row.holder != holder and row.expires_at >= now_naive:
        return None
    token = row.fencing_token + 1 if row.holder != holder else row.fencing_token
    conn.execute(
        text("UPDATE feed_lease SET holder = :holder, fencing_token = :token, expires_at = :expires "
             "WHERE feed = :feed"),
        {"holder": holder, "token": token, "feed": feed,
         "expires": _utc_naive(now + timedelta(seconds=ttl_seconds))})
    return token


def assert_fenced(conn: Connection, feed: str, token: int) -> None:
    stored = conn.execute(
        text("SELECT fencing_token FROM feed_lease WHERE feed = :feed FOR UPDATE"),
        {"feed": feed}).scalar()
    if stored != token:
        raise FencingError(f"token {token} is stale for {feed}; current is {stored}")
