"""ingest schema: feeds, leases, raw fetches, events, outbox, quarantine

Revision ID: 0001
Revises:
Create Date: 2026-11-03
"""
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

PARTITIONS = [
    ("p202611", "2026-12-01"),
    ("p202612", "2027-01-01"),
    ("p202701", "2027-02-01"),
    ("p202702", "2027-03-01"),
    ("p202703", "2027-04-01"),
]


def _partition_clause() -> str:
    parts = [f"PARTITION {name} VALUES LESS THAN (TO_DAYS('{limit}'))" for name, limit in PARTITIONS]
    parts.append("PARTITION pmax VALUES LESS THAN MAXVALUE")
    return ",\n  ".join(parts)


def upgrade() -> None:
    op.execute("""
CREATE TABLE feed_state (
  feed VARCHAR(64) PRIMARY KEY,
  cursor_value VARCHAR(255) NULL,
  last_polled_at DATETIME(3) NULL,
  last_success_at DATETIME(3) NULL,
  consecutive_failures INT NOT NULL DEFAULT 0,
  paused BOOLEAN NOT NULL DEFAULT FALSE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""")
    op.execute("""
CREATE TABLE feed_lease (
  feed VARCHAR(64) PRIMARY KEY,
  holder VARCHAR(64) NOT NULL,
  fencing_token BIGINT NOT NULL,
  expires_at DATETIME(3) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""")
    op.execute("""
CREATE TABLE raw_fetch (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  feed VARCHAR(64) NOT NULL,
  fetched_at DATETIME(3) NOT NULL,
  http_status SMALLINT NOT NULL,
  payload_hash CHAR(64) NOT NULL,
  body MEDIUMBLOB NOT NULL,
  KEY idx_feed_time (feed, fetched_at),
  KEY idx_feed_hash (feed, payload_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""")
    op.execute(f"""
CREATE TABLE normalized_events (
  event_id CHAR(64) NOT NULL,
  feed VARCHAR(64) NOT NULL,
  entity_id VARCHAR(128) NOT NULL,
  observed_at DATETIME(3) NOT NULL,
  lat DECIMAL(9,6) NULL,
  lon DECIMAL(9,6) NULL,
  payload JSON NOT NULL,
  state_hash CHAR(64) NOT NULL,
  raw_fetch_id BIGINT NULL,
  fencing_token BIGINT NOT NULL,
  trace_id CHAR(32) NULL,
  schema_version SMALLINT NOT NULL DEFAULT 1,
  PRIMARY KEY (event_id, observed_at),
  KEY idx_feed_time (feed, observed_at),
  KEY idx_entity_time (entity_id, observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY RANGE (TO_DAYS(observed_at)) (
  {_partition_clause()}
)
""")
    op.execute("""
CREATE TABLE quarantine (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  feed VARCHAR(64) NOT NULL,
  reason ENUM('schema_invalid','low_confidence','parse_error') NOT NULL,
  content JSON NOT NULL,
  parse_confidence DECIMAL(3,2) NULL,
  parser_id VARCHAR(32) NULL,
  raw_fetch_id BIGINT NULL,
  created_at DATETIME(3) NOT NULL,
  resolved BOOLEAN NOT NULL DEFAULT FALSE,
  KEY idx_unresolved (resolved, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""")
    op.execute("""
CREATE TABLE outbox (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_id CHAR(64) NOT NULL UNIQUE,
  topic VARCHAR(100) NOT NULL,
  partition_key VARCHAR(128) NOT NULL,
  payload JSON NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""")
    op.execute("""
CREATE TABLE outbox_delivery (
  outbox_id BIGINT NOT NULL,
  sink VARCHAR(32) NOT NULL,
  sent_at DATETIME(3) NULL,
  attempts INT NOT NULL DEFAULT 0,
  last_error VARCHAR(255) NULL,
  PRIMARY KEY (outbox_id, sink),
  KEY idx_pending (sink, sent_at, outbox_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
""")


def downgrade() -> None:
    for table in ("outbox_delivery", "outbox", "quarantine", "normalized_events",
                  "raw_fetch", "feed_lease", "feed_state"):
        op.execute(f"DROP TABLE IF EXISTS {table}")
