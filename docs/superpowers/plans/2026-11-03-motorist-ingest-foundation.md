# Motorist Ingest Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first working slice of the Motorist mobility pipeline: `mobility-ingest` polls one structured feed (carpark availability), normalizes it, saves events with a transactional outbox under a fencing lease, and relays them to Kafka and OpenSearch.

**Architecture:** A Python package with one poll loop per feed. Each poll takes a MySQL fencing lease, fetches through a feed adapter, skips unchanged bodies by hash, normalizes to canonical events, and writes events plus outbox rows in one MySQL transaction. A separate relay drains the outbox to pluggable sinks, using `FOR UPDATE SKIP LOCKED`, and marks each sink's delivery only after it acknowledges.

**Tech Stack:** Python 3.12, SQLAlchemy 2.x Core, Alembic, PyMySQL, jsonschema, confluent-kafka, opensearch-py, pytest, MySQL 8.0, Kafka and OpenSearch (integration tests only, via docker-compose).

**Spec:** `docs/superpowers/specs/2026-11-04-motorist-mobility-pipeline-design.md` (sections 2.4, 3, 3.2, 5.1, 6, 9.1). Read it before starting a task that cites it.

**Out of scope for this plan (separate plans, in order):**
1. `alert-engine`: rules, percolator matching, hysteresis, cooldown, webhook delivery with circuit breaker.
2. Incident NLP (spec 3.1, `incidents` table, lifecycle). **Blocked:** the spec requires a captured sample of real LTA incident messages before the rules are written. Collect that sample first.
3. Control plane and dashboard, `error_log` and `pipeline.errors`, Redis lease fast-path, Kubernetes manifests.
4. Chaos, load and restore-drill tests (spec 6.1, 9).

## Global Constraints

- Python 3.12. Pin every dependency in `pyproject.toml` to the latest stable release at Task 1; record the chosen versions there. The spec leaves version pins to this plan.
- MySQL 8.0, InnoDB, `utf8mb4`. All times are UTC `DATETIME(3)`; Python values are timezone-aware UTC `datetime`.
- One schema per service. Here: schema `ingest`. No cross-schema foreign keys. No foreign keys on `normalized_events` (partitioned).
- `event_id` is `CHAR(64)`: lowercase hex SHA-256.
- `schema_version` is an integer, starts at `1`, and is also sent as the Kafka header `schema_version`.
- Kafka producer: `acks=all`, `enable.idempotence=True`. Topic `events.normalized`, key `entity_id`, 12 partitions, 7-day retention.
- Relay row selection: `ORDER BY outbox_id LIMIT N FOR UPDATE SKIP LOCKED`. `sent_at` is set only after the sink acknowledges.
- Retention for this plan: `outbox` rows purged 3 days after all their sinks sent. Other retention is out of scope for this plan.
- Secrets are never stored in the database. Only `user_id` and webhook URLs are stored elsewhere, so this plan stores no personal data.
- Schema changes use Alembic migrations. Use expand-then-contract for any change to an existing column.
- Each commit message describes the change in the repo's existing style. Do not put a model identifier in a commit message, PR title or body, code comment, or other pushed artifact.

## Review Focus

Five inputs the spec implies but no task covers by default. Each is pinned by a named test in the owning task.

1. **Upstream returns HTTP 200 with a non-JSON body** (HTML error page, empty body). Must be quarantined as `parse_error`, never treated as "zero carparks". Test `test_non_json_body_is_quarantined_not_empty` in Task 4.
2. **The same carpark appears twice in one response.** Identical duplicates collapse to one event; conflicting duplicates are quarantined. Tests `test_identical_duplicate_records_collapse_to_one_event` and `test_conflicting_duplicate_records_are_quarantined` in Task 4.
3. **Null or missing coordinates** (`Location` empty or absent). Event saved with `lat` and `lon` NULL; no crash. Test `test_missing_location_yields_null_coordinates` in Task 4.
4. **Upstream adds fields we do not know.** Ignored, and the `event_id` and `state_hash` do not change. Test `test_unknown_upstream_fields_are_ignored_and_do_not_change_identity` in Task 4.
5. **Non-numeric or negative `AvailableLots`.** Quarantined as `schema_invalid`, not stored as a number. Test `test_non_numeric_or_negative_lots_are_quarantined` in Task 4.

## File Structure

```
pyproject.toml                         # deps and pins
docker-compose.test.yml                # MySQL 8.0 for tests; Kafka and OpenSearch added in Task 10
alembic.ini
migrations/env.py
migrations/versions/0001_ingest_schema.py
contracts/events/carpark_availability.v1.schema.json
src/mobility_ingest/
  __init__.py
  contract.py          # event_id, state_hash, NormalizedEvent, Quarantined
  schemas.py           # JSON Schema validation for event payloads
  normalize/carpark.py # raw LTA CarParkAvailability body -> NormalizeResult
  adapters.py          # FeedAdapter protocol, FetchResult, FixtureAdapter
  db.py                # engine factory, DDL-level helpers
  lease.py             # feed lease with fencing token
  ingest.py            # save_batch (fenced transaction) and poll_once
  outbox.py            # Sink protocol, relay_once, purge_outbox
  sinks/kafka.py       # KafkaSink
  sinks/opensearch.py  # OpenSearchSink
tests/
  conftest.py
  fixtures/carpark_availability_ok.json
  fixtures/carpark_availability_edge.json
  unit/test_contract.py
  unit/test_schemas.py
  unit/test_normalize_carpark.py
  integration/test_migrations.py
  integration/test_lease.py
  integration/test_ingest_save.py
  integration/test_poll_once.py
  integration/test_relay.py
  integration/test_sinks.py
```

Fixture data: the field names below follow LTA DataMall `CarParkAvailability` as documented (`CarParkID`, `Area`, `Development`, `Location`, `AvailableLots`, `LotType`, `Agency`). The spec states that live message wording is unverified, so Task 4 also records one real captured response into `tests/fixtures/` before the normalizer is treated as final.

## Canonical event (used by Tasks 2 to 9)

```python
@dataclass(frozen=True)
class NormalizedEvent:
    event_id: str          # 64 lowercase hex chars
    feed: str              # "carpark_availability"
    entity_id: str         # "{CarParkID}:{LotType}", e.g. "HE12:C"
    observed_at: datetime  # fetch time, UTC, millisecond precision
    lat: Decimal | None
    lon: Decimal | None
    payload: dict          # mapped canonical fields only (see Task 4)
    state_hash: str        # 64 hex, hash of payload
    schema_version: int    # 1
    trace_id: str          # 32 hex

@dataclass(frozen=True)
class Quarantined:
    reason: Literal["schema_invalid", "parse_error"]
    content: dict | str

@dataclass(frozen=True)
class NormalizeResult:
    events: list[NormalizedEvent]
    quarantined: list[Quarantined]
```

---

### Task 1: Repository skeleton and test infrastructure

**Files:**
- Create: `pyproject.toml`, `docker-compose.test.yml`, `.python-version`, `src/mobility_ingest/__init__.py`, `tests/conftest.py`, `tests/unit/test_smoke.py`
- Modify: `.gitignore` (already exists and is untracked; keep its `.env` rule as-is)

**Interfaces:**
- Consumes: nothing.
- Produces: `pytest` runs; `mysql_url` fixture in `tests/conftest.py` returns a SQLAlchemy URL for an empty database `ingest_test` created per session; tests marked `integration` need it.

- [ ] **Step 1: Write the failing smoke test**

```python
# tests/unit/test_smoke.py
def test_package_imports():
    import mobility_ingest
    assert mobility_ingest.__version__ == "0.1.0"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `pytest tests/unit/test_smoke.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'mobility_ingest'`

- [ ] **Step 3: Create the skeleton**

`pyproject.toml` declares the package `mobility_ingest` under `src/`, runtime dependencies (SQLAlchemy, Alembic, PyMySQL, jsonschema, confluent-kafka, opensearch-py) and dev dependencies (pytest). Add a pytest marker `integration`. `__init__.py` sets `__version__ = "0.1.0"`.

`docker-compose.test.yml` runs `mysql:8.0` on host port `3307`, root password `test`, with `--character-set-server=utf8mb4`.

`tests/conftest.py` defines `mysql_url` (session-scoped) that waits for MySQL to accept connections, creates `ingest_test`, and yields a `mysql+pymysql://root:test@127.0.0.1:3307/ingest_test` URL. It skips integration tests with a clear message if MySQL is unreachable.

- [ ] **Step 4: Run the smoke test to verify it passes**

Run: `pip install -e ".[dev]" && pytest tests/unit/test_smoke.py -v`
Expected: PASS

- [ ] **Step 5: Start MySQL and check it is reachable**

Run: `docker compose -f docker-compose.test.yml up -d mysql && pytest -m integration --collect-only -q`
Expected: collection succeeds. If Docker is unavailable in the environment, report that and run the unit tests only; integration tests in later tasks cannot be verified there.

- [ ] **Step 6: Commit**

```bash
git add .gitignore pyproject.toml docker-compose.test.yml .python-version src tests
git commit -m "chore: add package skeleton, test infrastructure and gitignore"
```

---

### Task 2: Deterministic `event_id` and `state_hash`

**Files:**
- Create: `src/mobility_ingest/contract.py`, `tests/unit/test_contract.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `compute_state_hash(payload: dict) -> str`
  - `compute_event_id(feed: str, entity_id: str, observed_at: datetime, state_hash: str) -> str`
  - `NormalizedEvent`, `Quarantined`, `NormalizeResult` dataclasses (exactly as in "Canonical event" above)

**Design decision (not in the spec, recorded here):** the spec says identity is a hash of "identity fields", and also that unchanged data must not produce a new event. A `state_hash` of the mapped payload is included in the identity, so a carpark whose value did not change produces no new `event_id`, and a changed value does. `state_hash` is stored in `normalized_events`, which the spec's DDL does not list; Task 5 adds it.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_contract.py
from datetime import datetime, timezone
import re
from mobility_ingest.contract import compute_event_id, compute_state_hash

T = datetime(2026, 11, 3, 12, 0, 0, 123000, tzinfo=timezone.utc)

def test_event_id_is_64_lowercase_hex():
    eid = compute_event_id("carpark_availability", "HE12", T, compute_state_hash({"AvailableLots": 5}))
    assert re.fullmatch(r"[0-9a-f]{64}", eid)

def test_same_inputs_give_same_event_id():
    h = compute_state_hash({"AvailableLots": 5})
    assert compute_event_id("carpark_availability", "HE12", T, h) == compute_event_id("carpark_availability", "HE12", T, h)

def test_changed_value_changes_event_id():
    a = compute_state_hash({"AvailableLots": 5})
    b = compute_state_hash({"AvailableLots": 6})
    assert compute_event_id("carpark_availability", "HE12", T, a) != compute_event_id("carpark_availability", "HE12", T, b)

def test_state_hash_ignores_key_order():
    assert compute_state_hash({"a": 1, "b": 2}) == compute_state_hash({"b": 2, "a": 1})

def test_observed_at_must_be_utc_aware():
    import pytest
    with pytest.raises(ValueError):
        compute_event_id("carpark_availability", "HE12", datetime(2026, 11, 3), "0" * 64)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/unit/test_contract.py -v`
Expected: FAIL with `ImportError` for `mobility_ingest.contract`

- [ ] **Step 3: Implement `contract.py`**

`compute_state_hash`: SHA-256 of `json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)` encoded UTF-8, as hex.
`compute_event_id`: reject naive datetimes with `ValueError`; format `observed_at` as UTC ISO-8601 with milliseconds and a `Z` suffix; SHA-256 of the JSON array `[feed, entity_id, observed_at_str, state_hash]` in the same canonical form; return hex.
Add the three dataclasses exactly as in "Canonical event".

- [ ] **Step 4: Run them to verify they pass**

Run: `pytest tests/unit/test_contract.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add src/mobility_ingest/contract.py tests/unit/test_contract.py
git commit -m "feat: add deterministic event_id and state_hash"
```

---

### Task 3: Event payload validation against a versioned JSON Schema

**Files:**
- Create: `contracts/events/carpark_availability.v1.schema.json`, `src/mobility_ingest/schemas.py`, `tests/unit/test_schemas.py`

**Interfaces:**
- Consumes: `Quarantined` shape from Task 2 (not imported; the caller builds it).
- Produces:
  - `class SchemaInvalid(Exception)` with `.errors: list[str]`
  - `validate_payload(schema_name: str, payload: dict) -> None` raises `SchemaInvalid`. `schema_name` is `"carpark_availability.v1"`; the file is `contracts/events/{schema_name}.schema.json`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_schemas.py
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
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest tests/unit/test_schemas.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Write the schema and the validator**

The JSON Schema requires `CarParkID`, `AvailableLots`, `LotType`, `Agency`; `Area`, `Development` and `Location` are optional strings; `AvailableLots` is an integer `>= 0`; `additionalProperties` is `true` (tolerant reader, spec 3.2). `schemas.py` loads the file relative to the repository root, caches it, and raises `SchemaInvalid(errors=[...])` built from `jsonschema` error paths.

- [ ] **Step 4: Run them to verify they pass**

Run: `pytest tests/unit/test_schemas.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add contracts src/mobility_ingest/schemas.py tests/unit/test_schemas.py
git commit -m "feat: add versioned carpark availability event schema"
```

---

### Task 4: Carpark normalizer and quarantine rules

**Files:**
- Create: `src/mobility_ingest/normalize/__init__.py`, `src/mobility_ingest/normalize/carpark.py`, `tests/unit/test_normalize_carpark.py`, `tests/fixtures/carpark_availability_ok.json`, `tests/fixtures/carpark_availability_edge.json`

**Interfaces:**
- Consumes: `compute_state_hash`, `compute_event_id`, `NormalizedEvent`, `Quarantined`, `NormalizeResult` (Task 2); `validate_payload`, `SchemaInvalid` (Task 3).
- Produces:
  - `normalize_carparks(body: bytes, fetched_at: datetime, trace_id: str) -> NormalizeResult`

**Mapping (exact):**
- Input: JSON object with key `value`, a list of records (LTA `CarParkAvailability` shape).
- Per record: validate against `carpark_availability.v1`; on `SchemaInvalid` add `Quarantined("schema_invalid", record)`.
- `entity_id` = `"{CarParkID}:{LotType}"` (one entity per lot type, so change detection and Kafka ordering are per lot type; `CarParkID` and `LotType` stay in `payload`).
- `lat`, `lon` = `Location` split on a single space, parsed as `Decimal`. If `Location` is missing, empty or not two numbers, both are `None`.
- `payload` = `{"CarParkID", "Area", "Development", "LotType", "Agency", "AvailableLots"}` with absent optional keys set to `None`. Unknown upstream keys are dropped here, before hashing.
- `observed_at` = `fetched_at` truncated to milliseconds.
- `state_hash` = `compute_state_hash(payload)`; `event_id` = `compute_event_id(feed="carpark_availability", ...)`.
- Duplicate records with the same `entity_id` and `LotType` in one response: identical payloads collapse to one event; differing payloads quarantine every copy as `schema_invalid`.
- Body that is not JSON, or JSON without a `value` list: `Quarantined("parse_error", body.decode("utf-8", "replace")[:2000])` and no events.

- [ ] **Step 1: Record a real response before finalizing the fixtures**

Fetch one live CarParkAvailability response with a LTA DataMall key (stored in `.env`, never printed) and save the body as `tests/fixtures/carpark_availability_ok.json`. If no key is available, write the fixture from the documented field names and mark it `"_source": "synthetic"` at the top of the file, and report that the live shape is still unverified.

- [ ] **Step 2: Write the failing tests**

```python
# tests/unit/test_normalize_carpark.py
import json
from datetime import datetime, timezone
from pathlib import Path
from mobility_ingest.normalize.carpark import normalize_carparks

FIX = Path(__file__).parent.parent / "fixtures"
T = datetime(2026, 11, 3, 12, 0, 0, 123456, tzinfo=timezone.utc)
TRACE = "a" * 32

def rec(**over):
    base = {"CarParkID": "HE12", "Area": "Bishan", "Development": "Blk 1",
            "Location": "1.35 103.84", "AvailableLots": 12, "LotType": "C", "Agency": "HDB"}
    return {**base, **over}

def body(*records):
    return json.dumps({"value": list(records)}).encode()

def test_ok_fixture_produces_events_and_no_quarantine():
    res = normalize_carparks((FIX / "carpark_availability_ok.json").read_bytes(), T, TRACE)
    assert res.events and res.quarantined == []

def test_non_json_body_is_quarantined_not_empty():
    res = normalize_carparks(b"<html>503 Service Unavailable</html>", T, TRACE)
    assert res.events == []
    assert [q.reason for q in res.quarantined] == ["parse_error"]

def test_identical_duplicate_records_collapse_to_one_event():
    res = normalize_carparks(body(rec(), rec()), T, TRACE)
    assert len(res.events) == 1 and res.quarantined == []

def test_conflicting_duplicate_records_are_quarantined():
    res = normalize_carparks(body(rec(AvailableLots=12), rec(AvailableLots=3)), T, TRACE)
    assert res.events == []
    assert [q.reason for q in res.quarantined] == ["schema_invalid", "schema_invalid"]

def test_missing_location_yields_null_coordinates():
    rec_no_loc = rec(); del rec_no_loc["Location"]
    res = normalize_carparks(body(rec_no_loc), T, TRACE)
    assert res.events[0].lat is None and res.events[0].lon is None

def test_unknown_upstream_fields_are_ignored_and_do_not_change_identity():
    a = normalize_carparks(body(rec()), T, TRACE).events[0]
    b = normalize_carparks(body(rec(NewField="x")), T, TRACE).events[0]
    assert "NewField" not in b.payload
    assert a.event_id == b.event_id and a.state_hash == b.state_hash

def test_non_numeric_or_negative_lots_are_quarantined():
    for bad in ("twelve", -1):
        res = normalize_carparks(body(rec(AvailableLots=bad)), T, TRACE)
        assert res.events == []
        assert [q.reason for q in res.quarantined] == ["schema_invalid"]

def test_observed_at_is_truncated_to_milliseconds():
    e = normalize_carparks(body(rec()), T, TRACE).events[0]
    assert e.observed_at == datetime(2026, 11, 3, 12, 0, 0, 123000, tzinfo=timezone.utc)
```

- [ ] **Step 3: Run them to verify they fail**

Run: `pytest tests/unit/test_normalize_carpark.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 4: Implement `normalize_carparks` to the mapping above**

Each rule maps to one test. Keep the duplicate check to a dict keyed by `(entity_id, LotType)` built before emitting events.

- [ ] **Step 5: Run them to verify they pass**

Run: `pytest tests/unit/test_normalize_carpark.py -v`
Expected: PASS (8 tests)

- [ ] **Step 6: Commit**

```bash
git add src/mobility_ingest/normalize tests/unit/test_normalize_carpark.py tests/fixtures
git commit -m "feat: normalize carpark availability with quarantine rules"
```

---

### Task 5: `ingest` schema migration

**Files:**
- Create: `alembic.ini`, `migrations/env.py`, `migrations/versions/0001_ingest_schema.py`, `src/mobility_ingest/db.py`, `tests/integration/test_migrations.py`

**Interfaces:**
- Consumes: `mysql_url` fixture (Task 1).
- Produces:
  - `make_engine(url: str) -> sqlalchemy.Engine`
  - Tables in schema `ingest`: `feed_state`, `feed_lease`, `raw_fetch`, `normalized_events`, `quarantine`, `outbox`, `outbox_delivery`.

**Deviations from the spec DDL (section 5.1), both deliberate:**
- `normalized_events.state_hash CHAR(64) NOT NULL` is added (Task 2 explains why).
- `normalized_events` has no `incident_key` or related columns; those belong to the incident plan.

- [ ] **Step 1: Write the failing tests**

```python
# tests/integration/test_migrations.py
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
```

`conftest.py` gains `migrated_url`: runs `alembic upgrade head` against a fresh database and yields its URL.

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest -m integration tests/integration/test_migrations.py -v`
Expected: FAIL with `ModuleNotFoundError` for `mobility_ingest.db`

- [ ] **Step 3: Write `db.py`, `alembic.ini`, `env.py` and migration `0001`**

Migration DDL follows spec section 5.1 for these tables, with three changes: the `normalized_events` primary key is `(event_id, observed_at)` and it is `PARTITION BY RANGE (TO_DAYS(observed_at))` with monthly partitions from `p202611` through `p202703` plus `pmax`; it adds `state_hash`; and it drops the `incident_key` and `incident_seq` columns that belong to the incident plan. Create the DDL with `op.execute` because Alembic does not model partitions. Foreign keys are not declared.

- [ ] **Step 4: Run them to verify they pass**

Run: `pytest -m integration tests/integration/test_migrations.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add alembic.ini migrations src/mobility_ingest/db.py tests/integration tests/conftest.py
git commit -m "feat: add ingest schema migration with partitioned events"
```

---

### Task 6: Feed lease with fencing token

**Files:**
- Create: `src/mobility_ingest/lease.py`, `tests/integration/test_lease.py`

**Interfaces:**
- Consumes: `make_engine` (Task 5).
- Produces:
  - `acquire(conn, feed: str, holder: str, ttl_seconds: int, now: datetime) -> int | None` returns the new fencing token, or `None` if another holder's lease is still valid.
  - `assert_fenced(conn, feed: str, token: int) -> None` locks the `feed_lease` row `FOR UPDATE` and raises `FencingError` if the stored token differs.
  - `class FencingError(Exception)`

- [ ] **Step 1: Write the failing tests**

```python
# tests/integration/test_lease.py
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
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest -m integration tests/integration/test_lease.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement `lease.py`**

`acquire`: insert the row on first use (`INSERT ... ON DUPLICATE KEY UPDATE feed=feed` then read it `FOR UPDATE`). If `holder` equals the current holder or `expires_at < now`, set `holder`, `expires_at = now + ttl`, and `fencing_token = fencing_token + 1` when the holder changed, and return the token; otherwise return `None`. Note that a renew by the same holder keeps the token.
`assert_fenced`: `SELECT fencing_token FROM feed_lease WHERE feed=%s FOR UPDATE` and compare.

- [ ] **Step 4: Run them to verify they pass**

Run: `pytest -m integration tests/integration/test_lease.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/mobility_ingest/lease.py tests/integration/test_lease.py
git commit -m "feat: add feed lease with fencing token"
```

---

### Task 7: Feed adapter and conditional fetch

**Files:**
- Create: `src/mobility_ingest/adapters.py`, `tests/integration/test_conditional_fetch.py`
- Modify: `src/mobility_ingest/ingest.py` (create module; `poll_once` is completed in Task 8 and Task 9)

**Interfaces:**
- Consumes: `make_engine`, tables `feed_state` and `raw_fetch` (Task 5).
- Produces:
  - `@dataclass(frozen=True) class FetchResult: status: int; body: bytes`
  - `class UpstreamError(Exception)` with `.retryable: bool`
  - `class FeedAdapter(Protocol): name: str; def fetch(self) -> FetchResult`
  - `class FixtureAdapter` takes `name: str` and `path: Path`, always returns status 200 with the file's bytes; `set_body(bytes)` changes what the next fetch returns.
  - `record_fetch(conn, feed: str, fetched_at: datetime, status: int, body: bytes) -> tuple[bool, int | None]` returns `(changed, raw_fetch_id)`. `changed` is `False` when the SHA-256 of `body` equals the most recent `raw_fetch.payload_hash` for this feed; in that case no row is inserted and `raw_fetch_id` is `None`, and `feed_state.last_success_at` is still updated.

- [ ] **Step 1: Write the failing tests**

```python
# tests/integration/test_conditional_fetch.py
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
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest -m integration tests/integration/test_conditional_fetch.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement the adapter and `record_fetch`**

`record_fetch` upserts `feed_state` (`INSERT ... ON DUPLICATE KEY UPDATE`) and sets `last_polled_at`, and `last_success_at` for status 200. Compare with the latest hash using `SELECT payload_hash FROM raw_fetch WHERE feed=%s ORDER BY id DESC LIMIT 1`. Store `body` as-is in this plan (the spec's `compressed` wording is handled in the ops plan).

- [ ] **Step 4: Run them to verify they pass**

Run: `pytest -m integration tests/integration/test_conditional_fetch.py -v`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/mobility_ingest/adapters.py src/mobility_ingest/ingest.py tests/integration/test_conditional_fetch.py
git commit -m "feat: add feed adapter and conditional fetch"
```

---

### Task 8: Fenced save transaction

**Files:**
- Modify: `src/mobility_ingest/ingest.py`
- Create: `tests/integration/test_ingest_save.py`

**Interfaces:**
- Consumes: `NormalizedEvent`, `Quarantined` (Task 2); `assert_fenced`, `FencingError` (Task 6); `record_fetch` (Task 7).
- Produces:
  - `@dataclass(frozen=True) class SaveResult: inserted: int; skipped_unchanged: int; skipped_duplicate: int; quarantined: int`
  - `save_batch(conn, *, feed: str, fencing_token: int, raw_fetch_id: int | None, events: list[NormalizedEvent], quarantined: list[Quarantined]) -> SaveResult`
  - Outbox topic is `events.normalized`, `partition_key = entity_id`, payload = the envelope JSON (`event_id`, `schema_version`, `type`, `feed`, `entity_id`, `observed_at` ISO-8601 UTC, `trace_id`, `payload`, `lat`, `lon`, `state_hash`).
  - Delivery rows for sinks `'kafka'` and `'opensearch'`, created with the outbox row.

**Rules:**
- First `assert_fenced(conn, feed, fencing_token)`; a stale token raises and nothing is written.
- For each event: if the latest stored `state_hash` for `entity_id` (`ORDER BY observed_at DESC LIMIT 1`) equals the event's `state_hash`, count as `skipped_unchanged` and write nothing.
- Otherwise `INSERT INTO normalized_events ... ON DUPLICATE KEY UPDATE event_id = event_id`. Rowcount 0 means `skipped_duplicate`. Rowcount 1 means insert the outbox row (`ON DUPLICATE KEY UPDATE event_id = event_id`) and two `outbox_delivery` rows.
- Each quarantined item is inserted into `quarantine` (`feed`, `reason`, `content` as JSON, `raw_fetch_id`, `created_at`).
- The caller owns the transaction; `save_batch` does not commit.

- [ ] **Step 1: Write the failing tests**

```python
# tests/integration/test_ingest_save.py
from datetime import datetime, timezone
import pytest
from sqlalchemy import text
from mobility_ingest.db import make_engine
from mobility_ingest.contract import compute_event_id, compute_state_hash, NormalizedEvent, Quarantined
from mobility_ingest.ingest import save_batch
from mobility_ingest.lease import acquire, FencingError

pytestmark = pytest.mark.integration
T = datetime(2026, 11, 3, 12, 0, 0, tzinfo=timezone.utc)
TRACE = "b" * 32

def ev(entity, lots, t=T):
    payload = {"CarParkID": entity, "Area": None, "Development": None, "LotType": "C",
               "Agency": "HDB", "AvailableLots": lots}
    sh = compute_state_hash(payload)
    return NormalizedEvent(compute_event_id("save_test", entity, t, sh), "save_test", entity,
                           t, None, None, payload, sh, 1, TRACE)

def fenced_conn(url, feed):
    eng = make_engine(url)
    c = eng.connect()
    tx = c.begin()
    token = acquire(c, feed, "node-1", 30, T)
    return c, tx, token

def test_inserts_event_outbox_and_two_delivery_rows(migrated_url):
    c, tx, tok = fenced_conn(migrated_url, "save_a")
    r = save_batch(c, feed="save_a", fencing_token=tok, raw_fetch_id=None,
                   events=[ev("E1", 5)], quarantined=[])
    assert r.inserted == 1
    sinks = c.execute(text("SELECT sink FROM outbox_delivery od JOIN outbox o ON o.id=od.outbox_id "
                           "WHERE o.partition_key='E1' ORDER BY sink")).scalars().all()
    assert sinks == ["kafka", "opensearch"]
    tx.rollback(); c.close()

def test_unchanged_value_is_skipped(migrated_url):
    c, tx, tok = fenced_conn(migrated_url, "save_b")
    save_batch(c, feed="save_b", fencing_token=tok, raw_fetch_id=None, events=[ev("E2", 5)], quarantined=[])
    r = save_batch(c, feed="save_b", fencing_token=tok, raw_fetch_id=None,
                   events=[ev("E2", 5, datetime(2026, 11, 3, 12, 1, 0, tzinfo=timezone.utc))], quarantined=[])
    assert r.inserted == 0 and r.skipped_unchanged == 1
    tx.rollback(); c.close()

def test_changed_value_is_inserted(migrated_url):
    c, tx, tok = fenced_conn(migrated_url, "save_c")
    save_batch(c, feed="save_c", fencing_token=tok, raw_fetch_id=None, events=[ev("E3", 5)], quarantined=[])
    r = save_batch(c, feed="save_c", fencing_token=tok, raw_fetch_id=None,
                   events=[ev("E3", 6, datetime(2026, 11, 3, 12, 1, 0, tzinfo=timezone.utc))], quarantined=[])
    assert r.inserted == 1
    tx.rollback(); c.close()

def test_stale_fencing_token_writes_nothing(migrated_url):
    c, tx, tok = fenced_conn(migrated_url, "save_d")
    with pytest.raises(FencingError):
        save_batch(c, feed="save_d", fencing_token=tok + 1, raw_fetch_id=None,
                   events=[ev("E4", 5)], quarantined=[])
    n = c.execute(text("SELECT COUNT(*) FROM normalized_events WHERE feed='save_d'")).scalar()
    assert n == 0
    tx.rollback(); c.close()

def test_quarantined_items_are_stored_not_dropped(migrated_url):
    c, tx, tok = fenced_conn(migrated_url, "save_e")
    r = save_batch(c, feed="save_e", fencing_token=tok, raw_fetch_id=None, events=[],
                   quarantined=[Quarantined("parse_error", "<html>")])
    assert r.quarantined == 1
    assert c.execute(text("SELECT COUNT(*) FROM quarantine WHERE feed='save_e'")).scalar() == 1
    tx.rollback(); c.close()
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest -m integration tests/integration/test_ingest_save.py -v`
Expected: FAIL with `ImportError` for `save_batch`

- [ ] **Step 3: Implement `save_batch` to the rules above**

Use parameter binding for every value. Serialize `observed_at` with the same formatter as `compute_event_id`. `SaveResult.skipped_duplicate` is counted separately from `skipped_unchanged`.

- [ ] **Step 4: Run them to verify they pass**

Run: `pytest -m integration tests/integration/test_ingest_save.py -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add src/mobility_ingest/ingest.py tests/integration/test_ingest_save.py
git commit -m "feat: add fenced transactional save with outbox and quarantine"
```

---

### Task 9: Outbox relay with sink protocol and purge

**Files:**
- Create: `src/mobility_ingest/outbox.py`, `tests/integration/test_relay.py`

**Interfaces:**
- Consumes: `make_engine`, tables `outbox` and `outbox_delivery` (Task 5).
- Produces:
  - `@dataclass(frozen=True) class OutboxRow: outbox_id: int; topic: str; partition_key: str; payload: dict; headers: dict[str, str]`
  - `class Sink(Protocol): name: str; def publish(self, rows: list[OutboxRow]) -> list[int]` returns the `outbox_id`s that were acknowledged. Raises on sink-level failure.
  - `relay_once(engine, sink: Sink, batch_size: int = 100) -> int` returns the number of rows acknowledged.
  - `purge_outbox(engine, older_than_days: int = 3, batch_size: int = 1000) -> int` deletes outbox rows whose deliveries are all sent and older than the cutoff, in batches, and returns the total deleted.
- `headers` for every row: `{"schema_version": str(payload["schema_version"])}`. This is the Kafka header from spec 3.2.

**Rules:**
- In one transaction per call: `SELECT ... FROM outbox_delivery od JOIN outbox o ON o.id=od.outbox_id WHERE od.sink=:sink AND od.sent_at IS NULL ORDER BY od.outbox_id LIMIT :n FOR UPDATE SKIP LOCKED`, then `sink.publish`, then `UPDATE outbox_delivery SET sent_at=now(3) WHERE sink=:sink AND outbox_id IN (...)` for the acknowledged ids.
- If `sink.publish` raises: `UPDATE ... SET attempts = attempts + 1, last_error = left(:err, 255)` for the batch and re-raise the exception to the caller. The transaction rolls back, so `sent_at` is not set.
- A sink never blocks another sink: deliveries are per `(outbox_id, sink)`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/integration/test_relay.py
import threading
from datetime import datetime, timezone, timedelta
import pytest
from sqlalchemy import text
from mobility_ingest.db import make_engine
from mobility_ingest.outbox import relay_once, purge_outbox, OutboxRow

pytestmark = pytest.mark.integration

class RecordingSink:
    def __init__(self, name, fail=False):
        self.name, self.fail, self.seen = name, fail, []
    def publish(self, rows):
        if self.fail:
            raise RuntimeError("sink down")
        self.seen.extend(r.outbox_id for r in rows)
        return [r.outbox_id for r in rows]

def seed(eng, n, feed_tag):
    with eng.begin() as c:
        for i in range(n):
            eid = f"{feed_tag}{i:060d}"
            c.execute(text("INSERT INTO outbox (event_id, topic, partition_key, payload) "
                           "VALUES (:e,'events.normalized',:k,:p)"),
                      {"e": eid, "k": "K", "p": '{"schema_version":1}'})
            oid = c.execute(text("SELECT id FROM outbox WHERE event_id=:e"), {"e": eid}).scalar()
            for s in ("kafka", "opensearch"):
                c.execute(text("INSERT INTO outbox_delivery (outbox_id, sink) VALUES (:o,:s)"), {"o": oid, "s": s})

def test_relay_marks_only_that_sink_sent(migrated_url):
    eng = make_engine(migrated_url); seed(eng, 3, "a")
    assert relay_once(eng, RecordingSink("kafka")) >= 3
    with eng.connect() as c:
        pending_os = c.execute(text("SELECT COUNT(*) FROM outbox_delivery WHERE sink='opensearch' AND sent_at IS NULL")).scalar()
        assert pending_os >= 3

def test_failing_sink_keeps_row_pending_and_counts_attempt(migrated_url):
    eng = make_engine(migrated_url); seed(eng, 1, "b")
    with pytest.raises(RuntimeError):
        relay_once(eng, RecordingSink("opensearch", fail=True))
    with eng.connect() as c:
        row = c.execute(text("SELECT attempts, sent_at, last_error FROM outbox_delivery od "
                             "JOIN outbox o ON o.id=od.outbox_id WHERE o.event_id LIKE 'b%' AND od.sink='opensearch'")).one()
    assert row.attempts == 1 and row.sent_at is None and "sink down" in row.last_error

def test_two_relays_never_take_the_same_row(migrated_url):
    eng = make_engine(migrated_url); seed(eng, 20, "c")
    s1, s2 = RecordingSink("kafka"), RecordingSink("kafka")
    threads = [threading.Thread(target=relay_once, args=(eng, s, 5)) for s in (s1, s2)]
    [t.start() for t in threads]; [t.join() for t in threads]
    assert not set(s1.seen) & set(s2.seen)

def test_purge_removes_only_fully_sent_old_rows(migrated_url):
    eng = make_engine(migrated_url); seed(eng, 1, "d")
    with eng.begin() as c:
        c.execute(text("UPDATE outbox_delivery SET sent_at = now(3) - INTERVAL 4 DAY WHERE outbox_id IN "
                       "(SELECT id FROM outbox WHERE event_id LIKE 'd%')"))
        c.execute(text("UPDATE outbox SET created_at = now(3) - INTERVAL 4 DAY WHERE event_id LIKE 'd%'"))
    assert purge_outbox(eng, older_than_days=3) >= 1
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest -m integration tests/integration/test_relay.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement `outbox.py` to the rules above**

`purge_outbox` must only delete an outbox row when it has no `outbox_delivery` row with `sent_at IS NULL`, and `created_at` is older than the cutoff. Delete the `outbox_delivery` rows first, then the `outbox` row, in the same batch transaction.

- [ ] **Step 4: Run them to verify they pass**

Run: `pytest -m integration tests/integration/test_relay.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/mobility_ingest/outbox.py tests/integration/test_relay.py
git commit -m "feat: add outbox relay with per-sink delivery and purge"
```

---

### Task 10: Kafka and OpenSearch sinks

**Files:**
- Create: `src/mobility_ingest/sinks/__init__.py`, `src/mobility_ingest/sinks/kafka.py`, `src/mobility_ingest/sinks/opensearch.py`, `tests/integration/test_sinks.py`
- Modify: `docker-compose.test.yml` (add Kafka in KRaft single-node mode with the topic `events.normalized` created with 12 partitions and 7-day retention; add OpenSearch single-node with security disabled, for tests only)

**Interfaces:**
- Consumes: `OutboxRow`, `Sink` (Task 9).
- Produces:
  - `class KafkaSink: name = "kafka"; __init__(self, bootstrap_servers: str)`. Producer config: `acks=all`, `enable.idempotence=True`, `linger.ms=5`. `publish` sends each row to `topic=row.topic`, `key=row.partition_key`, `value=json.dumps(row.payload)`, `headers=row.headers`, waits for all delivery reports, and returns the ids whose delivery succeeded. Any failure raises.
  - `class OpenSearchSink: name = "opensearch"; __init__(self, client)`. `publish` bulk-indexes into `events-YYYY.MM.DD` (index date from `payload["observed_at"]`) with `_id = payload["event_id"]`. Per-item errors are not acknowledged; the rest are. Returns acknowledged ids.

- [ ] **Step 1: Write the failing tests**

```python
# tests/integration/test_sinks.py
import json
import pytest
from mobility_ingest.outbox import OutboxRow
from mobility_ingest.sinks.kafka import KafkaSink
from mobility_ingest.sinks.opensearch import OpenSearchSink

pytestmark = pytest.mark.integration

PAYLOAD = {"schema_version": 1, "event_id": "e" * 64, "observed_at": "2026-11-03T12:00:00.000Z",
           "entity_id": "HE12", "feed": "carpark_availability"}
ROW = OutboxRow(outbox_id=1, topic="events.normalized", partition_key="HE12",
                payload=PAYLOAD, headers={"schema_version": "1"})

def test_kafka_sink_acknowledges_row(kafka_bootstrap):
    assert KafkaSink(kafka_bootstrap).publish([ROW]) == [1]

def test_opensearch_sink_indexes_by_event_id_and_is_idempotent(opensearch_client):
    sink = OpenSearchSink(opensearch_client)
    assert sink.publish([ROW]) == [1]
    assert sink.publish([ROW]) == [1]  # replay overwrites, does not duplicate
    hits = opensearch_client.count(index="events-2026.11.03")["count"]
    assert hits == 1
```

`conftest.py` gains `kafka_bootstrap` and `opensearch_client` fixtures that skip with a message when the services are not running, and create the topic `events.normalized` in the Kafka fixture.

- [ ] **Step 2: Run them to verify they fail**

Run: `docker compose -f docker-compose.test.yml up -d kafka opensearch && pytest -m integration tests/integration/test_sinks.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement both sinks**

Use `confluent_kafka.Producer` with a delivery callback that collects per-message results and calls `flush()`. For OpenSearch, use `opensearchpy.helpers.bulk` with `raise_on_error=False` and read per-item results.

- [ ] **Step 4: Run them to verify they pass**

Run: `pytest -m integration tests/integration/test_sinks.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/mobility_ingest/sinks docker-compose.test.yml tests/integration/test_sinks.py tests/conftest.py
git commit -m "feat: add Kafka and OpenSearch outbox sinks"
```

---

### Task 11: Poll loop that ties the slice together

**Files:**
- Modify: `src/mobility_ingest/ingest.py` (add `poll_once`)
- Create: `tests/integration/test_poll_once.py`

**Interfaces:**
- Consumes: everything from Tasks 2 to 8.
- Produces:
  - `@dataclass(frozen=True) class PollResult: status: str; inserted: int; skipped_unchanged: int; quarantined: int` where `status` is one of `"skipped_lease"`, `"skipped_unchanged"`, `"ok"`, `"upstream_error"`.
  - `poll_once(engine, *, adapter: FeedAdapter, normalize: Callable[[bytes, datetime, str], NormalizeResult], holder: str, lease_ttl_seconds: int, now: datetime, trace_id: str) -> PollResult`

**Sequence (exact):**
1. In a transaction: `acquire` the lease for `adapter.name`. If `None`, return `skipped_lease` and make no other writes.
2. `adapter.fetch()`. On `UpstreamError`: write `feed_state.consecutive_failures + 1` and return `upstream_error`. Do not create events (spec F5).
3. In the same transaction: `record_fetch`. If unchanged, return `skipped_unchanged`.
4. `normalize(body, now, trace_id)`.
5. `save_batch` with the fencing token from step 1. Commit. Return `ok` with the counts.

- [ ] **Step 1: Write the failing tests**

```python
# tests/integration/test_poll_once.py
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
```

- [ ] **Step 2: Run them to verify they fail**

Run: `pytest -m integration tests/integration/test_poll_once.py -v`
Expected: FAIL with `ImportError` for `poll_once`

- [ ] **Step 3: Implement `poll_once` to the sequence above**

Keep the whole sequence in one connection and one transaction so the lease and the save commit together. The upstream-error path commits its `feed_state` update and returns.

- [ ] **Step 4: Run them to verify they pass**

Run: `pytest -m integration tests/integration/test_poll_once.py -v`
Expected: PASS (4 tests)

- [ ] **Step 5: Run the full suite**

Run: `pytest -v && pytest -m integration -v`
Expected: all tests pass, with the integration tests run against the docker-compose services.

- [ ] **Step 6: Commit**

```bash
git add src/mobility_ingest/ingest.py tests/integration/test_poll_once.py
git commit -m "feat: add poll loop tying lease, fetch, normalize and save together"
```

---

## Self-Review

**Spec coverage (this plan's scope: spec 2.4, 3 without NLP, 3.2, 5.1 ingest schema, 6 for ingest and relay, 9.1 cases F1 to F4, F5, F7, F9, F13 to F18, F21, F45):**
- F1 (lease, single poller): Task 6 and Task 11.
- F2 (pause): not implemented. `feed_state.paused` exists in the schema; the pause check is added in the control plan. Gap accepted and listed.
- F3 (stale fencing): Task 6 and Task 8.
- F4, F5 (fetch and backoff): Task 7 and Task 11. Backoff timing with jitter is not implemented here; `UpstreamError.retryable` carries the signal for the scheduler, which is in the ops plan. Gap accepted.
- F7, F9 (normalize, quarantine): Task 4.
- F13, F14, F15: Task 8 and Task 11.
- F16 to F18, F21: Task 9.
- F19, F20 (sink outage): covered by per-sink delivery rows in Task 9. The chaos verification is in the ops plan.
- F45 (identical body): Task 7.
- Retention for outbox: Task 9. Other retention: ops plan.
- Not covered and intentionally deferred: F8, F10 to F12b, F46 (incident NLP), all of section 4 (alert-engine), section 7 (control plane), section 4.2.

**Step scan:** Each step names a file, a signature or a command and its expected result. Task 4 Step 4 and Task 8 Step 3 describe behavior by rules rather than code bodies, which fits the skill's "a code body only for an algorithm the signature and tests do not determine". The rules in both tasks map one-to-one to the tests above them.

**Type consistency:** `NormalizedEvent`, `Quarantined`, `NormalizeResult` are defined once in Task 2 and used unchanged in Tasks 4, 8 and 11. `acquire` and `assert_fenced` signatures in Task 6 match their use in Tasks 8 and 11. `record_fetch` returns `(bool, int | None)` in Task 7 and is used that way in Task 11. `OutboxRow` and `Sink` are defined in Task 9 and used in Task 10. `poll_once` takes `normalize` as a callable of the same shape as `normalize_carparks`.

**Review Focus:** all five lines have named tests in Task 4 (and Task 3 for the tolerant-reader rule that Task 4 depends on). Nothing is left empty.

**Proportion:** the spec is 1,258 lines. This plan is the first of about four. Each task is a short list of interfaces plus test code, not the implementation.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-11-03-motorist-ingest-foundation.md`. Please review the plan and confirm it captures what you want before implementation.

Two things need your input:
- **Scope:** this plan covers the ingest foundation only. `alert-engine`, the incident NLP (blocked on a sample of real incident messages), and the control plane each need their own plan. Should the next plan be `alert-engine`?
- **Execution method:** for this plan I recommend **Subagent-driven**. Tasks 6 to 11 depend on each other's interfaces, and a wrong lease or fencing rule would ship silent data loss, so a fresh reviewer per task is worth the cost. Native would be cheaper if you prefer speed over independent review.

Which execution approach would you like?

- **REQUIRED SUB-SKILL when chosen:** superpowers:subagent-driven-development, or superpowers:executing-plans for Native.
