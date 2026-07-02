#!/usr/bin/env python
"""
Migration 0042 upgrade + downgrade harness against CockroachDB.

Connects to a running CockroachDB instance, creates a scratch database,
builds the pre-0042 schema subset, seeds rows, then exercises the
patched migration's upgrade() and downgrade() paths end to end.

Usage (inside the POC compose network):
    docker compose run --rm --entrypoint "" \
        -v "$PWD/../scripts/test-migration-0042.py:/tmp/test-migration-0042.py:ro" \
        airflow-init python /tmp/test-migration-0042.py
"""
from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path

import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text

# ---------------------------------------------------------------------------
# 1. Create a scratch database
# ---------------------------------------------------------------------------
admin_engine = sa.create_engine(
    "cockroachdb://root@cockroachdb:26257/defaultdb?sslmode=disable",
    isolation_level="AUTOCOMMIT",
)
with admin_engine.connect() as c:
    c.execute(text("DROP DATABASE IF EXISTS mig0042 CASCADE"))
    c.execute(text("CREATE DATABASE mig0042"))
admin_engine.dispose()

engine = sa.create_engine("cockroachdb://root@cockroachdb:26257/mig0042?sslmode=disable")

# ---------------------------------------------------------------------------
# 2. Create pre-0042 schema: task_instance + 7 FK child tables
# ---------------------------------------------------------------------------
DDL_STMTS = [
    """
    CREATE TABLE IF NOT EXISTS task_instance (
        dag_id STRING NOT NULL,
        task_id STRING NOT NULL,
        run_id STRING NOT NULL,
        map_index INT8 NOT NULL,
        queued_dttm TIMESTAMPTZ NULL,
        start_date TIMESTAMPTZ NULL,
        CONSTRAINT task_instance_pkey PRIMARY KEY (dag_id, task_id, run_id, map_index)
    )
    """,
]

FK_TABLES = [
    ("rendered_task_instance_fields", "rtif_ti_fkey"),
    ("task_fail", "task_fail_ti_fkey"),
    ("task_instance_history", "task_instance_history_ti_fkey"),
    ("task_instance_note", "task_instance_note_ti_fkey"),
    ("task_map", "task_map_task_instance_fkey"),
    ("task_reschedule", "task_reschedule_ti_fkey"),
    ("xcom", "xcom_task_instance_fkey"),
]

for table_name, fk_name in FK_TABLES:
    DDL_STMTS.append(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        dag_id STRING NOT NULL,
        task_id STRING NOT NULL,
        run_id STRING NOT NULL,
        map_index INT8 NOT NULL,
        CONSTRAINT {fk_name} FOREIGN KEY (dag_id, task_id, run_id, map_index)
            REFERENCES task_instance (dag_id, task_id, run_id, map_index)
            ON DELETE CASCADE
    )
    """)

with engine.begin() as conn:
    for stmt in DDL_STMTS:
        conn.execute(text(stmt))

print("Schema created.")

# ---------------------------------------------------------------------------
# 3. Seed 2500 task_instance rows (exercises 3 batches at batch_size 1000)
# ---------------------------------------------------------------------------
SEED_COUNT = 2500
with engine.begin() as conn:
    existing = conn.execute(text("SELECT count(*) FROM task_instance")).scalar()
    if existing < SEED_COUNT:
        rows_to_insert = SEED_COUNT - existing
        insert_stmt = text("""
            INSERT INTO task_instance (dag_id, task_id, run_id, map_index, queued_dttm, start_date)
            VALUES (:dag_id, :task_id, :run_id, :map_index, now(), now())
        """)
        params = [
            {
                "dag_id": f"dag_{i // 100}",
                "task_id": f"task_{i % 100}",
                "run_id": f"run_{i}",
                "map_index": 0,
            }
            for i in range(existing, SEED_COUNT)
        ]
        conn.execute(insert_stmt, params)
    count = conn.execute(text("SELECT count(*) FROM task_instance")).scalar()
    print(f"Seeded {count} task_instance rows.")

# ---------------------------------------------------------------------------
# 4. Load the installed migration module
# ---------------------------------------------------------------------------
# Find the migration file in site-packages
migration_glob = list(
    Path(sys.prefix).glob(
        "**/airflow/migrations/versions/0042_3_0_0_add_uuid_primary_key_to_task_instance_*.py"
    )
)
if not migration_glob:
    # Also check lib/python*/site-packages
    migration_glob = list(
        Path("/home/airflow/.local").glob(
            "**/airflow/migrations/versions/0042_3_0_0_add_uuid_primary_key_to_task_instance_*.py"
        )
    )
if not migration_glob:
    print("ERROR: Could not find migration 0042 in installed packages")
    sys.exit(1)

migration_path = migration_glob[0]
print(f"Loading migration from: {migration_path}")

spec = importlib.util.spec_from_file_location("migration_0042", migration_path)
migration_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(migration_module)

# ---------------------------------------------------------------------------
# 5. Run upgrade()
# ---------------------------------------------------------------------------
print("\n=== UPGRADE ===")
backfill_start = time.monotonic()

with engine.begin() as conn:
    ctx = MigrationContext.configure(conn)
    ops = Operations(ctx)
    migration_module.op = ops
    migration_module.upgrade()

backfill_elapsed = time.monotonic() - backfill_start
print(f"Upgrade completed in {backfill_elapsed:.2f}s")

# ---------------------------------------------------------------------------
# 6. Upgrade assertions
# ---------------------------------------------------------------------------
errors = []

with engine.connect() as conn:
    # All 2500 rows have non-null UUID id
    null_ids = conn.execute(text("SELECT count(*) FROM task_instance WHERE id IS NULL")).scalar()
    if null_ids != 0:
        errors.append(f"FAIL: {null_ids} rows still have NULL id after upgrade")
    else:
        print(f"OK: All {SEED_COUNT} rows have non-null id")

    total = conn.execute(text("SELECT count(*) FROM task_instance")).scalar()
    if total != SEED_COUNT:
        errors.append(f"FAIL: Expected {SEED_COUNT} rows, found {total}")
    else:
        print(f"OK: Row count is {total}")

    # Check indexes
    idx_rows = conn.execute(
        text("SELECT DISTINCT index_name FROM [SHOW INDEXES FROM task_instance]")
    ).fetchall()
    idx_names = {row[0] for row in idx_rows}
    expected_indexes = {"task_instance_pkey", "task_instance_composite_key"}
    if idx_names != expected_indexes:
        errors.append(f"FAIL: Expected indexes {expected_indexes}, got {idx_names}")
    else:
        print(f"OK: Indexes are {idx_names}")

    # Check primary key is on (id).
    # CockroachDB's SHOW INDEXES lists stored columns too; filter to key columns only.
    pk_cols = conn.execute(
        text("""
            SELECT column_name FROM [SHOW INDEXES FROM task_instance]
            WHERE index_name = 'task_instance_pkey'
              AND storing = false AND implicit = false
            ORDER BY seq_in_index
        """)
    ).fetchall()
    pk_col_names = [row[0] for row in pk_cols]
    if pk_col_names != ["id"]:
        errors.append(f"FAIL: PK columns expected ['id'], got {pk_col_names}")
    else:
        print("OK: Primary key is on (id)")

    # Check FKs exist (at least one referencing task_instance)
    fk_count = conn.execute(
        text("""
            SELECT count(*)
            FROM information_schema.referential_constraints
            WHERE constraint_schema = 'public'
        """)
    ).scalar()
    if fk_count == 0:
        errors.append("FAIL: No foreign keys found after upgrade")
    else:
        print(f"OK: {fk_count} foreign key constraints present")

if errors:
    print("\nUPGRADE FAILURES:")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)

print("\nAll upgrade assertions passed.")

# ---------------------------------------------------------------------------
# 7. Run downgrade()
# ---------------------------------------------------------------------------
print("\n=== DOWNGRADE ===")

with engine.begin() as conn:
    ctx = MigrationContext.configure(conn)
    ops = Operations(ctx)
    migration_module.op = ops
    migration_module.downgrade()

print("Downgrade completed.")

# ---------------------------------------------------------------------------
# 8. Downgrade assertions
# ---------------------------------------------------------------------------
errors = []

with engine.connect() as conn:
    # id column should be gone
    col_rows = conn.execute(
        text("SELECT column_name FROM [SHOW COLUMNS FROM task_instance]")
    ).fetchall()
    col_names = [row[0] for row in col_rows]
    if "id" in col_names:
        errors.append("FAIL: 'id' column still exists after downgrade")
    else:
        print("OK: 'id' column removed")

    # PK back on composite
    pk_cols = conn.execute(
        text("""
            SELECT column_name FROM [SHOW INDEXES FROM task_instance]
            WHERE index_name = 'task_instance_pkey'
              AND storing = false AND implicit = false
            ORDER BY seq_in_index
        """)
    ).fetchall()
    pk_col_names = [row[0] for row in pk_cols]
    expected_pk = ["dag_id", "task_id", "run_id", "map_index"]
    if pk_col_names != expected_pk:
        errors.append(f"FAIL: PK columns expected {expected_pk}, got {pk_col_names}")
    else:
        print(f"OK: Primary key restored to {pk_col_names}")

    # Only task_instance_pkey index should remain
    idx_rows = conn.execute(
        text("SELECT DISTINCT index_name FROM [SHOW INDEXES FROM task_instance]")
    ).fetchall()
    idx_names = {row[0] for row in idx_rows}
    if idx_names != {"task_instance_pkey"}:
        errors.append(f"FAIL: Expected only task_instance_pkey index, got {idx_names}")
    else:
        print(f"OK: Only index is {idx_names}")

    # FKs should exist
    fk_count = conn.execute(
        text("""
            SELECT count(*)
            FROM information_schema.referential_constraints
            WHERE constraint_schema = 'public'
        """)
    ).scalar()
    if fk_count == 0:
        errors.append("FAIL: No foreign keys found after downgrade")
    else:
        print(f"OK: {fk_count} foreign key constraints present")

if errors:
    print("\nDOWNGRADE FAILURES:")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)

print("\nAll downgrade assertions passed.")
print(f"\nSUMMARY: {SEED_COUNT} rows, upgrade+downgrade passed, backfill took {backfill_elapsed:.2f}s")

engine.dispose()
