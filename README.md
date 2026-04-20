# CockroachDB + Apache Airflow Integration PoC

Proof-of-concept for using CockroachDB with Apache Airflow 3.x in two ways:

1. **Metadata Backend** — Replace PostgreSQL as Airflow's internal database
2. **Data Source** — Query/write CockroachDB from DAG workflows via a provider package

> **Status:** Proof-of-concept, not a supported product. The integration works end-to-end on Airflow 3.2.0 + CockroachDB v25.4 LTS with the workarounds documented here. For the upstream conversation, see Airflow [discussion #65453](https://github.com/apache/airflow/discussions/65453) and [`cockroachdb/sqlalchemy-cockroachdb#301`](https://github.com/cockroachdb/sqlalchemy-cockroachdb/pull/301).

## Motivation

Apache Airflow officially supports PostgreSQL, MySQL, and SQLite as metadata backends. CockroachDB is PostgreSQL wire-compatible and offers significant advantages for Airflow deployments:

- **Built-in high availability**: No need for PostgreSQL replication/failover infrastructure
- **Horizontal scalability**: Scales writes across nodes for large Airflow deployments
- **Multi-region support**: CockroachDB's geo-partitioned data enables multi-region Airflow setups
- **Simplified infrastructure**: One database instead of managing separate PostgreSQL and application databases

This PoC validates both integration paths and provides the foundation for
upstream contributions and a self-maintained compatibility layer.

## Project Structure

```
cockroachdb-airflow-poc/
├── README.md
├── LICENSE
├── docker/
│   ├── docker-compose.yml                 # Full stack: CockroachDB + Airflow
│   └── Dockerfile.airflow                 # Custom Airflow image with CRDB dialect
├── src/
│   ├── compatibility/                     # CockroachDB compatibility layer
│   │   ├── cockroachdb_sqlalchemy_plugin.py  # timestampdiff compiler extension
│   │   ├── retry_middleware.py            # Transaction retry on 40001 errors
│   │   └── migration_utils.py            # Audit Airflow migrations for CRDB compat
│   ├── provider/                          # Airflow provider package prototype
│   │   ├── hooks/cockroachdb.py           # CockroachDB Hook (DbApiHook)
│   │   ├── dialects/cockroachdb.py        # CockroachDB Dialect (upsert, introspection)
│   │   └── assets/cockroachdb.py          # URI sanitizer
│   └── tests/
│       └── test_cockroachdb_hook.py
├── plugins/
│   └── cockroachdb_compat_plugin.py       # Airflow plugin loader for compiler extensions
├── scripts/
│   ├── validate-poc.sh                    # End-to-end PoC validation
│   └── audit-airflow-migrations.sh        # Scan migrations for CRDB incompatibilities
└── examples/
    └── dags/
        ├── example_cockroachdb_dag.py
        └── example_cockroachdb_health_check.py
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- At least 4GB RAM available for containers
- CockroachDB v24.1+ (v25.2+ recommended for LTS support)

### 1. Configure Versions

Copy the environment template, then optionally override any of the version pins:

```bash
cp docker/.env.example docker/.env
```

Defaults (work out of the box):

```bash
# docker/.env
COCKROACHDB_VERSION=v25.4.8
AIRFLOW_VERSION=3.2.0
AIRFLOW_PYTHON_VERSION=3.12
SQLALCHEMY_COCKROACHDB_VERSION=2.0.3
```

### 2. Start the Stack

```bash
cd docker
docker compose up -d
```

This starts:
- **CockroachDB** (single-node, insecure) on port 26257 (SQL) and 8081 (DB Console)
- **Airflow 3.x** (API server + scheduler + dag-processor + triggerer) on port 8080

**Note on Airflow 3.x architecture**: Airflow 3.x separates the webserver into an `api-server`, and requires a dedicated `dag-processor` for DAG file parsing. Both are included in this stack.

The init process automatically:
1. Creates the `airflow` database in CockroachDB
2. Configures `serial_normalization = sql_sequence` for PostgreSQL compatibility
3. Enables READ COMMITTED isolation (avoids scheduler crashes on 40001 errors)
4. Creates `uuid_generate_v7()` compatibility function
5. Runs `airflow db migrate` against CockroachDB (retries on DDL visibility race)
6. Loads the SQLAlchemy compiler plugin for `timestampdiff` compatibility
7. Configures admin user via SimpleAuthManager (Airflow 3.x)

### 3. Access the UIs

| Service              | URL                     | Credentials |
|----------------------|-------------------------|-------------|
| Airflow Web UI       | http://localhost:8080    | admin / (see api-server logs) |
| CockroachDB Console  | http://localhost:8081    | N/A         |

### 4. Validate

```bash
./scripts/validate-poc.sh
```

### 5. Run Example DAGs

In the Airflow UI, enable and trigger the `cockroachdb_demo` DAG.

## CockroachDB Compatibility Configuration

### Metadata Backend Configuration

| Setting | Value | Purpose |
|---------|-------|---------|
| `serial_normalization` | `sql_sequence` | Match PostgreSQL SERIAL behavior |
| Isolation level | `READ COMMITTED` | Avoid 40001 scheduler crashes (SERIALIZABLE causes `WriteTooOldError` under contention) |
| `uuid_generate_v7()` UDF | `gen_random_uuid()` wrapper | Airflow 3.x migration compat |
| `SQL_ALCHEMY_CONN_ASYNC` | `cockroachdb+asyncpg://` | Airflow 3.x requires async engine (auto-derivation doesn't support `cockroachdb://` scheme) |
| Compiler plugin | `cockroachdb_compat_plugin.py` | Translates `timestampdiff()` to `EXTRACT(EPOCH FROM ...)` with NUMERIC cast |

### Data Source Configuration

| Setting | Value | Purpose |
|---------|-------|---------|
| Connection URI | `postgresql://` or `cockroachdb://` | CockroachDB accepts both via wire compatibility |
| Connection type | `postgres` in Airflow UI | Use existing PostgreSQL connection type |
| Transaction retry | Built into Hook | `retry_middleware.py` handles 40001 with exponential backoff |

## Current Status

### What Works

- ✅ `airflow db migrate` — 68 tables created successfully
- ✅ DAG parsing and scheduling
- ✅ Task execution (LocalExecutor) with correct output
- ✅ Airflow UI (API server, DAG grid)
- ✅ Example DAGs with CockroachDB CRUD + distributed SQL
- ✅ SQLAlchemy compiler plugin for `timestampdiff` compatibility
- ✅ CockroachDB provider package (Hook, Dialect, Asset URI)

### Known Limitations

- ⚠️ Scheduler crashes under write contention (40001 errors) — READ COMMITTED reduces but doesn't eliminate. Proper fix requires transaction retry logic in Airflow's scheduler
- ⚠️ Advisory locks (`pg_advisory_lock`) not available — affects multi-scheduler HA coordination efficiency. Single-scheduler deployments are unaffected

## Auditing Airflow Migrations

Scan Airflow's Alembic migrations for CockroachDB-incompatible patterns:

```bash
./scripts/audit-airflow-migrations.sh 3.2.0
```

## Running Tests

```bash
cd src
python -m pytest tests/ -v
```

## Integration Approaches

| Approach | Description | Effort | Risk |
|----------|-------------|--------|------|
| **A: Upstream PR** | CockroachDB as a supported Airflow metadata backend | High | Medium-High |
| **B: Provider Package** | CockroachDB as a data source in DAGs | Low | Low |

**Recommended path**: A + B in parallel. The narrowly-scoped Airflow changes for Path A are being discussed upstream (see _Next Steps_); the data-source provider for Path B is prototyped here.

## Next Steps

1. ~~Run the PoC~~ ✅ — `airflow db migrate` succeeds, DAGs execute correctly
2. ~~Audit migrations~~ ✅ — 111 files audited, 12 findings, all handled
3. ~~Submit sqlalchemy-cockroachdb PR~~ ✅ — `@compiles(timestampdiff, "cockroachdb")` open at [PR #301](https://github.com/cockroachdb/sqlalchemy-cockroachdb/pull/301)
4. ~~Open Airflow GitHub Discussion~~ ✅ — [#65453](https://github.com/apache/airflow/discussions/65453); maintainer guidance was to take it to the devlist first
5. ~~Send `[DISCUSS]` to `dev@airflow.apache.org`~~ ✅ — sent 2026-04-18; thread at [lists.apache.org/thread/t6jo4th3sn23jmr34m6gcxzw4k8mo4pc](https://lists.apache.org/thread/t6jo4th3sn23jmr34m6gcxzw4k8mo4pc). Awaiting maintainer feedback before drafting the three PRs.
6. **Submit Airflow PRs after devlist signal** — three narrow PRs framed as PostgreSQL-compatibility improvements
7. **Track CockroachDB advisory-lock support** — would enable scheduler HA coordination without retry-loop overhead

## References

- [CockroachDB PostgreSQL Compatibility](https://www.cockroachlabs.com/docs/stable/postgresql-compatibility)
- [Airflow Database Backend Docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html)
- [sqlalchemy-cockroachdb](https://github.com/cockroachdb/sqlalchemy-cockroachdb)
- [CockroachDB Issue #32537 — Airflow ORM Support](https://github.com/cockroachdb/cockroach/issues/32537)
- [Airflow Issue #14438 — CockroachDB DDL Issues](https://github.com/apache/airflow/issues/14438)
- [Airflow Issue #46175 — New DB Backend Request (Rejected)](https://github.com/apache/airflow/issues/46175)
- [Airflow Issue #40882 — Retry Logic Bugs](https://github.com/apache/airflow/issues/40882)
- [Airflow Custom Provider Guide](https://airflow.apache.org/docs/apache-airflow-providers/howto/create-custom-providers.html)
