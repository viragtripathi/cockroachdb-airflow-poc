# CockroachDB + Apache Airflow PoC

A proof of concept that runs Apache Airflow 3.x with CockroachDB as the metadata
database. It exists to test and demonstrate a small set of upstream changes, not to be
deployed as-is. One `docker compose up` gives you CockroachDB v26.3, Airflow 3.3.0 (or
3.2.1, toggled via `AIRFLOW_VERSION` in `docker/.env`) with two schedulers, and a
validation script that exercises the whole thing, including an HA stress test.

## Where the real fixes live

Everything CockroachDB-specific is being upstreamed. This repo is the test bed.

| Change | Where | Status |
|---|---|---|
| `timestampdiff()` compilation | [sqlalchemy-cockroachdb #301](https://github.com/cockroachdb/sqlalchemy-cockroachdb/pull/301) | Merged, ships in 2.0.4 |
| Async engine URI for `cockroachdb://` | [apache/airflow #69260](https://github.com/apache/airflow/pull/69260) | Open |
| task_instance UUID migration without pgcrypto | [apache/airflow #69555](https://github.com/apache/airflow/pull/69555) | Open |
| Scheduler survives serialization conflicts (40001) | [apache/airflow #69556](https://github.com/apache/airflow/pull/69556) | Open |
| HA advisory lock coordination on CockroachDB | [apache/airflow #69557](https://github.com/apache/airflow/pull/69557) | Open |
| Task-start upsert on cockroachdb (3.3.0 regression for the dialect) | [apache/airflow #69640](https://github.com/apache/airflow/pull/69640) | Open |

Until the Airflow PRs merge and ship in a release, this stack applies them as a small
patch on top of the official Airflow image. The build adapts to the Airflow version:
`airflow-crdb-compat.patch` (base, four files) for 3.2.1 and 3.3.0, plus
`airflow-crdb-upsert-3.3.patch` (one extra sqlalchemy.py hunk) only on 3.3.0. The patches
are literally the diffs of those PRs. When they land, they go away.

## Two ways to connect

The stack supports both, switched by `CONN_SCHEME` in `docker/.env`.

**Route A, the default: `cockroachdb://` plus the patch.** Airflow uses the
sqlalchemy-cockroachdb dialect and the patched code paths. This is a preview of what
stock Airflow will do once the PRs merge. The full validation matrix passes under both
SERIALIZABLE and READ COMMITTED, two schedulers, zero crashes.

**Route B: `postgresql://`, no patch.** Airflow is told it is talking to Postgres.
This needs two shims the init container sets up automatically: stub
`pg_advisory_lock`/`pg_advisory_unlock` functions (Airflow's migration lock calls the
session-level variants, which CockroachDB does not have) and a version-string shim
(SQLAlchemy's Postgres dialect cannot parse CockroachDB's `version()` output). It passes
validation under READ COMMITTED. Under SERIALIZABLE load the schedulers crash on
serialization conflicts, because the fix for that is in the patch this route skips.
Use this route only to see what works without any Airflow changes.

## Requirements

- Docker with about 4 GB free for containers
- CockroachDB v26.3 or later. The HA scheduler coordination uses transaction-scoped
  advisory locks, which first appear in 26.3. The compose file defaults to the
  `cockroachdb/cockroach-unstable` image until 26.3 reaches GA.
- The stack is tested against Airflow 3.3.0 (default) and 3.2.1, toggled via
  `AIRFLOW_VERSION` in `docker/.env`.

## Quick start

```bash
cp docker/.env.example docker/.env
cd docker
docker compose up -d
../scripts/validate-poc.sh
```

Airflow UI at http://localhost:8080 (admin, password in `docker/.env`), CockroachDB
console at http://localhost:8081.

The init container creates the database and a dedicated non-root `airflow` user. Do not
connect Airflow as root: CockroachDB pins root sessions to SERIALIZABLE, so isolation
settings would silently not apply.

### Knobs

All in `docker/.env`:

| Variable | Default | What it does |
|---|---|---|
| `AIRFLOW_VERSION` | `3.3.0` | `3.2.1` or `3.3.0` |
| `COCKROACHDB_IMAGE` / `COCKROACHDB_VERSION` | `cockroachdb/cockroach-unstable` / `v26.3.0-beta.2` | Which CockroachDB to run |
| `CONN_SCHEME` | `cockroachdb` | `cockroachdb` = Route A, `postgresql` = Route B |
| `APPLY_CRDB_PATCH` | `true` | Set `false` to build the Airflow image without the patch (Route B) |
| `CRDB_ISOLATION` | `serializable` | `read_committed` switches the database default for the airflow user |

## What the validation covers

`scripts/validate-poc.sh` checks, among other things: fresh `airflow db migrate` with no
manual setup, the async engine URI being derived rather than hand-configured, a stress
run of 8 concurrent DAG runs (240 tasks) against two schedulers with zero scheduler
restarts, advisory lock usage visible in `pg_locks`, and serialization conflict counts
from the scheduler logs. `scripts/test-migration-0042.py` additionally runs the
task_instance UUID migration up and down against a seeded database.

## Repo layout

```
docker/            compose stack, Airflow image, the compat patch
scripts/           validate-poc.sh, migration harness, migration audit
examples/dags/     demo, health check, and stress DAGs
src/provider/      prototype hook/dialect for using CockroachDB as a data source in DAGs
src/compatibility/ retry middleware and migration audit helpers (pre-date the upstream PRs)
```

## Known limitations

- `airflow db migrate --use-migration-files` stops on an old Airflow migration that adds
  a primary key to a table CockroachDB created with a hidden rowid key. The default
  migration path does not hit this and is the one validated here.
- Route B is a compatibility experiment, not a recommendation. See above.

## References

- [sqlalchemy-cockroachdb](https://github.com/cockroachdb/sqlalchemy-cockroachdb)
- [Airflow devlist thread for these changes](https://lists.apache.org/thread/t6jo4th3sn23jmr34m6gcxzw4k8mo4pc)
- [CockroachDB PostgreSQL compatibility](https://www.cockroachlabs.com/docs/stable/postgresql-compatibility)
