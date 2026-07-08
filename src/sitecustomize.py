# Auto-loaded by Python's site module at startup (because /opt/airflow/src
# is on PYTHONPATH). Applies the PGDialect version-parser patch when
# CONN_SCHEME=postgresql, so SQLAlchemy can handle CockroachDB's version()
# output before Airflow's settings.py creates the engine.
#
# Has no effect when CONN_SCHEME=cockroachdb (the default).

try:
    from compatibility.pg_version_compat import install
    install()
except Exception:
    pass  # silently skip if compatibility module isn't available
