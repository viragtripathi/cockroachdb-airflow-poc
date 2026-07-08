# Airflow plugin: load CockroachDB compatibility patches for postgresql:// route.
#
# This plugin is loaded by Airflow's plugin manager before any DB connection is
# made. When CONN_SCHEME=postgresql it patches SQLAlchemy's PGDialect version
# parser so CockroachDB's version() output doesn't crash initialization.
#
# Has no effect when CONN_SCHEME=cockroachdb (the default).

from airflow.plugins_manager import AirflowPlugin
from compatibility.pg_version_compat import install as install_pg_version_compat

install_pg_version_compat()


class CrdbPgCompatPlugin(AirflowPlugin):
    name = "crdb_pg_compat"
