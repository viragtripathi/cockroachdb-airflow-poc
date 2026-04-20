"""
Airflow plugin that registers CockroachDB SQLAlchemy compatibility patches.

This plugin is automatically discovered by Airflow when placed in the plugins/ directory.
It registers compiler extensions that translate MySQL-style SQL functions
(e.g., timestampdiff) to CockroachDB/PostgreSQL equivalents.
"""

from airflow.plugins_manager import AirflowPlugin

# Import triggers the compiler registration
import compatibility.cockroachdb_sqlalchemy_plugin  # noqa: F401


class CockroachDBCompatPlugin(AirflowPlugin):
    name = "cockroachdb_compat"
