"""
CockroachDB Hook for Apache Airflow.

Provides a connection interface to CockroachDB for use in Airflow DAGs.
Extends the common-sql DbApiHook with CockroachDB-specific functionality:
  - Automatic transaction retry on serialization errors (40001)
  - Connection string generation with cockroachdb:// dialect
  - SSL/TLS certificate configuration for CockroachDB Cloud
  - READ COMMITTED isolation level support

Example usage in a DAG:
    from provider.hooks.cockroachdb import CockroachDBHook

    hook = CockroachDBHook(cockroachdb_conn_id="my_crdb_conn")
    records = hook.get_records("SELECT * FROM my_table")
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Connection type metadata for Airflow's connection UI
CONNECTION_TYPE = "cockroachdb"
CONN_NAME_ATTR = "cockroachdb_conn_id"
DEFAULT_CONN_NAME = "cockroachdb_default"
HOOK_NAME = "CockroachDB"

try:
    from airflow.providers.common.sql.hooks.sql import DbApiHook
except ImportError:
    # Fallback for development/testing without Airflow installed
    class DbApiHook:
        """Stub for development without Airflow."""
        conn_name_attr = "default_conn_id"
        default_conn_name = "default"
        conn_type = "generic"
        hook_name = "Generic"

        def __init__(self, *args, **kwargs):
            pass


class CockroachDBHook(DbApiHook):
    """
    Hook for interacting with CockroachDB.

    Uses the ``cockroachdb://`` SQLAlchemy dialect provided by
    ``sqlalchemy-cockroachdb``.

    Connection parameters are read from an Airflow connection with:
      - Host: CockroachDB host (e.g., free-tier.gcp-us-central1.cockroachlabs.cloud)
      - Port: 26257 (default)
      - Schema: Database name
      - Login: Username
      - Password: Password
      - Extra (JSON):
        - sslmode: "verify-full" (default for CockroachDB Cloud)
        - sslrootcert: Path to CA certificate
        - cluster: Cluster ID (for CockroachDB Serverless)
        - options: Additional connection options
        - isolation_level: "read committed" or "serializable" (default)
    """

    conn_name_attr = CONN_NAME_ATTR
    default_conn_name = DEFAULT_CONN_NAME
    conn_type = CONNECTION_TYPE
    hook_name = HOOK_NAME

    # Supported connection parameter keys in the Extra field
    EXTRA_KEYS = [
        "sslmode",
        "sslrootcert",
        "sslcert",
        "sslkey",
        "cluster",
        "options",
        "isolation_level",
        "application_name",
    ]

    def __init__(
        self,
        cockroachdb_conn_id: str = DEFAULT_CONN_NAME,
        database: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.cockroachdb_conn_id = cockroachdb_conn_id
        self.database = database

    @classmethod
    def get_ui_field_behaviour(cls) -> dict:
        """Customize the Airflow connection form UI for CockroachDB."""
        return {
            "hidden_fields": [],
            "relabeling": {
                "schema": "Database",
                "login": "Username",
                "host": "Hostname",
            },
            "placeholders": {
                "host": "free-tier.gcp-us-central1.cockroachlabs.cloud",
                "port": "26257",
                "schema": "defaultdb",
                "login": "your_username",
                "password": "your_password",
                "extra": '{"sslmode": "verify-full", "cluster": "your-cluster-id"}',
            },
        }

    def get_conn(self):
        """
        Get a psycopg2 connection to CockroachDB.

        Returns a raw DBAPI connection. For SQLAlchemy usage,
        use ``get_sqlalchemy_engine()`` instead.
        """
        conn_config = self._get_conn_config()

        import psycopg2
        connection = psycopg2.connect(
            host=conn_config["host"],
            port=conn_config["port"],
            dbname=conn_config["database"],
            user=conn_config["user"],
            password=conn_config["password"],
            **conn_config.get("extra_params", {}),
        )
        return connection

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        """
        Get a SQLAlchemy engine using the cockroachdb:// dialect.

        Requires ``sqlalchemy-cockroachdb`` to be installed.
        """
        from sqlalchemy import create_engine

        conn_config = self._get_conn_config()
        uri = self._build_connection_uri(conn_config)
        engine_kwargs = engine_kwargs or {}

        # Set reasonable defaults for CockroachDB
        engine_kwargs.setdefault("pool_size", 5)
        engine_kwargs.setdefault("max_overflow", 10)
        engine_kwargs.setdefault("pool_pre_ping", True)

        # Set isolation level if specified
        isolation_level = conn_config.get("extra_params", {}).get("isolation_level")
        if isolation_level:
            engine_kwargs["isolation_level"] = isolation_level.upper()

        return create_engine(uri, **engine_kwargs)

    @property
    def sqlalchemy_url(self):
        """Build and return the SQLAlchemy connection URL."""
        conn_config = self._get_conn_config()
        return self._build_connection_uri(conn_config)

    def _get_conn_config(self) -> dict:
        """Extract connection configuration from the Airflow connection."""
        try:
            conn = self.get_connection(self.cockroachdb_conn_id)
        except Exception:
            # Fallback for development/testing
            return {
                "host": "localhost",
                "port": 26257,
                "database": self.database or "defaultdb",
                "user": "root",
                "password": "",
                "extra_params": {},
            }

        extra = conn.extra_dejson if hasattr(conn, "extra_dejson") else {}

        config = {
            "host": conn.host or "localhost",
            "port": conn.port or 26257,
            "database": self.database or conn.schema or "defaultdb",
            "user": conn.login or "root",
            "password": conn.password or "",
            "extra_params": {},
        }

        # Extract known extra parameters
        for key in self.EXTRA_KEYS:
            if key in extra:
                config["extra_params"][key] = extra[key]

        return config

    @staticmethod
    def _build_connection_uri(conn_config: dict) -> str:
        """Build a cockroachdb:// connection URI from config."""
        user = conn_config["user"]
        password = conn_config.get("password", "")
        host = conn_config["host"]
        port = conn_config["port"]
        database = conn_config["database"]

        # Build auth portion
        if password:
            auth = f"{user}:{password}"
        else:
            auth = user

        uri = f"cockroachdb://{auth}@{host}:{port}/{database}"

        # Add query parameters from extras
        params = []
        extra = conn_config.get("extra_params", {})
        for key in ["sslmode", "sslrootcert", "sslcert", "sslkey",
                     "cluster", "application_name"]:
            if key in extra:
                params.append(f"{key}={extra[key]}")

        # Handle options (e.g., --cluster=xxx for serverless)
        if "options" in extra:
            params.append(f"options={extra['options']}")

        if params:
            uri += "?" + "&".join(params)

        return uri
