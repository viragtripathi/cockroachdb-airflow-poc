# Patch SQLAlchemy's PostgreSQL dialect to handle CockroachDB's version() output.
#
# When connecting via postgresql://, SQLAlchemy calls pg_catalog.version() and
# parses the result with a regex that expects "PostgreSQL X.Y.Z". CockroachDB
# returns "CockroachDB CCL vNN.M..." which fails that regex. pg_catalog is a
# virtual schema in CockroachDB, so we can't override the function there.
#
# This module monkey-patches PGDialect._get_server_version_info to fall back to
# parsing the server_version GUC (SHOW server_version) when the regex fails.
# Only active when CONN_SCHEME=postgresql.

import logging
import os
import re

log = logging.getLogger(__name__)

_ORIGINAL_GET_SERVER_VERSION = None


def _patched_get_server_version_info(self, connection):
    """Parse server version, falling back to SHOW server_version for CockroachDB."""
    try:
        return _ORIGINAL_GET_SERVER_VERSION(self, connection)
    except (AssertionError, Exception):
        # The pg_catalog.version() string was unparseable (CockroachDB).
        # Fall back to SHOW server_version which CockroachDB sets to a
        # Postgres-compatible value like "18.0.0".
        v = connection.exec_driver_sql("SHOW server_version").scalar()
        m = re.match(r"(\d+)\.(\d+)(?:\.(\d+))?", v)
        if m:
            info = tuple(int(x) for x in m.groups() if x is not None)
            log.info("CockroachDB server_version parsed as %s", info)
            return info
        raise AssertionError(
            "Could not determine version from server_version '%s'" % v
        )


def install():
    """Monkey-patch PGDialect if CONN_SCHEME=postgresql."""
    if os.environ.get("CONN_SCHEME", "cockroachdb") != "postgresql":
        return

    global _ORIGINAL_GET_SERVER_VERSION

    from sqlalchemy.dialects.postgresql.base import PGDialect

    if _ORIGINAL_GET_SERVER_VERSION is not None:
        return  # already patched

    _ORIGINAL_GET_SERVER_VERSION = PGDialect._get_server_version_info
    PGDialect._get_server_version_info = _patched_get_server_version_info
    log.info("Patched PGDialect._get_server_version_info for CockroachDB")
