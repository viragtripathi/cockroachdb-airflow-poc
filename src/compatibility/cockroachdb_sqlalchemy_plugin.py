"""
CockroachDB SQLAlchemy compatibility patches for Apache Airflow.

Airflow's ORM has dialect-specific code paths for PostgreSQL, MySQL, and SQLite.
CockroachDB uses the 'cockroachdb' dialect name, so it falls through to the
MySQL/generic code path which generates incompatible SQL (e.g., timestampdiff).

This module registers SQLAlchemy compiler extensions that translate MySQL-style
functions to their CockroachDB/PostgreSQL equivalents:

  - timestampdiff(MICROSECOND, start, end) → EXTRACT(EPOCH FROM (end - start)) * 1000000
  - timestampdiff(SECOND, start, end)      → EXTRACT(EPOCH FROM (end - start))

Usage:
    Import this module early in the Airflow startup to register the compiler hooks.
    It's loaded automatically via the Airflow plugin in plugins/cockroachdb_compat_plugin.py.
"""

import logging

from sqlalchemy import extract
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import GenericFunction

logger = logging.getLogger(__name__)


class timestampdiff(GenericFunction):
    """Custom timestampdiff function that can be compiled per-dialect."""

    type = None
    name = "timestampdiff"
    inherit_cache = True


@compiles(timestampdiff, "cockroachdb")
def _compile_timestampdiff_cockroachdb(element, compiler, **kwargs):
    """
    Compile timestampdiff for CockroachDB using EXTRACT(EPOCH FROM ...).

    Handles: timestampdiff(MICROSECOND, start, end) and timestampdiff(SECOND, start, end)
    """
    args = list(element.clauses)
    if len(args) != 3:
        raise ValueError(f"timestampdiff expects 3 arguments, got {len(args)}")

    # First arg is the interval unit (rendered as a text literal like MICROSECOND)
    unit = compiler.process(args[0], **kwargs).strip().upper()
    start_expr = compiler.process(args[1], **kwargs)
    end_expr = compiler.process(args[2], **kwargs)

    # Generate PostgreSQL-compatible EXTRACT expression.
    # CAST to NUMERIC to avoid CockroachDB "unsupported binary operator: <float> / <decimal>"
    # when Airflow divides the result by CAST(1000000 AS NUMERIC).
    epoch_diff = f"CAST(EXTRACT(EPOCH FROM ({end_expr} - {start_expr})) AS NUMERIC)"

    if "MICROSECOND" in unit:
        return f"({epoch_diff} * 1000000)"
    elif "MILLISECOND" in unit:
        return f"({epoch_diff} * 1000)"
    elif "SECOND" in unit:
        return epoch_diff
    elif "MINUTE" in unit:
        return f"({epoch_diff} / 60)"
    elif "HOUR" in unit:
        return f"({epoch_diff} / 3600)"
    elif "DAY" in unit:
        return f"({epoch_diff} / 86400)"
    else:
        # Default to seconds
        return epoch_diff


def register():
    """Register the CockroachDB compiler extensions. Called on import."""
    logger.info("CockroachDB SQLAlchemy compatibility patches registered (timestampdiff)")


# Auto-register on import
register()
