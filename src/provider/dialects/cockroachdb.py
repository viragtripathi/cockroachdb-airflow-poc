"""
CockroachDB Dialect for Apache Airflow.

Extends the common-sql Dialect base class to provide CockroachDB-specific
SQL generation for upserts, primary key lookups, and column introspection.

This follows the pattern established by the PostgreSQL provider's dialect.
"""

import logging
from typing import Sequence

logger = logging.getLogger(__name__)

try:
    from airflow.providers.common.sql.dialects.dialect import Dialect
except ImportError:
    # Stub for development without Airflow
    class Dialect:
        """Stub base class for development."""
        def __init__(self, hook=None, **kwargs):
            self.hook = hook


class CockroachDBDialect(Dialect):
    """
    CockroachDB dialect for Airflow's common-sql framework.

    Provides CockroachDB-optimized implementations of:
      - Primary key discovery
      - Column name retrieval
      - Upsert (INSERT ... ON CONFLICT) generation
    """

    def get_primary_keys(self, table: str, schema: str | None = None) -> list[str]:
        """
        Get primary key column names for a table.

        Uses CockroachDB's information_schema, which is compatible with PostgreSQL.
        Filters out CockroachDB's hidden `rowid` column that is auto-generated
        when no explicit primary key is defined.
        """
        schema_clause = f"AND tc.table_schema = '{schema}'" if schema else ""

        query = f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_name = '{table}'
              {schema_clause}
              AND kcu.column_name != 'rowid'
            ORDER BY kcu.ordinal_position
        """
        records = self.hook.get_records(query)
        return [row[0] for row in records] if records else []

    def get_column_names(
        self,
        table: str,
        schema: str | None = None,
        predicate: str = "",
    ) -> list[str]:
        """
        Get column names for a table.

        Filters out CockroachDB's hidden columns (like auto-generated rowid).
        """
        schema_clause = f"AND table_schema = '{schema}'" if schema else ""
        predicate_clause = f"AND {predicate}" if predicate else ""

        query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table}'
              {schema_clause}
              {predicate_clause}
              AND is_hidden = 'NO'
            ORDER BY ordinal_position
        """
        records = self.hook.get_records(query)
        return [row[0] for row in records] if records else []

    def generate_replace_sql(
        self,
        table: str,
        values: Sequence,
        target_fields: Sequence[str],
        replace: bool = True,
        replace_index: Sequence[str] | None = None,
    ) -> str:
        """
        Generate an UPSERT statement using CockroachDB's
        INSERT ... ON CONFLICT ... DO UPDATE SET syntax.

        This is compatible with PostgreSQL's upsert syntax, which
        CockroachDB fully supports.

        Args:
            table: Target table name
            values: Values to insert (used for placeholder count)
            target_fields: Column names for the insert
            replace: If True, generate upsert. If False, generate plain INSERT.
            replace_index: Columns for the ON CONFLICT clause (conflict target)

        Returns:
            SQL string with placeholders
        """
        placeholders = ", ".join(["%s"] * len(target_fields))
        columns = ", ".join(target_fields)

        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        if replace and replace_index:
            conflict_columns = ", ".join(replace_index)
            update_columns = [
                f"{col} = EXCLUDED.{col}"
                for col in target_fields
                if col not in replace_index
            ]

            if update_columns:
                update_clause = ", ".join(update_columns)
                sql += f" ON CONFLICT ({conflict_columns}) DO UPDATE SET {update_clause}"
            else:
                sql += f" ON CONFLICT ({conflict_columns}) DO NOTHING"

        return sql
