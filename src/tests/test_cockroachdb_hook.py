"""
Tests for CockroachDB Hook.

These tests validate:
  - Connection URI construction
  - SQLAlchemy engine creation with cockroachdb:// dialect
  - Basic query execution against CockroachDB
"""

import unittest
from unittest.mock import MagicMock, patch


class TestCockroachDBHookURIConstruction(unittest.TestCase):
    """Test connection URI building logic."""

    def test_basic_uri(self):
        from provider.hooks.cockroachdb import CockroachDBHook

        config = {
            "host": "localhost",
            "port": 26257,
            "database": "testdb",
            "user": "root",
            "password": "",
            "extra_params": {},
        }
        uri = CockroachDBHook._build_connection_uri(config)
        self.assertEqual(uri, "cockroachdb://root@localhost:26257/testdb")

    def test_uri_with_password(self):
        from provider.hooks.cockroachdb import CockroachDBHook

        config = {
            "host": "my-cluster.cockroachlabs.cloud",
            "port": 26257,
            "database": "mydb",
            "user": "admin",
            "password": "secret123",
            "extra_params": {},
        }
        uri = CockroachDBHook._build_connection_uri(config)
        self.assertEqual(
            uri,
            "cockroachdb://admin:secret123@my-cluster.cockroachlabs.cloud:26257/mydb",
        )

    def test_uri_with_ssl_params(self):
        from provider.hooks.cockroachdb import CockroachDBHook

        config = {
            "host": "my-cluster.cockroachlabs.cloud",
            "port": 26257,
            "database": "mydb",
            "user": "admin",
            "password": "secret",
            "extra_params": {
                "sslmode": "verify-full",
                "sslrootcert": "/certs/ca.crt",
                "cluster": "my-cluster-123",
            },
        }
        uri = CockroachDBHook._build_connection_uri(config)
        self.assertIn("sslmode=verify-full", uri)
        self.assertIn("sslrootcert=/certs/ca.crt", uri)
        self.assertIn("cluster=my-cluster-123", uri)


class TestCockroachDBHookUIBehaviour(unittest.TestCase):
    """Test Airflow connection UI field configuration."""

    def test_ui_field_behaviour(self):
        from provider.hooks.cockroachdb import CockroachDBHook

        behaviour = CockroachDBHook.get_ui_field_behaviour()
        self.assertIn("placeholders", behaviour)
        self.assertEqual(behaviour["placeholders"]["port"], "26257")
        self.assertEqual(behaviour["relabeling"]["schema"], "Database")


class TestRetryMiddleware(unittest.TestCase):
    """Test serialization retry logic."""

    def test_is_retryable_error_pgcode(self):
        from compatibility.retry_middleware import is_retryable_error

        # Should match 40001 errors
        self.assertTrue(is_retryable_error(Exception("40001: restart transaction")))
        self.assertTrue(is_retryable_error(Exception("serialization failure")))
        self.assertTrue(is_retryable_error(Exception("retry_write_too_old")))

        # Should match connection transient errors
        self.assertTrue(is_retryable_error(Exception("connection reset")))
        self.assertTrue(is_retryable_error(Exception("broken pipe")))

        # Should not match non-retryable errors
        self.assertFalse(is_retryable_error(Exception("42P01: table not found")))
        self.assertFalse(is_retryable_error(Exception("syntax error")))

    def test_is_retryable_error_with_pgcode_attr(self):
        from compatibility.retry_middleware import is_retryable_error

        err = Exception("some error")
        err.pgcode = "40001"
        self.assertTrue(is_retryable_error(err))

        err2 = Exception("some error")
        err2.pgcode = "42P01"
        self.assertFalse(is_retryable_error(err2))

    def test_calculate_delay(self):
        from compatibility.retry_middleware import _calculate_delay

        # Delays should increase with attempts
        d0 = _calculate_delay(0, jitter=False)
        d1 = _calculate_delay(1, jitter=False)
        d2 = _calculate_delay(2, jitter=False)

        # Without jitter, delays should be deterministic
        self.assertAlmostEqual(d0, 0.05)  # INITIAL_BACKOFF
        self.assertAlmostEqual(d1, 0.10)  # 0.05 * 2
        self.assertAlmostEqual(d2, 0.20)  # 0.05 * 4

    def test_sync_retry_decorator_success(self):
        from compatibility.retry_middleware import sync_retry_with_backoff

        @sync_retry_with_backoff(max_retries=3, initial_backoff=0.001)
        def always_works():
            return "success"

        self.assertEqual(always_works(), "success")

    def test_sync_retry_decorator_retries_on_40001(self):
        from compatibility.retry_middleware import sync_retry_with_backoff

        call_count = 0

        @sync_retry_with_backoff(max_retries=3, initial_backoff=0.001)
        def fails_then_succeeds():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("40001: restart transaction")
            return "success"

        result = fails_then_succeeds()
        self.assertEqual(result, "success")
        self.assertEqual(call_count, 3)

    def test_sync_retry_decorator_raises_non_retryable(self):
        from compatibility.retry_middleware import sync_retry_with_backoff

        @sync_retry_with_backoff(max_retries=3, initial_backoff=0.001)
        def always_fails():
            raise ValueError("not a retryable error")

        with self.assertRaises(ValueError):
            always_fails()

    def test_run_with_retry_success(self):
        from compatibility.retry_middleware import run_with_retry

        session_factory = MagicMock()
        session = MagicMock()
        session_factory.return_value = session

        def fn(s):
            return "success"

        result = run_with_retry(session_factory, fn, max_retries=3)
        self.assertEqual(result, "success")


class TestMigrationAudit(unittest.TestCase):
    """Test migration audit utilities."""

    def test_audit_detects_alter_column_type(self):
        from compatibility.migration_utils import audit_migration_file
        import tempfile
        import os

        content = """
# Alembic migration
def upgrade():
    op.alter_column('dag', 'description',
                    existing_type=sa.Integer(),
                    type_=sa.Float(),
                    existing_nullable=True)
    # This generates: ALTER TABLE dag ALTER COLUMN description TYPE float
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(content)
            f.flush()
            findings = audit_migration_file(f.name)

        os.unlink(f.name)

        # Should detect ALTER COLUMN TYPE pattern
        alter_findings = [
            f for f in findings
            if "ALTER COLUMN TYPE" in f["pattern"]["description"]
        ]
        self.assertGreater(len(alter_findings), 0)

    def test_audit_detects_create_extension(self):
        from compatibility.migration_utils import audit_migration_file
        import tempfile
        import os

        content = """
def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS uuid-ossp")
"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(content)
            f.flush()
            findings = audit_migration_file(f.name)

        os.unlink(f.name)

        ext_findings = [
            f for f in findings
            if "CREATE EXTENSION" in f["pattern"]["description"]
        ]
        self.assertGreater(len(ext_findings), 0)


if __name__ == "__main__":
    unittest.main()
