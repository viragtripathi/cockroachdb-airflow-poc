"""
Utilities for auditing and patching Airflow Alembic migrations for
CockroachDB compatibility.

This module provides tools to:
1. Audit existing Airflow migrations for CockroachDB-incompatible operations
2. Generate compatibility patches where needed

Usage:
    python -m compatibility.migration_utils audit
    python -m compatibility.migration_utils check-version
"""

import re
import sys
import logging

logger = logging.getLogger(__name__)

# Known problematic DDL patterns in Airflow migrations
INCOMPATIBLE_PATTERNS = [
    {
        "pattern": r"ALTER\s+(?:TABLE\s+\w+\s+)?ALTER\s+COLUMN\s+\w+\s+(?:SET\s+DATA\s+)?TYPE",
        "description": "ALTER COLUMN TYPE — may require CockroachDB 23.1+ or workaround",
        "severity": "HIGH",
        "workaround": "CockroachDB 23.1+ supports most ALTER COLUMN TYPE operations. "
                      "For unsupported conversions, use a migration shim that creates a "
                      "new column, copies data, drops old column, and renames.",
    },
    {
        "pattern": r"CREATE\s+SEQUENCE",
        "description": "CREATE SEQUENCE — supported in CockroachDB but behavior differs",
        "severity": "MEDIUM",
        "workaround": "Set cluster setting sql.defaults.serial_normalization = 'sql_sequence' "
                      "to match PostgreSQL behavior. CockroachDB supports CREATE SEQUENCE natively.",
    },
    {
        "pattern": r"uuid_generate_v7",
        "description": "uuid_generate_v7() — not available in CockroachDB",
        "severity": "HIGH",
        "workaround": "Create a compatibility UDF: CREATE FUNCTION uuid_generate_v7() "
                      "RETURNS UUID LANGUAGE SQL AS $$ SELECT gen_random_uuid() $$;",
    },
    {
        "pattern": r"SAVEPOINT",
        "description": "SAVEPOINT — limited support in CockroachDB",
        "severity": "LOW",
        "workaround": "CockroachDB supports savepoints for retry handling. "
                      "sqlalchemy-cockroachdb handles this automatically.",
    },
    {
        "pattern": r"DEFERRABLE|INITIALLY\s+DEFERRED",
        "description": "Deferred constraints — not supported in CockroachDB",
        "severity": "MEDIUM",
        "workaround": "Remove DEFERRABLE clause. CockroachDB checks constraints immediately. "
                      "Restructure operations to avoid needing deferred constraints.",
    },
    {
        "pattern": r"LOCK\s+TABLE",
        "description": "LOCK TABLE — not supported in CockroachDB",
        "severity": "MEDIUM",
        "workaround": "CockroachDB uses optimistic concurrency control. "
                      "Replace explicit locks with SELECT ... FOR UPDATE or retry logic.",
    },
    {
        "pattern": r"NOTIFY|LISTEN",
        "description": "LISTEN/NOTIFY — not supported in CockroachDB",
        "severity": "LOW",
        "workaround": "Use CockroachDB changefeeds or polling as alternatives.",
    },
    {
        "pattern": r"CREATE\s+EXTENSION",
        "description": "CREATE EXTENSION — extensions not supported in CockroachDB",
        "severity": "HIGH",
        "workaround": "Check if the extension functionality is built into CockroachDB. "
                      "Many pg extensions (e.g., uuid-ossp) have native equivalents.",
    },
]


def audit_migration_file(filepath):
    """
    Audit a single Alembic migration file for CockroachDB-incompatible patterns.

    Returns a list of findings, each a dict with:
        - line_number: int
        - line: str
        - pattern: dict (from INCOMPATIBLE_PATTERNS)
    """
    findings = []
    try:
        with open(filepath, "r") as f:
            lines = f.readlines()
    except (OSError, IOError) as e:
        logger.warning("Could not read %s: %s", filepath, e)
        return findings

    for i, line in enumerate(lines, 1):
        for pattern_info in INCOMPATIBLE_PATTERNS:
            if re.search(pattern_info["pattern"], line, re.IGNORECASE):
                findings.append({
                    "line_number": i,
                    "line": line.strip(),
                    "pattern": pattern_info,
                })
    return findings


def audit_migrations_directory(migrations_dir):
    """
    Audit all migration files in a directory.

    Returns a dict mapping filepath -> list of findings.
    """
    import glob
    import os

    results = {}
    pattern = os.path.join(migrations_dir, "**", "*.py")
    for filepath in sorted(glob.glob(pattern, recursive=True)):
        findings = audit_migration_file(filepath)
        if findings:
            results[filepath] = findings
    return results


def print_audit_report(results):
    """Print a formatted audit report."""
    if not results:
        print("No CockroachDB-incompatible patterns found.")
        return

    total_findings = sum(len(f) for f in results.values())
    severity_counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}

    print(f"\n{'='*72}")
    print("CockroachDB Compatibility Audit Report")
    print(f"{'='*72}\n")

    for filepath, findings in results.items():
        print(f"\n--- {filepath} ---")
        for finding in findings:
            severity = finding["pattern"]["severity"]
            severity_counts[severity] += 1
            print(f"  Line {finding['line_number']} [{severity}]: {finding['pattern']['description']}")
            print(f"    Code: {finding['line']}")
            print(f"    Fix:  {finding['pattern']['workaround']}")
            print()

    print(f"\n{'='*72}")
    print(f"Total findings: {total_findings}")
    print(f"  HIGH:   {severity_counts['HIGH']}")
    print(f"  MEDIUM: {severity_counts['MEDIUM']}")
    print(f"  LOW:    {severity_counts['LOW']}")
    print(f"{'='*72}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m compatibility.migration_utils audit <migrations_dir>")
        print("       python -m compatibility.migration_utils check-version")
        sys.exit(1)

    command = sys.argv[1]

    if command == "audit":
        if len(sys.argv) < 3:
            print("Error: migrations directory required")
            print("Usage: python -m compatibility.migration_utils audit <migrations_dir>")
            sys.exit(1)
        results = audit_migrations_directory(sys.argv[2])
        print_audit_report(results)
    elif command == "check-version":
        print("Checking CockroachDB version compatibility...")
        print("This command connects to CockroachDB and checks feature support.")
        print("(Not yet implemented — requires active DB connection)")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
