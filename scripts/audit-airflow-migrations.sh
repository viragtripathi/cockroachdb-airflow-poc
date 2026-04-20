#!/usr/bin/env bash
###############################################################################
# Audit Airflow Alembic Migrations for CockroachDB Compatibility
#
# Downloads the Airflow source and runs the migration audit tool to identify
# CockroachDB-incompatible DDL patterns.
#
# Usage:
#   ./scripts/audit-airflow-migrations.sh [airflow_version]
#
# Example:
#   ./scripts/audit-airflow-migrations.sh 3.2.0
###############################################################################

set -euo pipefail

AIRFLOW_VERSION="${1:-3.2.0}"
WORK_DIR=$(mktemp -d)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Airflow Migration Audit for CockroachDB Compatibility ==="
echo "Airflow version: $AIRFLOW_VERSION"
echo "Working directory: $WORK_DIR"
echo ""

# Download Airflow source
# Airflow 3.x split the codebase: apache-airflow is a metapackage,
# actual code (including migrations) is in apache-airflow-core.
echo "Downloading Airflow $AIRFLOW_VERSION source..."
pip install --no-deps --target="$WORK_DIR/airflow_src" "apache-airflow-core==$AIRFLOW_VERSION" 2>/dev/null || {
    echo "Falling back to git clone..."
    git clone --depth 1 --branch "$AIRFLOW_VERSION" \
        https://github.com/apache/airflow.git "$WORK_DIR/airflow_src" 2>/dev/null || {
        echo "ERROR: Could not download Airflow $AIRFLOW_VERSION"
        exit 1
    }
}

# Find migrations directory
MIGRATIONS_DIR=""
for candidate in \
    "$WORK_DIR/airflow_src/airflow/migrations/versions" \
    "$WORK_DIR/airflow_src/lib/python*/site-packages/airflow/migrations/versions"; do
    if [ -d "$candidate" ] || ls $candidate 2>/dev/null; then
        MIGRATIONS_DIR=$(ls -d $candidate 2>/dev/null | head -1)
        break
    fi
done

if [ -z "$MIGRATIONS_DIR" ]; then
    echo "ERROR: Could not find Airflow migrations directory"
    echo "Searched in: $WORK_DIR/airflow_src/"
    find "$WORK_DIR/airflow_src" -name "versions" -type d 2>/dev/null || true
    exit 1
fi

echo "Found migrations at: $MIGRATIONS_DIR"
echo "Migration files: $(ls "$MIGRATIONS_DIR"/*.py 2>/dev/null | wc -l | tr -d ' ')"
echo ""

# Run the Python audit tool
echo "Running CockroachDB compatibility audit..."
echo ""
python3 "$PROJECT_DIR/src/compatibility/migration_utils.py" audit "$MIGRATIONS_DIR"

# Save results
REPORT_DIR="$PROJECT_DIR/audit-reports"
mkdir -p "$REPORT_DIR"
REPORT_FILE="$REPORT_DIR/migration-audit-airflow-${AIRFLOW_VERSION}.txt"
python3 "$PROJECT_DIR/src/compatibility/migration_utils.py" audit "$MIGRATIONS_DIR" > "$REPORT_FILE" 2>&1
echo ""
echo "Report saved to: $REPORT_FILE"

# Cleanup
rm -rf "$WORK_DIR"
echo "Done."
