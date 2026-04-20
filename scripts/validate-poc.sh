#!/usr/bin/env bash
###############################################################################
# CockroachDB + Airflow PoC Validation Script
#
# Runs a series of checks to validate the PoC is working correctly:
#   1. CockroachDB connectivity and configuration
#   2. Airflow metadata tables created in CockroachDB
#   3. Airflow API server health
#   4. DAG discovery
#
# Usage:
#   ./scripts/validate-poc.sh
#
# Prerequisites:
#   - docker compose up -d (from docker/ directory)
#   - Wait for airflow-init to complete
###############################################################################

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0

check() {
    local description="$1"
    local command="$2"
    local expected="${3:-}"

    printf "%-50s " "$description"

    output=$(eval "$command" 2>&1) || true

    if [ -n "$expected" ]; then
        if echo "$output" | grep -qi "$expected"; then
            echo -e "${GREEN}PASS${NC}"
            ((PASS++))
        else
            echo -e "${RED}FAIL${NC}"
            echo "  Expected: $expected"
            echo "  Got: $output"
            ((FAIL++))
        fi
    else
        if [ $? -eq 0 ] && [ -n "$output" ]; then
            echo -e "${GREEN}PASS${NC} ($output)"
            ((PASS++))
        else
            echo -e "${RED}FAIL${NC}"
            echo "  Output: $output"
            ((FAIL++))
        fi
    fi
}

echo "============================================================"
echo "  CockroachDB + Airflow PoC Validation"
echo "============================================================"
echo ""

# --- CockroachDB Checks ---
echo "--- CockroachDB ---"

check "CockroachDB is reachable" \
    "docker compose -f docker/docker-compose.yml exec -T cockroachdb cockroach sql --insecure -e 'SELECT 1'" \
    "1"

check "Airflow database exists" \
    "docker compose -f docker/docker-compose.yml exec -T cockroachdb cockroach sql --insecure -e \"SELECT datname FROM pg_database WHERE datname='airflow'\"" \
    "airflow"

check "Serial normalization is sql_sequence" \
    "docker compose -f docker/docker-compose.yml exec -T cockroachdb cockroach sql --insecure -e \"SHOW CLUSTER SETTING sql.defaults.serial_normalization\"" \
    "sql_sequence"

check "READ COMMITTED enabled" \
    "docker compose -f docker/docker-compose.yml exec -T cockroachdb cockroach sql --insecure -e \"SHOW CLUSTER SETTING sql.txn.read_committed_isolation.enabled\"" \
    "true"

check "CockroachDB version" \
    "docker compose -f docker/docker-compose.yml exec -T cockroachdb cockroach sql --insecure -e \"SELECT value FROM crdb_internal.node_build_info WHERE field = 'Tag'\"" \
    ""

echo ""

# --- Airflow Metadata Tables ---
echo "--- Airflow Metadata (in CockroachDB) ---"

check "Airflow tables exist in CockroachDB" \
    "docker compose -f docker/docker-compose.yml exec -T cockroachdb cockroach sql --insecure --database=airflow -e \"SELECT count(*) FROM information_schema.tables WHERE table_schema='public'\"" \
    ""

check "dag table exists" \
    "docker compose -f docker/docker-compose.yml exec -T cockroachdb cockroach sql --insecure --database=airflow -e \"SELECT count(*) FROM information_schema.tables WHERE table_name='dag'\"" \
    "1"

check "task_instance table exists" \
    "docker compose -f docker/docker-compose.yml exec -T cockroachdb cockroach sql --insecure --database=airflow -e \"SELECT count(*) FROM information_schema.tables WHERE table_name='task_instance'\"" \
    "1"

check "alembic_version populated" \
    "docker compose -f docker/docker-compose.yml exec -T cockroachdb cockroach sql --insecure --database=airflow -e \"SELECT version_num FROM alembic_version LIMIT 1\"" \
    ""

echo ""

# --- Airflow Health ---
echo "--- Airflow Services ---"

check "Airflow API server healthy" \
    "curl -sf http://localhost:8080/api/v2/version 2>/dev/null | head -c 200" \
    "version"

check "Airflow scheduler running" \
    "docker compose -f docker/docker-compose.yml ps --format '{{.Service}} {{.Status}}' | grep scheduler" \
    "running"

echo ""

# --- DAG Discovery ---
echo "--- DAG Discovery ---"

check "cockroachdb_demo DAG loaded" \
    "docker compose -f docker/docker-compose.yml exec -T airflow-api-server airflow dags list 2>/dev/null | grep cockroachdb_demo" \
    "cockroachdb_demo"

check "cockroachdb_health_check DAG loaded" \
    "docker compose -f docker/docker-compose.yml exec -T airflow-api-server airflow dags list 2>/dev/null | grep cockroachdb_health_check" \
    "cockroachdb_health_check"

echo ""

# --- Summary ---
echo "============================================================"
echo "  Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}"
echo "============================================================"

if [ $FAIL -gt 0 ]; then
    echo -e "\n${YELLOW}Some checks failed. Review the output above for details.${NC}"
    echo "Common issues:"
    echo "  - Containers not fully started: docker compose logs -f"
    echo "  - DB migration failed: docker compose logs airflow-init"
    exit 1
else
    echo -e "\n${GREEN}All checks passed! The PoC is working.${NC}"
fi
