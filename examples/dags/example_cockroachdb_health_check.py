"""
Example DAG: CockroachDB Cluster Health Check

A monitoring DAG that checks CockroachDB cluster health metrics.
Demonstrates Airflow as an operational tool for CockroachDB management.

This DAG:
  1. Checks cluster node status
  2. Validates database connectivity and latency
  3. Reports table sizes and range distribution

Runs on a schedule (every 30 minutes) for continuous monitoring.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "cockroachdb-poc",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def check_cluster_version(**context):
    """Check CockroachDB cluster version and basic connectivity."""
    from airflow.hooks.base import BaseHook
    import psycopg2
    import time

    conn = BaseHook.get_connection("cockroachdb_default")
    start = time.time()
    db_conn = psycopg2.connect(
        host=conn.host,
        port=conn.port or 26257,
        dbname=conn.schema or "defaultdb",
        user=conn.login,
        password=conn.password,
    )
    latency_ms = (time.time() - start) * 1000

    cursor = db_conn.cursor()

    cursor.execute("SELECT version()")
    version = cursor.fetchone()[0]

    cursor.execute("SELECT value FROM crdb_internal.node_build_info WHERE field = 'Tag'")
    tag = cursor.fetchone()[0]

    cursor.close()
    db_conn.close()

    print(f"CockroachDB Version: {version}")
    print(f"Build Tag: {tag}")
    print(f"Connection Latency: {latency_ms:.1f}ms")

    return {"version": version, "tag": tag, "latency_ms": latency_ms}


def check_node_status(**context):
    """Query CockroachDB node status from crdb_internal."""
    from airflow.hooks.base import BaseHook
    import psycopg2

    conn = BaseHook.get_connection("cockroachdb_default")
    db_conn = psycopg2.connect(
        host=conn.host,
        port=conn.port or 26257,
        dbname=conn.schema or "defaultdb",
        user=conn.login,
        password=conn.password,
    )
    cursor = db_conn.cursor()

    cursor.execute("""
        SELECT node_id, address, is_live, locality
        FROM crdb_internal.gossip_nodes
        ORDER BY node_id
    """)
    nodes = cursor.fetchall()

    print("\n=== Cluster Node Status ===")
    print(f"{'Node':<8} {'Address':<30} {'Live':<8} {'Locality':<30}")
    print("-" * 76)
    for node in nodes:
        print(f"{node[0]:<8} {node[1]:<30} {node[2]!s:<8} {node[3] or 'N/A':<30}")

    live_count = sum(1 for n in nodes if n[2])
    total_count = len(nodes)
    print(f"\nNodes: {live_count}/{total_count} live")

    cursor.close()
    db_conn.close()

    if live_count < total_count:
        raise ValueError(f"WARNING: Only {live_count}/{total_count} nodes are live!")

    return {"live": live_count, "total": total_count}


def check_database_sizes(**context):
    """Report database and table sizes."""
    from airflow.hooks.base import BaseHook
    import psycopg2

    conn = BaseHook.get_connection("cockroachdb_default")
    db_conn = psycopg2.connect(
        host=conn.host,
        port=conn.port or 26257,
        dbname=conn.schema or "defaultdb",
        user=conn.login,
        password=conn.password,
    )
    cursor = db_conn.cursor()

    cursor.execute("""
        SELECT table_name,
               pg_size_pretty(range_size_mb * 1024 * 1024) as size,
               ranges,
               range_size_mb
        FROM (
            SELECT table_name,
                   count(*) as ranges,
                   COALESCE(sum(range_size_mb), 0) as range_size_mb
            FROM crdb_internal.ranges_no_leases r
            JOIN crdb_internal.tables t ON r.table_id = t.table_id
            WHERE t.database_name = current_database()
            GROUP BY table_name
        ) sub
        ORDER BY range_size_mb DESC
        LIMIT 20
    """)
    tables = cursor.fetchall()

    if tables:
        print("\n=== Top Tables by Size ===")
        print(f"{'Table':<40} {'Size':<15} {'Ranges':<10}")
        print("-" * 65)
        for row in tables:
            print(f"{row[0]:<40} {row[1]:<15} {row[2]:<10}")

    cursor.close()
    db_conn.close()

    return {"table_count": len(tables)}


with DAG(
    dag_id="cockroachdb_health_check",
    default_args=default_args,
    description="CockroachDB cluster health monitoring",
    schedule=timedelta(minutes=30),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cockroachdb", "monitoring", "health"],
) as dag:

    t1 = PythonOperator(
        task_id="check_cluster_version",
        python_callable=check_cluster_version,
    )

    t2 = PythonOperator(
        task_id="check_node_status",
        python_callable=check_node_status,
    )

    t3 = PythonOperator(
        task_id="check_database_sizes",
        python_callable=check_database_sizes,
    )

    # Run all checks in parallel
    [t1, t2, t3]
