"""
Example DAG: CockroachDB Operations

Demonstrates using CockroachDB as a data source in Airflow DAGs.
This DAG:
  1. Creates a sample table in CockroachDB
  2. Inserts test data
  3. Queries and logs the results
  4. Cleans up

Prerequisites:
  - CockroachDB running and accessible
  - An Airflow connection named 'cockroachdb_default' configured
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# DAG default args
default_args = {
    "owner": "cockroachdb-poc",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def create_table(**context):
    """Create a sample table in CockroachDB."""
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
        CREATE TABLE IF NOT EXISTS airflow_poc_demo (
            id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            name STRING NOT NULL,
            value DECIMAL(10, 2),
            created_at TIMESTAMPTZ DEFAULT now(),
            region STRING DEFAULT 'us-east-1'
        )
    """)
    db_conn.commit()
    cursor.close()
    db_conn.close()
    print("Table 'airflow_poc_demo' created successfully")


def insert_data(**context):
    """Insert sample data into CockroachDB."""
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

    sample_data = [
        ("Airflow Task 1", 100.50, "us-east-1"),
        ("Airflow Task 2", 200.75, "us-west-2"),
        ("Airflow Task 3", 300.00, "eu-west-1"),
        ("Airflow Task 4", 150.25, "us-east-1"),
        ("Airflow Task 5", 450.00, "ap-southeast-1"),
    ]

    cursor.executemany(
        "INSERT INTO airflow_poc_demo (name, value, region) VALUES (%s, %s, %s)",
        sample_data,
    )
    db_conn.commit()
    row_count = cursor.rowcount
    cursor.close()
    db_conn.close()
    print(f"Inserted {row_count} rows into 'airflow_poc_demo'")
    return row_count


def query_data(**context):
    """Query and display data from CockroachDB."""
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

    # Aggregate query — demonstrates CockroachDB's distributed SQL
    cursor.execute("""
        SELECT region, COUNT(*) as cnt, SUM(value) as total
        FROM airflow_poc_demo
        GROUP BY region
        ORDER BY total DESC
    """)
    results = cursor.fetchall()

    print("\n=== CockroachDB Query Results ===")
    print(f"{'Region':<20} {'Count':<10} {'Total Value':<15}")
    print("-" * 45)
    for row in results:
        print(f"{row[0]:<20} {row[1]:<10} ${row[2]:<14.2f}")

    cursor.close()
    db_conn.close()
    return results


def cleanup(**context):
    """Drop the sample table."""
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
    cursor.execute("DROP TABLE IF EXISTS airflow_poc_demo")
    db_conn.commit()
    cursor.close()
    db_conn.close()
    print("Table 'airflow_poc_demo' dropped successfully")


with DAG(
    dag_id="cockroachdb_demo",
    default_args=default_args,
    description="Demo DAG showing CockroachDB as a data source",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["cockroachdb", "poc", "demo"],
) as dag:

    t1 = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    t2 = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data,
    )

    t3 = PythonOperator(
        task_id="query_data",
        python_callable=query_data,
    )

    t4 = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
    )

    t1 >> t2 >> t3 >> t4
