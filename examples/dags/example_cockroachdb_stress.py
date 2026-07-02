"""Stress DAG: many short parallel tasks to generate scheduler critical-section contention."""

from __future__ import annotations

import time
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def quick_task(task_number: int) -> str:
    time.sleep(0.5)
    return f"task {task_number} done"


with DAG(
    dag_id="cockroachdb_stress",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=8,
    tags=["cockroachdb", "stress"],
) as dag:
    for i in range(30):
        PythonOperator(
            task_id=f"quick_task_{i:02d}",
            python_callable=quick_task,
            op_kwargs={"task_number": i},
        )
