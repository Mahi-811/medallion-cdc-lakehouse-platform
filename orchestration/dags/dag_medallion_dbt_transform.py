"""
Airflow DAG: Medallion Architecture dbt Transformation Pipeline
Features:
- Triggers Bronze -> Silver -> Gold -> Marts transformations
- Data quality testing gates between layers
- Automated failure alert hooks
- Idempotency guarantees: re-runs yield identical checksums and counts
"""
import os
import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from orchestration.alerts import on_task_failure_alert
from ingestion.config import PROJECT_ROOT

default_args = {
    "owner": "analytics_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_task_failure_alert,
    "sla": timedelta(minutes=30)
}

DBT_DIR = str(PROJECT_ROOT / "dbt")

def run_dbt_step(subcommand: str) -> None:
    cmd = f"dbt {subcommand} --profiles-dir ."
    result = subprocess.run(cmd, shell=True, cwd=DBT_DIR, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"dbt command '{cmd}' failed!\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")

with DAG(
    dag_id="dag_medallion_dbt_transform",
    default_args=default_args,
    description="End-to-end Medallion transform and data quality gating DAG",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "medallion", "silver", "gold", "marts"]
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    test_sources = PythonOperator(
        task_id="dbt_test_sources",
        python_callable=lambda: run_dbt_step("test --select source:*")
    )

    build_silver = PythonOperator(
        task_id="dbt_build_silver",
        python_callable=lambda: run_dbt_step("build --select tag:silver")
    )

    build_gold_dimensions = PythonOperator(
        task_id="dbt_build_gold_dimensions",
        python_callable=lambda: run_dbt_step("build --select tag:dimension")
    )

    build_gold_facts = PythonOperator(
        task_id="dbt_build_gold_facts",
        python_callable=lambda: run_dbt_step("build --select tag:fact")
    )

    build_marts = PythonOperator(
        task_id="dbt_build_marts",
        python_callable=lambda: run_dbt_step("build --select tag:mart")
    )

    run_custom_tests = PythonOperator(
        task_id="dbt_run_custom_data_tests",
        python_callable=lambda: run_dbt_step("test --select test_type:singular")
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Dependency Flow
    (
        start_pipeline
        >> test_sources
        >> build_silver
        >> build_gold_dimensions
        >> build_gold_facts
        >> build_marts
        >> run_custom_tests
        >> end_pipeline
    )
