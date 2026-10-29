"""
Airflow DAG: Batch CSV Drops Validation & Ingestion
Features:
- Senses arriving files in landing bucket
- Validates data against Pydantic schema contracts
- Automatically routes invalid files to quarantine with diagnostic error report
- Lands valid records append-only into Bronze
- Alerting hooks on failure
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from orchestration.alerts import on_task_failure_alert
from ingestion.csv_validator import CSVValidator
from ingestion.config import CSV_LANDING_DIR

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_task_failure_alert,
    "sla": timedelta(minutes=15)
}

def process_arriving_csv_drops(**context):
    validator = CSVValidator()
    landing_files = list(CSV_LANDING_DIR.glob("*.csv"))
    results = []

    for f in landing_files:
        res = validator.process_landing_file(f)
        results.append(res)

    return {"processed_count": len(results), "details": results}

with DAG(
    dag_id="dag_batch_csv_validation_and_load",
    default_args=default_args,
    description="Validates supplier CSV drops with schema contracts & quarantine routing",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "csv", "bronze", "validation"]
) as dag:

    start_task = EmptyOperator(task_id="start_csv_scan")

    validate_and_load_task = PythonOperator(
        task_id="process_arriving_csv_drops",
        python_callable=process_arriving_csv_drops,
        provide_context=True
    )

    end_task = EmptyOperator(task_id="end_csv_scan")

    start_task >> validate_and_load_task >> end_task
