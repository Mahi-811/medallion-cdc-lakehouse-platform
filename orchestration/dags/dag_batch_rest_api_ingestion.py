"""
Airflow DAG: Daily REST API Ingestion (Open-Meteo Weather)
Features:
- Pagination & rate limiting
- Exponential backoff retries (3 retries, 2m delay)
- Task-level SLA (20 minutes)
- Failure alerting webhook callback
- Idempotent partition writes
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from orchestration.alerts import on_task_failure_alert
from ingestion.api_extractor import APIExtractor

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_task_failure_alert,
    "sla": timedelta(minutes=20)
}

def extract_and_land_weather(**context):
    execution_date = context["execution_date"].date()
    # Idempotent 1-day slice based on execution date
    start_date = execution_date
    end_date = execution_date

    extractor = APIExtractor()
    records = extractor.extract_weather(start_date=start_date, end_date=end_date)
    landed_count = extractor.land_to_bronze(records)
    return {"landed_records": landed_count, "execution_date": str(execution_date)}

with DAG(
    dag_id="dag_batch_rest_api_ingestion",
    default_args=default_args,
    description="Daily ingestion of meteorological indicators with retries, SLA, and idempotency",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "api", "bronze", "weather"]
) as dag:

    start_task = EmptyOperator(task_id="start_api_sync")

    ingest_weather_task = PythonOperator(
        task_id="extract_and_land_weather",
        python_callable=extract_and_land_weather,
        provide_context=True
    )

    end_task = EmptyOperator(task_id="end_api_sync")

    start_task >> ingest_weather_task >> end_task
