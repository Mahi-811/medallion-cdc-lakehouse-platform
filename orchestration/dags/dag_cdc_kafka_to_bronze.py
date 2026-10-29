"""
Airflow DAG: CDC Kafka Consumer & Bronze Micro-batch Drainer
Features:
- Micro-batch drain of Debezium CDC mutation topics
- Captures row-level CRUD operations (c, u, d, r)
- Ingestion metadata stamping (_loaded_at, _source, _batch_id, _cdc_op, _cdc_ts_ms)
- Idempotent offset management
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from orchestration.alerts import on_task_failure_alert
from ingestion.cdc_kafka_consumer import CDCKafkaConsumer
from ingestion.oltp_generator import OLTPDataSimulator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": on_task_failure_alert,
    "sla": timedelta(minutes=10)
}

def drain_cdc_microbatch(**context):
    # In live environments, consumer connects to Kafka broker.
    # In self-contained runner/orchestration mode, drains simulated operational mutations.
    sim = OLTPDataSimulator()
    mutations = sim.simulate_mutations(cycle_count=10)

    consumer = CDCKafkaConsumer()
    res = consumer.process_cdc_events(mutations)
    return {"status": "success", "counts": res}

with DAG(
    dag_id="dag_cdc_kafka_to_bronze",
    default_args=default_args,
    description="Drains CDC topics into Bronze append-only tables with micro-batch lineage",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "cdc", "kafka", "bronze"]
) as dag:

    start_task = EmptyOperator(task_id="start_cdc_drain")

    drain_task = PythonOperator(
        task_id="drain_cdc_microbatch",
        python_callable=drain_cdc_microbatch,
        provide_context=True
    )

    end_task = EmptyOperator(task_id="end_cdc_drain")

    start_task >> drain_task >> end_task
