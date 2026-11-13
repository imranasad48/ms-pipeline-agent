"""Airflow DAG: ERP products -> warehouse.dim_products."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from dim_products import pipeline

with DAG(
    dag_id="dim_products",
    description="ERP product catalog with cold-chain flag and storage requirements (full reload)",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["erp", "dim_products", "generated"],
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=pipeline.extract)
    transform = PythonOperator(task_id="transform", python_callable=pipeline.transform)
    load = PythonOperator(task_id="load", python_callable=pipeline.load)
    validate = PythonOperator(task_id="validate", python_callable=pipeline.validate)

    extract >> transform >> load >> validate
