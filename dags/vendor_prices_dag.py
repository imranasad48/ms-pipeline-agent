"""Airflow DAG: weekly vendor price sheet workbooks -> warehouse.vendor_prices (price history)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from vendor_prices import pipeline

with DAG(
    dag_id="vendor_prices",
    description="Weekly vendor price sheets (xlsx), all weeks kept as price history, full reload",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["manual", "vendor_prices", "generated"],
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=pipeline.extract)
    transform = PythonOperator(task_id="transform", python_callable=pipeline.transform)
    load = PythonOperator(task_id="load", python_callable=pipeline.load)
    validate = PythonOperator(task_id="validate", python_callable=pipeline.validate)

    extract >> transform >> load >> validate
