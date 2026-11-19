"""Airflow DAG: manual weekly stock count workbooks -> warehouse.stock_counts."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from stock_counts import pipeline

with DAG(
    dag_id="stock_counts",
    description="Weekly manual stock count Excel workbooks (defensive parsing, full reload)",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["manual", "stock_counts", "generated"],
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=pipeline.extract)
    transform = PythonOperator(task_id="transform", python_callable=pipeline.transform)
    load = PythonOperator(task_id="load", python_callable=pipeline.load)
    validate = PythonOperator(task_id="validate", python_callable=pipeline.validate)

    extract >> transform >> load >> validate
