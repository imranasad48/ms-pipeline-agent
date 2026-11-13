"""Airflow DAG: CRM sales orders + 3PL shipment events -> warehouse.delivery_performance."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from delivery_performance import pipeline

with DAG(
    dag_id="delivery_performance",
    description="One row per CRM order with latest 3PL shipment status, pickup/delivery/promised "
                "timestamps and late flag (full reload)",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["crm", "3pl", "delivery_performance", "generated"],
) as dag:
    extract_orders = PythonOperator(task_id="extract_orders", python_callable=pipeline.extract_orders)
    extract_shipments = PythonOperator(task_id="extract_shipments",
                                       python_callable=pipeline.extract_shipments)
    transform = PythonOperator(task_id="transform", python_callable=pipeline.transform)
    load = PythonOperator(task_id="load", python_callable=pipeline.load)
    validate = PythonOperator(task_id="validate", python_callable=pipeline.validate)

    [extract_orders, extract_shipments] >> transform >> load >> validate
