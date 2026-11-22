"""Airflow DAG: run the deterministic detection engine daily against the DIRTY testbed.

Incidents are upserted into detection/incidents.db (mounted read-write from the host), so
`python -m healing` / the dashboard see them with no extra step. No LLM involved.
Runs for the logical date ({{ ds }}); trigger with conf {"business_date": "YYYY-MM-DD"} to
check another day (the testbed holds 2026-08-14 .. 2026-11-11).
"""
import os
import time
from collections import Counter
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DIRTY_DATA_ROOT = "/opt/testbed/data"


def run_detection(ds: str, dag_run=None, **_) -> dict:
    # File mtimes in the testbed are naive Karachi local time (see detection/README.md);
    # the freshness check must read them in the same zone.
    os.environ.setdefault("TZ", "Asia/Karachi")
    time.tzset()
    import datetime as dt
    from detection.schedule import infer_date_range
    from detection.service import run_and_store

    conf = (dag_run.conf if dag_run else None) or {}
    day = conf.get("business_date") or ds
    root = os.environ.get("DETECTION_DATA_ROOT", DIRTY_DATA_ROOT)
    # The testbed is a finite synthetic window. Outside it every file would look "missing",
    # so a scheduled run past the end of the data is skipped rather than reported as an outage.
    lo, hi = infer_date_range(root)
    if not lo <= dt.date.fromisoformat(day) <= hi:
        print(f"detection {day}: outside testbed range {lo}..{hi}; skipped")
        return {"business_date": day, "incidents": None, "skipped": True}
    incidents = run_and_store(day, data_root=root)
    by_sev = Counter(i.severity for i in incidents)
    print(f"detection {day}: {len(incidents)} incident(s) {dict(by_sev)}")
    for i in incidents:
        print(f"  [{i.severity}] {i.pipeline_id}/{i.source_id} {i.check_id}")
    return {"business_date": day, "incidents": len(incidents), "by_severity": dict(by_sev)}


with DAG(
    dag_id="detection_watch",
    description="Deterministic data-quality watcher over the dirty testbed (writes detection/incidents.db)",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["detection", "monitoring"],
) as dag:
    PythonOperator(task_id="detect", python_callable=run_detection)
