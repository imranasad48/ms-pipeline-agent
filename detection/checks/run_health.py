"""Run-health family: the warehouse table exists, has rows, and was loaded recently; and the
pipeline's latest Airflow run did not fail.

Airflow state comes from the run-state snapshot file (detection/airflow_state.py), never from
docker or Airflow. The DAG of a pipeline is the DAG whose dag_id is the manifest's pipeline_id
(dags/<pipeline_id>_dag.py). Per pipeline found in a fresh snapshot:
  run_health.airflow_run_failed  latest run's state is failed            -> critical
  run_health.dag_paused          DAG is paused                           -> info
  run_health.dag_never_run       DAG is active but has no run yet        -> info
A pipeline missing from the snapshot, a running/queued/successful latest run, or a missing or
stale snapshot give nothing (absence of evidence is not a failure).
"""
from __future__ import annotations

import datetime as dt
import sqlite3

from ..airflow_state import airflow_dags
from ..config import resolve_severity
from ..incidents import Finding
from ..manifest import Manifest
from .context import Context


def _parse_ts(text) -> dt.datetime | None:
    try:
        t = dt.datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)


def check_run_health(manifest: Manifest, business_date: dt.date, ctx: Context) -> list[Finding]:
    """Warehouse checks (below) plus the Airflow run-state checks."""
    return check_warehouse(manifest, business_date, ctx) + check_airflow(manifest, ctx)


def check_airflow(manifest: Manifest, ctx: Context) -> list[Finding]:
    dags = airflow_dags(ctx)
    if dags is None:
        return []
    dag = dags.get(manifest.pipeline_id)
    if not isinstance(dag, dict):
        return []
    sid = f"airflow.{manifest.pipeline_id}"
    run = dag.get("latest_run") or None
    ctx.stats.evaluate("run_health.airflow")

    def finding(kind, expected, observed):
        ctx.stats.fire(kind)
        return [Finding(sid, kind, "run_health", resolve_severity(ctx.config, kind),
                        expected, observed, [f"dag:{manifest.pipeline_id}"])]

    if run and run.get("state") == "failed":
        return finding("run_health.airflow_run_failed", {"latest_run_state": "success"},
                       {"dag_id": manifest.pipeline_id, "run_id": run.get("run_id"),
                        "logical_date": run.get("logical_date"), "state": "failed",
                        "failed_tasks": list(run.get("failed_tasks") or []),
                        "start": run.get("start"), "end": run.get("end")})
    if dag.get("paused"):
        return finding("run_health.dag_paused", {"dag_paused": False},
                       {"dag_id": manifest.pipeline_id, "paused": True,
                        "latest_run_id": (run or {}).get("run_id")})
    if run is None:
        return finding("run_health.dag_never_run", {"dag_runs": ">= 1"},
                       {"dag_id": manifest.pipeline_id, "dag_runs": 0})
    return []


def check_warehouse(manifest: Manifest, business_date: dt.date, ctx: Context) -> list[Finding]:
    """Table present, non-empty, MAX(loaded_at) not too far before day end. Tables without a
    loaded_at column only get the existence/non-empty checks."""
    cfg, table = ctx.config, manifest.dest_table
    sid = f"warehouse.{table}"
    ctx.stats.evaluate("run_health.table")

    def finding(kind, expected, observed):
        ctx.stats.fire(kind)
        return [Finding(sid, kind, "run_health", resolve_severity(cfg, kind),
                        expected, observed, [str(ctx.warehouse_db.name)])]

    if not ctx.warehouse_db.exists():
        return finding("run_health.table_missing", {"table": table},
                       {"reason": "warehouse db not found"})
    con = sqlite3.connect(f"file:{ctx.warehouse_db}?mode=ro", uri=True)
    try:
        cols = [r[1] for r in con.execute(f'PRAGMA table_info("{table}")')]
        if not cols:
            return finding("run_health.table_missing", {"table": table},
                           {"reason": "table not found"})
        n = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        if n == 0:
            return finding("run_health.table_empty", {"min_rows": 1}, {"rows": 0})
        if "loaded_at" not in cols:
            return []
        latest = _parse_ts(con.execute(f'SELECT MAX(loaded_at) FROM "{table}"').fetchone()[0])
    finally:
        con.close()
    ctx.stats.evaluate("run_health.load_stale")
    day_end = dt.datetime.combine(business_date + dt.timedelta(days=1), dt.time.min,
                                  tzinfo=dt.timezone.utc)
    max_lag = cfg["run_health"]["max_load_lag_hours"]
    lag_h = (day_end - latest).total_seconds() / 3600 if latest else float("inf")
    ctx.stats.utilisation("run_health.load_stale", max(lag_h, 0.0), max_lag)
    if lag_h > max_lag:
        return finding("run_health.load_stale", {"max_lag_hours": max_lag},
                       {"latest_loaded_at": latest.isoformat() if latest else None,
                        "lag_hours": round(lag_h, 1)})
    return []
