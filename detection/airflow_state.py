"""Airflow run state for detection, read from a JSON snapshot file (never from docker/Airflow).

The snapshot is written by the dashboard's background Airflow status refresh and by
``python -m dashboard airflow-snapshot`` (dashboard/airflow.py: snapshot_doc). Shape:

    {"written_at": "2026-11-28T13:05:00+00:00", "source": "airflow",
     "dags": {"<dag_id>": {"paused": false,
                           "latest_run": {"run_id", "logical_date", "state", "start", "end",
                                          "failed_tasks": [...]} | null}}}

Path: ``config.yaml -> run_health.airflow_snapshot`` (repo-relative), overridden by the
environment variable DETECTION_AIRFLOW_SNAPSHOT. A missing, unreadable or too old snapshot
(``airflow_snapshot_max_age_hours``) gives no incidents and exactly one log line per run context:
absence of evidence is not a failure.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path

from .config import REPO_ROOT


def snapshot_path(cfg: dict) -> Path:
    env = os.environ.get("DETECTION_AIRFLOW_SNAPSHOT")
    if env:
        return Path(env)
    p = Path(cfg["run_health"].get("airflow_snapshot", "detection/airflow_runs.json"))
    return p if p.is_absolute() else REPO_ROOT / p


def _ts(text) -> dt.datetime | None:
    try:
        t = dt.datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)


def read_snapshot(cfg: dict, now: dt.datetime) -> tuple[dict | None, str]:
    """(dags, "") when usable, else (None, why)."""
    path = snapshot_path(cfg)
    max_age = float(cfg["run_health"].get("airflow_snapshot_max_age_hours", 6))
    try:
        doc = json.loads(path.read_text())
    except FileNotFoundError:
        return None, f"no Airflow run-state snapshot at {path}"
    except (OSError, ValueError) as ex:
        return None, f"Airflow run-state snapshot {path} is unreadable ({type(ex).__name__})"
    written = _ts(doc.get("written_at")) if isinstance(doc, dict) else None
    if written is None or not isinstance(doc.get("dags"), dict):
        return None, f"Airflow run-state snapshot {path} has no written_at/dags"
    age_h = (now - written).total_seconds() / 3600
    if age_h > max_age:
        return None, (f"Airflow run-state snapshot {path} is {age_h:.1f} h old "
                      f"(limit {max_age:g} h)")
    return doc["dags"], ""


def airflow_dags(ctx) -> dict | None:
    """The snapshot's DAG map for this run context (read once, cached), or None. Logs once."""
    key = ("airflow_snapshot",)
    if key not in ctx.cache:
        dags, why = read_snapshot(ctx.config, ctx.now)
        ctx.cache[key] = dags
        if dags is None:
            print(f"[detection] run_health: Airflow run-state checks skipped: {why}", file=sys.stderr)
    return ctx.cache[key]
