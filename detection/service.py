"""One call that runs detection for a business date and stores the incidents.

Used by the Airflow DAG (dags/detection_watch_dag.py); every path defaults to the
environment-configurable values in config.py, so host and container run the same code.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

from . import config as C
from .checks.context import Context
from .engine import run_date
from .incidents import IncidentStore
from .manifest import load_manifests


def run_and_store(business_date: dt.date | str, data_root: Path | str | None = None,
                  manifests_dir: Path | None = None, db: Path | None = None,
                  warehouse: Path | None = None, store: bool = True) -> list:
    """Run every check for ``business_date``; upsert incidents into the incident DB."""
    if isinstance(business_date, str):
        business_date = dt.date.fromisoformat(business_date)
    root = data_root or C.DEFAULT_DATA_ROOT
    if not root:
        raise ValueError("no data root: pass data_root or set DETECTION_DATA_ROOT")
    cfg = C.load_config()
    manifests = load_manifests(manifests_dir or C.DEFAULT_MANIFESTS, cfg)
    if not manifests:
        raise ValueError(f"no manifests in {manifests_dir or C.DEFAULT_MANIFESTS}")
    ctx = Context(Path(root).resolve(), Path(warehouse or C.DEFAULT_WAREHOUSE), cfg)
    incidents = run_date(manifests, business_date, ctx)
    if store:
        st = IncidentStore(db or C.DEFAULT_INCIDENTS_DB)
        try:
            st.save(incidents)
        finally:
            st.close()
    return incidents
