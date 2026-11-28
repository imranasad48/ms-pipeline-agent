"""In-memory stand-in for dashboard.airflow.AirflowCLI (no docker, no network).

Scenario knobs (attributes):
  available     False -> is_available() reports Airflow down
  import_error  str   -> parse() reports it as an import error of the DAG file
  catchup       bool  -> what parse() reports for the DAG's catchup
  outcome       "success" | "failed" | "hang"   what a triggered run ends as
  register_after  number of dag_info() calls before the DAG shows up as registered
On "success" the fake does what the real DAG run would: runs the pipeline's step functions from
repo_root/dags/<id>/pipeline.py with DATA_ROOT / WAREHOUSE_DB set, so the warehouse gets the table.
Every call is recorded in .calls; the time of each call in .call_threads (to prove pages never call it).
"""
from __future__ import annotations

import importlib.util
import os
import threading
from pathlib import Path

from dashboard.airflow import NOT_RUNNING, PROTECTED


class FakeAirflow:
    def __init__(self, repo_root: Path | None = None, data_root: Path | None = None, warehouse: Path | None = None,
                 outcome: str = "success", available: bool = True, catchup: bool = False,
                 import_error: str = "", register_after: int = 0):
        self.repo_root, self.data_root, self.warehouse = repo_root, data_root, warehouse
        self.outcome, self.available, self.catchup = outcome, available, catchup
        self.import_error, self.register_after = import_error, register_after
        self.calls: list = []
        self.call_threads: list = []
        self.paused: dict = {}
        self.runs: dict = {}
        self._info_calls = 0
        self.logs: dict = {}

    def _rec(self, *c):
        self.calls.append(c)
        self.call_threads.append(threading.current_thread().name)

    def is_available(self):
        self._rec("is_available")
        return (True, "") if self.available else (False, NOT_RUNNING)

    def parse(self, dag_id, sync=False):
        self._rec("parse", dag_id, sync)
        if self.import_error:
            return {"errors": {f"/opt/airflow/dags/{dag_id}_dag.py": "Traceback ...\n" + self.import_error},
                    "found": False, "catchup": None, "schedule": None}
        if sync:
            self.register_after = 0
        return {"errors": {}, "found": True, "catchup": self.catchup, "schedule": "0 0 * * *"}

    def dag_info(self, dag_id):
        self._rec("dag_info", dag_id)
        self._info_calls += 1
        if self._info_calls <= self.register_after:
            return None
        return {"paused": self.paused.get(dag_id, True), "schedule": "At 00:00", "last_run_state": None,
                "last_run_time": None, "last_run_id": None, "active": True, "has_import_errors": False}

    def status_all(self):
        self._rec("status_all")
        return {d: {"paused": p, "schedule": "At 00:00", "last_run_state": "success",
                    "last_run_time": "2026-11-28T10:00:00+00:00", "last_run_id": "x", "active": True,
                    "has_import_errors": False} for d, p in self.paused.items()}

    def unpause(self, dag_id):
        assert dag_id not in PROTECTED
        self._rec("unpause", dag_id)
        self.paused[dag_id] = False

    def pause(self, dag_id):
        assert dag_id not in PROTECTED
        self._rec("pause", dag_id)
        self.paused[dag_id] = True

    def trigger(self, dag_id, run_id):
        assert dag_id not in PROTECTED
        self._rec("trigger", dag_id, run_id)
        if self.outcome == "success":
            try:
                self._execute(dag_id)
            except Exception as ex:                       # the task fails, as it would in Airflow
                self.runs[run_id] = {"state": "failed", "tasks": [{"task_id": "load", "state": "failed", "start": "1"}]}
                self.logs[(run_id, "load")] = f"ERROR Task failed with exception\n{type(ex).__name__}: {ex}"
                return
            self.runs[run_id] = {"state": "success", "tasks": [{"task_id": "load", "state": "success", "start": "1"}]}
        elif self.outcome == "failed":
            self.runs[run_id] = {"state": "failed", "tasks": [
                {"task_id": "extract", "state": "success", "start": "1"},
                {"task_id": "transform", "state": "failed", "start": "2"},
                {"task_id": "load", "state": "upstream_failed", "start": ""}]}
            self.logs[(run_id, "transform")] = "INFO starting\nKeyError: 'customer_id'\nTask failed"
        else:
            self.runs[run_id] = {"state": "running", "tasks": []}

    def run(self, dag_id, run_id):
        self._rec("run", dag_id, run_id)
        return self.runs.get(run_id, {"state": None, "tasks": []})

    def task_log_tail(self, dag_id, run_id, task_id, lines=40):
        return self.logs.get((run_id, task_id), "(no log)")

    def _execute(self, dag_id):
        """What the Airflow run does: the pipeline's steps, against the data Airflow reads."""
        path = Path(self.repo_root) / "dags" / dag_id / "pipeline.py"
        old = {k: os.environ.get(k) for k in ("DATA_ROOT", "WAREHOUSE_DB", "STAGING_DIR")}
        os.environ.update(DATA_ROOT=str(self.data_root), WAREHOUSE_DB=str(self.warehouse),
                          STAGING_DIR=str(Path(self.warehouse).parent / f"stage_{dag_id}"))
        try:
            spec = importlib.util.spec_from_file_location(f"fake_af_{dag_id}", path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            for step in ("extract", "transform", "load"):
                getattr(mod, step)()
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
