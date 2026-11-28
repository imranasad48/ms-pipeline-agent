"""Airflow adapter for the dashboard: switch a pipeline's DAG on and run it through Airflow.

`AirflowCLI` talks to the docker-compose Airflow with the airflow CLI / a short Python snippet run
via ``docker compose exec -T airflow-scheduler ...`` (always an argument list, never a shell).
The REST API on :8080 is not used: this deployment only accepts browser sessions there.

The CLI takes ~2 s per call, so pages never call it: they read `StatusCache`, which refreshes in a
background thread. Only jobs (background threads) call the adapter directly.

`orchestrate()` is the one sequence used by both generation jobs and "Enable schedule and run now":
wait until Airflow has parsed the DAG (import errors fail it) -> refuse catchup=True -> unpause ->
trigger one run -> poll it to success/failed. Any unsuccessful end pauses the DAG again.

Tests use a fake with the same methods (dashboard/tests/fake_airflow.py).
"""
from __future__ import annotations

import datetime as dt
import json
import os
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# DAGs the dashboard must never pause, unpause or trigger.
PROTECTED = frozenset({"crm_sales_orders", "delivery_performance", "dim_products", "sensor_readings",
                       "stock_counts", "detection_watch", "smoke_test"})
NOT_RUNNING = "Airflow is not running; start it with: docker compose up -d"
TERMINAL_RUN = ("success", "failed")

_PARSE = r'''
import json, sys
from airflow.models.dagbag import DagBag
b = DagBag(sys.argv[1], include_examples=False, safe_mode=False)
d = b.dags.get(sys.argv[2])
if d is not None and not b.import_errors and sys.argv[3] == "sync":
    b.sync_to_db()
print("@@" + json.dumps({"errors": {k: str(v)[-3000:] for k, v in b.import_errors.items()},
    "found": d is not None, "catchup": getattr(d, "catchup", None),
    "schedule": getattr(getattr(d, "timetable", None), "summary", None)}))
'''

_STATUS = r'''
import json, sys
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.utils.session import create_session
iso = lambda t: t.isoformat() if t else None
out = {}
with create_session() as s:
    q = s.query(DagModel)
    if len(sys.argv) > 1:
        q = q.filter(DagModel.dag_id == sys.argv[1])
    for d in q.all():
        r = s.query(DagRun).filter(DagRun.dag_id == d.dag_id).order_by(DagRun.id.desc()).first()
        failed = sorted(t.task_id for t in s.query(TaskInstance).filter(
            TaskInstance.dag_id == d.dag_id, TaskInstance.run_id == r.run_id,
            TaskInstance.state == "failed")) if r else []
        out[d.dag_id] = {"paused": bool(d.is_paused), "active": bool(d.is_active),
            "schedule": d.timetable_description, "has_import_errors": bool(d.has_import_errors),
            "last_run_id": r.run_id if r else None, "last_run_state": str(r.state) if r and r.state else None,
            "last_run_time": iso(r.end_date or r.start_date or r.execution_date) if r else None,
            "last_run_logical_date": iso(r.execution_date) if r else None,
            "last_run_start": iso(r.start_date) if r else None, "last_run_end": iso(r.end_date) if r else None,
            "last_run_failed_tasks": failed}
print("@@" + json.dumps(out))
'''

_RUN = r'''
import json, sys
from airflow.models import DagRun, TaskInstance
from airflow.utils.session import create_session
with create_session() as s:
    r = s.query(DagRun).filter(DagRun.dag_id == sys.argv[1], DagRun.run_id == sys.argv[2]).first()
    tis = s.query(TaskInstance).filter(TaskInstance.dag_id == sys.argv[1], TaskInstance.run_id == sys.argv[2]).all()
    print("@@" + json.dumps({"state": str(r.state) if r and r.state else None,
        "tasks": [{"task_id": t.task_id, "state": str(t.state) if t.state else None,
                   "start": t.start_date.isoformat() if t.start_date else ""} for t in tis]}))
'''


def _withhold(line: str) -> str:
    return "[line withheld: mentions the evaluation answer key]" if "error_manifest" in line else line


class AirflowError(Exception):
    pass


class AirflowCLI:
    data_dir_container = "/opt/testbed/data_clean"

    def __init__(self, compose_dir: Path = REPO, service: str = "airflow-scheduler",
                 health_url: str = "http://localhost:8080/health", logs_dir: Path | None = None,
                 timeout: float = 90):
        self.compose_dir, self.service, self.health_url = Path(compose_dir), service, health_url
        self.logs_dir = Path(logs_dir or Path(compose_dir) / "logs")
        self.timeout = timeout

    # ------------------------------------------------------------ plumbing
    def _exec(self, args: list, timeout: float | None = None) -> str:
        argv = ["docker", "compose", "exec", "-T", self.service, *args]
        try:
            p = subprocess.run(argv, cwd=str(self.compose_dir), stdin=subprocess.DEVNULL, capture_output=True,
                               text=True, timeout=timeout or self.timeout)
        except (OSError, subprocess.TimeoutExpired) as ex:
            raise AirflowError(f"{' '.join(args[:3])}: {type(ex).__name__}: {ex}") from None
        if p.returncode != 0:
            err = [ln for ln in (p.stderr or p.stdout).splitlines() if ln.strip()][-3:]
            raise AirflowError(f"{' '.join(args[:3])} exited {p.returncode}: {' | '.join(err)}")
        return p.stdout

    def _py(self, script: str, *args: str) -> dict:
        out = self._exec(["python", "-c", script, *args])
        for ln in reversed(out.splitlines()):
            if ln.startswith("@@"):
                return json.loads(ln[2:])
        raise AirflowError("no result from the Airflow helper")

    # ------------------------------------------------------------ operations
    def is_available(self) -> tuple[bool, str]:
        try:
            with urllib.request.urlopen(self.health_url, timeout=3) as r:
                h = json.loads(r.read().decode())
        except Exception:
            return False, NOT_RUNNING
        bad = [k for k in ("metadatabase", "scheduler") if (h.get(k) or {}).get("status") != "healthy"]
        if bad:
            return False, f"Airflow is up but its {' and '.join(bad)} is not healthy. {NOT_RUNNING}"
        return True, ""

    def parse(self, dag_id: str, sync: bool = False) -> dict:
        """Parse dags/<dag_id>_dag.py with Airflow's own DagBag in the container (import errors,
        catchup, schedule). With sync, register just this DAG in Airflow's database."""
        return self._py(_PARSE, f"/opt/airflow/dags/{dag_id}_dag.py", dag_id, "sync" if sync else "-")

    def status_all(self) -> dict:
        return self._py(_STATUS)

    def dag_info(self, dag_id: str) -> dict | None:
        return self._py(_STATUS, dag_id).get(dag_id)

    def unpause(self, dag_id: str) -> None:
        self._guard(dag_id)
        self._exec(["airflow", "dags", "unpause", dag_id])

    def pause(self, dag_id: str) -> None:
        self._guard(dag_id)
        self._exec(["airflow", "dags", "pause", dag_id])

    def trigger(self, dag_id: str, run_id: str) -> None:
        self._guard(dag_id)
        self._exec(["airflow", "dags", "trigger", dag_id, "--run-id", run_id])

    def run(self, dag_id: str, run_id: str) -> dict:
        """{"state": queued|running|success|failed|None, "tasks": [{task_id, state, start}]}"""
        return self._py(_RUN, dag_id, run_id)

    def task_log_tail(self, dag_id: str, run_id: str, task_id: str, lines: int = 40) -> str:
        d = self.logs_dir / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}"
        def attempt(p: Path) -> int:
            n = p.stem.split("=", 1)[-1]
            return int(n) if n.isdigit() else 0
        logs = sorted(d.glob("attempt=*.log"), key=attempt)
        if not logs:
            return f"(no task log found under logs/dag_id={dag_id}/run_id={run_id}/task_id={task_id}/)"
        text = logs[-1].read_text(errors="replace").splitlines()[-lines:]
        return "\n".join(_withhold(ln) for ln in text)

    @staticmethod
    def _guard(dag_id: str) -> None:
        if dag_id in PROTECTED:
            raise AirflowError(f"'{dag_id}' is one of the original DAGs; the dashboard never changes it.")


# ---------------------------------------------------------------- the sequence
def failed_task(af, dag_id: str, run_id: str, info: dict | None = None) -> tuple[str | None, str]:
    try:
        info = info or af.run(dag_id, run_id)
        tasks = sorted(info.get("tasks") or [], key=lambda t: t.get("start") or "~")
        bad = next((t for t in tasks if t.get("state") == "failed"), None) or next(
            (t for t in tasks if t.get("state") in ("upstream_failed", "up_for_retry")), None)
        if not bad:
            return None, ""
        return bad["task_id"], af.task_log_tail(dag_id, run_id, bad["task_id"])
    except Exception as ex:
        return None, f"(could not read the task log: {ex})"


def orchestrate(af, dag_id: str, run_id: str, deadline: float, log=print, cancelled=lambda: False,
                poll_s: float = 5.0, on_unpaused=lambda: None) -> dict:
    """wait parsed -> catchup check -> unpause -> trigger -> poll. Returns
    {ok, outcome: success|failed|cancelled|timeout, error, state, failed_task, log_tail, schedule, catchup,
     unpaused}. On any unsuccessful end the DAG is paused again (if this call unpaused it)."""
    res = {"ok": False, "outcome": "failed", "error": "", "state": None, "failed_task": None, "log_tail": "",
           "schedule": None, "catchup": None, "unpaused": False}

    def stop(outcome, error):
        res.update(outcome=outcome, error=error)
        if res["unpaused"]:
            try:
                af.pause(dag_id)
                log(f"airflow: DAG '{dag_id}' paused again")
            except Exception as ex:
                res["error"] += f" | could not pause the DAG: {ex}"
                log(f"airflow: could not pause DAG '{dag_id}': {ex}")
        return res

    def interrupted():
        if cancelled():
            return "cancelled", "Cancelled by the user."
        if time.monotonic() > deadline:
            return "timeout", "Time limit reached while waiting for Airflow."
        return None

    if dag_id in PROTECTED:
        return stop("failed", f"'{dag_id}' is one of the original DAGs; the dashboard never changes it.")
    try:
        # 1. parsed by Airflow (and registered in its database)
        log(f"airflow: parsing dags/{dag_id}_dag.py with Airflow")
        while True:
            p = af.parse(dag_id)
            if p.get("errors"):
                msg = "; ".join(f"{Path(k).name}: {v.strip().splitlines()[-1] if v.strip() else v}"
                                for k, v in p["errors"].items())
                res["log_tail"] = "\n".join(_withhold(ln) for ln in "\n\n".join(p["errors"].values()).splitlines()[-40:])
                return stop("failed", f"Airflow cannot import the DAG: {msg}")
            if not p.get("found"):
                return stop("failed", f"dags/{dag_id}_dag.py parses but defines no DAG with dag_id '{dag_id}'.")
            res["catchup"], res["schedule"] = p.get("catchup"), p.get("schedule")
            if af.dag_info(dag_id) is not None:
                break
            log("airflow: DAG not registered yet; registering this one DAG file")
            af.parse(dag_id, sync=True)
            if af.dag_info(dag_id) is not None:
                break
            x = interrupted()
            if x:
                return stop(*x)
            time.sleep(poll_s)
        log(f"airflow: DAG '{dag_id}' parsed (schedule {res['schedule']}, catchup {res['catchup']})")
        # 2. catchup must be off, or unpausing would backfill every interval since start_date
        if res["catchup"]:
            return stop("failed", f"DAG '{dag_id}' has catchup=True; unpausing it would backfill every "
                                  "interval since its start_date. It was left paused.")
        x = interrupted()
        if x:
            return stop(*x)
        # 3. unpause, 4. trigger
        af.unpause(dag_id)
        res["unpaused"] = True
        on_unpaused()
        log(f"airflow: DAG '{dag_id}' unpaused (it now runs on its schedule)")
        af.trigger(dag_id, run_id)
        log(f"airflow: triggered run {run_id}")
        # 5. poll
        last = None
        while True:
            info = af.run(dag_id, run_id)
            st = info.get("state")
            if st != last:
                log(f"airflow: run {run_id} is {st or 'not created yet'}")
                last = st
            res["state"] = st
            if st == "success":
                res.update(ok=True, outcome="success")
                return res
            if st == "failed":
                task, tail = failed_task(af, dag_id, run_id, info)
                res.update(failed_task=task, log_tail=tail)
                return stop("failed", f"The Airflow run failed" + (f" at task '{task}'." if task else "."))
            x = interrupted()
            if x:
                task, tail = failed_task(af, dag_id, run_id, info)
                res.update(failed_task=task, log_tail=tail)
                return stop(*x)
            time.sleep(poll_s)
    except Exception as ex:
        return stop("failed", f"Airflow error: {ex}")


# ---------------------------------------------------------------- run-state snapshot
DEFAULT_SNAPSHOT = REPO / "detection" / "airflow_runs.json"


def snapshot_doc(dags: dict, now: dt.datetime | None = None) -> dict:
    """The JSON detection reads (detection/checks/run_health.py): one entry per DAG with its
    paused flag and latest run (run id, logical date, state, start/end, failed tasks)."""
    out = {}
    for dag_id, i in sorted(dags.items()):
        run = None
        if i.get("last_run_id"):
            run = {"run_id": i["last_run_id"], "logical_date": i.get("last_run_logical_date"),
                   "state": i.get("last_run_state"), "start": i.get("last_run_start"),
                   "end": i.get("last_run_end"), "failed_tasks": list(i.get("last_run_failed_tasks") or [])}
        out[dag_id] = {"paused": bool(i.get("paused")), "latest_run": run}
    return {"written_at": (now or dt.datetime.now(dt.timezone.utc)).isoformat(timespec="seconds"),
            "source": "airflow", "dags": out}


def write_snapshot(dags: dict, path: Path = DEFAULT_SNAPSHOT, now: dt.datetime | None = None) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(snapshot_doc(dags, now), indent=1, sort_keys=True))
    os.replace(tmp, path)                     # readers never see a half-written file
    return path


def export_snapshot(af, path: Path = DEFAULT_SNAPSHOT) -> Path:
    """On demand (`python -m dashboard airflow-snapshot`): query Airflow once and write the file."""
    ok, msg = af.is_available()
    if not ok:
        raise AirflowError(msg)
    return write_snapshot(af.status_all(), path)


# ---------------------------------------------------------------- page cache
class StatusCache:
    """Last known Airflow status for all DAGs; get() never blocks (refreshes in a background thread).
    With snapshot_path, every successful refresh also writes the run-state snapshot for detection."""

    def __init__(self, af, ttl: float = 30.0, snapshot_path: Path | None = None):
        self.af, self.ttl, self.snapshot_path = af, ttl, snapshot_path
        self._data: dict | None = None
        self._at = 0.0
        self._error = ""
        self._busy = False
        self._lock = threading.Lock()

    def get(self) -> dict:
        """{"dags": {...} | None, "age_s": float | None, "error": str}"""
        with self._lock:
            stale = time.monotonic() - self._at > self.ttl
            if stale and not self._busy and self.af is not None:
                self._busy = True
                threading.Thread(target=self._refresh, daemon=True, name="airflow-status").start()
            return {"dags": self._data, "age_s": (time.monotonic() - self._at) if self._data is not None else None,
                    "error": self._error}

    def invalidate(self) -> None:
        with self._lock:
            self._at = 0.0

    def _refresh(self) -> None:
        try:
            ok, msg = self.af.is_available()
            data, err = (self.af.status_all(), "") if ok else (None, msg)
            if data is not None and self.snapshot_path is not None:
                write_snapshot(data, self.snapshot_path)
        except Exception as ex:
            data, err = None, f"{type(ex).__name__}: {ex}"
        with self._lock:
            if data is not None:
                self._data = data
            self._error = err
            self._at = time.monotonic()
            self._busy = False
