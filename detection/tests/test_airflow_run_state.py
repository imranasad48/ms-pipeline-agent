"""run_health reads Airflow run state from a snapshot file (fixtures only; no docker, no Airflow)."""
from __future__ import annotations

import datetime as dt
import json

import pytest

from detection import config as C
from detection.checks.context import Context
from detection.checks.run_health import check_airflow
from detection.manifest import Manifest

NOW = dt.datetime(2026, 11, 28, 14, 0, tzinfo=dt.timezone.utc)
FAILED = {"run_id": "scheduled__2026-11-27T00:00:00+00:00", "logical_date": "2026-11-27T00:00:00+00:00",
          "state": "failed", "start": "2026-11-28T00:00:05+00:00", "end": "2026-11-28T00:01:10+00:00",
          "failed_tasks": ["transform"]}


def snap(tmp_path, dags, written=NOW - dt.timedelta(minutes=5)):
    p = tmp_path / "airflow_runs.json"
    p.write_text(json.dumps({"written_at": written.isoformat(), "source": "airflow", "dags": dags}))
    return p


def run(tmp_path, monkeypatch, dags=None, path=None, pid="crm_customers", ctx=None, **kw):
    if path is None:
        path = snap(tmp_path, dags, **kw)
    monkeypatch.setenv("DETECTION_AIRFLOW_SNAPSHOT", str(path))
    ctx = ctx or Context(tmp_path, tmp_path / "wh.db", C.load_config(), now=NOW)
    return check_airflow(Manifest(pid, (), "t", ""), ctx), ctx


def test_latest_run_failed_is_a_critical_incident_naming_run_and_task(tmp_path, monkeypatch):
    f, _ = run(tmp_path, monkeypatch, {"crm_customers": {"paused": False, "latest_run": FAILED}})
    assert [(x.check_id, x.severity, x.family, x.source_id) for x in f] == [
        ("run_health.airflow_run_failed", "critical", "run_health", "airflow.crm_customers")]
    assert f[0].observed["run_id"] == FAILED["run_id"] and f[0].observed["failed_tasks"] == ["transform"]
    assert f[0].observed["logical_date"] == FAILED["logical_date"]


def test_failed_run_of_a_paused_dag_is_still_critical(tmp_path, monkeypatch):
    f, _ = run(tmp_path, monkeypatch, {"crm_customers": {"paused": True, "latest_run": FAILED}})
    assert [(x.check_id, x.severity) for x in f] == [("run_health.airflow_run_failed", "critical")]


@pytest.mark.parametrize("dag,expect", [
    ({"paused": True, "latest_run": {**FAILED, "state": "success", "failed_tasks": []}},
     [("run_health.dag_paused", "info")]),
    ({"paused": True, "latest_run": None}, [("run_health.dag_paused", "info")]),
    ({"paused": False, "latest_run": None}, [("run_health.dag_never_run", "info")]),
    ({"paused": False, "latest_run": {**FAILED, "state": "success", "failed_tasks": []}}, []),
    ({"paused": False, "latest_run": {**FAILED, "state": "running", "failed_tasks": []}}, []),
    ({"paused": False, "latest_run": {**FAILED, "state": "queued", "failed_tasks": []}}, []),
])
def test_paused_or_never_run_is_at_most_info(tmp_path, monkeypatch, dag, expect):
    f, _ = run(tmp_path, monkeypatch, {"crm_customers": dag})
    assert [(x.check_id, x.severity) for x in f] == expect
    assert all(x.severity != "critical" for x in f if x.check_id != "run_health.airflow_run_failed")


def test_pipeline_without_a_dag_in_the_snapshot_gives_nothing(tmp_path, monkeypatch):
    f, _ = run(tmp_path, monkeypatch, {"other": {"paused": False, "latest_run": FAILED}})
    assert f == []


@pytest.mark.parametrize("make,why", [
    (lambda p: p / "absent.json", "no Airflow run-state snapshot"),
    (lambda p: snap(p, {"crm_customers": {"paused": False, "latest_run": FAILED}},
                    written=NOW - dt.timedelta(hours=7)), "7.0 h old (limit 6 h)"),
    (lambda p: (p / "bad.json").write_text("{not json") and p / "bad.json", "unreadable"),
    (lambda p: (p / "odd.json").write_text("[]") and p / "odd.json", "has no written_at/dags"),
])
def test_missing_or_old_snapshot_gives_no_incident_and_one_log_line(tmp_path, monkeypatch, capsys, make, why):
    path = make(tmp_path)
    f, ctx = run(tmp_path, monkeypatch, path=path)
    f2, _ = run(tmp_path, monkeypatch, path=path, pid="vendor_prices", ctx=ctx)   # same run context
    assert f == [] and f2 == []
    err = [ln for ln in capsys.readouterr().err.splitlines() if "Airflow run-state checks skipped" in ln]
    assert len(err) == 1 and why in err[0]


def test_engine_reports_the_failed_run_for_the_pipeline(tmp_path, monkeypatch):
    """Through run_date: the incident carries the pipeline id and business date like any other."""
    from detection.engine import run_date
    import sqlite3
    db = tmp_path / "wh.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE t (x)")
    con.execute("INSERT INTO t VALUES (1)")
    con.commit()
    con.close()
    monkeypatch.setenv("DETECTION_AIRFLOW_SNAPSHOT",
                       str(snap(tmp_path, {"p": {"paused": False, "latest_run": FAILED}})))
    inc = run_date([Manifest("p", (), "t", "")], dt.date(2026, 11, 27),
                   Context(tmp_path, db, C.load_config(), now=NOW))
    assert [(i.pipeline_id, i.check_id, i.severity, i.business_date) for i in inc] == [
        ("p", "run_health.airflow_run_failed", "critical", "2026-11-27")]
