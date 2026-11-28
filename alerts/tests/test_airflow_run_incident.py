"""A failed Airflow run, as detection reports it, is never auto-healed and DOES raise PIPELINE BROKE.

The incident is produced by detection's own run_health check from a snapshot fixture (no docker),
stored exactly as detection stores it, then judged by the unchanged orchestrator and alert code."""
from __future__ import annotations

import dataclasses
import datetime as dt
import json
import sqlite3

from alerts.alerter import BROKE
from alerts.tests.test_alerts import PFX, _emls, _read, mk  # noqa: F401
from detection import config as C
from detection.checks.context import Context
from detection.checks.run_health import check_airflow
from detection.incidents import to_incident
from detection.manifest import Manifest
from healing.incidents import pick_next_incident
from orchestrator.watcher import is_eligible, pick_eligible

NOW = dt.datetime(2026, 11, 28, 14, 0, tzinfo=dt.timezone.utc)


def _failed_run_incident(tmp_path, monkeypatch):
    p = tmp_path / "airflow_runs.json"
    p.write_text(json.dumps({"written_at": NOW.isoformat(), "source": "airflow", "dags": {"toy": {
        "paused": False, "latest_run": {"run_id": "scheduled__2026-08-13T00:00:00+00:00",
                                        "logical_date": "2026-08-13T00:00:00+00:00", "state": "failed",
                                        "start": None, "end": None, "failed_tasks": ["load"]}}}}))
    monkeypatch.setenv("DETECTION_AIRFLOW_SNAPSHOT", str(p))
    [f] = check_airflow(Manifest("toy", (), "t", ""), Context(tmp_path, tmp_path / "wh.db", C.load_config(), now=NOW))
    return to_incident(f, "toy", dt.date(2026, 8, 14), NOW.isoformat(timespec="seconds"))


def _store(toy, inc):
    row = dataclasses.asdict(inc)
    con = sqlite3.connect(toy.db)
    cols = [r[1] for r in con.execute("PRAGMA table_info(incidents)")]
    con.execute(f"INSERT INTO incidents ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
                [row[c] for c in cols])
    con.commit()
    con.close()
    return {k: row[k] for k in cols}


def test_failed_airflow_run_is_not_auto_healed(toy, tmp_path, monkeypatch):
    inc = _failed_run_incident(tmp_path, monkeypatch)
    assert (inc.check_id, inc.severity) == ("run_health.airflow_run_failed", "critical")
    row = _store(toy, inc)
    assert not is_eligible(row, PFX)                               # orchestrator allowlist / never run_health.*
    picked = pick_eligible(toy.settings, PFX)
    assert picked is None or picked["incident_id"] != inc.incident_id
    nxt = pick_next_incident(toy.settings)                          # `python -m healing heal-next`
    assert nxt is None or nxt["incident_id"] != inc.incident_id


def test_failed_airflow_run_raises_pipeline_broke(toy, tmp_path, monkeypatch, mk):  # noqa: F811
    a = mk()                                                        # seeds what is already on record
    inc = _failed_run_incident(tmp_path, monkeypatch)
    _store(toy, inc)
    assert a.scan_new_incidents() == 1 and a.scan_new_incidents() == 0
    [m] = [_read(p) for p in _emls(mk.outbox)]
    assert " ".join(m["Subject"].split()) == f"[pipeline-agent] {BROKE}: toy run_health.airflow_run_failed (2026-08-14)"
    assert inc.incident_id in m.get_payload()
