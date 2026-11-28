"""Approve / reject HTTP handlers against a toy proposal (fake agent, no LLM)."""
from __future__ import annotations

import http.client
import sys
import sqlite3
import threading
import urllib.parse

import pytest

from dashboard.data import Paths
from dashboard.jobs import GenConfig, JobManager
from dashboard.server import make_server
from dashboard.tests.fake_airflow import FakeAirflow
from healing.heal import run_heal
from healing.proposals import list_proposals
from healing.tests.conftest import good_agent


@pytest.fixture
def live(toy, tmp_path):
    case, verdict = run_heal(toy.settings, "inc_aaaa1111", agent_fn=good_agent)
    assert verdict == "READY_FOR_APPROVAL"
    s = toy.settings
    paths = Paths(manifests=s.repo_root / "manifests", warehouse=tmp_path / "none.db",
                  incidents=s.incidents_db, proposals=s.index_path,
                  eval_summary=tmp_path / "none.md", site=tmp_path / "site",
                  orch_status=tmp_path / "st.json", history=tmp_path / "history.jsonl")
    jobs = JobManager(GenConfig(repo_root=s.repo_root, data_root=tmp_path / "data", warehouse=tmp_path / "none.db",
                                jobs_dir=tmp_path / "jobs", history=tmp_path / "history.jsonl",
                                generator=[sys.executable, "-c", "raise SystemExit('generator not configured')"]),
                     FakeAirflow())
    srv = make_server(paths, s, 0, jobs=jobs)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield toy, case.proposal_id, srv.server_address[1]
    srv.shutdown()
    srv.server_close()


def _req(port, method, path, body=None, headers=None):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    c.request(method, path, body=urllib.parse.urlencode(body) if body else None,
              headers={"Content-Type": "application/x-www-form-urlencoded", **(headers or {})})
    r = c.getresponse()
    data = r.read().decode()
    c.close()
    return r.status, r.getheader("Location"), data


def _inc_status(toy, iid):
    con = sqlite3.connect(toy.db)
    try:
        return con.execute("SELECT status FROM incidents WHERE incident_id=?", (iid,)).fetchone()[0]
    finally:
        con.close()


def test_page_shows_ready_card_with_buttons_diff_validation_and_refresh(live):
    toy, pid, port = live
    status, _, html = _req(port, "GET", "/")
    assert status == 200
    assert "Awaiting your approval" in html and pid in html
    assert 'action="/approve"' in html and 'action="/reject"' in html
    assert 'id="live-region"' in html and "DOMParser" in html and "location.reload" not in html
    assert "Diagnosis" in html and "Proposed change" in html and 'class="add"' in html
    assert "Validation (before vs after)" in html and "<table>" in html


def test_approve_applies_and_resolves_incident(live):
    toy, pid, port = live
    live_pipeline = toy.repo / "dags/toy/pipeline.py"
    before = live_pipeline.read_text()
    status, loc, _ = _req(port, "POST", "/approve", {"proposal_id": pid, "confirm": "apply"})
    assert status == 303 and loc.startswith("/?msg=Applied")
    assert live_pipeline.read_text() != before                       # the patch landed
    assert {p["proposal_id"]: p["status"] for p in list_proposals(toy.settings)}[pid] == "APPLIED"
    assert _inc_status(toy, "inc_aaaa1111") == "resolved_pending_verification"
    _, _, html = _req(port, "GET", "/")                              # next refresh shows it
    assert "Awaiting your approval" not in html
    assert "resolved_pending_verification" in html and "APPLIED" in html


def test_reject_archives_with_reason(live):
    toy, pid, port = live
    status, loc, _ = _req(port, "POST", "/reject", {"proposal_id": pid, "reason": "not now"})
    assert status == 303 and "Rejected" in urllib.parse.unquote_plus(loc)
    p = {p["proposal_id"]: p for p in list_proposals(toy.settings)}[pid]
    assert p["status"] == "REJECTED" and p["reject_reason"] == "not now"
    assert _inc_status(toy, "inc_aaaa1111") == "open"                # rejection leaves it open
    assert (toy.settings.proposals_dir / "_archive" / pid / "rejection.json").exists()


def test_reject_requires_reason(live):
    toy, pid, port = live
    _, loc, _ = _req(port, "POST", "/reject", {"proposal_id": pid, "reason": "  "})
    assert "err=1" in loc
    assert {p["proposal_id"]: p["status"] for p in list_proposals(toy.settings)}[pid] == "READY_FOR_APPROVAL"


def test_double_approve_is_refused_not_crashing(live):
    toy, pid, port = live
    _req(port, "POST", "/approve", {"proposal_id": pid, "confirm": "apply"})
    status, loc, _ = _req(port, "POST", "/approve", {"proposal_id": pid, "confirm": "apply"})
    assert status == 303 and "err=1" in loc and "Refused" in urllib.parse.unquote_plus(loc)
    _, _, html = _req(port, "GET", loc)
    assert "Refused" in html


def test_unknown_proposal_and_cross_origin_and_routes(live):
    toy, pid, port = live
    _, loc, _ = _req(port, "POST", "/approve", {"proposal_id": "p-nope", "confirm": "apply"})
    assert "err=1" in loc
    status, _, _ = _req(port, "POST", "/approve", {"proposal_id": pid}, {"Origin": "http://evil.example"})
    assert status == 403
    assert {p["proposal_id"]: p["status"] for p in list_proposals(toy.settings)}[pid] == "READY_FOR_APPROVAL"
    assert _req(port, "GET", "/nope")[0] == 404
    assert _req(port, "POST", "/nope", {})[0] == 404


def test_flash_message_is_escaped(live):
    _, _, port = live
    _, _, html = _req(port, "GET", "/?msg=%3Cscript%3Ealert(1)%3C/script%3E")
    assert "<script>alert(1)" not in html


def test_approve_refused_while_generation_job_active_reject_still_allowed(live, tmp_path):
    import json
    import os
    toy, pid, port = live
    d = tmp_path / "jobs" / "g-20261128-010101-abcd"                # same tmp_path as the fixture's jobs dir
    d.mkdir(parents=True)
    (d / "job.json").write_text(json.dumps({"job_id": d.name, "pipeline_id": "newpipe",
                                            "status": "generating", "pid": os.getpid()}))
    _, _, html = _req(port, "GET", "/")
    card = html.split("Awaiting your approval", 1)[1].split("</section>", 1)[0]
    assert "approve after it finishes" in card and 'action="/approve"' not in card
    assert 'action="/reject"' in card and "/jobs/g-20261128-010101-abcd" in card
    status, loc, _ = _req(port, "POST", "/approve", {"proposal_id": pid, "confirm": "apply"})
    assert status == 303 and "err=1" in loc
    assert "approve after it finishes" in urllib.parse.unquote_plus(loc)
    assert {p["proposal_id"]: p["status"] for p in list_proposals(toy.settings)}[pid] == "READY_FOR_APPROVAL"
    assert _inc_status(toy, "inc_aaaa1111") == "open"
    status, loc, _ = _req(port, "POST", "/reject", {"proposal_id": pid, "reason": "not now"})
    assert "err=1" not in loc
    assert {p["proposal_id"]: p["status"] for p in list_proposals(toy.settings)}[pid] == "REJECTED"
