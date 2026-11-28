"""Governance page: contracts, autonomy policy (single source of truth), audit trail, actor logging."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from dashboard.__main__ import build
from dashboard.data import REPO, Paths
from dashboard.governance import assemble_governance, load_audit, render_governance
from dashboard.tests.test_server import _req, live  # noqa: F401  (fixture re-use)
from healing.proposals import reject_proposal
from orchestrator.watcher import ALL_PROPOSAL_STATUSES, load_config


def _paths(tmp_path, proposals_index: Path, manifests: Path = REPO / "manifests") -> Paths:
    return Paths(manifests=manifests, warehouse=tmp_path / "none.db", incidents=tmp_path / "none.db",
                 proposals=proposals_index, eval_summary=tmp_path / "none.md", site=tmp_path / "site",
                 orch_status=tmp_path / "st.json")


def _mk_proposal(base: Path, pid: str, status: str, verdict: str, archived=False, **extra):
    d = (base / "_archive" if archived else base) / pid
    d.mkdir(parents=True)
    (d / "proposal.md").write_text(f"# Proposal {pid}\n\n**Verdict: {verdict}**\n\n## Diagnosis\n\nbecause\n")
    idx = base / "index.json"
    cur = json.loads(idx.read_text()) if idx.exists() else {}
    cur[pid] = {"status": status, "pipeline_id": "toy", "check_id": "duplicates.key:id",
                "incident_id": "inc_" + pid[-4:], "decision": "FIX",
                "created_at": "2026-11-28T01:00:00", **extra}
    idx.write_text(json.dumps(cur))
    return d


def test_governance_renders_every_manifest(tmp_path):
    g = assemble_governance(_paths(tmp_path, tmp_path / "index.json"))
    html = render_governance(g)
    manifests = sorted((REPO / "manifests").glob("*.yaml"))
    assert len(g["contracts"]) == len(manifests) >= 5
    assert html.count('<details class="card">') == len(manifests)
    for c in g["contracts"]:
        assert c["pipeline_id"] in html
        for chk in c["checks"]:
            assert chk["id"] in html
    assert "Autonomy policy" in html and "Audit trail" in html
    assert "setInterval" not in html and "refresh" not in html.lower().replace("no auto-refresh", "")


def test_allowlist_shown_equals_orchestrator_constant(tmp_path):
    html = render_governance(assemble_governance(_paths(tmp_path, tmp_path / "index.json")))
    allow = load_config()["auto_heal_prefixes"]
    assert allow
    for p in allow:
        assert f"<code>{p}*</code>" in html
    for s in ALL_PROPOSAL_STATUSES:
        assert f"<code>{s}</code>" in html
    assert "run_health." in html and "auto-heal: no" in html


def test_audit_includes_archived_and_failed_validation(tmp_path):
    _mk_proposal(tmp_path, "p-aaaa0001", "FAILED_VALIDATION", "FAILED_VALIDATION")
    _mk_proposal(tmp_path, "p-aaaa0002", "REJECTED", "READY_FOR_APPROVAL", archived=True,
                 reject_reason="smoke test", decided_by="alice", decided_via="dashboard",
                 decided_at="2026-11-28T02:00:00")
    _mk_proposal(tmp_path, "p-aaaa0003", "READY_FOR_APPROVAL", "READY_FOR_APPROVAL")
    (tmp_path / "_archive" / "p-orphan01").mkdir()          # on disk, not in the index
    g = assemble_governance(_paths(tmp_path, tmp_path / "index.json"))
    ids = [r["proposal_id"] for r in g["audit"]]
    assert {"p-aaaa0001", "p-aaaa0002", "p-aaaa0003", "p-orphan01"} <= set(ids)
    c = g["audit_counts"]
    assert (c["total"], c["failed_validation"], c["rejected"], c["awaiting"], c["applied"]) == (4, 1, 1, 1, 0)
    html = render_governance(g)
    assert "p-aaaa0001" in html and "FAILED_VALIDATION" in html
    assert "alice (dashboard)" in html and "smoke test" in html


def test_missing_actor_renders_not_recorded(tmp_path):
    _mk_proposal(tmp_path, "p-old00001", "APPLIED", "READY_FOR_APPROVAL")     # legacy: no decided_by
    rows = load_audit(tmp_path / "index.json")
    assert rows[0]["decided_by"] == "not recorded" and rows[0]["decided_at"] == "not recorded"
    html = render_governance(assemble_governance(_paths(tmp_path, tmp_path / "index.json")))
    assert "not recorded" in html


def test_reject_records_actor_channel_and_timestamp(toy, monkeypatch):
    from healing.heal import run_heal
    from healing.tests.conftest import good_agent
    monkeypatch.setenv("PIPELINE_AGENT_USER", "reviewer1")
    case, _ = run_heal(toy.settings, "inc_aaaa1111", agent_fn=good_agent)
    reject_proposal(toy.settings, case.proposal_id, "not now", channel="cli")
    idx = json.loads(toy.settings.index_path.read_text())[case.proposal_id]
    assert idx["decided_by"] == "reviewer1" and idx["decided_via"] == "cli" and idx["decided_at"]
    rej = json.loads((toy.settings.proposals_dir / "_archive" / case.proposal_id / "rejection.json").read_text())
    assert rej["decided_by"] == "reviewer1" and rej["decided_via"] == "cli" and rej["reason"] == "not now"
    row = {r["proposal_id"]: r for r in load_audit(toy.settings.index_path)}[case.proposal_id]
    assert (row["decided_by"], row["decided_via"], row["reason"]) == ("reviewer1", "cli", "not now")


def test_actor_falls_back_to_os_user(monkeypatch):
    import getpass
    from healing.proposals import decision_record
    monkeypatch.delenv("PIPELINE_AGENT_USER", raising=False)
    monkeypatch.setattr(getpass, "getuser", lambda: "osuser")
    r = decision_record("dashboard")
    assert (r["decided_by"], r["decided_via"]) == ("osuser", "dashboard") and r["decided_at"]


def test_dashboard_reject_and_approve_log_channel_dashboard(live, monkeypatch):  # noqa: F811
    toy, pid, port = live
    status, _, _ = _req(port, "POST", "/reject", {"proposal_id": pid, "reason": "nope"})
    assert status == 303
    idx = json.loads(toy.settings.index_path.read_text())[pid]
    assert idx["decided_via"] == "dashboard" and idx["decided_by"]
    _, _, html = _req(port, "GET", "/governance")
    assert pid in html and "nope" in html and "(dashboard)" in html


def test_governance_route_nav_and_no_refresh(live):  # noqa: F811
    _, _, port = live
    status, _, html = _req(port, "GET", "/governance")
    assert status == 200 and "setInterval" not in html and "http-equiv" not in html
    assert 'href="/"' in html
    _, _, main = _req(port, "GET", "/")
    assert 'href="/governance"' in main and 'id="live-region"' in main


def test_static_build_includes_governance(tmp_path):
    p = _paths(tmp_path, tmp_path / "index.json")
    out = build(p)
    assert out.exists() and (p.site / "governance.html").exists()
    assert "governance.html" in out.read_text()
    assert "index.html" in (p.site / "governance.html").read_text()
