"""Eligibility, picking and the watch loop (fake heal/detect functions -- no LLM)."""
from __future__ import annotations

import json
import sqlite3

from healing.heal import run_heal
from healing.proposals import update_index
from healing.tests.conftest import good_agent
from orchestrator.watcher import Orchestrator, is_eligible, load_config, pick_eligible

PFX = load_config()["auto_heal_prefixes"]


def _inc(check, sev="warn", status="open"):
    return {"check_id": check, "severity": sev, "status": status}


def _add(toy, iid, check, sev="warn", date="2026-08-14", status="open"):
    con = sqlite3.connect(toy.db)
    con.execute("INSERT INTO incidents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (iid, "2026-09-01T00:00:00", date, "toy", "toy_csv", check, "x", sev,
                 "{}", "{}", json.dumps(["crm/toy_2026-08-14.csv"]), status))
    con.commit()
    con.close()


def test_default_allowlist():
    assert set(PFX) == {"schema.", "value_range.", "duplicates.", "volume.", "nulls."}


def test_eligibility_rules():
    assert is_eligible(_inc("schema.renamed:a->b"), PFX)
    assert is_eligible(_inc("nulls.spike:x", "critical"), PFX)
    assert not is_eligible(_inc("freshness.late_file"), PFX)           # not on the allowlist
    assert not is_eligible(_inc("referential.orphan:x"), PFX)
    assert not is_eligible(_inc("run_health.table_missing", "critical"), PFX)
    assert not is_eligible(_inc("run_health.load_stale"), ("run_health.",))   # never, even if listed
    assert not is_eligible(_inc("schema.new_column:x", "info"), PFX)   # info is not healable
    assert not is_eligible(_inc("schema.renamed:a->b", status="resolved_pending_verification"), PFX)


def test_pick_oldest_eligible_skipping_others(toy):
    _add(toy, "inc_0old_fresh", "freshness.late_file", date="2026-07-01")   # older but not allowlisted
    _add(toy, "inc_0old_rh", "run_health.table_empty", "critical", date="2026-07-01")
    assert pick_eligible(toy.settings, PFX)["incident_id"] in {"inc_aaaa1111", "inc_bbbb2222"}


def test_pick_business_date_scope(toy):
    _add(toy, "inc_other_day", "schema.renamed:a->b", date="2026-10-20")
    assert pick_eligible(toy.settings, PFX, "2026-10-20")["incident_id"] == "inc_other_day"
    assert pick_eligible(toy.settings, PFX, "2026-01-01") is None


def test_pick_skips_any_proposal_status_and_in_flight(toy):
    first = pick_eligible(toy.settings, PFX)["incident_id"]
    for status in ("READY_FOR_APPROVAL", "ESCALATED", "APPLIED", "FAILED", "REJECTED"):
        update_index(toy.settings, "p-1", incident_id=first, status=status)
        assert pick_eligible(toy.settings, PFX)["incident_id"] != first, status
    update_index(toy.settings, "p-1", incident_id="other")             # no longer blocks `first`
    assert pick_eligible(toy.settings, PFX)["incident_id"] == first
    assert pick_eligible(toy.settings, PFX, in_flight={first})["incident_id"] != first


def _orch(toy, tmp_path, heal_fn, **kw):
    cfg = {"auto_heal_prefixes": PFX, "poll_seconds": 0}
    return Orchestrator(toy.settings, cfg, heal_fn=heal_fn, status_path=tmp_path / "st.json",
                        sleep=lambda s: None, **kw)


def test_loop_heals_each_incident_once_sequentially(toy, tmp_path):
    seen, active = [], []

    def heal(settings, iid):
        assert not active, "two heals overlapped"
        active.append(iid)
        seen.append(iid)
        update_index(settings, f"p-{iid}", incident_id=iid, status="READY_FOR_APPROVAL")
        active.pop()
        return None, "READY_FOR_APPROVAL"

    _orch(toy, tmp_path, heal).run(max_cycles=6)
    assert sorted(seen) == ["inc_aaaa1111", "inc_bbbb2222"]          # exactly once each
    assert json.loads((tmp_path / "st.json").read_text())["state"] == "stopped"


def test_in_flight_incident_not_picked_again(toy, tmp_path):
    o = _orch(toy, tmp_path, None)
    calls = []

    def heal(settings, iid):
        calls.append(iid)
        assert iid in o.in_flight
        assert pick_eligible(settings, PFX, in_flight=o.in_flight)["incident_id"] != iid
        return None, "FAILED"       # FAILED without an index entry: still must not loop within a run

    o.heal_fn = heal
    assert o.heal_one() and o.heal_one()
    assert o.in_flight == set()


def test_heal_crash_does_not_kill_loop(toy, tmp_path):
    def heal(settings, iid):
        raise RuntimeError("boom")
    o = _orch(toy, tmp_path, heal)
    assert o.heal_one() is True            # swallowed, logged
    assert o.in_flight == set()


def test_detection_runs_on_its_interval(toy, tmp_path):
    t = [0.0]
    runs = []
    o = _orch(toy, tmp_path, lambda s, i: (None, "x"), business_date="2026-08-14", detect_every=10,
              detect_fn=lambda d: runs.append(d) or [], )
    o.clock = lambda: t[0]
    o.maybe_detect(); t[0] = 5; o.maybe_detect(); t[0] = 10; o.maybe_detect()
    assert runs == ["2026-08-14", "2026-08-14"]


def test_end_to_end_with_toy_agent(toy, tmp_path):
    """Orchestrator -> real run_heal flow (fake agent) -> READY proposal on disk."""
    o = _orch(toy, tmp_path, lambda s, i: run_heal(s, i, agent_fn=good_agent))
    assert o.heal_one()
    ready = [p for p in __import__("healing.proposals", fromlist=["x"]).list_proposals(toy.settings)
             if p["status"] == "READY_FOR_APPROVAL"]
    assert len(ready) == 1
