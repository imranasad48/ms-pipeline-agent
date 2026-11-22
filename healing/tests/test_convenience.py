"""heal-next selection, open-incident summary, and the sanctioned status write on apply."""
from __future__ import annotations

import json
import sqlite3

from detection.incidents import PENDING_VERIFICATION, mark_resolved_pending_verification
from healing.heal import run_heal
from healing.incidents import (explain_pick, open_incident_summary, open_incidents,
                               pick_next_incident)
from healing.proposals import apply_proposal, update_index

from .conftest import good_agent


def _status(toy, iid):
    con = sqlite3.connect(toy.db)
    try:
        return con.execute("SELECT status FROM incidents WHERE incident_id=?", (iid,)).fetchone()[0]
    finally:
        con.close()


def _add(toy, iid, check, sev, date="2026-08-14", status="open"):
    con = sqlite3.connect(toy.db)
    con.execute("INSERT INTO incidents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (iid, "2026-09-01T00:00:00", date, "toy", "toy_csv", check, "x", sev,
                 "{}", "{}", json.dumps(["crm/toy_2026-08-14.csv"]), status))
    con.commit()
    con.close()


def test_pick_is_oldest_open_warn_or_critical(toy):
    _add(toy, "inc_old", "nulls.spike:amount", "critical", date="2026-08-01")
    assert pick_next_incident(toy.settings)["incident_id"] == "inc_old"


def test_pick_skips_run_health_info_and_non_open(toy):
    _add(toy, "inc_a_rh", "run_health.table_missing", "critical", date="2026-07-01")
    _add(toy, "inc_b_info", "schema.new_column:x", "info", date="2026-07-02")
    _add(toy, "inc_c_done", "nulls.spike:amount", "warn", date="2026-07-03",
         status=PENDING_VERIFICATION)
    pick = pick_next_incident(toy.settings)
    assert pick["incident_id"] in {"inc_aaaa1111", "inc_bbbb2222"}      # the fixture's own two


def test_pick_passes_over_incidents_with_a_live_proposal(toy):
    first = pick_next_incident(toy.settings)["incident_id"]
    update_index(toy.settings, "p-1", incident_id=first, status="READY_FOR_APPROVAL")
    assert pick_next_incident(toy.settings)["incident_id"] != first
    update_index(toy.settings, "p-1", status="REJECTED")                # rejected -> eligible again
    assert pick_next_incident(toy.settings)["incident_id"] == first


def test_pick_returns_none_when_nothing_healable(toy):
    con = sqlite3.connect(toy.db)
    con.execute("UPDATE incidents SET status='closed'")
    con.commit()
    con.close()
    assert pick_next_incident(toy.settings) is None
    assert "nothing to heal" in explain_pick(toy.settings)


def test_explain_pick_names_the_incident(toy):
    text = explain_pick(toy.settings)
    assert pick_next_incident(toy.settings)["incident_id"] in text
    assert "oldest" in text


def test_open_incident_summary_counts_and_proposal_column(toy):
    _add(toy, "inc_rh", "run_health.table_missing", "critical")
    update_index(toy.settings, "p-9", incident_id="inc_aaaa1111", status="ESCALATED")
    s = open_incident_summary(toy.settings)
    assert s["total"] == 3 and s["healable"] == 2
    assert s["by_severity"] == {"warn": 2, "critical": 1}
    assert {r["incident_id"]: r["proposal"] for r in s["rows"]}["inc_aaaa1111"] == "ESCALATED"
    assert len(open_incidents(toy.settings, healable_only=True)) == 2


def test_apply_marks_incident_resolved_pending_verification(toy):
    case, _ = run_heal(toy.settings, "inc_aaaa1111", agent_fn=good_agent)
    assert _status(toy, "inc_aaaa1111") == "open"          # a mere proposal changes nothing
    apply_proposal(toy.settings, case.proposal_id)
    assert _status(toy, "inc_aaaa1111") == PENDING_VERIFICATION
    assert _status(toy, "inc_bbbb2222") == "open"          # other incidents untouched


def test_sanctioned_write_only_touches_open_rows_status(toy):
    assert mark_resolved_pending_verification(toy.db, "inc_bbbb2222") is True
    assert mark_resolved_pending_verification(toy.db, "inc_bbbb2222") is False   # no longer open
    assert mark_resolved_pending_verification(toy.db, "inc_missing") is False
    assert mark_resolved_pending_verification(toy.db.with_name("nope.db"), "x") is False
    con = sqlite3.connect(toy.db)
    assert con.execute("SELECT observed_json FROM incidents WHERE incident_id='inc_bbbb2222'"
                       ).fetchone()[0] == "{}"
    con.close()
