"""No LLM anywhere: a fake agent stands in for the SDK session."""
from __future__ import annotations

import json

import pytest

from healing import verdict as V
from healing.case import _guard, assemble_case
from healing.heal import run_baseline, run_heal
from healing.proposals import (apply_proposal, list_proposals, manifest_policy,
                               reject_proposal, unified_diff)

from .conftest import _out, good_agent

INC = "inc_aaaa1111"


# ---------------------------------------------------------- case assembly
def test_case_assembly_copies_and_scopes(toy):
    case = assemble_case(toy.settings, INC)
    for rel in ("dags/toy/pipeline.py", "dags/toy/__init__.py", "manifests/toy.yaml"):
        assert (case.originals / rel).read_bytes() == (toy.repo / rel).read_bytes()
        assert (case.staging / rel).read_bytes() == (toy.repo / rel).read_bytes()
    assert (case.staging / "case/raw_files/crm/toy_2026-08-14.csv").exists()
    assert (case.scoped_root / "crm/toy_2026-08-14.csv").exists()
    # the other incident's file is quarantined; the answer key is never copied
    assert not (case.scoped_root / "crm/toyb_2026-08-14.csv").exists()
    scope = json.loads((case.staging / "case/scope.json").read_text())
    assert scope["quarantined"] == ["crm/toyb_2026-08-14.csv"]
    assert not list(case.root.rglob("error_manifest*"))
    meta = json.loads((case.root / "case.json").read_text())
    assert meta["steps"] == ["extract", "transform", "load", "validate"]


def test_unknown_incident_exits(toy):
    with pytest.raises(SystemExit):
        assemble_case(toy.settings, "inc_nope")


def test_answer_key_guard():
    with pytest.raises(PermissionError):
        _guard("/x/data/error_manifest.json")
    with pytest.raises(PermissionError):
        _guard("/x/data/error_manifest.xlsx")


# ---------------------------------------------------------------- baseline
def test_baseline_records_failure_and_skips_downstream(toy):
    case = assemble_case(toy.settings, INC)
    b = run_baseline(toy.settings, case)
    assert b["pipeline"]["step:extract"] == "fail"
    assert b["pipeline"]["step:transform"] == "skipped"
    assert b["pipeline"]["step:validate"] == "skipped"
    assert "missing columns" in b["errors"]["step:extract"]
    assert any("schema.renamed" in k for k in b["detection_failing"])
    assert (case.root / "baseline.json").exists()


def test_control_run_identifies_scope_artifacts(toy):
    # without the incident's file the toy has no rows -> has_rows fails regardless of any patch
    case = assemble_case(toy.settings, INC)
    b = run_baseline(toy.settings, case)
    assert b["control"]["step:extract"] == "pass"
    assert b["scope_artifacts"] == ["step:validate", "validate:has_rows"]
    assert (case.work / "control_root").exists()


def test_baseline_on_healthy_data_passes_everything(toy):
    (toy.data / "crm/toy_2026-08-14.csv").write_text("id,amount\na,1.5\nb,2.5\n")
    case = assemble_case(toy.settings, INC)
    b = run_baseline(toy.settings, case)
    assert set(b["pipeline"].values()) == {"pass"}
    assert b["pipeline"]["validate:has_rows"] == "pass"


# ----------------------------------------------------------- verdict logic
P, F, S = V.PASS, V.FAIL, V.SKIPPED


def test_verdict_ready():
    before = {"step:extract": F, "step:load": S, "validate:a": S, "validate:b": P}
    after = {"step:extract": P, "step:load": P, "validate:a": P, "validate:b": P}
    d = V.decide(before, after, V.select_targets(before, None))
    assert d["verdict"] == V.READY and d["target_keys"] == ["step:extract"]


def test_verdict_target_still_failing():
    before = {"step:extract": F}
    assert V.decide(before, {"step:extract": F}, ["step:extract"])["verdict"] == V.FAILED


def test_verdict_regression_blocks():
    before = {"step:extract": F, "validate:b": P}
    after = {"step:extract": P, "validate:b": F}
    d = V.decide(before, after, ["step:extract"])
    assert d["verdict"] == V.FAILED and d["regressions"] == ["validate:b"]


def test_verdict_masked_failure_surfacing_blocks():
    before = {"step:extract": F, "step:transform": S}
    after = {"step:extract": P, "step:transform": F}
    d = V.decide(before, after, ["step:extract"])
    assert d["verdict"] == V.FAILED and d["unmasked_failures"] == ["step:transform"]


def test_verdict_scope_artifact_is_excused_but_listed():
    before = {"step:extract": F, "validate:partial": S}
    after = {"step:extract": P, "validate:partial": F}
    d = V.decide(before, after, ["step:extract"], scope_artifacts=["validate:partial"])
    assert d["verdict"] == V.READY and d["scope_artifacts"] == ["validate:partial"]
    # ...but a regression is never excused
    d = V.decide({"step:extract": F, "validate:partial": P},
                 {"step:extract": P, "validate:partial": F}, ["step:extract"], ["validate:partial"])
    assert d["verdict"] == V.FAILED


def test_verdict_never_ran_at_baseline_but_fails_now_blocks():
    d = V.decide({"step:extract": F}, {"step:extract": P, "validate:new_check": F}, ["step:extract"])
    assert d["verdict"] == V.FAILED and d["unmasked_failures"] == ["validate:new_check"]


def test_verdict_no_targets_cannot_be_ready():
    assert V.decide({"step:a": P}, {"step:a": P}, [])["verdict"] == V.FAILED


def test_select_targets_falls_back_to_detection_check():
    before = {"step:a": P, "detect:s:nulls.spike:x": F}
    assert V.select_targets(before, "detect:s:nulls.spike:x") == ["detect:s:nulls.spike:x"]


def test_detection_merge_marks_new_firing_as_regression():
    b, a = V.merge_detection({}, {"s:new"}, {"s:old"})
    assert b == {"detect:s:old": F, "detect:s:new": P}
    d = V.decide(b, a, [])
    assert d["regressions"] == ["detect:s:new"]


# ------------------------------------------------------------ end-to-end
def test_heal_ready_for_approval(toy):
    case, verdict = run_heal(toy.settings, INC, agent_fn=good_agent)
    assert verdict == V.READY
    v = json.loads((case.root / "validation.json").read_text())
    assert v["after"]["step:extract"] == "pass" and v["after"]["validate:has_rows"] == "pass"
    text = (case.root / "proposal.md").read_text()
    assert "READY_FOR_APPROVAL" in text and "```diff" in text and "| step:extract |" in text
    assert list_proposals(toy.settings)[0]["status"] == V.READY
    # live repo untouched until apply
    assert "amt" not in (toy.repo / "dags/toy/pipeline.py").read_text()


def test_heal_agent_that_does_not_fix_fails_validation(toy):
    def lazy(settings, case):
        p = case.staging / "dags/toy/pipeline.py"
        p.write_text(p.read_text() + "\n# no-op\n")
        _out(case, "FIX", "did nothing useful")
        return {"ok": True, "error": None}
    _, verdict = run_heal(toy.settings, INC, agent_fn=lazy)
    assert verdict == V.FAILED


def test_heal_escalate_has_no_patch(toy):
    def esc(settings, case):
        _out(case, "ESCALATE", "looks like a real event")
        return {"ok": True, "error": None}
    case, verdict = run_heal(toy.settings, INC, agent_fn=esc)
    assert verdict == V.ESCALATED
    assert "Diagnosis" in (case.root / "proposal.md").read_text()
    with pytest.raises(SystemExit):
        apply_proposal(toy.settings, case.proposal_id)


def test_heal_agent_crash_is_failed_not_exception(toy):
    _, verdict = run_heal(toy.settings, INC, agent_fn=lambda s, c: {"ok": False, "error": "boom"})
    assert verdict == V.FAILED


def test_heal_detects_agent_writing_to_live_files(toy):
    def naughty(settings, case):
        (toy.repo / "dags/toy/pipeline.py").write_text("# hijacked\n")
        return good_agent(settings, case)
    case, verdict = run_heal(toy.settings, INC, agent_fn=naughty)
    assert verdict == V.FAILED
    assert "INTEGRITY VIOLATION" in (case.root / "proposal.md").read_text()


def test_manifest_change_requires_version_bump(toy):
    def edits_manifest(settings, case):
        good_agent(settings, case)
        m = case.staging / "manifests/toy.yaml"
        m.write_text(m.read_text() + "\n# touched\nnote: x\n")
        return {"ok": True, "error": None}
    case, verdict = run_heal(toy.settings, INC, agent_fn=edits_manifest)
    assert verdict == V.FAILED
    assert manifest_policy(case.root, "toy")["policy:manifest_versioned"] == "fail"


# --------------------------------------------------------- apply / reject
def test_apply_copies_staged_over_live(toy):
    case, _ = run_heal(toy.settings, INC, agent_fn=good_agent)
    diff = unified_diff(toy.settings, case.root)
    assert "+            r.fieldnames" in diff
    files = apply_proposal(toy.settings, case.proposal_id)
    assert files == ["dags/toy/pipeline.py"]
    assert "'amt'" in (toy.repo / "dags/toy/pipeline.py").read_text()
    assert list_proposals(toy.settings)[0]["status"] == "APPLIED"
    with pytest.raises(SystemExit):           # cannot apply twice
        apply_proposal(toy.settings, case.proposal_id)


def test_apply_refuses_failed_proposal(toy):
    case, _ = run_heal(toy.settings, INC, agent_fn=lambda s, c: {"ok": False, "error": "x"})
    with pytest.raises(SystemExit):
        apply_proposal(toy.settings, case.proposal_id)


def test_apply_refuses_when_live_drifted(toy):
    case, _ = run_heal(toy.settings, INC, agent_fn=good_agent)
    live = toy.repo / "dags/toy/pipeline.py"
    live.write_text(live.read_text() + "\n# someone else edited\n")
    with pytest.raises(SystemExit):
        apply_proposal(toy.settings, case.proposal_id)
    assert "someone else" in live.read_text()


def test_reject_archives_with_reason(toy):
    case, _ = run_heal(toy.settings, INC, agent_fn=good_agent)
    dest = reject_proposal(toy.settings, case.proposal_id, "not now")
    assert dest.parent.name == "_archive" and not case.root.exists()
    assert json.loads((dest / "rejection.json").read_text())["reason"] == "not now"
    row = list_proposals(toy.settings)[0]
    assert row["status"] == "REJECTED" and row["reject_reason"] == "not now"
    assert "amt" not in (toy.repo / "dags/toy/pipeline.py").read_text()
