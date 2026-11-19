"""The full `heal` flow: case -> baseline -> agent -> validation -> proposal."""
from __future__ import annotations

import json
import shutil

import anyio

from detection import config as dcfg

from . import agent as agent_mod
from .case import Case, assemble_case, sha256
from .checks import detection_failures, run_pipeline
from .proposals import (manifest_policy, render_proposal, unified_diff, update_index,
                        changed_files)
from .settings import Settings
from .verdict import (ESCALATED, FAILED, FAIL, PASS, READY, decide, merge_detection,
                      select_targets, table_rows)


def _flat(res: dict) -> dict:
    return dict(res["results"])


def _live_snapshot(settings: Settings, case: Case) -> dict:
    """Hashes of live files + the answer-key-free guard targets, to prove the agent stayed in staging."""
    snap = {r: sha256(settings.repo_root / r) for r in case.live_files}
    for sub in ("dags", "manifests", "detection"):
        for p in sorted((settings.repo_root / sub).rglob("*")):
            if p.is_file() and "__pycache__" not in p.parts and p.suffix in {".py", ".yaml"}:
                snap[str(p.relative_to(settings.repo_root))] = sha256(p)
    return snap


def run_baseline(settings: Settings, case: Case) -> dict:
    """Step 2: unpatched pipeline on the case data + detection checks."""
    res = run_pipeline(settings, case.originals, case.pipeline_id, case.steps,
                       case.scoped_root, case.work, "baseline")
    det = detection_failures(settings, case.originals / f"manifests/{case.pipeline_id}.yaml",
                             case.business_date)
    # CONTROL: same scope minus the incident's own files. Whatever fails here is an
    # artifact of the case being a partial slice, not of the incident or the patch.
    control_root = case.work / "control_root"
    shutil.rmtree(control_root, ignore_errors=True)
    shutil.copytree(case.scoped_root, control_root)
    for f in json.loads(case.incident["affected_files_json"]):
        (control_root / f.split("#", 1)[0]).unlink(missing_ok=True)
    ctl = run_pipeline(settings, case.originals, case.pipeline_id, case.steps,
                       control_root, case.work, "control")
    baseline = {"pipeline": res["results"], "errors": res["errors"],
                "detection_failing": sorted(det),
                "control": ctl["results"],
                "scope_artifacts": sorted(k for k, v in ctl["results"].items() if v == FAIL)}
    (case.root / "baseline.json").write_text(json.dumps(baseline, indent=1))
    (case.staging / "case" / "baseline.json").write_text(json.dumps(baseline, indent=1))
    return baseline


def run_validation(settings: Settings, case: Case, baseline: dict) -> dict:
    """Step 4: same checks on the staged (patched) copy; decide the verdict."""
    res = run_pipeline(settings, case.staging, case.pipeline_id, case.steps,
                       case.scoped_root, case.work, "patched")
    mrel = f"manifests/{case.pipeline_id}.yaml"
    after_failing = detection_failures(
        settings, (case.staging if (case.staging / mrel).exists() else case.originals) / mrel,
        case.business_date)
    inc = case.incident
    det_key = f"{inc['source_id']}:{inc['check_id']}"
    db, da = merge_detection(baseline["pipeline"], after_failing, set(baseline["detection_failing"]))
    before, after = {**baseline["pipeline"], **db}, {**res["results"], **da}
    pol = manifest_policy(case.root, case.pipeline_id)
    pol_status = pol.pop("policy:manifest_versioned")
    before["policy:manifest_versioned"], after["policy:manifest_versioned"] = PASS, pol_status
    targets = select_targets(before, f"detect:{det_key}")
    d = decide(before, after, targets, baseline.get("scope_artifacts", []))
    if pol_status != PASS:
        d["verdict"] = FAILED
        d["regressions"] = sorted(d["regressions"] + ["policy:manifest_versioned"])
    out = {**d, "rows": table_rows(before, after, targets, baseline.get("scope_artifacts", [])), "before": before, "after": after,
           "errors_after": res["errors"], "errors_before": baseline["errors"]}
    (case.root / "validation.json").write_text(json.dumps(out, indent=1))
    return out


def run_heal(settings: Settings, incident_id: str, agent_fn=None) -> tuple[Case, str]:
    """Run the whole flow. `agent_fn(settings, case)` is injectable (tests use a fake)."""
    case = assemble_case(settings, incident_id)
    print(f"[1/5] case assembled: {case.proposal_id}  ({case.pipeline_id}, {case.incident['check_id']})")
    baseline = run_baseline(settings, case)
    failing = [k for k, v in baseline["pipeline"].items() if v == FAIL]
    print(f"[2/5] baseline: failing={failing}  errors={baseline['errors']}")

    before_snap = _live_snapshot(settings, case)
    agent_fn = agent_fn or (lambda s, c: anyio.run(agent_mod.run_agent, s, c))
    print("[3/5] agent session ...")
    session = agent_fn(settings, case)
    violations = [k for k, v in _live_snapshot(settings, case).items() if before_snap.get(k) != v]

    decision = agent_mod.read_decision(case)
    diag_path = case.staging / "agent_out" / "diagnosis.md"
    diagnosis = diag_path.read_text() if diag_path.exists() else ""
    diff = unified_diff(settings, case.root)
    notes, validation = [], None

    if violations:
        verdict = FAILED
        notes.append(f"INTEGRITY VIOLATION: live files changed during the agent session: {violations}")
    elif not session.get("ok") or decision is None:
        verdict = FAILED
        notes.append(f"Agent session produced no decision ({session.get('error') or 'no decision.json'}).")
    elif decision["decision"] == "ESCALATE":
        verdict = ESCALATED
        if changed_files(settings, case.root):
            verdict = FAILED
            notes.append("Agent chose ESCALATE but also modified staged files.")
        (case.root / "validation.json").write_text(json.dumps({"skipped": "escalated"}))
        print("[4/5] validation skipped (ESCALATED)")
    else:
        print("[4/5] validating patched pipeline ...")
        validation = run_validation(settings, case, baseline)
        verdict = validation["verdict"]
        if not diff.strip():
            verdict = FAILED
            notes.append("Agent said FIX but staged files are identical to live.")
    proposal = render_proposal(case, verdict, decision, diagnosis, diff, validation, notes)
    (case.root / "proposal.md").write_text(proposal)
    update_index(settings, case.proposal_id, incident_id=incident_id,
                 pipeline_id=case.pipeline_id, check_id=case.incident["check_id"],
                 status=verdict, decision=(decision or {}).get("decision"),
                 created_at=json.loads((case.root / "case.json").read_text())["created_at"])
    print(f"[5/5] verdict: {verdict}  -> {case.root / 'proposal.md'}")
    return case, verdict
