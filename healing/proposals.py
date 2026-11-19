"""Proposal index, diffs, proposal.md, and apply / reject mechanics."""
from __future__ import annotations

import datetime as dt
import difflib
import json
import shutil
from pathlib import Path

import yaml

from .case import sha256
from .settings import Settings
from .verdict import READY, table_rows

APPLIED, REJECTED = "APPLIED", "REJECTED"


# ------------------------------------------------------------------ index
def _read_index(settings: Settings) -> dict:
    p = settings.index_path
    return json.loads(p.read_text()) if p.exists() else {}


def update_index(settings: Settings, proposal_id: str, **fields) -> None:
    settings.proposals_dir.mkdir(parents=True, exist_ok=True)
    idx = _read_index(settings)
    idx.setdefault(proposal_id, {}).update(fields)
    idx[proposal_id]["updated_at"] = dt.datetime.now().isoformat(timespec="seconds")
    settings.index_path.write_text(json.dumps(idx, indent=1, sort_keys=True))


def list_proposals(settings: Settings) -> list:
    return [{"proposal_id": k, **v} for k, v in sorted(_read_index(settings).items())]


def proposal_dir(settings: Settings, proposal_id: str) -> Path:
    for base in (settings.proposals_dir, settings.proposals_dir / "_archive"):
        if (base / proposal_id).is_dir():
            return base / proposal_id
    raise SystemExit(f"unknown proposal {proposal_id}")


# ------------------------------------------------------------------- diff
def changed_files(settings: Settings, root: Path) -> list:
    """Staged files whose content differs from the live repo file."""
    meta = json.loads((root / "case.json").read_text())
    return [r for r in meta["live_files"]
            if (root / "staging" / r).read_bytes() != (settings.repo_root / r).read_bytes()]


def unified_diff(settings: Settings, root: Path) -> str:
    out = []
    for r in changed_files(settings, root):
        a = (settings.repo_root / r).read_text().splitlines(keepends=True)
        b = (root / "staging" / r).read_text().splitlines(keepends=True)
        out += difflib.unified_diff(a, b, f"live/{r}", f"staged/{r}")
    return "".join(out)


def manifest_policy(root: Path, pipeline_id: str) -> dict:
    """If the staged manifest changed: version must increase and change_log must grow."""
    rel = f"manifests/{pipeline_id}.yaml"
    old = yaml.safe_load((root / "originals" / rel).read_text())
    new = yaml.safe_load((root / "staging" / rel).read_text())
    if old == new:
        return {"policy:manifest_versioned": "pass", "note": "manifest unchanged"}
    ok = (new.get("version", 0) > old.get("version", 0)
          and len(new.get("change_log") or []) > len(old.get("change_log") or []))
    return {"policy:manifest_versioned": "pass" if ok else "fail",
            "note": f"version {old.get('version')} -> {new.get('version')}"}


# ------------------------------------------------------------ proposal.md
def render_proposal(case, verdict: str, decision: dict | None, diagnosis: str,
                    diff: str, validation: dict | None, notes: list) -> str:
    inc = case.incident
    L = [f"# Proposal {case.proposal_id}", "",
         f"**Verdict: {verdict}**", "",
         f"- Incident: `{inc['incident_id']}`  ({inc['severity']})  `{inc['check_id']}`",
         f"- Pipeline: `{case.pipeline_id}`   Source: `{inc['source_id']}`   "
         f"Business date: {inc['business_date']}",
         f"- Affected files: {', '.join(json.loads(inc['affected_files_json']))}",
         f"- Agent decision: {decision['decision'] if decision else 'none'}"
         + (f"  (attempts: {decision.get('attempts')})" if decision else ""), ""]
    for n in notes:
        L.append(f"> {n}")
    L += ["", "## Diagnosis", "", diagnosis.strip() or "_no diagnosis written_", ""]
    if verdict != "ESCALATED":
        L += ["## Proposed change", "", "```diff", diff.rstrip() or "(no changes)", "```", ""]
    if validation and "rows" in validation:
        v = validation
        L += ["## Validation (deterministic, same case data, before vs after)", "",
              "| check | before | after | note |", "|---|---|---|---|"]
        shown = [r for r in v["rows"] if r[3] or r[2] != "pass"]
        L += [f"| {k} | {b} | {a} | {n} |" for k, b, a, n in shown]
        hidden = len(v["rows"]) - len(shown)
        if hidden:
            L.append(f"| _{hidden} other checks_ | | pass | _unchanged or newly passing; full list in validation.json_ |")
        L += ["", f"Targets: {', '.join(v['target_keys']) or 'none'}  |  "
              f"regressions: {', '.join(v['regressions']) or 'none'}  |  "
              f"unmasked failures: {', '.join(v['unmasked_failures']) or 'none'}", "",
              "_Scope note: the case runs on one business date's files with other incidents' "
              "files removed. Checks that also fail on a control run without this incident's "
              "files are marked 'scope artifact' and do not block approval._", ""]
    L += ["## Decision", ""]
    if verdict == READY:
        L.append(f"Approve:  `python -m healing apply {case.proposal_id}`")
        L.append(f"Decline:  `python -m healing reject {case.proposal_id} --reason \"...\"`")
    elif verdict == "ESCALATED":
        L.append("No patch proposed. Needs a human decision (see diagnosis). "
                 f"Close with: `python -m healing reject {case.proposal_id} --reason \"...\"`")
    else:
        L.append("Do NOT apply. Validation failed; see table. "
                 f"Archive with: `python -m healing reject {case.proposal_id} --reason \"...\"`")
    return "\n".join(L) + "\n"


# ---------------------------------------------------------- apply / reject
def apply_proposal(settings: Settings, proposal_id: str, force_drift: bool = False) -> list:
    """Copy changed staged files over the live ones (human-run)."""
    root = proposal_dir(settings, proposal_id)
    status = _read_index(settings).get(proposal_id, {}).get("status")
    if status != READY:
        raise SystemExit(f"{proposal_id} is {status}; only {READY} proposals can be applied")
    meta = json.loads((root / "case.json").read_text())
    changed = changed_files(settings, root)
    if not changed:
        raise SystemExit("nothing to apply: staged files equal live files")
    for r in changed:   # refuse if live moved on since the case was assembled
        if sha256(settings.repo_root / r) != meta["original_sha256"][r] and not force_drift:
            raise SystemExit(f"live {r} changed since this proposal was made; re-run heal "
                             "(or --force-drift)")
    for r in changed:
        shutil.copy2(root / "staging" / r, settings.repo_root / r)
    update_index(settings, proposal_id, status=APPLIED, applied_files=changed)
    return changed


def reject_proposal(settings: Settings, proposal_id: str, reason: str) -> Path:
    """Archive the proposal under proposals/_archive/ with the reason."""
    root = proposal_dir(settings, proposal_id)
    if root.parent.name == "_archive":
        raise SystemExit(f"{proposal_id} is already archived")
    if _read_index(settings).get(proposal_id, {}).get("status") == APPLIED:
        raise SystemExit(f"{proposal_id} was already applied")
    (root / "rejection.json").write_text(json.dumps(
        {"reason": reason, "rejected_at": dt.datetime.now().isoformat(timespec="seconds")}))
    dest = settings.proposals_dir / "_archive" / proposal_id
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(root), str(dest))
    update_index(settings, proposal_id, status=REJECTED, reject_reason=reason)
    return dest
