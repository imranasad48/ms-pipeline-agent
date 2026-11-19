"""Step 1 -- deterministic case assembly (no LLM).

SAFETY: only files named by the incident / manifest are ever copied. The
evaluation answer key (error_manifest.json|.xlsx in the data folder) is never
read, listed or copied; `_guard` refuses any path that mentions it.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import yaml

from detection import config as dcfg
from detection.manifest import load_manifest
from detection.schedule import expected_files

from .settings import Settings


def _guard(path) -> None:
    if "error_manifest" in str(path):
        raise PermissionError(f"healing must never touch the answer key: {path}")


def sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


@dataclass
class Case:
    proposal_id: str
    root: Path                 # proposals/<id>
    incident: dict
    pipeline_id: str
    steps: list                # function names, manifest order
    live_files: list           # repo-relative paths the agent may change
    business_date: dt.date

    @property
    def staging(self) -> Path:
        return self.root / "staging"

    @property
    def originals(self) -> Path:
        return self.root / "originals"

    @property
    def scoped_root(self) -> Path:
        return self.staging / "case" / "data_root"

    @property
    def work(self) -> Path:
        return self.root / "work"


def load_incident(settings: Settings, incident_id: str) -> dict:
    con = sqlite3.connect(settings.incidents_db)
    con.row_factory = sqlite3.Row
    try:
        row = con.execute("SELECT * FROM incidents WHERE incident_id=?", (incident_id,)).fetchone()
        if row is None:
            raise SystemExit(f"incident {incident_id} not found in {settings.incidents_db}")
        inc = dict(row)
        inc["same_day"] = [dict(r) for r in con.execute(
            "SELECT * FROM incidents WHERE business_date=? AND pipeline_id=? AND incident_id<>?",
            (inc["business_date"], inc["pipeline_id"], incident_id))]
        return inc
    finally:
        con.close()


def _clean_rel(f: str) -> str:
    return f.split("#", 1)[0]


def quarantine_files(incident: dict) -> set:
    """Files hit by OTHER warn/critical incidents the same day (excluding the target's files).

    They are left out of the case data root so the target failure is isolated:
    a single failing file elsewhere would otherwise mask whether the fix works.
    """
    mine = set(json.loads(incident["affected_files_json"]))
    out = set()
    for o in incident["same_day"]:
        if o["severity"] == "info":
            continue
        out |= set(json.loads(o["affected_files_json"]))
    return {_clean_rel(f) for f in out - mine}


def build_scoped_root(settings: Settings, det_manifest, business_date: dt.date,
                      dest: Path, quarantine: set) -> dict:
    """Copy the business date's files for the pipeline's sources into `dest`."""
    dcfg_ = dcfg.load_config()
    copied, quarantined = [], []
    for spec in det_manifest.sources:
        for slot in expected_files(spec, business_date, settings.data_root, dcfg_):
            rel = _clean_rel(slot.rel)
            _guard(slot.path)
            if not slot.path.exists():
                continue
            if rel in quarantine:
                quarantined.append(rel)
                continue
            if rel not in copied:
                (dest / rel).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(slot.path, dest / rel)
                copied.append(rel)
    return {"business_date": business_date.isoformat(), "files_copied": len(copied),
            "copied": sorted(copied), "quarantined": sorted(set(quarantined))}


def assemble_case(settings: Settings, incident_id: str, now: dt.datetime | None = None) -> Case:
    """Create proposals/<id>/ with originals, staged copies, scoped data and raw evidence."""
    inc = load_incident(settings, incident_id)
    pid = inc["pipeline_id"]
    mpath = settings.repo_root / "manifests" / f"{pid}.yaml"
    raw_manifest = yaml.safe_load(mpath.read_text())
    steps = []
    for s in raw_manifest["steps"]:
        if s["function"] not in steps:
            steps.append(s["function"])
    live = {f"manifests/{pid}.yaml"}
    for s in raw_manifest["steps"]:
        live.add(s["file"])
        init = Path(s["file"]).parent / "__init__.py"
        if (settings.repo_root / init).exists():
            live.add(str(init))
    live_files = sorted(live)

    stamp = (now or dt.datetime.now()).strftime("%Y%m%d%H%M%S")
    prop_id = f"p-{incident_id.replace('inc_', '')[:8]}-{stamp}"
    root = settings.proposals_dir / prop_id
    case = Case(prop_id, root, inc, pid, steps, live_files,
                dt.date.fromisoformat(inc["business_date"]))
    for rel in live_files:
        src = settings.repo_root / rel
        for base in (case.originals, case.staging):
            (base / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, base / rel)
    case.work.mkdir(parents=True, exist_ok=True)
    (case.staging / "agent_out").mkdir(exist_ok=True)

    det_manifest = load_manifest(mpath, dcfg.load_config())
    scope = build_scoped_root(settings, det_manifest, case.business_date, case.scoped_root,
                              quarantine_files(inc))
    raw_dir = case.staging / "case" / "raw_files"
    for f in json.loads(inc["affected_files_json"]):
        src = settings.data_root / _clean_rel(f)
        _guard(src)
        if src.exists() and src.is_file():
            (raw_dir / _clean_rel(f)).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, raw_dir / _clean_rel(f))

    (case.staging / "case" / "incident.json").write_text(json.dumps(inc, indent=1, default=str))
    (case.staging / "case" / "scope.json").write_text(json.dumps(scope, indent=1))
    (root / "case.json").write_text(json.dumps({
        "proposal_id": prop_id, "incident_id": incident_id, "pipeline_id": pid,
        "business_date": inc["business_date"], "steps": steps, "live_files": live_files,
        "original_sha256": {r: sha256(case.originals / r) for r in live_files},
        "data_root": str(settings.data_root), "scope": scope,
        "created_at": dt.datetime.now().isoformat(timespec="seconds"),
    }, indent=1))
    return case
