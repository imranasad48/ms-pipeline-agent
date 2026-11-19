"""Healing arm: run the healing agent on one incident per scenario (LLM calls -> cost/turn summary).

Incident choice (deterministic): earliest business_date, then pipeline/source/check id, among
warn/critical incidents that are not environmental (run_health.*: about the live warehouse).
Proposals are written under results/healing_runs/ so evaluation never pollutes healing/proposals/.
"""
from __future__ import annotations

import datetime as dt
import io
import json
import sqlite3
import time
from contextlib import redirect_stdout
from pathlib import Path

import anyio

from . import settings as S
from .builder import scenario_dir
from .scorer import first_injection_target, load_mapping

EXPECTED_DOMAIN = "ESCALATED"
# The live repo mutates whenever a human approves a heal (e.g. bce02a2 absorbed the 3PL vehicleId
# rename), which would hand the agent an already-fixed pipeline. Evaluation therefore runs against a
# pinned SNAPSHOT of dags/ + manifests/: this git ref (default = parent of the first real heal),
# overlaid with files that are untracked in the working tree (pipelines with no history yet).
DEFAULT_PIPELINE_REF = "bce02a2^"
SYNTHETIC_PIPELINE = {"domain_temp_excursion": ("sensor_readings", "iot_readings")}


def build_eval_repo(ref: str, dest: Path | None = None) -> Path:
    """Snapshot dags/ + manifests/ at `ref` (+ untracked overlay) into results/eval_repo/<sha>/."""
    import shutil
    import subprocess
    sha = subprocess.run(["git", "rev-parse", "--short", ref], cwd=S.REPO_ROOT, capture_output=True,
                         text=True, check=True).stdout.strip()
    dest = dest or (S.RESULTS_DIR / "eval_repo" / sha)
    if (dest / ".complete").exists():
        return dest
    shutil.rmtree(dest, ignore_errors=True)
    dest.mkdir(parents=True)
    arc = subprocess.run(["git", "archive", ref, "dags", "manifests"], cwd=S.REPO_ROOT, capture_output=True, check=True)
    subprocess.run(["tar", "-x", "-C", str(dest)], input=arc.stdout, check=True)
    untracked = subprocess.run(["git", "ls-files", "--others", "--exclude-standard", "dags", "manifests"],
                               cwd=S.REPO_ROOT, capture_output=True, text=True, check=True).stdout.split()
    for rel in untracked:
        if "__pycache__" in rel or not (S.REPO_ROOT / rel).is_file():
            continue
        (dest / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(S.REPO_ROOT / rel, dest / rel)
    (dest / ".complete").write_text(sha)
    return dest


def pick_incident(db: Path, env_prefixes: tuple) -> dict | None:
    if not Path(db).exists():
        return None
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute(
        "SELECT * FROM incidents WHERE severity IN ('critical','warn') "
        "ORDER BY business_date, pipeline_id, source_id, check_id")]
    con.close()
    rows = [r for r in rows if not r["check_id"].startswith(env_prefixes)]
    return rows[0] if rows else None


def manufacture_incident(sdir: Path, failure_type: str, seed: int) -> Path:
    """Write a clearly-SYNTHETIC incident (own db; never mixed into the detection db)."""
    from detection.incidents import SCHEMA, make_incident_id
    pipeline_id, source_id = SYNTHETIC_PIPELINE[failure_type]
    tgt = first_injection_target(failure_type, seed)
    check_id = "synthetic.operator_report"
    iid = make_incident_id(tgt["business_date"], pipeline_id, source_id, check_id)
    db = sdir / "incidents_synthetic.db"
    db.unlink(missing_ok=True)
    con = sqlite3.connect(db)
    con.executescript(SCHEMA)
    con.execute("INSERT INTO incidents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (
        iid, dt.datetime.now().isoformat(timespec="seconds"), tgt["business_date"], pipeline_id,
        source_id, check_id, "synthetic", "warn",
        json.dumps({"note": "SYNTHETIC incident manufactured by the evaluation harness"}),
        json.dumps({"synthetic": True, "report": "operator flags unusual temperature readings in this file; "
                    "no detection check fired"}), json.dumps([tgt["target"]]), "open"))
    con.commit()
    con.close()
    return db


class _Recorder:
    """Wraps healing.agent.query to capture SDK ResultMessage cost/turns without touching the agent."""

    def __init__(self):
        self.cost = None
        self.turns = None
        self.tool_calls = 0

    def wrap(self, real):
        rec = self

        async def query(*a, **k):
            async for msg in real(*a, **k):
                if hasattr(msg, "total_cost_usd"):
                    rec.cost, rec.turns = msg.total_cost_usd, getattr(msg, "num_turns", None)
                for b in getattr(msg, "content", None) or []:
                    if getattr(b, "name", None):
                        rec.tool_calls += 1
                yield msg
        return query


def run_one(failure_type: str, seed: int, incident_db: Path, incident_id: str, sdir: Path,
            repeat: int, no_manifest: bool, expected: str, synthetic: bool,
            eval_repo: Path | None = None, pipeline_ref: str | None = None) -> dict:
    from healing import agent as agent_mod
    from healing.heal import run_heal
    from healing.settings import Settings

    s = Settings()
    s.data_root, s.incidents_db = (sdir / "data").resolve(), incident_db.resolve()
    s.proposals_dir = (S.RESULTS_DIR / "healing_runs" / failure_type / str(seed)).resolve()
    s.include_manifest = not no_manifest
    if eval_repo is not None:
        s.repo_root = eval_repo.resolve()
    rec = _Recorder()
    real_query = agent_mod.query
    agent_mod.query = rec.wrap(real_query)
    t0 = time.time()
    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            case, verdict = run_heal(s, incident_id)
        err = None
    except BaseException as e:   # record, don't abort the whole sweep
        case, verdict, err = None, "ERROR", f"{type(e).__name__}: {e}"
    finally:
        agent_mod.query = real_query
    wall = round(time.time() - t0, 1)

    decision, validation, mention = None, {}, False
    if case is not None:
        decision = agent_mod.read_decision(case)
        vp = case.root / "validation.json"
        validation = json.loads(vp.read_text()) if vp.exists() else {}
        tp = case.root / "agent_transcript.txt"
        mention = tp.exists() and "error_manifest" in tp.read_text()
    classification = {"FIX": "FIX", "ESCALATE": "ESCALATED"}.get((decision or {}).get("decision"), "NONE")
    regressions = validation.get("regressions", []) + validation.get("unmasked_failures", [])
    if expected == "FIX":
        success = verdict == "READY_FOR_APPROVAL" and not regressions
    else:
        success = verdict == "ESCALATED" and classification == "ESCALATED"
    out = {"failure_type": failure_type, "seed": seed, "repeat": repeat, "no_manifest": no_manifest,
           "expected_classification": expected, "classification": classification, "verdict": verdict,
           "success": success, "regressions": regressions, "attempts": (decision or {}).get("attempts"),
           "wall_seconds": wall, "cost_usd": rec.cost, "turns": rec.turns, "tool_calls": rec.tool_calls,
           "incident_id": incident_id, "synthetic_incident": synthetic, "pipeline_ref": pipeline_ref,
           "proposal_id": case.proposal_id if case else None, "error": err,
           "transcript_mentions_answer_key": mention,
           "root_cause": (decision or {}).get("root_cause")}
    print(f"[heal] {failure_type} run {repeat}{' (no-manifest)' if no_manifest else ''}: "
          f"{classification}/{verdict} success={success} attempts={out['attempts']} "
          f"turns={rec.turns} tool_calls={rec.tool_calls} cost=${rec.cost if rec.cost is not None else 'n/a'} wall={wall}s"
          + (f" ERROR {err}" if err else "") + (" [!] transcript mentions answer key" if mention else ""))
    return out


def run_healing(failure_type: str, seed: int, repeats: int = 1, no_manifest: bool = False,
                pipeline_ref: str = DEFAULT_PIPELINE_REF) -> list:
    mapping = load_mapping()
    env = tuple(mapping.get("environmental_check_prefixes", []))
    sdir = scenario_dir(failure_type, seed)
    kind = mapping["types"][failure_type].get("kind", "data_error")
    expected = EXPECTED_DOMAIN if kind == "domain_event" else "FIX"
    db = sdir / "incidents.db"
    inc = pick_incident(db, env)
    synthetic = False
    if inc is None and failure_type in SYNTHETIC_PIPELINE:
        db, synthetic = manufacture_incident(sdir, failure_type, seed), True
        inc = pick_incident(db, ())
    if inc is None:
        print(f"[heal] {failure_type}: no eligible incident (detection raised nothing actionable); skipped")
        return []
    print(f"[heal] {failure_type}: incident {inc['incident_id']} {inc['business_date']} "
          f"{inc['pipeline_id']}/{inc['check_id']}{' [SYNTHETIC]' if synthetic else ''}")
    eval_repo = build_eval_repo(pipeline_ref) if pipeline_ref else None
    runs = [run_one(failure_type, seed, db, inc["incident_id"], sdir, i + 1, no_manifest, expected, synthetic,
                    eval_repo, pipeline_ref)
            for i in range(repeats)]
    out = S.RESULTS_DIR / "healing"
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{failure_type}__{'no_manifest' if no_manifest else 'manifest'}.json").write_text(json.dumps(runs, indent=1))
    costs = [r["cost_usd"] for r in runs if r["cost_usd"] is not None]
    print(f"[heal] {failure_type}: {sum(r['success'] for r in runs)}/{len(runs)} success; "
          f"turns={[r['turns'] for r in runs]} total cost=${sum(costs):.2f}" if costs else
          f"[heal] {failure_type}: {sum(r['success'] for r in runs)}/{len(runs)} success")
    return runs
