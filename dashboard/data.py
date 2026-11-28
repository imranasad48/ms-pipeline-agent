"""Assemble everything the dashboard shows from the repo's own files (read-only).

Each loader takes explicit paths so tests can point it at fixtures; defaults mirror the
repo layout and the same environment variables detection uses.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import yaml

REPO = Path(__file__).resolve().parent.parent
SEVERITIES = ("critical", "warn", "info")


@dataclass
class Paths:
    manifests: Path = field(default_factory=lambda: Path(os.environ.get("DETECTION_MANIFESTS", REPO / "manifests")))
    warehouse: Path = field(default_factory=lambda: Path(os.environ.get("WAREHOUSE_DB", REPO / "warehouse" / "warehouse.db")))
    incidents: Path = field(default_factory=lambda: Path(os.environ.get("DETECTION_DB", REPO / "detection" / "incidents.db")))
    proposals: Path = REPO / "healing" / "proposals" / "index.json"
    eval_summary: Path = REPO / "evaluation" / "results" / "summary.md"
    site: Path = REPO / "dashboard" / "site"
    orch_status: Path = REPO / "orchestrator" / "status.json"


# ---------------------------------------------------------------- pipelines
def _table_state(con: sqlite3.Connection, table: str) -> dict:
    cols = [r[1] for r in con.execute(f'PRAGMA table_info("{table}")')]
    if not cols:
        return {"rows": None, "last_loaded_at": None, "note": "table not in warehouse"}
    rows = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    if "loaded_at" not in cols:
        return {"rows": rows, "last_loaded_at": None, "note": "table has no loaded_at column"}
    last = con.execute(f'SELECT MAX(loaded_at) FROM "{table}"').fetchone()[0]
    return {"rows": rows, "last_loaded_at": last, "note": "" if last else "no rows"}


def load_pipelines(manifests_dir: Path, warehouse_db: Path) -> list[dict]:
    """One row per manifest: name, version, destination table, row count, last loaded_at."""
    con = None
    if Path(warehouse_db).exists():
        con = sqlite3.connect(f"file:{warehouse_db}?mode=ro", uri=True)
    out = []
    try:
        for p in sorted(Path(manifests_dir).glob("*.yaml")):
            m = yaml.safe_load(p.read_text())
            table = (m.get("destination") or {}).get("table", "")
            state = (_table_state(con, table) if con and table
                     else {"rows": None, "last_loaded_at": None, "note": "warehouse not found"})
            out.append({"pipeline_id": m["pipeline_id"], "version": m.get("version", 1),
                        "destination": table, "sources": [s["id"] for s in m.get("sources", [])],
                        "brief": m.get("brief", ""), **state})
    finally:
        if con:
            con.close()
    return out


# ---------------------------------------------------------------- incidents
def load_incidents(db: Path) -> pd.DataFrame:
    """All incidents as a DataFrame (empty frame with the right columns if the DB is absent)."""
    cols = ["incident_id", "detected_at", "business_date", "pipeline_id", "source_id",
            "check_id", "check_family", "severity", "status"]
    if not Path(db).exists():
        return pd.DataFrame(columns=cols)
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return pd.read_sql_query(f"SELECT {', '.join(cols)} FROM incidents", con)
    finally:
        con.close()


def summarize_incidents(df: pd.DataFrame) -> dict:
    """Open-incident breakdowns: by severity, family, source (+ status totals, newest list)."""
    status_counts = df["status"].value_counts().to_dict() if len(df) else {}
    op = df[df["status"] == "open"]

    def counts(col):
        return {str(k): int(v) for k, v in op[col].value_counts().items()} if len(op) else {}

    sev = counts("severity")
    by_source = (op.groupby(["source_id", "severity"]).size().unstack(fill_value=0)
                 if len(op) else pd.DataFrame())
    sources = []
    for sid, row in by_source.iterrows():
        sources.append({"source_id": sid, **{s: int(row.get(s, 0)) for s in SEVERITIES},
                        "total": int(row.sum())})
    sources.sort(key=lambda r: (-r["total"], r["source_id"]))
    newest = op.sort_values(["business_date", "severity"], ascending=[False, True]).head(40)
    return {"total": int(len(df)), "open": int(len(op)), "status_counts": status_counts,
            "by_severity": {s: sev.get(s, 0) for s in SEVERITIES},
            "by_family": dict(sorted(counts("check_family").items(), key=lambda kv: -kv[1])),
            "by_source": sources, "by_pipeline": counts("pipeline_id"),
            "date_range": ([str(df["business_date"].min()), str(df["business_date"].max())]
                           if len(df) else None),
            "recent_open": newest.to_dict("records"),
            "recent_closed": (df[df["status"] != "open"].sort_values(
                ["business_date", "detected_at"], ascending=False).head(10).to_dict("records")
                if len(df) else [])}


INCIDENT_FILTERS = ("severity", "pipeline", "family", "date")
_FILTER_COLUMN = {"severity": "severity", "pipeline": "pipeline_id", "family": "check_family",
                  "date": "business_date"}


def parse_incident_filters(query: dict) -> dict:
    """{'severity': ['warn'], ...} (parse_qs shape) or plain str values -> {name: value}, known keys only."""
    out = {}
    for k in INCIDENT_FILTERS:
        v = query.get(k)
        v = (v[0] if v else "") if isinstance(v, list) else (v or "")
        if str(v).strip():
            out[k] = str(v).strip()
    return out


def filter_incidents(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """Rows of `df` matching every filter exactly (unknown values simply match nothing)."""
    for k, v in filters.items():
        if k in _FILTER_COLUMN and len(df):
            df = df[df[_FILTER_COLUMN[k]].astype(str) == v]
    return df


# ---------------------------------------------------------------- proposals
def load_proposals(index_path: Path) -> list[dict]:
    """Healing proposals from healing/proposals/index.json, newest first."""
    p = Path(index_path)
    idx = json.loads(p.read_text()) if p.exists() else {}
    rows = [{"proposal_id": k, "status": v.get("status", ""), "decision": v.get("decision") or "",
             "pipeline_id": v.get("pipeline_id", ""), "check_id": v.get("check_id", ""),
             "incident_id": v.get("incident_id", ""), "updated_at": v.get("updated_at", ""),
             "note": v.get("reject_reason", "")} for k, v in idx.items()]
    return sorted(rows, key=lambda r: r["updated_at"], reverse=True)


def summarize_proposals(rows: list[dict]) -> dict:
    counts: dict = {}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    return counts


_SECTION = re.compile(r"^## (.+)$", re.M)


def load_proposal_detail(index_path: Path, proposal_id: str) -> dict:
    """Diagnosis / diff / validation table of one proposal, cut out of its proposal.md."""
    out = {"diagnosis": "", "diff": "", "validation": "", "header": ""}
    for base in (Path(index_path).parent, Path(index_path).parent / "_archive"):
        f = base / proposal_id / "proposal.md"
        if f.exists():
            break
    else:
        return out
    text = f.read_text()
    parts = _SECTION.split(text)          # [preamble, title1, body1, title2, body2, ...]
    out["header"] = parts[0]
    for title, body in zip(parts[1::2], parts[2::2]):
        if title.startswith("Diagnosis"):
            out["diagnosis"] = body.strip()
        elif title.startswith("Proposed change"):
            m = re.search(r"```diff\n(.*?)```", body, re.S)
            out["diff"] = m.group(1).rstrip() if m else ""
        elif title.startswith("Validation"):
            out["validation"] = body.strip()
    return out


def load_orchestrator_status(path: Path) -> dict:
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return {}


# --------------------------------------------------------------- evaluation
def load_eval_summary(path: Path) -> dict | None:
    """The latest evaluation summary (markdown) split into {'generated', 'markdown'}; None if absent."""
    p = Path(path)
    if not p.exists():
        return None
    text = p.read_text()
    m = re.search(r"generated:\s*(\S+)", text)
    return {"generated": m.group(1) if m else "", "markdown": text}


# -------------------------------------------------------------- audit trail
NOT_RECORDED = "not recorded"


def _read_json(path: Path) -> dict:
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return {}


def _verdict_of(root: Path | None) -> str:
    if root is None:
        return NOT_RECORDED
    try:
        m = re.search(r"\*\*Verdict: (\S+)\*\*", (root / "proposal.md").read_text())
    except OSError:
        return NOT_RECORDED
    return m.group(1) if m else NOT_RECORDED


def load_audit(index_path: Path) -> list[dict]:
    """Every proposal ever created, newest first: index.json entries plus any proposal
    directory (live or _archive/) the index does not know about. Missing facts stay
    'not recorded' -- nothing is inferred or backfilled."""
    index_path = Path(index_path)
    base = index_path.parent
    idx = _read_json(index_path)
    dirs: dict[str, Path] = {}
    for sub in (base / "_archive", base):          # live dir wins over archive on a clash
        if sub.is_dir():
            for d in sub.iterdir():
                if d.is_dir() and d.name.startswith("p-"):
                    dirs[d.name] = d
    rows = []
    for pid in sorted(set(idx) | set(dirs)):
        v, root = idx.get(pid, {}), dirs.get(pid)
        case = _read_json(root / "case.json") if root else {}
        inc = _read_json(root / "staging/case/incident.json") if root else {}
        rej = _read_json(root / "rejection.json") if root else {}
        dec = _read_json(root / "staging/agent_out/decision.json") if root else {}
        check_id = v.get("check_id") or inc.get("check_id") or ""
        rows.append({
            "proposal_id": pid,
            "created_at": v.get("created_at") or case.get("created_at") or NOT_RECORDED,
            "pipeline_id": v.get("pipeline_id") or case.get("pipeline_id") or NOT_RECORDED,
            "check_id": check_id, "check_kind": check_id.split(":")[0] or NOT_RECORDED,
            "incident_id": v.get("incident_id") or case.get("incident_id") or NOT_RECORDED,
            "decision": v.get("decision") or dec.get("decision") or NOT_RECORDED,
            "verdict": _verdict_of(root),
            "status": v.get("status") or "not in index",
            "decided_by": v.get("decided_by") or rej.get("decided_by") or NOT_RECORDED,
            "decided_via": v.get("decided_via") or rej.get("decided_via") or "",
            "decided_at": v.get("decided_at") or rej.get("decided_at") or rej.get("rejected_at") or NOT_RECORDED,
            "reason": v.get("reject_reason") or rej.get("reason") or "",
            "human_decision": v.get("status") in ("APPLIED", "REJECTED"),
        })
    return sorted(rows, key=lambda r: (r["created_at"] if r["created_at"] != NOT_RECORDED else "", r["proposal_id"]),
                  reverse=True)


def summarize_audit(rows: list[dict]) -> dict:
    n = lambda *st: sum(r["status"] in st for r in rows)  # noqa: E731
    return {"total": len(rows), "applied": n("APPLIED"), "rejected": n("REJECTED"),
            "failed_validation": n("FAILED_VALIDATION", "FAILED"), "escalated": n("ESCALATED"),
            "awaiting": n("READY_FOR_APPROVAL")}



# ------------------------------------------------------------------ one call
def assemble(paths: Paths | None = None) -> dict:
    """Everything the page needs, as plain data."""
    paths = paths or Paths()
    inc = load_incidents(paths.incidents)
    props = load_proposals(paths.proposals)
    status_of = dict(zip(inc["incident_id"], inc["status"])) if len(inc) else {}
    for r in props:
        r["incident_status"] = status_of.get(r["incident_id"], "")
    ready = [{**r, **load_proposal_detail(paths.proposals, r["proposal_id"])}
             for r in props if r["status"] == "READY_FOR_APPROVAL"]
    audit = load_audit(paths.proposals)
    for r in audit:
        r["detail"] = load_proposal_detail(paths.proposals, r["proposal_id"])
    return {"audit": audit, "audit_counts": summarize_audit(audit),
            "ready": ready, "orchestrator": load_orchestrator_status(paths.orch_status),
            "pipelines": load_pipelines(paths.manifests, paths.warehouse),
            "incidents": summarize_incidents(inc),
            "proposals": props, "proposal_counts": summarize_proposals(props),
            "evaluation": load_eval_summary(paths.eval_summary)}
