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
            "by_source": sources,
            "date_range": ([str(df["business_date"].min()), str(df["business_date"].max())]
                           if len(df) else None),
            "recent_open": newest.to_dict("records")}


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


# --------------------------------------------------------------- evaluation
def load_eval_summary(path: Path) -> dict | None:
    """The latest evaluation summary (markdown) split into {'generated', 'markdown'}; None if absent."""
    p = Path(path)
    if not p.exists():
        return None
    text = p.read_text()
    m = re.search(r"generated:\s*(\S+)", text)
    return {"generated": m.group(1) if m else "", "markdown": text}


# ------------------------------------------------------------------ one call
def assemble(paths: Paths | None = None) -> dict:
    """Everything the page needs, as plain data."""
    paths = paths or Paths()
    inc = load_incidents(paths.incidents)
    props = load_proposals(paths.proposals)
    return {"pipelines": load_pipelines(paths.manifests, paths.warehouse),
            "incidents": summarize_incidents(inc),
            "proposals": props, "proposal_counts": summarize_proposals(props),
            "evaluation": load_eval_summary(paths.eval_summary)}
