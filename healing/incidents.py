"""Read-only views over detection's incident DB for the healing CLI (no writes here)."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from .proposals import list_proposals
from .settings import Settings

# Incidents about the live warehouse, not the data feeding it: nothing a pipeline patch can heal.
SKIP_PREFIX = "run_health."


def _rows(db: Path, sql: str, args=()) -> list:
    if not Path(db).exists():
        return []
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in con.execute(sql, args)]
    finally:
        con.close()


def open_incidents(settings: Settings, healable_only: bool = False) -> list:
    """Open incidents, oldest first (business date, then detection time, then id).
    ``healable_only``: warn/critical, excluding run_health.*"""
    where = "status='open'"
    if healable_only:
        where += f" AND severity IN ('warn','critical') AND check_id NOT LIKE '{SKIP_PREFIX}%'"
    return _rows(settings.incidents_db,
                 f"SELECT * FROM incidents WHERE {where} "
                 "ORDER BY business_date, detected_at, incident_id")


LIVE_STATUSES = ("READY_FOR_APPROVAL", "ESCALATED", "APPLIED")


def incidents_with_proposals(settings: Settings, statuses=LIVE_STATUSES) -> set:
    """Incident ids that already have a proposal in one of ``statuses``."""
    return {p["incident_id"] for p in list_proposals(settings)
            if p.get("incident_id") and p.get("status") in statuses}


def pick_next_incident(settings: Settings) -> dict | None:
    """The incident ``heal-next`` works on: oldest open warn/critical, skipping run_health.*.
    Incidents that already have a live (not rejected) proposal are passed over, so repeated
    heal-next calls move through the queue instead of redoing the same one."""
    busy = incidents_with_proposals(settings)
    for inc in open_incidents(settings, healable_only=True):
        if inc["incident_id"] not in busy:
            return inc
    return None


def explain_pick(settings: Settings) -> str:
    """Human-readable account of what heal-next would pick and why (used by --dry-run)."""
    cand = open_incidents(settings, healable_only=True)
    allopen = open_incidents(settings)
    pick = pick_next_incident(settings)
    lines = [f"open incidents: {len(allopen)}; healable (warn/critical, not run_health.*): {len(cand)}"]
    if pick is None:
        return "\n".join(lines + ["nothing to heal."])
    lines += [f"would heal: {pick['incident_id']}",
              f"  {pick['severity']}  {pick['pipeline_id']} / {pick['source_id']}  {pick['check_id']}",
              f"  business date {pick['business_date']}, detected {pick['detected_at']}",
              "  why: oldest by (business_date, detected_at) among healable open incidents"
              " without a live proposal",
              f"  then runs: python -m healing heal --incident-id {pick['incident_id']}"]
    return "\n".join(lines)


def open_incident_summary(settings: Settings) -> dict:
    """{'total', 'by_severity', 'by_check_kind', 'healable', 'rows'} for ``list --open-incidents``."""
    rows = open_incidents(settings)
    by_prop = {}
    for p in list_proposals(settings):
        if p.get("incident_id"):
            by_prop[p["incident_id"]] = p.get("status", "")
    for r in rows:
        r["proposal"] = by_prop.get(r["incident_id"], "")
    sev, kind = {}, {}
    for r in rows:
        sev[r["severity"]] = sev.get(r["severity"], 0) + 1
        k = r["check_id"].split(":", 1)[0]
        kind[k] = kind.get(k, 0) + 1
    healable = [r for r in rows if r["severity"] in ("warn", "critical")
                and not r["check_id"].startswith(SKIP_PREFIX)]
    return {"total": len(rows), "by_severity": sev, "by_check_kind": kind,
            "healable": len(healable), "rows": rows}
