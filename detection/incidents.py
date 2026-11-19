"""Findings, incidents, and the SQLite incident store."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

FAMILIES = ("schema", "volume_nulls", "freshness", "run_health")
SEVERITY_ORDER = {"critical": 0, "warn": 1, "info": 2}


@dataclass
class Finding:
    """A pipeline-agnostic check result (one source / check / date)."""

    source_id: str
    check_id: str                 # e.g. "schema.renamed:quantity->qty"
    family: str
    severity: str
    expected: dict
    observed: dict
    files: list = field(default_factory=list)


@dataclass
class Incident:
    """A finding stamped with pipeline, business date and a stable id."""

    incident_id: str
    detected_at: str
    business_date: str
    pipeline_id: str
    source_id: str
    check_id: str
    check_family: str
    severity: str
    expected_json: str
    observed_json: str
    affected_files_json: str
    status: str = "open"


def make_incident_id(business_date, pipeline_id, source_id, check_id) -> str:
    """Deterministic id: same date/pipeline/source/check -> same id on every run."""
    blob = "|".join([str(business_date), pipeline_id, source_id, check_id])
    return "inc_" + hashlib.sha1(blob.encode()).hexdigest()[:16]


def _dump(obj) -> str:
    return json.dumps(obj, sort_keys=True, default=str)


def to_incident(f: Finding, pipeline_id: str, business_date: dt.date,
                detected_at: str) -> Incident:
    return Incident(
        incident_id=make_incident_id(business_date, pipeline_id, f.source_id, f.check_id),
        detected_at=detected_at, business_date=business_date.isoformat(),
        pipeline_id=pipeline_id, source_id=f.source_id, check_id=f.check_id,
        check_family=f.family, severity=f.severity,
        expected_json=_dump(f.expected), observed_json=_dump(f.observed),
        affected_files_json=_dump(sorted(f.files)))


SCHEMA = """
CREATE TABLE IF NOT EXISTS incidents (
    incident_id         TEXT PRIMARY KEY,
    detected_at         TEXT NOT NULL,
    business_date       TEXT NOT NULL,
    pipeline_id         TEXT NOT NULL,
    source_id           TEXT NOT NULL,
    check_id            TEXT NOT NULL,
    check_family        TEXT NOT NULL,
    severity            TEXT NOT NULL CHECK (severity IN ('info','warn','critical')),
    expected_json       TEXT NOT NULL,
    observed_json       TEXT NOT NULL,
    affected_files_json TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'open'
);
CREATE INDEX IF NOT EXISTS ix_incidents_date ON incidents(business_date);
"""

_COLS = ("incident_id detected_at business_date pipeline_id source_id check_id "
         "check_family severity expected_json observed_json affected_files_json status").split()


class IncidentStore:
    """Thin wrapper over incidents.db."""

    def __init__(self, path: Path):
        self.con = sqlite3.connect(path)
        self.con.row_factory = sqlite3.Row
        self.con.executescript(SCHEMA)

    def save(self, incidents: list[Incident]) -> None:
        """Upsert by id. A re-run refreshes evidence but keeps detected_at and status."""
        self.con.executemany(
            f"INSERT INTO incidents ({','.join(_COLS)}) VALUES ({','.join('?' * len(_COLS))}) "
            "ON CONFLICT(incident_id) DO UPDATE SET severity=excluded.severity, "
            "expected_json=excluded.expected_json, observed_json=excluded.observed_json, "
            "affected_files_json=excluded.affected_files_json",
            [tuple(getattr(i, c) for c in _COLS) for i in incidents])
        self.con.commit()

    def for_date(self, business_date: str) -> list[sqlite3.Row]:
        return self.con.execute(
            "SELECT * FROM incidents WHERE business_date=? ORDER BY "
            "CASE severity WHEN 'critical' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END, "
            "pipeline_id, source_id, check_id", (business_date,)).fetchall()

    def close(self) -> None:
        self.con.close()
