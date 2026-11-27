"""At-most-once ledger, one row per incident (its own SQLite file; detection's DB is never touched)."""
from __future__ import annotations

import datetime as dt
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
  incident_id TEXT PRIMARY KEY, trigger TEXT NOT NULL, proposal_id TEXT, recipient TEXT,
  backend TEXT, sent_at TEXT NOT NULL, outcome TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""
SEEDED = "seeded"


def _now() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


class Ledger:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._con() as con:
            con.executescript(SCHEMA)

    def _con(self):
        return sqlite3.connect(self.path, timeout=10)

    def first_run(self) -> bool:
        with self._con() as con:
            return con.execute("SELECT 1 FROM meta WHERE key='seeded_at'").fetchone() is None

    def seed(self, incident_ids: list) -> int:
        """Mark incidents already on record as seen so they can never alert (backlog guard)."""
        with self._con() as con:
            con.executemany("INSERT OR IGNORE INTO alerts VALUES (?,?,?,?,?,?,?)",
                            [(i, SEEDED, None, None, None, _now(), SEEDED) for i in incident_ids])
            con.execute("INSERT OR REPLACE INTO meta VALUES ('seeded_at', ?)", (_now(),))
        return len(incident_ids)

    def claim(self, incident_id: str, trigger: str, proposal_id: str | None, recipient: str) -> bool:
        """Reserve the incident's single email BEFORE sending (a crash mid-send never causes a re-send)."""
        with self._con() as con:
            cur = con.execute("INSERT OR IGNORE INTO alerts VALUES (?,?,?,?,?,?,?)",
                              (incident_id, trigger, proposal_id, recipient, None, _now(), "pending"))
            return cur.rowcount == 1

    def finish(self, incident_id: str, backend: str, outcome: str) -> None:
        with self._con() as con:
            con.execute("UPDATE alerts SET backend=?, outcome=?, sent_at=? WHERE incident_id=?",
                        (backend, outcome, _now(), incident_id))

    def seen(self, incident_id: str) -> bool:
        with self._con() as con:
            return con.execute("SELECT 1 FROM alerts WHERE incident_id=?", (incident_id,)).fetchone() is not None

    def rows(self, include_seeded: bool = False) -> list[dict]:
        with self._con() as con:
            con.row_factory = sqlite3.Row
            q = "SELECT * FROM alerts" + ("" if include_seeded else f" WHERE trigger<>'{SEEDED}'")
            return [dict(r) for r in con.execute(q + " ORDER BY sent_at DESC, incident_id")]

    def seeded_count(self) -> int:
        with self._con() as con:
            return con.execute(f"SELECT COUNT(*) FROM alerts WHERE trigger='{SEEDED}'").fetchone()[0]
