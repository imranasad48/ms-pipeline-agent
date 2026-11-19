"""Toy repo: one tiny pipeline whose source CSV has a renamed column (amount -> amt)."""
from __future__ import annotations

import json
import sqlite3
import textwrap
from pathlib import Path

import pytest
import yaml

from healing.settings import Settings

PIPELINE = textwrap.dedent('''
    import csv, glob, os, sqlite3

    def _dr(): return os.environ["DATA_ROOT"]
    def _stage(n): return os.path.join(os.environ["STAGING_DIR"], n)

    def extract():
        rows = []
        for f in sorted(glob.glob(os.path.join(_dr(), "crm/toy_*.csv"))):
            with open(f, newline="") as fh:
                r = csv.DictReader(fh)
                missing = {"id", "amount"} - set(r.fieldnames)
                if missing:
                    raise ValueError(f"missing columns {sorted(missing)}")
                rows += list(r)
        import json; json.dump(rows, open(_stage("rows.json"), "w"))

    def transform():
        import json
        rows = json.load(open(_stage("rows.json")))
        json.dump([{"id": r["id"], "amount": float(r["amount"])} for r in rows],
                  open(_stage("t.json"), "w"))

    def load():
        import json
        rows = json.load(open(_stage("t.json")))
        con = sqlite3.connect(os.environ["WAREHOUSE_DB"])
        con.execute("DROP TABLE IF EXISTS toy")
        con.execute("CREATE TABLE toy (id TEXT PRIMARY KEY, amount REAL)")
        con.executemany("INSERT INTO toy VALUES (?,?)", [(r["id"], r["amount"]) for r in rows])
        con.commit(); con.close()

    def validate():
        con = sqlite3.connect(os.environ["WAREHOUSE_DB"])
        r = {"has_rows": con.execute("SELECT COUNT(*) FROM toy").fetchone()[0] > 0,
             "amount_positive": con.execute("SELECT COUNT(*) FROM toy WHERE amount<=0").fetchone()[0] == 0}
        for k, ok in r.items():
            print(f"[validate] {'PASS' if ok else 'FAIL'} {k}")
        if not all(r.values()):
            raise AssertionError("Validation failed")
''')

COLS = {"id": "string", "amount": "float64"}


def _source(sid, prefix):
    return {"id": sid, "format": "csv", "path_pattern": f"${{DATA_ROOT}}/crm/{prefix}_*.csv",
            "expected_schema": dict(COLS),
            "observed": {"files": 1, "total_rows": 3, "null_rates": {"id": 0.0, "amount": 0.0}}}


MANIFEST = {
    "manifest_version": 1, "pipeline_id": "toy", "version": 1,
    "sources": [_source("toy_csv", "toy"), _source("toyb_csv", "toyb")],
    "steps": [{"id": s, "file": "dags/toy/pipeline.py", "function": s, "description": s}
              for s in ("extract", "transform", "load", "validate")],
    "destination": {"type": "sqlite", "table": "toy", "path": "${WAREHOUSE_DB}"},
}


class Toy:
    def __init__(self, root: Path):
        self.repo = root / "repo"
        self.data = root / "data"
        (self.repo / "dags/toy").mkdir(parents=True)
        (self.repo / "manifests").mkdir()
        (self.repo / "dags/toy/pipeline.py").write_text(PIPELINE)
        (self.repo / "dags/toy/__init__.py").write_text("")
        (self.repo / "manifests/toy.yaml").write_text(yaml.safe_dump(MANIFEST, sort_keys=False))
        (self.data / "crm").mkdir(parents=True)
        (self.data / "crm/toy_2026-08-14.csv").write_text("id,amt\na,1.5\nb,2.5\nc,3.5\n")
        (self.data / "crm/toyb_2026-08-14.csv").write_text("id,amount\nx,1\ny,2\nz,3\n")
        (self.data / "crm/error_manifest.json").write_text("ANSWER KEY")
        self.db = root / "incidents.db"
        con = sqlite3.connect(self.db)
        con.executescript(open(Path(__file__).parents[2] / "detection/incidents.py").read()
                          .split('SCHEMA = """')[1].split('"""')[0])
        self._add(con, "inc_aaaa1111", "toy_csv", "schema.renamed:amount->amt", "warn",
                  ["crm/toy_2026-08-14.csv"])
        self._add(con, "inc_bbbb2222", "toyb_csv", "nulls.spike:amount", "warn",
                  ["crm/toyb_2026-08-14.csv"])
        con.commit(); con.close()
        self.settings = Settings(repo_root=self.repo, data_root=self.data, incidents_db=self.db,
                                 proposals_dir=self.repo / "proposals",
                                 warehouse_db=root / "none.db")

    @staticmethod
    def _add(con, iid, source, check, sev, files):
        con.execute("INSERT INTO incidents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (iid, "2026-09-01T00:00:00", "2026-08-14", "toy", source, check, "schema",
                     sev, "{}", "{}", json.dumps(files), "open"))


def good_agent(settings, case):
    """Fake agent: maps the renamed column back, writes diagnosis + decision."""
    p = case.staging / "dags/toy/pipeline.py"
    p.write_text(p.read_text().replace(
        "            r = csv.DictReader(fh)\n",
        "            r = csv.DictReader(fh)\n"
        "            r.fieldnames = ['amount' if n == 'amt' else n for n in r.fieldnames]\n"))
    _out(case, "FIX", "Vendor renamed amount to amt.")
    return {"ok": True, "error": None}


def _out(case, decision, why):
    (case.staging / "agent_out/diagnosis.md").write_text(f"Root cause: {why}\n")
    (case.staging / "agent_out/decision.json").write_text(
        json.dumps({"decision": decision, "root_cause": why, "attempts": 1}))


@pytest.fixture
def toy(tmp_path) -> Toy:
    return Toy(tmp_path)
