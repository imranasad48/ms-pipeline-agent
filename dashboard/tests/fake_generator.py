"""Stand-in for agent/generate_pipeline.py in tests (no LLM, no network).

Same CLI shape: ... --data-root <dir> --brief "<text>" (--data-root is required here, so
the dashboard must always pass it). Runs in the repo root (cwd) like the real agent and reads
the pipeline id from the 'Pipeline id: <id>' line the dashboard adds to the brief.

--mode ok           write a tiny valid pipeline (reads ${DATA_ROOT}/crm/customers_*.csv) and succeed
--mode report_fail  write partial files, print 'FINAL STATUS: FAILED ...', exit 0
--mode crash        write partial files, exit 3
--mode sleep        write partial files, then sleep (for cancel / timeout / restart tests)
--mode bad_load     valid files whose load() raises
--mode touch        succeed but also edit an existing manifest (must be refused + restored)
--mode table_clash  succeed but target an existing warehouse table
--mode no_status    write valid files but never print FINAL STATUS
"""
import argparse
import os
import re
import sys
import time
from pathlib import Path

PIPELINE = '''import csv, glob, json, os, sqlite3

TABLE = "{table}"

def _stage(n):
    d = os.environ.get("STAGING_DIR", "/tmp/{pid}")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, n)

def extract():
    rows = []
    for f in sorted(glob.glob(os.path.join(os.environ["DATA_ROOT"], "crm", "customers_*.csv"))):
        with open(f, newline="") as fh:
            rows += list(csv.DictReader(fh))
    json.dump(rows, open(_stage("rows.json"), "w"))

def transform():
    rows = json.load(open(_stage("rows.json")))
    json.dump([{{"id": r["id"], "name": r["name"]}} for r in rows], open(_stage("t.json"), "w"))

def load():
    {load_body}
    rows = json.load(open(_stage("t.json")))
    con = sqlite3.connect(os.environ["WAREHOUSE_DB"])
    con.execute(f"DROP TABLE IF EXISTS {{TABLE}}")
    con.execute(f"CREATE TABLE {{TABLE}} (id TEXT, name TEXT)")
    con.executemany(f"INSERT INTO {{TABLE}} VALUES (?,?)", [(r["id"], r["name"]) for r in rows])
    con.commit(); con.close()

def validate():
    con = sqlite3.connect(os.environ["WAREHOUSE_DB"])
    n = con.execute(f"SELECT COUNT(*) FROM {{TABLE}}").fetchone()[0]
    print(f"[validate] {{'PASS' if n > 0 else 'FAIL'}} has_rows")
'''

MANIFEST = '''manifest_version: 1
pipeline_id: {pid}
brief: "fake"
sources:
  - id: crm_customers
    format: csv
    path_pattern: "${{DATA_ROOT}}/crm/customers_*.csv"
steps:
  - {{id: extract, file: dags/{pid}/pipeline.py, function: extract}}
  - {{id: transform, file: dags/{pid}/pipeline.py, function: transform}}
  - {{id: load, file: dags/{pid}/pipeline.py, function: load}}
  - {{id: validate, file: dags/{pid}/pipeline.py, function: validate}}
validation_checks:
  - {{id: has_rows, type: row_count, description: "table not empty", params: {{}}}}
destination: {{type: sqlite, table: {table}, path: "${{WAREHOUSE_DB}}"}}
version: 1
'''


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="ok")
    ap.add_argument("--brief", required=True)
    ap.add_argument("--data-root", required=True)
    a = ap.parse_args()
    print("fake generator starting")
    print(f"DATA-ROOT: {a.data_root}")
    print("BRIEF-BEGIN")
    print(a.brief)
    print("BRIEF-END")
    print("peeking at error_manifest.json would be withheld from the log")
    pid = re.search(r"Pipeline id: ([a-z][a-z0-9_]*)", a.brief).group(1)
    table = "customers" if a.mode == "table_clash" else f"{pid}_t"
    load_body = 'raise RuntimeError("disk full (fake)")' if a.mode == "bad_load" else "pass"
    root = Path.cwd()
    (root / "dags" / pid).mkdir(parents=True, exist_ok=True)
    (root / "dags" / pid / "__init__.py").write_text("")
    (root / "dags" / pid / "pipeline.py").write_text(PIPELINE.format(pid=pid, table=table, load_body=load_body))
    sys.stdout.flush()
    if a.mode in ("report_fail", "crash", "sleep"):
        (root / "manifests" / f"{pid}.yaml").write_text("partial: true\n")
        if a.mode == "report_fail":
            print("FINAL STATUS: FAILED could not infer a key")
            return 0
        if a.mode == "crash":
            print("Traceback: boom", file=sys.stderr)
            return 3
        for i in range(600):
            print(f"working {i}", flush=True)
            time.sleep(0.1)
        return 0
    (root / "dags" / f"{pid}_dag.py").write_text(f"# fake DAG for {pid}\n")
    (root / "manifests" / f"{pid}.yaml").write_text(MANIFEST.format(pid=pid, table=table))
    if a.mode == "touch":
        victim = [m for m in sorted((root / "manifests").glob("*.yaml")) if m.stem != pid][0]
        victim.write_text(victim.read_text() + "# edited by fake\n")
    if a.mode != "no_status":
        print("FINAL STATUS: SUCCESS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
