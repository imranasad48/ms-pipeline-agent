"""Validation when the incident's file is the ONLY file of its source for the day.

The normal control run (baseline minus the incident's files) then has nothing to read and the
extract step fails, so it cannot tell scope artifacts from patch problems. Fallback: rerun the
control with the files *emptied* (zero rows) instead of removed. Real case: the archived
p-099c78b2-* proposals (dedupe of duplicates.key:order_id+line_no on the day's only CRM file).
"""
from __future__ import annotations

import json
import sqlite3

from healing import verdict as V
from healing.case import assemble_case
from healing.heal import run_baseline, run_heal

from .conftest import PIPELINE, _out

INC = "inc_aaaa1111"
DUP_CSV = "id,amount\na,1\nb,2\nb,2\nc,3\nd,4\ne,5\n"      # 'b' duplicated; only 5 distinct rows

# strict extract (like the real pipeline: no input file -> hard failure) + dup/partial-slice checks
STRICT = (PIPELINE
          .replace('    rows = []\n    for f in sorted(glob.glob(os.path.join(_dr(), "crm/toy_*.csv"))):',
                   '    rows = []\n    files = sorted(glob.glob(os.path.join(_dr(), "crm/toy_*.csv")))\n'
                   '    if not files:\n        raise FileNotFoundError("no toy files")\n'
                   '    for f in files:')
          .replace('"amount_positive": con.execute("SELECT COUNT(*) FROM toy WHERE amount<=0").fetchone()[0] == 0}',
                   '"unique_id": con.execute("SELECT COUNT(*)-COUNT(DISTINCT id) FROM toy").fetchone()[0] == 0,\n'
                   '             "min_rows_10": con.execute("SELECT COUNT(*) FROM toy").fetchone()[0] >= 10}')
          .replace('CREATE TABLE toy (id TEXT PRIMARY KEY, amount REAL)', 'CREATE TABLE toy (id TEXT, amount REAL)'))


def _setup(toy, csv=DUP_CSV):
    assert "FileNotFoundError" in STRICT and "unique_id" in STRICT
    (toy.repo / "dags/toy/pipeline.py").write_text(STRICT)
    (toy.data / "crm/toy_2026-08-14.csv").write_text(csv)
    con = sqlite3.connect(toy.db)
    con.execute("UPDATE incidents SET check_id='duplicates.key:id' WHERE incident_id=?", (INC,))
    con.commit(); con.close()


def dedupe_agent(settings, case):
    p = case.staging / "dags/toy/pipeline.py"
    p.write_text(p.read_text().replace("    import json; json.dump(rows, open(_stage(\"rows.json\"), \"w\"))",
                                       "    rows = list({r['id']: r for r in rows}.values())\n"
                                       "    import json; json.dump(rows, open(_stage(\"rows.json\"), \"w\"))"))
    _out(case, "FIX", "Duplicate rows in the feed; dedupe on id.")
    return {"ok": True, "error": None}


def noop_agent(settings, case):
    p = case.staging / "dags/toy/pipeline.py"
    p.write_text(p.read_text() + "\n# no-op\n")
    _out(case, "FIX", "Pretend fix.")
    return {"ok": True, "error": None}


def test_plain_control_cannot_run_so_emptied_control_is_used(toy):
    _setup(toy)
    b = run_baseline(toy.settings, assemble_case(toy.settings, INC))
    assert b["pipeline"]["validate:unique_id"] == "fail"          # the incident's own symptom
    assert b["control_mode"] == "emptied_source"
    assert b["control"]["step:extract"] == "pass"
    # failing with an empty source = not attributable to the patch; unique_id is NOT among them
    assert set(b["scope_artifacts"]) == {"step:validate", "validate:has_rows", "validate:min_rows_10"}
    assert "validate:unique_id" not in b["scope_artifacts"]


def test_good_dedupe_patch_on_only_file_of_the_day_is_ready(toy):
    _setup(toy)
    case, verdict = run_heal(toy.settings, INC, agent_fn=dedupe_agent)
    v = json.loads((case.root / "validation.json").read_text())
    assert verdict == V.READY, v
    assert v["after"]["validate:unique_id"] == "pass"
    assert v["after"]["validate:min_rows_10"] == "fail"           # partial-slice artifact stays red...
    assert "validate:min_rows_10" in v["scope_artifacts"]         # ...and is reported as such
    assert v["regressions"] == [] and v["unmasked_failures"] == []


def test_fallback_still_requires_the_real_target_to_pass(toy):
    _setup(toy)
    _, verdict = run_heal(toy.settings, INC, agent_fn=noop_agent)
    assert verdict == V.FAILED


def test_fallback_still_blocks_regressions(toy):
    _setup(toy)

    def breaking_agent(settings, case):            # dedupes but drops a passing guarantee
        dedupe_agent(settings, case)
        p = case.staging / "dags/toy/pipeline.py"
        p.write_text(p.read_text().replace('"has_rows": con.execute("SELECT COUNT(*) FROM toy").fetchone()[0] > 0',
                                           '"has_rows": False'))
        return {"ok": True, "error": None}

    case, verdict = run_heal(toy.settings, INC, agent_fn=breaking_agent)
    assert verdict == V.FAILED
    assert "validate:has_rows" in json.loads((case.root / "validation.json").read_text())["regressions"]


def test_unknown_format_keeps_strict_behaviour(toy):
    _setup(toy)
    from healing import heal as H
    assert H._emptied_content(toy.data / "crm/x.xlsx") is None
    assert H._emptied_content(toy.data / "crm/toy_2026-08-14.csv") == "id,amount\n"


def test_decide_excuse_targets_unit():
    P, F = V.PASS, V.FAIL
    before = {"validate:dup": F, "validate:partial": F}
    after = {"validate:dup": P, "validate:partial": F}
    strict = V.decide(before, after, ["validate:dup", "validate:partial"], ["validate:partial"])
    assert strict["verdict"] == V.FAILED                          # old behaviour unchanged
    ok = V.decide(before, after, ["validate:dup", "validate:partial"], ["validate:partial"], excuse_targets=True)
    assert ok["verdict"] == V.READY
    # nothing left to verify once everything is excused -> never READY
    none = V.decide({"validate:partial": F}, {"validate:partial": F}, ["validate:partial"],
                    ["validate:partial"], excuse_targets=True)
    assert none["verdict"] == V.FAILED
