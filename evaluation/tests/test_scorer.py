import json
import sqlite3
from pathlib import Path

from evaluation import scorer

MAP = {
    "artifact_sources": {"tpl.shipments": "tpl", "iot.readings": "iot", "crm.customers": None,
                         "erp.batches": None},
    "freshness_only_artifacts": ["iot.readings"],
    "environmental_check_prefixes": ["run_health."],
    "types": {
        "schema_rename_column": {"checks": ["schema.renamed:*", "schema.missing_column:*"],
                                 "collateral": ["schema.new_column:*"], "needs_records": True},
        "dq_null_spike": {"checks": ["nulls.spike:*"]},
        "dq_duplicate_rows": {"blind": True},
        "domain_delivery_delay": {"kind": "domain_event"},
    }}


def inj(i, ftype, target, date, artifact="tpl.shipments", data=True):
    return {"error_id": f"E{i}", "failure_type": ftype, "target": target, "business_date": date,
            "artifact_id": artifact, "is_data_error": data, "source": "x"}


def inc(check, date, files, source="tpl", sev="warn"):
    return {"source_id": source, "check_id": check, "business_date": date, "severity": sev,
            "files": files, "pipelines": ["p"], "incident_ids": ["i"], "check_family": "f"}


def status(res):
    return {r["error_id"]: r["status"] for r in res["rows"]}


def test_caught_and_missed_by_file_and_date():
    entries = [inj(1, "schema_rename_column", "3pl/a_2026-08-14T01.json", "2026-08-14"),
               inj(2, "schema_rename_column", "3pl/a_2026-08-14T02.json", "2026-08-14"),
               inj(3, "schema_rename_column", "3pl/a_2026-08-15T01.json", "2026-08-15")]
    incs = [inc("schema.renamed:x->y", "2026-08-14", ["3pl/a_2026-08-14T01.json"]),
            inc("schema.renamed:x->y", "2026-08-15", ["3pl/a_2026-08-14T01.json"])]   # right check, wrong file
    r = scorer.score_scenario("schema_rename_column", entries, incs, MAP)
    assert status(r) == {"E1": "caught", "E2": "missed", "E3": "missed"}
    assert r["detection_recall"] == round(1 / 3, 4)
    assert r["detection_recall_in_scope"] == round(1 / 3, 4)


def test_wrong_check_family_does_not_catch():
    entries = [inj(1, "dq_null_spike", "3pl/a.json", "2026-08-14")]
    r = scorer.score_scenario("dq_null_spike", entries, [inc("freshness.late_file", "2026-08-14", ["3pl/a.json"])], MAP)
    assert status(r) == {"E1": "missed"}
    assert r["detection_precision"] == 0.0


def test_uncovered_reasons():
    entries = [inj(1, "dq_duplicate_rows", "3pl/a.json", "2026-08-14"),
               inj(2, "dq_null_spike", "crm/c.csv", "2026-08-14", artifact="crm.customers"),
               inj(3, "dq_null_spike", "iot/r_2026-08-14.jsonl", "2026-08-14", artifact="iot.readings")]
    # blind type, with an incidental incident on the same file
    r = scorer.score_scenario("dq_duplicate_rows", entries[:1],
                              [inc("volume.row_count", "2026-08-14", ["3pl/a.json"])], MAP)
    assert status(r) == {"E1": "uncovered"} and r["incidental_flags_on_uncovered"] == 1
    r = scorer.score_scenario("dq_null_spike", entries[1:], [], MAP)
    assert status(r) == {"E2": "uncovered", "E3": "uncovered"}
    assert set(r["uncovered_reasons"]) == {"unmonitored artifact",
                                           "detector does not parse this format (freshness checks only)"}
    assert r["detection_recall"] == 0.0 and r["detection_recall_in_scope"] is None


def test_zero_record_file_is_uncovered(tmp_path):
    (tmp_path / "3pl").mkdir()
    (tmp_path / "3pl" / "empty.json").write_text(json.dumps({"events": []}))
    (tmp_path / "3pl" / "full.json").write_text(json.dumps({"events": [{"a": 1}]}))
    entries = [inj(1, "schema_rename_column", "3pl/empty.json", "2026-08-14"),
               inj(2, "schema_rename_column", "3pl/full.json", "2026-08-14")]
    r = scorer.score_scenario("schema_rename_column", entries, [], MAP, data_root=tmp_path)
    assert status(r) == {"E1": "uncovered", "E2": "missed"}


def test_precision_collateral_and_environmental_excluded():
    entries = [inj(1, "schema_rename_column", "3pl/a.json", "2026-08-14")]
    incs = [inc("schema.renamed:x->y", "2026-08-14", ["3pl/a.json"]),
            inc("schema.new_column:y", "2026-08-14", ["3pl/a.json"]),          # collateral => TP for precision
            inc("nulls.spike:z", "2026-08-20", ["3pl/b.json"]),                # unrelated => FP
            inc("run_health.table_missing", "2026-08-14", [], source="wh", sev="critical")]
    r = scorer.score_scenario("schema_rename_column", entries, incs, MAP)
    assert r["incidents_total"] == 3 and r["incidents_matched"] == 2
    assert r["detection_precision"] == round(2 / 3, 4)
    assert r["environmental_incidents"] == 1
    assert "nulls.spike:z" in r["unmatched_incident_checks"]


def test_collateral_never_counts_as_caught():
    entries = [inj(1, "schema_rename_column", "3pl/a.json", "2026-08-14")]
    r = scorer.score_scenario("schema_rename_column", entries,
                              [inc("schema.new_column:y", "2026-08-14", ["3pl/a.json"])], MAP)
    assert status(r) == {"E1": "missed"}


def test_domain_events_scored_inversely():
    entries = [inj(1, "domain_delivery_delay", "3pl/s_2026-08-14T*.json", "2026-08-14", data=True and False),
               inj(2, "domain_delivery_delay", "3pl/s_2026-08-15T*.json", "2026-08-15", data=False),
               inj(3, "domain_delivery_delay", "batches", "2026-08-16", artifact="erp.batches", data=False)]
    incs = [inc("nulls.spike:q", "2026-08-14", ["3pl/s_2026-08-14T05.json"])]
    r = scorer.score_scenario("domain_delivery_delay", entries, incs, MAP)
    d = r["domain"]
    assert (d["injections"], d["flagged"], d["correctly_ignored"], d["unmonitored"]) == (3, 1, 2, 1)
    assert r["kind"] == "domain_event" and r["detection_recall"] is None


def test_load_incidents_dedupes_across_pipelines(tmp_path):
    db = tmp_path / "i.db"
    con = sqlite3.connect(db)
    con.executescript("""CREATE TABLE incidents (incident_id TEXT, detected_at TEXT, business_date TEXT,
      pipeline_id TEXT, source_id TEXT, check_id TEXT, check_family TEXT, severity TEXT, expected_json TEXT,
      observed_json TEXT, affected_files_json TEXT, status TEXT);""")
    for i, (pid, f) in enumerate([("p1", "a.json"), ("p2", "b.json")]):
        con.execute("INSERT INTO incidents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (f"i{i}", "t", "2026-08-14", pid, "tpl", "schema.renamed:x->y", "schema", "warn", "{}", "{}",
                     json.dumps([f + "#t"]), "open"))
    con.commit(); con.close()
    out = scorer.load_incidents(db)
    assert len(out) == 1 and out[0]["files"] == ["a.json", "b.json"] and len(out[0]["incident_ids"]) == 2


def test_aggregate():
    a = {"kind": "data_error", "injections": 10, "caught": 6, "missed": 2, "uncovered": 2,
         "incidents_total": 8, "incidents_matched": 6}
    b = {"kind": "domain_event", "injections": 5, "caught": 0, "missed": 0, "uncovered": 0,
         "incidents_total": 3, "incidents_matched": 0}
    g = scorer.aggregate([a, b])
    assert g["detection_recall"] == 0.6 and g["detection_recall_in_scope"] == 0.75
    assert g["detection_precision"] == 0.75


def test_only_scorer_reads_the_answer_key():
    pkg = Path(scorer.__file__).parent
    readers = [p.name for p in pkg.glob("*.py") if "error_manifest" in p.read_text()]
    assert set(readers) <= {"scorer.py", "builder.py", "healing_arm.py"}   # builder only MOVES it; healing_arm greps transcripts for the name
    assert "read_text" not in "".join(l for l in (pkg / "builder.py").read_text().splitlines()
                                      if "error_manifest" in l)
