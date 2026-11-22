"""Data-assembly tests (fixtures in tmp dirs); one smoke test that the page renders."""
from __future__ import annotations

import json
import sqlite3

import pandas as pd
import pytest
import yaml

from dashboard.data import (Paths, assemble, load_eval_summary, load_incidents, load_pipelines,
                            load_proposals, summarize_incidents, summarize_proposals)
from dashboard.render import markdown_to_html, render

SCHEMA = """CREATE TABLE incidents (incident_id TEXT PRIMARY KEY, detected_at TEXT, business_date TEXT,
pipeline_id TEXT, source_id TEXT, check_id TEXT, check_family TEXT, severity TEXT,
expected_json TEXT, observed_json TEXT, affected_files_json TEXT, status TEXT)"""


@pytest.fixture
def world(tmp_path):
    man = tmp_path / "manifests"
    man.mkdir()
    for pid, table, ver in (("p_a", "ta", 2), ("p_b", "tb", 1), ("p_c", "tc", 1)):
        (man / f"{pid}.yaml").write_text(yaml.safe_dump({
            "pipeline_id": pid, "version": ver, "sources": [{"id": f"{pid}_src"}],
            "destination": {"table": table}}))
    wh = tmp_path / "wh.db"
    con = sqlite3.connect(wh)
    con.execute("CREATE TABLE ta (x, loaded_at)")
    con.executemany("INSERT INTO ta VALUES (?,?)", [(1, "2026-11-01T01:00:00Z"), (2, "2026-11-02T01:00:00Z")])
    con.execute("CREATE TABLE tb (x)")
    con.execute("INSERT INTO tb VALUES (1)")
    con.commit(); con.close()
    db = tmp_path / "inc.db"
    con = sqlite3.connect(db)
    con.execute(SCHEMA)
    rows = [("i1", "d", "2026-08-14", "p_a", "s1", "schema.renamed:a->b", "schema", "warn", "{}", "{}", "[]", "open"),
            ("i2", "d", "2026-08-15", "p_a", "s1", "nulls.spike:q", "volume_nulls", "critical", "{}", "{}", "[]", "open"),
            ("i3", "d", "2026-08-15", "p_b", "s2", "schema.new_column:z", "schema", "info", "{}", "{}", "[]", "open"),
            ("i4", "d", "2026-08-16", "p_b", "s2", "schema.renamed:c->d", "schema", "warn", "{}", "{}", "[]",
             "resolved_pending_verification")]
    con.executemany("INSERT INTO incidents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit(); con.close()
    idx = tmp_path / "index.json"
    idx.write_text(json.dumps({
        "p-1": {"status": "APPLIED", "pipeline_id": "p_a", "check_id": "c", "incident_id": "i1",
                "updated_at": "2026-11-02T10:00:00"},
        "p-2": {"status": "REJECTED", "reject_reason": "nope", "updated_at": "2026-11-03T10:00:00"},
        "p-3": {"status": "READY_FOR_APPROVAL", "decision": "FIX", "updated_at": "2026-11-01T10:00:00"}}))
    summ = tmp_path / "summary.md"
    summ.write_text("# Evaluation summary\n\n- generated: 2026-11-19T21:38:46\n\n| a | b |\n|---|---|\n| 1 | **2** |\n")
    return Paths(manifests=man, warehouse=wh, incidents=db, proposals=idx, eval_summary=summ,
                 site=tmp_path / "site")


def test_pipelines_report_version_destination_and_last_loaded(world):
    rows = {p["pipeline_id"]: p for p in load_pipelines(world.manifests, world.warehouse)}
    assert rows["p_a"]["version"] == 2 and rows["p_a"]["destination"] == "ta"
    assert rows["p_a"]["last_loaded_at"] == "2026-11-02T01:00:00Z" and rows["p_a"]["rows"] == 2
    assert rows["p_b"]["last_loaded_at"] is None and "no loaded_at" in rows["p_b"]["note"]
    assert rows["p_c"]["rows"] is None and "not in warehouse" in rows["p_c"]["note"]


def test_pipelines_survive_a_missing_warehouse(world, tmp_path):
    rows = load_pipelines(world.manifests, tmp_path / "absent.db")
    assert len(rows) == 3 and all(r["note"] == "warehouse not found" for r in rows)


def test_incident_summary_counts_only_open(world):
    s = summarize_incidents(load_incidents(world.incidents))
    assert (s["total"], s["open"]) == (4, 3)
    assert s["by_severity"] == {"critical": 1, "warn": 1, "info": 1}
    assert s["by_family"] == {"schema": 2, "volume_nulls": 1}
    assert s["status_counts"]["resolved_pending_verification"] == 1
    assert s["date_range"] == ["2026-08-14", "2026-08-16"]
    by = {r["source_id"]: r for r in s["by_source"]}
    assert by["s1"]["total"] == 2 and by["s1"]["critical"] == 1 and by["s2"]["info"] == 1
    assert {r["incident_id"] for r in s["recent_open"]} == {"i1", "i2", "i3"}


def test_incident_summary_with_no_database(tmp_path):
    df = load_incidents(tmp_path / "absent.db")
    assert isinstance(df, pd.DataFrame) and df.empty
    s = summarize_incidents(df)
    assert s["open"] == 0 and s["by_severity"] == {"critical": 0, "warn": 0, "info": 0}
    assert s["date_range"] is None and s["recent_open"] == []


def test_proposals_newest_first_with_counts(world):
    rows = load_proposals(world.proposals)
    assert [r["proposal_id"] for r in rows] == ["p-2", "p-1", "p-3"]
    assert rows[0]["note"] == "nope"
    assert summarize_proposals(rows) == {"REJECTED": 1, "APPLIED": 1, "READY_FOR_APPROVAL": 1}
    assert load_proposals(world.proposals.with_name("none.json")) == []


def test_eval_summary_optional(world, tmp_path):
    ev = load_eval_summary(world.eval_summary)
    assert ev["generated"] == "2026-11-19T21:38:46"
    assert load_eval_summary(tmp_path / "none.md") is None


def test_markdown_tables_and_escaping():
    html = markdown_to_html("## Hi\n\n| a | b |\n|---|---|\n| <x> | **2** |\n\n- item `c`\n")
    assert "<table>" in html and "&lt;x&gt;" in html and "<b>2</b>" in html
    assert "<li>item <code>c</code></li>" in html


def test_page_renders_all_sections_without_external_assets(world):
    html = render(assemble(world))
    for needle in ("Pipelines", "Open incidents", "Healing proposals", "Latest evaluation",
                   "p_a", "READY_FOR_APPROVAL", "schema.renamed:a-&gt;b"):
        assert needle in html
    assert "http://" not in html and "https://" not in html and "<script" not in html


def test_page_without_evaluation_or_db(tmp_path):
    p = Paths(manifests=tmp_path, warehouse=tmp_path / "w.db", incidents=tmp_path / "i.db",
              proposals=tmp_path / "p.json", eval_summary=tmp_path / "s.md", site=tmp_path / "site")
    html = render(assemble(p))
    assert "no evaluation" in html
