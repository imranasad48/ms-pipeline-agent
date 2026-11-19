import sqlite3

from evaluation import healing_arm as H


def _db(tmp_path, rows):
    db = tmp_path / "i.db"
    con = sqlite3.connect(db)
    con.executescript("""CREATE TABLE incidents (incident_id TEXT, detected_at TEXT, business_date TEXT,
      pipeline_id TEXT, source_id TEXT, check_id TEXT, check_family TEXT, severity TEXT, expected_json TEXT,
      observed_json TEXT, affected_files_json TEXT, status TEXT);""")
    for i, (d, p, s, c, sev) in enumerate(rows):
        con.execute("INSERT INTO incidents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (f"i{i}", "t", d, p, s, c, "f", sev, "{}", "{}", "[]", "open"))
    con.commit(); con.close()
    return db


def test_pick_incident_deterministic_skips_env_and_info(tmp_path):
    db = _db(tmp_path, [("2026-08-14", "p", "s", "run_health.table_missing", "critical"),
                        ("2026-08-14", "p", "s", "schema.new_column:z", "info"),
                        ("2026-08-16", "p", "s", "schema.renamed:b->c", "warn"),
                        ("2026-08-15", "q", "s", "schema.renamed:a->b", "critical"),
                        ("2026-08-15", "p", "s", "schema.renamed:a->b", "warn")])
    inc = H.pick_incident(db, ("run_health.",))
    assert (inc["business_date"], inc["pipeline_id"]) == ("2026-08-15", "p")


def test_pick_incident_none(tmp_path):
    assert H.pick_incident(_db(tmp_path, [("2026-08-14", "p", "s", "run_health.x", "critical")]), ("run_health.",)) is None
    assert H.pick_incident(tmp_path / "missing.db", ()) is None
