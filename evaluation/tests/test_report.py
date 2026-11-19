import json

from evaluation import report as R

DET = {"failure_type": "dq_null_spike", "kind": "data_error", "seed": 1, "injections": 10, "caught": 8,
       "missed": 1, "uncovered": 1, "incidental_flags_on_uncovered": 0, "detection_recall": 0.8,
       "detection_recall_in_scope": 0.8889, "detection_precision": 1.0, "incidents_total": 8,
       "incidents_matched": 8, "unmatched_incident_checks": [], "domain": None}
DOM = {"failure_type": "domain_delivery_delay", "kind": "domain_event", "seed": 1, "injections": 4, "caught": 0,
       "missed": 0, "uncovered": 0, "incidental_flags_on_uncovered": 0, "detection_recall": None,
       "detection_recall_in_scope": None, "detection_precision": None, "incidents_total": 0, "incidents_matched": 0,
       "unmatched_incident_checks": [],
       "domain": {"injections": 4, "flagged": 1, "correctly_ignored": 3, "unmonitored": 0, "rows": []}}


def heal(t, repeat, cls, verdict, success, regs=(), exp="FIX", nm=False):
    return {"failure_type": t, "seed": 1, "repeat": repeat, "no_manifest": nm, "expected_classification": exp,
            "classification": cls, "verdict": verdict, "success": success, "regressions": list(regs),
            "attempts": 2, "wall_seconds": 30.0, "incident_id": "inc_x", "synthetic_incident": exp != "FIX"}


def test_heal_summary_counts():
    runs = [heal("schema_rename_column", 1, "FIX", "READY_FOR_APPROVAL", True),
            heal("schema_rename_column", 2, "FIX", "READY_FOR_APPROVAL", False, regs=["step:load"]),
            heal("schema_rename_column", 3, "ESCALATED", "ESCALATED", False)]
    s = R.heal_summary(runs)[0]
    assert (s["runs"], s["fix"], s["escalated"], s["success"], s["safe_fix"], s["false_fix"]) == (3, 2, 1, 1, 1, 1)


def test_write_report_from_canned_results(tmp_path):
    (tmp_path / "detection").mkdir()
    (tmp_path / "healing").mkdir()
    for d in (DET, DOM):
        (tmp_path / "detection" / f"{d['failure_type']}.json").write_text(json.dumps(d))
    (tmp_path / "healing" / "x.json").write_text(json.dumps([
        heal("schema_rename_column", 1, "FIX", "READY_FOR_APPROVAL", True),
        heal("domain_temp_excursion", 1, "ESCALATED", "ESCALATED", True, exp="ESCALATED")]))
    path = R.write_report(1, tmp_path)
    md = path.read_text()
    assert "dq_null_spike" in md and "80%" in md and "Domain events" in md
    assert "domain_temp_excursion" in md and "SYNTHETIC" in md
    assert (tmp_path / "results.csv").exists()
    assert (tmp_path / "results.csv").read_text().startswith("arm,failure_type,run,metric_or_verdict,value")


def test_report_without_results(tmp_path):
    md = R.write_report(1, tmp_path).read_text()
    assert "no detection results yet" in md and "no healing runs yet" in md
