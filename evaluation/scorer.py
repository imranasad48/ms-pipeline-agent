"""THE GRADER.  This module is the ONLY code in the repo allowed to open error_manifest.json
(the planted ground truth). Detection and healing must never read it; the scenario builder
stores it outside the data root (scenario/ground_truth/) precisely so only this file does.

Per injection (DATA ERROR kinds) status is one of
    caught     an incident matching the mapping hit the injection's source+date(+file)
    missed     the type is observable by some check family, but no incident matched
    uncovered  invisible by design: type marked `blind` in mapping.yaml, OR the artifact is
               not monitored by any manifest. Never silently dropped.
Metrics:
    detection_recall           caught / all data-error injections           (headline, honest)
    detection_recall_in_scope  caught / (caught + missed)                   (excludes uncovered)
    detection_precision        matched incidents / total incidents          (see below)
Incidents are de-duplicated across pipelines on (source_id, check_id, business_date); an
incident is "matched" if it lines up with >=1 injection under checks+collateral patterns.
Environmental incidents (run_health.*: about the live warehouse, not the scenario) are
excluded from precision and reported separately.
DOMAIN EVENT injections are scored inversely: flagged (bad) vs ignored (good).
"""
from __future__ import annotations

import datetime as dt
import fnmatch
import json
import sqlite3
from pathlib import Path

import yaml

from . import settings as S
from .builder import scenario_dir


def load_mapping(path: Path | None = None) -> dict:
    return yaml.safe_load(Path(path or S.MAPPING_PATH).read_text())


def load_incidents(db: Path) -> list[dict]:
    """Incidents de-duplicated across pipelines; affected files unioned."""
    if not Path(db).exists():
        return []
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    merged: dict = {}
    for r in con.execute("SELECT * FROM incidents ORDER BY business_date, pipeline_id, source_id, check_id"):
        key = (r["source_id"], r["check_id"], r["business_date"])
        d = merged.setdefault(key, {"source_id": r["source_id"], "check_id": r["check_id"],
                                    "business_date": r["business_date"], "severity": r["severity"],
                                    "check_family": r["check_family"], "pipelines": [], "files": set(),
                                    "incident_ids": []})
        d["pipelines"].append(r["pipeline_id"])
        d["incident_ids"].append(r["incident_id"])
        d["files"].update(f.split("#", 1)[0] for f in json.loads(r["affected_files_json"]))
    con.close()
    return [{**d, "files": sorted(d["files"])} for d in merged.values()]


def _norm(target: str) -> str:
    return target.split("#", 1)[0]


def _date_ok(inc_date: str, inj_date: str, window: int) -> bool:
    return abs((dt.date.fromisoformat(inc_date) - dt.date.fromisoformat(inj_date)).days) <= window


def _location_matches(inc: dict, inj: dict, window: int) -> bool:
    """Same source (via mapping), date within window, and file named by the incident (if it names any)."""
    return _date_ok(inc["business_date"], inj["business_date"], window) and _file_matches(inc, inj)


def _file_matches(inc: dict, inj: dict) -> bool:
    target = _norm(inj["target"])
    if "/" not in target:            # ERP-style target (a table name): match on source + date only
        return True
    if not inc["files"]:
        return True
    return any(fnmatch.fnmatch(f, target) for f in inc["files"])


def _matches_any(check_id: str, patterns: list) -> bool:
    return any(fnmatch.fnmatch(check_id, p) for p in patterns)


def _record_count(data_root: Path | None, target: str):
    """Records in a JSON-envelope target file (None = unknown / not applicable)."""
    if data_root is None or "/" not in target:
        return None
    p = Path(data_root) / _norm(target)
    try:
        d = json.loads(p.read_text())
    except (OSError, ValueError):
        return None
    for key in ("events", "records"):
        if isinstance(d, dict) and isinstance(d.get(key), list):
            return len(d[key])
    return None


def score_scenario(failure_type: str, entries: list, incidents: list, mapping: dict,
                   data_root: Path | None = None) -> dict:
    """Pure scoring of one scenario. `entries` = manifest entries, `incidents` = load_incidents().
    `data_root` (optional) lets the grader check whether an injection had anything to act on."""
    tmap = mapping["types"][failure_type]
    window = int(tmap.get("date_window_days", 0))
    blind = bool(tmap.get("blind"))
    primary, collateral = tmap.get("checks", []), tmap.get("collateral", [])
    freshness_only = set(mapping.get("freshness_only_artifacts", []))
    content_checks = bool(primary) and not all(p.startswith("freshness.") for p in primary)
    art_src = mapping["artifact_sources"]
    env_prefix = tuple(mapping.get("environmental_check_prefixes", []))

    env = [i for i in incidents if i["check_id"].startswith(env_prefix)]
    scored_inc = [i for i in incidents if not i["check_id"].startswith(env_prefix)]

    def src_incidents(inj):
        sid = art_src.get(inj["artifact_id"])
        return [i for i in scored_inc if sid is not None and i["source_id"] == sid]

    data_inj = [e for e in entries if e["is_data_error"]]
    dom_inj = [e for e in entries if not e["is_data_error"]]
    matched_ids: set = set()
    rows, status_by_error = [], {}

    for inj in data_inj:
        sid = art_src.get(inj["artifact_id"])
        cands = [i for i in src_incidents(inj) if _location_matches(i, inj, window)]
        if sid is None:
            status, why = "uncovered", "unmonitored artifact"
        elif blind:
            status, why = "uncovered", "no check family can see this failure type"
        elif inj["artifact_id"] in freshness_only and content_checks:
            status, why = "uncovered", "detector does not parse this format (freshness checks only)"
        elif tmap.get("needs_records") and _record_count(data_root, inj["target"]) == 0:
            status, why = "uncovered", "target file has zero records: no observable effect"
        else:
            hit = [i for i in cands if _matches_any(i["check_id"], primary)]
            status, why = ("caught", "") if hit else ("missed", "no matching incident")
        incidental = bool(cands) if status == "uncovered" and sid is not None else False
        status_by_error[inj["error_id"]] = status
        rows.append({"error_id": inj["error_id"], "status": status, "why": why,
                     "incidental_flag": incidental, "artifact_id": inj["artifact_id"],
                     "target": inj["target"], "business_date": inj["business_date"]})
        for i in cands:
            if _matches_any(i["check_id"], primary + collateral):
                matched_ids.add((i["source_id"], i["check_id"], i["business_date"]))

    n = len(data_inj)
    caught = sum(r["status"] == "caught" for r in rows)
    missed = sum(r["status"] == "missed" for r in rows)
    uncovered = sum(r["status"] == "uncovered" for r in rows)
    matched_inc = sum((i["source_id"], i["check_id"], i["business_date"]) in matched_ids for i in scored_inc)

    domain = None
    if dom_inj:
        flagged_rows = []
        for inj in dom_inj:
            sid = art_src.get(inj["artifact_id"])
            hit = [i for i in src_incidents(inj)
                   if i["severity"] != "info" and _location_matches(i, inj, window)]
            flagged_rows.append({"error_id": inj["error_id"], "flagged": bool(hit),
                                 "monitored": sid is not None, "target": inj["target"],
                                 "business_date": inj["business_date"],
                                 "checks": sorted({i["check_id"] for i in hit})})
        domain = {"injections": len(dom_inj), "flagged": sum(r["flagged"] for r in flagged_rows),
                  "correctly_ignored": sum(not r["flagged"] for r in flagged_rows),
                  "unmonitored": sum(not r["monitored"] for r in flagged_rows), "rows": flagged_rows}

    def ratio(a, b):
        return None if b == 0 else round(a / b, 4)

    return {
        "failure_type": failure_type, "kind": "domain_event" if tmap.get("kind") == "domain_event" else "data_error",
        "injections": n, "caught": caught, "missed": missed, "uncovered": uncovered,
        "uncovered_reasons": {w: sum(r["why"] == w for r in rows if r["status"] == "uncovered")
                              for w in sorted({r["why"] for r in rows if r["status"] == "uncovered"})},
        "incidental_flags_on_uncovered": sum(r["incidental_flag"] for r in rows),
        "detection_recall": ratio(caught, n),
        "detection_recall_in_scope": ratio(caught, caught + missed),
        "incidents_total": len(scored_inc), "incidents_matched": matched_inc,
        "detection_precision": ratio(matched_inc, len(scored_inc)) if n else None,
        "environmental_incidents": len(env), "domain": domain, "rows": rows,
        "unmatched_incident_checks": sorted({i["check_id"] for i in scored_inc
                                             if (i["source_id"], i["check_id"], i["business_date"]) not in matched_ids})[:15],
    }


def aggregate(results: list) -> dict:
    de = [r for r in results if r["kind"] == "data_error"]
    tot = lambda k: sum(r[k] for r in de)
    n, c, m = tot("injections"), tot("caught"), tot("missed")
    inc, mat = tot("incidents_total"), tot("incidents_matched")
    return {"injections": n, "caught": c, "missed": m, "uncovered": tot("uncovered"),
            "detection_recall": round(c / n, 4) if n else None,
            "detection_recall_in_scope": round(c / (c + m), 4) if c + m else None,
            "detection_precision": round(mat / inc, 4) if inc else None,
            "incidents_total": inc, "incidents_matched": mat}


def score_types(types: list, seed: int, root: Path | None = None, mapping: dict | None = None,
                out_dir: Path | None = None) -> list:
    mapping = mapping or load_mapping()
    out = (out_dir or S.RESULTS_DIR) / "detection"
    out.mkdir(parents=True, exist_ok=True)
    results = []
    for t in types:
        sdir = scenario_dir(t, seed, root)
        gt = sdir / "ground_truth" / "error_manifest.json"
        if not gt.exists():
            raise SystemExit(f"scenario {t}/{seed} not built")
        db = sdir / "incidents.db"
        if not db.exists():
            raise SystemExit(f"scenario {t}/{seed}: run `run-detection` first")
        entries = json.loads(gt.read_text())["entries"]      # <-- the one and only read of the answer key
        res = score_scenario(t, entries, load_incidents(db), mapping, sdir / "data")
        res["seed"] = seed
        (out / f"{t}.json").write_text(json.dumps(res, indent=1))
        results.append(res)
    return results


def first_injection_target(failure_type: str, seed: int, root: Path | None = None) -> dict:
    """(business_date, target file, artifact) of the first injection in a scenario.

    Exists so the healing arm can manufacture an incident for a domain event WITHOUT opening
    the answer key itself: reading error_manifest.json stays confined to this module.
    """
    gt = scenario_dir(failure_type, seed, root) / "ground_truth" / "error_manifest.json"
    entries = sorted(json.loads(gt.read_text())["entries"], key=lambda e: (e["business_date"], e["target"]))
    if not entries:
        raise SystemExit(f"scenario {failure_type}/{seed} has no injections")
    e = entries[0]
    return {"business_date": e["business_date"], "target": e["target"], "artifact_id": e["artifact_id"],
            "source": e["source"]}
