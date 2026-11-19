"""Console tables + results/summary.md, results.csv, per-scenario JSON."""
from __future__ import annotations

import csv
import datetime as dt
import json
from pathlib import Path

from . import settings as S


def _pct(x):
    return "n/a" if x is None else f"{100 * x:.0f}%"


def md_table(headers, rows) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def detection_rows(results):
    return [[r["failure_type"], r["injections"], r["caught"], r["missed"], r["uncovered"],
             _pct(r["detection_recall"]), _pct(r["detection_recall_in_scope"]),
             _pct(r["detection_precision"]), f'{r["incidents_matched"]}/{r["incidents_total"]}']
            for r in results if r["kind"] == "data_error"]


DET_HEADERS = ["failure type", "injections", "caught", "missed", "uncovered", "recall",
               "recall (in scope)", "precision", "matched/incidents"]


def domain_rows(results):
    return [[r["failure_type"], r["domain"]["injections"], r["domain"]["flagged"],
             r["domain"]["correctly_ignored"], r["domain"]["unmonitored"]]
            for r in results if r.get("domain")]


DOM_HEADERS = ["domain event", "injections", "flagged (bad)", "correctly ignored", "of which unmonitored source"]


def print_detection_table(results) -> None:
    from .scorer import aggregate
    print(md_table(DET_HEADERS, detection_rows(results)))
    a = aggregate(results)
    print(f"\nAGGREGATE (data-error scenarios): recall {_pct(a['detection_recall'])} "
          f"({a['caught']}/{a['injections']}; {a['uncovered']} uncovered), "
          f"recall in scope {_pct(a['detection_recall_in_scope'])}, "
          f"precision {_pct(a['detection_precision'])} ({a['incidents_matched']}/{a['incidents_total']})")
    if domain_rows(results):
        print("\nDomain events (should NOT be flagged):\n" + md_table(DOM_HEADERS, domain_rows(results)))


# ------------------------------------------------------------------ healing summary
HEAL_HEADERS = ["failure type", "runs", "expected", "classified FIX", "ESCALATED", "success", "safe-fix",
                "false-fix", "mean attempts", "mean wall s"]


def heal_summary(runs: list) -> list:
    """Aggregate raw per-run healing records into one row per (type, no_manifest)."""
    by: dict = {}
    for r in runs:
        by.setdefault((r["failure_type"], r.get("no_manifest", False)), []).append(r)
    rows = []
    for (t, nm), rs in sorted(by.items()):
        n = len(rs)
        exp = rs[0]["expected_classification"]
        fix = sum(r["classification"] == "FIX" for r in rs)
        esc = sum(r["classification"] == "ESCALATED" for r in rs)
        succ = sum(bool(r["success"]) for r in rs)
        safe = sum(r["verdict"] == "READY_FOR_APPROVAL" and not r["regressions"] and bool(r["success"]) for r in rs)
        false_fix = sum(r["verdict"] == "READY_FOR_APPROVAL" and bool(r["regressions"] or not r["success"]) for r in rs)
        att = [r["attempts"] for r in rs if r.get("attempts") is not None]
        rows.append({"failure_type": t, "no_manifest": nm, "runs": n, "expected": exp,
                     "fix": fix, "escalated": esc, "success": succ, "safe_fix": safe,
                     "false_fix": false_fix,
                     "mean_attempts": round(sum(att) / len(att), 2) if att else None,
                     "mean_wall_s": round(sum(r["wall_seconds"] for r in rs) / n, 1)})
    return rows


def heal_table_rows(summary):
    return [[f'{s["failure_type"]}{" (no-manifest)" if s["no_manifest"] else ""}', s["runs"], s["expected"],
             s["fix"], s["escalated"], f'{s["success"]}/{s["runs"]}', f'{s["safe_fix"]}/{s["runs"]}',
             f'{s["false_fix"]}/{s["runs"]}', s["mean_attempts"] if s["mean_attempts"] is not None else "-",
             s["mean_wall_s"]] for s in summary]


def load_results(seed: int, results_dir: Path | None = None):
    rd = results_dir or S.RESULTS_DIR
    det = [json.loads(p.read_text()) for p in sorted((rd / "detection").glob("*.json"))] if (rd / "detection").exists() else []
    det = [d for d in det if d.get("seed", seed) == seed]
    heal = []
    if (rd / "healing").exists():
        for p in sorted((rd / "healing").glob("*.json")):
            heal += [r for r in json.loads(p.read_text()) if r.get("seed", seed) == seed]
    return det, heal


def write_report(seed: int, results_dir: Path | None = None) -> Path:
    from .scorer import aggregate
    rd = results_dir or S.RESULTS_DIR
    det, heal = load_results(seed, rd)
    rd.mkdir(parents=True, exist_ok=True)
    L = ["# Evaluation summary", "",
         "## Run metadata", "",
         f"- generated: {dt.datetime.now().isoformat(timespec='seconds')}",
         f"- generator seed: {seed}; scenarios: one failure type each, 90 days, end date {S.DEFAULT_END_DATE}",
         f"- detection scenarios scored: {len([d for d in det])}; healing runs: {len(heal)}"
         + (f" (repeats per type: {max(sum(1 for h in heal if h['failure_type']==t and h.get('no_manifest',False)==nm) for t, nm in {(h['failure_type'], h.get('no_manifest', False)) for h in heal})})" if heal else ""),
         f"- manifest-ablation runs present: {'yes' if any(h.get('no_manifest') for h in heal) else 'no'}", "",
         "## Detection", ""]
    if det:
        L += [md_table(DET_HEADERS, detection_rows(det)), ""]
        a = aggregate(det)
        L += [f"**Aggregate (data-error scenarios):** recall {_pct(a['detection_recall'])} "
              f"({a['caught']}/{a['injections']} injections; {a['uncovered']} uncovered, {a['missed']} missed), "
              f"recall in scope {_pct(a['detection_recall_in_scope'])}, "
              f"precision {_pct(a['detection_precision'])} ({a['incidents_matched']}/{a['incidents_total']} incidents).", "",
              "*Recall counts uncovered injections (blind check families / unmonitored artifacts) as not caught; "
              "'in scope' excludes them. Precision excludes environmental `run_health.*` incidents.*", ""]
        notes = [f"- `{d['failure_type']}`: unmatched incident checks (sample): {', '.join(d['unmatched_incident_checks'])}"
                 for d in det if d["kind"] == "data_error" and d["unmatched_incident_checks"]]
        if notes:
            L += ["Unmatched incidents (counted against precision):", ""] + notes + [""]
        unc = [f"- `{d['failure_type']}`: {d['uncovered']} uncovered ({'; '.join(f'{n} x {w}' for w, n in d.get('uncovered_reasons', {}).items())}); "
               f"{d['incidental_flags_on_uncovered']} incidentally flagged by an unrelated incident"
               for d in det if d["kind"] == "data_error" and d["uncovered"]]
        if unc:
            L += ["Uncovered injections:", ""] + unc + [""]
        if domain_rows(det):
            L += ["### Domain events (inverse scoring)", "", md_table(DOM_HEADERS, domain_rows(det)), ""]
    else:
        L += ["_no detection results yet_", ""]
    L += ["## Healing", ""]
    if heal:
        summ = heal_summary(heal)
        L += [md_table(HEAL_HEADERS, heal_table_rows(summ)), "",
              "*success = FIX types: READY_FOR_APPROVAL with zero regressions; escalation types: ESCALATED with no patch. "
              "false-fix = READY_FOR_APPROVAL but regressions or not a correct outcome.*", ""]
        for h in heal:
            L.append(f"- `{h['failure_type']}` run {h['repeat']}: {h['classification']} / {h['verdict']}, "
                     f"attempts {h.get('attempts')}, {h['wall_seconds']}s, incident `{h['incident_id']}`"
                     f"{' (SYNTHETIC incident)' if h.get('synthetic_incident') else ''}")
        L.append("")
    else:
        L += ["_no healing runs yet_", ""]
    (rd / "summary.md").write_text("\n".join(L) + "\n")
    with open(rd / "results.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["arm", "failure_type", "run", "metric_or_verdict", "value"])
        for d in det:
            for k in ("injections", "caught", "missed", "uncovered", "detection_recall",
                      "detection_recall_in_scope", "detection_precision", "incidents_total", "incidents_matched"):
                w.writerow(["detection", d["failure_type"], 0, k, d[k]])
        for h in heal:
            for k in ("classification", "verdict", "success", "attempts", "wall_seconds", "no_manifest"):
                w.writerow(["healing", h["failure_type"], h["repeat"], k, h.get(k)])
    return rd / "summary.md"
