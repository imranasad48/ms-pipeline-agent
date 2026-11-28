"""CLI:  python -m dashboard {build,serve,dump}

build  -> dashboard/site/*.html: every page that can be static (index, pipelines, pipeline-<id>,
          incidents[-<severity>], proposals[-<status>], governance), relative links between them
serve  -> live server on http://localhost:8099: /, /pipelines, /pipeline/<id>, /incidents, /proposals,
          /governance; rebuilt per request, approve (two-step) / reject; and (live only) /new + /jobs to
          generate a pipeline from a prompt (data root: $HEAL_DATA_ROOT, time limit:
          $PIPELINE_GEN_TIME_LIMIT_MIN, default 20)
dump   -> the same data as plain text (what the page shows), for terminals and tests
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .data import Paths, assemble
from .pages import build_site


def build(paths: Paths) -> Path:
    """Write every static page; returns the path of index.html."""
    build_site(paths)
    return paths.site / "index.html"


def dump(paths: Paths) -> str:
    d = assemble(paths)
    inc = d["incidents"]
    L = ["== PIPELINES =="]
    for p in d["pipelines"]:
        L.append(f"  {p['pipeline_id']:<22} v{p['version']}  -> {p['destination']:<22} "
                 f"loaded_at={p['last_loaded_at'] or 'n/a':<22} rows={p['rows']}  {p['note']}")
    L += ["", f"== OPEN INCIDENTS: {inc['open']} of {inc['total']} =="
          + (f"  ({inc['date_range'][0]} .. {inc['date_range'][1]})" if inc["date_range"] else ""),
          "  by severity: " + ", ".join(f"{k}={v}" for k, v in inc["by_severity"].items()),
          "  by family:   " + ", ".join(f"{k}={v}" for k, v in inc["by_family"].items()),
          "  by source:"]
    for r in inc["by_source"]:
        L.append(f"    {r['source_id']:<26} critical={r['critical']:<3} warn={r['warn']:<3} "
                 f"info={r['info']:<3} total={r['total']}")
    L.append("  most recent open:")
    for r in inc["recent_open"][:8]:
        L.append(f"    {r['business_date']} {r['severity']:<8} {r['pipeline_id']}/{r['source_id']}  {r['check_id']}")
    L += ["", "== HEALING PROPOSALS ==  " + ", ".join(f"{k}={v}" for k, v in sorted(d["proposal_counts"].items()))]
    for r in d["proposals"]:
        L.append(f"  {r['proposal_id']}  {r['status']:<18} {r['pipeline_id']}  {r['check_id']}  {r['note'][:60]}")
    L += ["", "== LATEST EVALUATION =="]
    ev = d["evaluation"]
    if ev:
        L += ["  " + ln for ln in ev["markdown"].splitlines()
              if ln.startswith(("- generated", "**Aggregate", "| schema_type_change", "| dq_dup", "| iot_flat"))]
    else:
        L.append("  (no summary.md)")
    return "\n".join(L)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m dashboard")
    ap.add_argument("cmd", choices=["build", "serve", "dump", "airflow-snapshot"])
    ap.add_argument("--port", type=int, default=8099)
    ap.add_argument("--out", default=None, help="airflow-snapshot: file to write "
                                                 "(default detection/airflow_runs.json)")
    a = ap.parse_args(argv)
    paths = Paths()
    if a.cmd == "dump":
        print(dump(paths))
        return 0
    if a.cmd == "build":
        out = build(paths)
        print(f"built {out} (+ {len(list(paths.site.glob('*.html'))) - 1} more pages in {paths.site})")
        return 0
    from .airflow import DEFAULT_SNAPSHOT, AirflowCLI, AirflowError, StatusCache, export_snapshot
    if a.cmd == "airflow-snapshot":
        try:
            print(f"wrote {export_snapshot(AirflowCLI(), a.out or DEFAULT_SNAPSHOT)}")
        except AirflowError as ex:
            print(f"airflow-snapshot: {ex}", file=sys.stderr)
            return 1
        return 0
    from healing.settings import Settings
    from .jobs import GenConfig, JobManager
    from .server import make_server
    jobs = JobManager(GenConfig(warehouse=paths.warehouse))
    srv = make_server(paths, Settings(), a.port, jobs=jobs,
                      airflow_status=StatusCache(jobs.airflow, snapshot_path=DEFAULT_SNAPSHOT))
    print(f"live dashboard http://localhost:{a.port}  (rebuilds per request; Ctrl-C to stop)")
    print(f"  new pipelines: data root {jobs.cfg.data_root}  ->  warehouse {jobs.cfg.warehouse}  "
          f"(time limit {jobs.cfg.time_limit_s / 60:g} min; generator: {' '.join(jobs.cfg.generator_argv())})")
    print(f"  Airflow runs read {jobs.cfg.airflow_data_container} (host {jobs.cfg.airflow_data}) and write "
          f"{jobs.cfg.airflow_warehouse}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
