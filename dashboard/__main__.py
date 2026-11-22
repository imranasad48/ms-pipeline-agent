"""CLI:  python -m dashboard {build,serve,dump}

build  -> dashboard/site/index.html (static, self-contained)
serve  -> build, then serve dashboard/site on http://localhost:8099
dump   -> the same data as plain text (what the page shows), for terminals and tests
"""
from __future__ import annotations

import argparse
import functools
import http.server
import sys
from pathlib import Path

from .data import Paths, assemble
from .render import render


def build(paths: Paths) -> Path:
    paths.site.mkdir(parents=True, exist_ok=True)
    out = paths.site / "index.html"
    out.write_text(render(assemble(paths)))
    return out


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
    ap.add_argument("cmd", choices=["build", "serve", "dump"])
    ap.add_argument("--port", type=int, default=8099)
    a = ap.parse_args(argv)
    paths = Paths()
    if a.cmd == "dump":
        print(dump(paths))
        return 0
    out = build(paths)
    print(f"built {out}")
    if a.cmd == "serve":
        handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(paths.site))
        with http.server.ThreadingHTTPServer(("127.0.0.1", a.port), handler) as srv:
            print(f"serving http://localhost:{a.port}  (Ctrl-C to stop)")
            try:
                srv.serve_forever()
            except KeyboardInterrupt:
                pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
