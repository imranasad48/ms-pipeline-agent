"""CLI:  python -m detection {run,calibrate,replay,incidents,baseline} ...

NOTE (research design): detection code must NEVER read
../data_generation/data/error_manifest.json (or .xlsx). That file is the
evaluation answer key; peeking would invalidate the whole experiment.
Nothing in this package opens it -- thresholds come only from the manifests
and from the CLEAN dataset (data_clean).
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

import yaml

from . import config as C
from .checks.context import Context
from .engine import date_range, learn_baselines, run_date
from .incidents import IncidentStore
from .manifest import load_manifests
from .report import print_incidents, replay_table, table
from .schedule import infer_date_range

_date = dt.date.fromisoformat


def _common(p: argparse.ArgumentParser, data_root: bool = True) -> None:
    if data_root:
        p.add_argument("--data-root", type=Path, default=C.DEFAULT_DATA_ROOT,
                       required=C.DEFAULT_DATA_ROOT is None,
                       help="data root (default: $DETECTION_DATA_ROOT)")
    p.add_argument("--manifests", type=Path, default=C.DEFAULT_MANIFESTS)
    p.add_argument("--config", type=Path, default=None)
    p.add_argument("--warehouse", type=Path,
                   default=C.DEFAULT_WAREHOUSE)
    p.add_argument("--db", "--incidents-db", dest="db", type=Path, default=C.DEFAULT_INCIDENTS_DB,
                   help="incidents database (default detection/incidents.db)")


def _setup(args):
    cfg = C.load_config(args.config)
    manifests = load_manifests(args.manifests, cfg)
    if not manifests:
        sys.exit(f"no manifests in {args.manifests}")
    ctx = Context(Path(args.data_root).resolve() if hasattr(args, "data_root") else Path("."),
                  args.warehouse, cfg)
    return manifests, ctx


def cmd_run(args) -> int:
    manifests, ctx = _setup(args)
    inc = run_date(manifests, _date(args.business_date), ctx)
    store = IncidentStore(args.db)
    store.save(inc)
    print(f"{args.business_date}: {len(inc)} incident(s) -> {args.db}")
    print_incidents(store.for_date(args.business_date))
    store.close()
    return 0


def cmd_replay(args) -> int:
    manifests, ctx = _setup(args)
    store = None if args.no_store else IncidentStore(args.db)
    per_day = {}
    for d in date_range(_date(args.from_), _date(args.to)):
        per_day[d.isoformat()] = run_date(manifests, d, ctx)
        if store:
            store.save(per_day[d.isoformat()])
    print(replay_table(per_day))
    if store:
        print(f"\nincidents stored in {args.db}")
        store.close()
    return 0


def cmd_calibrate(args) -> int:
    manifests, ctx = _setup(args)
    lo, hi = (_date(args.from_) if args.from_ else None, _date(args.to) if args.to else None)
    auto_lo, auto_hi = infer_date_range(ctx.data_root)
    lo, hi = lo or auto_lo, hi or auto_hi
    total, bad = 0, []
    for d in date_range(lo, hi):
        inc = run_date(manifests, d, ctx)        # never persisted
        total += len(inc)
        bad += inc
    s = ctx.stats
    kinds = sorted(set(s.evaluated) | set(s.fired))
    rows = [[k, s.evaluated[k], s.fired[k]] for k in kinds]
    print(f"calibration over {lo}..{hi} ({(hi - lo).days + 1} days) on {ctx.data_root}\n")
    print(table(["check", "evaluations", "incidents"], rows))
    print("\nthreshold headroom (worst observed value / trip point; must stay < 1.00):")
    print(table(["check", "worst utilisation"],
                [[k, f"{v:.2f}"] for k, v in sorted(s.worst.items())]))
    print(f"\nTOTAL INCIDENTS: {total}")
    for i in bad[:20]:
        print(f"  {i.business_date} {i.pipeline_id}/{i.source_id} {i.check_id} {i.observed_json[:160]}")
    print("CALIBRATED" if total == 0 else "NOT CALIBRATED")
    return 0 if total == 0 else 1


def cmd_incidents(args) -> int:
    store = IncidentStore(args.db)
    print_incidents(store.for_date(args.date))
    store.close()
    return 0


def cmd_baseline(args) -> int:
    """Learn per-file row-count [min,max] per source from CLEAN data."""
    manifests, ctx = _setup(args)
    lo, hi = infer_date_range(ctx.data_root)
    learned = learn_baselines(manifests, date_range(lo, hi), ctx)
    text = yaml.safe_dump(learned, sort_keys=True)
    print(text)
    if args.write:
        C.DEFAULT_BASELINES.write_text(
            f"# Learned by `python -m detection baseline` from {ctx.data_root} ({lo}..{hi}).\n"
            + text)
        print(f"wrote {C.DEFAULT_BASELINES}")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m detection",
                                 description="Deterministic data-pipeline watcher (no LLM).")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("run", help="run all checks for one business date")
    p.add_argument("--business-date", required=True)
    _common(p)
    p.set_defaults(fn=cmd_run)
    p = sub.add_parser("replay", help="day-by-day sweep, prints incident table")
    p.add_argument("--from", dest="from_", required=True)
    p.add_argument("--to", required=True)
    p.add_argument("--no-store", action="store_true")
    _common(p)
    p.set_defaults(fn=cmd_replay)
    p = sub.add_parser("calibrate", help="replay CLEAN data; must yield 0 incidents")
    p.add_argument("--from", dest="from_")
    p.add_argument("--to")
    _common(p)
    p.set_defaults(fn=cmd_calibrate)
    p = sub.add_parser("incidents", help="pretty-print one day's stored incidents")
    p.add_argument("--date", required=True)
    _common(p, data_root=False)
    p.set_defaults(fn=cmd_incidents)
    p = sub.add_parser("baseline", help="learn row-count bands, value ranges, IoT cadence from clean data")
    p.add_argument("--write", action="store_true", help="write detection/baselines.yaml")
    _common(p)
    p.set_defaults(fn=cmd_baseline)
    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
