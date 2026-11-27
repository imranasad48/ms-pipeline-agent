"""CLI:  python -m orchestrator watch [--detect-every N --business-date D --data-root PATH]"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from healing.settings import Settings

from .watcher import Orchestrator, load_config


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m orchestrator")
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("watch", help="poll incidents.db and auto-heal eligible incidents")
    w.add_argument("--data-root", type=Path, help="data root (default $HEAL_DATA_ROOT)")
    w.add_argument("--incidents-db", type=Path)
    w.add_argument("--config", type=Path)
    w.add_argument("--poll-seconds", type=float, help="default: config / $POLL_SECONDS (5)")
    w.add_argument("--detect-every", type=float, metavar="N",
                   help="demo mode: re-run detection every N seconds (needs --business-date)")
    w.add_argument("--business-date", help="with --detect-every: the date to scan; "
                   "also limits the heal queue to that date")
    w.add_argument("--no-alerts", action="store_true", help="disable email alerts (see python -m alerts)")
    w.add_argument("--no-manifest", action="store_true", help="ablation (see healing heal)")
    a = ap.parse_args(argv)
    if a.detect_every and not a.business_date:
        ap.error("--detect-every requires --business-date")

    sys.stdout.reconfigure(line_buffering=True)
    s = Settings()
    if a.data_root:
        s.data_root = a.data_root.resolve()
    if a.incidents_db:
        s.incidents_db = a.incidents_db.resolve()
    s.include_manifest = not a.no_manifest
    cfg = load_config(a.config)
    if a.poll_seconds:
        cfg["poll_seconds"] = a.poll_seconds

    from healing.heal import run_heal

    def detect(date):
        from detection.service import run_and_store
        return run_and_store(date, data_root=s.data_root, db=s.incidents_db)

    alerter = None
    if not a.no_alerts:
        try:
            from alerts.alerter import Alerter
            alerter = Alerter(s, cfg["auto_heal_prefixes"], business_date=a.business_date)
            print(f"alerts: ON  {alerter.describe()}  (backlog seeded: {alerter.ledger.seeded_count()} incident(s))",
                  flush=True)
        except Exception as e:       # alerting is optional: never block the orchestrator from starting
            print(f"alerts: DISABLED (could not initialise: {type(e).__name__}: {e})", flush=True)
    else:
        print("alerts: OFF (--no-alerts)", flush=True)
    Orchestrator(s, cfg, business_date=a.business_date, detect_every=a.detect_every,
                 detect_fn=detect, heal_fn=run_heal, alerter=alerter).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
