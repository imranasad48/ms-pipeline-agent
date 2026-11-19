"""CLI:  python -m healing {heal,list,show,apply,reject} ...

NOTE (research design): healing code and the healing agent must NEVER read
../data_generation/data/error_manifest.json (or .xlsx) -- it is the evaluation
answer key. Nothing here opens it; case assembly copies only files named by
the incident/manifest, and refuses any path containing "error_manifest".
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .proposals import apply_proposal, list_proposals, proposal_dir, reject_proposal
from .report import print_list
from .settings import Settings


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m healing")
    ap.add_argument("--data-root", type=Path, help="dirty data root (default ../data_generation/data)")
    ap.add_argument("--incidents-db", type=Path)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("heal"); p.add_argument("--incident-id", required=True)
    sub.add_parser("list")
    p = sub.add_parser("show"); p.add_argument("proposal_id")
    p = sub.add_parser("apply"); p.add_argument("proposal_id")
    p.add_argument("--force-drift", action="store_true")
    p = sub.add_parser("reject"); p.add_argument("proposal_id"); p.add_argument("--reason", required=True)
    a = ap.parse_args(argv)

    s = Settings()
    if a.data_root:
        s.data_root = a.data_root.resolve()
    if a.incidents_db:
        s.incidents_db = a.incidents_db.resolve()
    if a.cmd == "heal":
        from .heal import run_heal
        _, verdict = run_heal(s, a.incident_id)
        return 0 if verdict in ("READY_FOR_APPROVAL", "ESCALATED") else 1
    if a.cmd == "list":
        print_list(list_proposals(s))
    elif a.cmd == "show":
        root = proposal_dir(s, a.proposal_id)
        print((root / "proposal.md").read_text())
    elif a.cmd == "apply":
        files = apply_proposal(s, a.proposal_id, a.force_drift)
        print(f"applied {a.proposal_id}: " + ", ".join(files))
    elif a.cmd == "reject":
        print(f"archived to {reject_proposal(s, a.proposal_id, a.reason)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
