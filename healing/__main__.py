"""CLI:  python -m healing {heal,heal-next,list,show,apply,reject} ...

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
from .report import print_list, print_open_incidents
from .settings import Settings


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m healing")
    ap.add_argument("--data-root", type=Path, help="dirty data root (default ../data_generation/data)")
    ap.add_argument("--incidents-db", type=Path)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("heal"); p.add_argument("--incident-id", required=True)
    p.add_argument("--no-manifest", action="store_true",
                   help="ablation: leave the pipeline manifest out of the agent's staged case")
    p = sub.add_parser("heal-next",
                       help="heal the oldest open warn/critical incident (skips run_health.*)")
    p.add_argument("--dry-run", action="store_true", help="only explain which incident would be picked")
    p.add_argument("--no-manifest", action="store_true")
    p = sub.add_parser("list")
    p.add_argument("--open-incidents", action="store_true",
                   help="show open incidents (oldest first) instead of proposals")
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
        s.include_manifest = not a.no_manifest
        from .heal import run_heal
        _, verdict = run_heal(s, a.incident_id)
        return 0 if verdict in ("READY_FOR_APPROVAL", "ESCALATED") else 1
    if a.cmd == "heal-next":
        from .incidents import explain_pick, pick_next_incident
        print(explain_pick(s))
        pick = pick_next_incident(s)
        if a.dry_run or pick is None:
            return 0
        s.include_manifest = not a.no_manifest
        from .heal import run_heal
        _, verdict = run_heal(s, pick["incident_id"])
        return 0 if verdict in ("READY_FOR_APPROVAL", "ESCALATED") else 1
    if a.cmd == "list":
        if a.open_incidents:
            from .incidents import open_incident_summary
            print_open_incidents(open_incident_summary(s))
        else:
            print_list(list_proposals(s))
    elif a.cmd == "show":
        root = proposal_dir(s, a.proposal_id)
        print((root / "proposal.md").read_text())
    elif a.cmd == "apply":
        from dashboard.jobs import refuse_if_generating; refuse_if_generating()  # noqa: E702 (dashboard job active)
        files = apply_proposal(s, a.proposal_id, a.force_drift)
        print(f"applied {a.proposal_id}: " + ", ".join(files))
        print("incident status -> resolved_pending_verification (if it was open)")
    elif a.cmd == "reject":
        print(f"archived to {reject_proposal(s, a.proposal_id, a.reason)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
