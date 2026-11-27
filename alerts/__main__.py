"""CLI:  python -m alerts {test,list}"""
from __future__ import annotations

import argparse
import sys

from .ledger import Ledger
from .alerter import DEFAULT_LEDGER
from .mailer import FileMailer, build_message, select_backend


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m alerts")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("test", help="send one labelled test message through the configured backend")
    ls = sub.add_parser("list", help="print the alert ledger, newest first")
    ls.add_argument("--all", action="store_true", help="include backlog-guard 'seeded' rows")
    a = ap.parse_args(argv)
    if a.cmd == "test":
        mailer = select_backend()
        sender = getattr(mailer, "sender", "pipeline-agent@localhost")
        msg = build_message("[pipeline-agent] TEST: alert delivery check",
                            "This is a TEST message from `python -m alerts test`. No incident is involved.\n",
                            getattr(mailer, "recipient", ""), sender)
        try:
            where = mailer.send(msg)
            print(f"backend: {mailer.name}\nsent to: {where}")
        except Exception as e:
            where = FileMailer().send(msg)
            print(f"backend: {mailer.name} FAILED ({type(e).__name__}); fell back to file\nwritten to: {where}")
            return 1
        return 0
    led = Ledger(DEFAULT_LEDGER)
    rows = led.rows(include_seeded=a.all)
    for r in rows:
        print(f"{r['sent_at']}  {r['incident_id']}  {r['trigger']:<16} {r['backend'] or '-':<5} "
              f"{r['proposal_id'] or '-'}  {r['recipient'] or '-'}  {r['outcome']}")
    print(f"{len(rows)} alert(s)" + ("" if a.all else f"  ({led.seeded_count()} backlog incidents seeded; --all to show)"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
