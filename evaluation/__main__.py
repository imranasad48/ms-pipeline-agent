"""CLI:  python -m evaluation {build,run-detection,score,run-healing,report} ..."""
from __future__ import annotations

import argparse
import sys

from . import settings as S


def _types(a):
    from .builder import resolve_types
    return resolve_types(a.types)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m evaluation")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p, repeats=False):
        p.add_argument("--types", required=True, help="comma list or 'all'")
        p.add_argument("--seed", type=int, default=S.DEFAULT_SEED)

    p = sub.add_parser("build"); common(p)
    p.add_argument("--end-date", default=S.DEFAULT_END_DATE)
    p.add_argument("--force", action="store_true")
    p = sub.add_parser("run-detection"); common(p)
    p = sub.add_parser("score"); common(p)
    p = sub.add_parser("run-healing"); common(p)
    p.add_argument("--repeats", type=int, default=1)
    p.add_argument("--no-manifest", action="store_true", help="ablation: agent gets no manifest")
    p.add_argument("--pipeline-ref", default=None,
                   help="git ref whose dags/+manifests/ the agent is graded against "
                        "(default: parent of the first real heal; '' = live working tree)")
    p = sub.add_parser("report")
    p.add_argument("--seed", type=int, default=S.DEFAULT_SEED)
    a = ap.parse_args(argv)

    if a.cmd == "build":
        from .builder import build_scenario
        for t in _types(a):
            build_scenario(t, a.seed, a.end_date, force=a.force)
    elif a.cmd == "run-detection":
        from .detection_arm import run_detection
        for t in _types(a):
            run_detection(t, a.seed)
    elif a.cmd == "score":
        from .scorer import score_types
        from .report import print_detection_table
        print_detection_table(score_types(_types(a), a.seed))
    elif a.cmd == "run-healing":
        from .healing_arm import run_healing
        for t in _types(a):
            run_healing(t, a.seed, a.repeats, a.no_manifest, **({} if a.pipeline_ref is None else {"pipeline_ref": a.pipeline_ref}))
    elif a.cmd == "report":
        from .report import write_report
        print(write_report(a.seed))
    return 0


if __name__ == "__main__":
    sys.exit(main())
