"""Plain-text listing."""
from detection.report import table


def print_list(rows: list) -> None:
    if not rows:
        print("no proposals")
        return
    print(table(["proposal", "status", "pipeline", "check", "decision", "updated"],
                [[r["proposal_id"], r.get("status", ""), r.get("pipeline_id", ""),
                  r.get("check_id", ""), r.get("decision") or "-", r.get("updated_at", "")]
                 for r in rows]))
