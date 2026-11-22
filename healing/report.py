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


def print_open_incidents(summary: dict, limit: int = 25) -> None:
    """Summary + oldest-first table of open incidents, marking what heal-next may take."""
    print(f"open incidents: {summary['total']}   healable (warn/critical, not run_health): "
          f"{summary['healable']}")
    print("by severity: " + (", ".join(f"{k}={v}" for k, v in sorted(summary["by_severity"].items())) or "-"))
    kinds = sorted(summary["by_check_kind"].items(), key=lambda kv: -kv[1])
    print("by check:    " + (", ".join(f"{k}={v}" for k, v in kinds[:10]) or "-"))
    rows = summary["rows"][:limit]
    if not rows:
        return
    print()
    print(table(["incident", "date", "sev", "pipeline", "check", "proposal"],
                [[r["incident_id"], r["business_date"], r["severity"], r["pipeline_id"],
                  r["check_id"][:44], r["proposal"] or "-"] for r in rows]))
    if summary["total"] > limit:
        print(f"... {summary['total'] - limit} more (oldest {limit} shown)")
