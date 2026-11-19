"""Plain-text tables for the CLI."""
from __future__ import annotations

import json
from collections import Counter

from .incidents import FAMILIES, SEVERITY_ORDER


def table(headers: list, rows: list) -> str:
    """Render a simple left-aligned text table."""
    cells = [[str(c) for c in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in cells)) if cells else len(h)
              for i, h in enumerate(headers)]
    line = lambda r: "  ".join(c.ljust(w) for c, w in zip(r, widths)).rstrip()
    return "\n".join([line(headers), line(["-" * w for w in widths]), *map(line, cells)])


def day_row(date: str, incidents: list) -> list:
    sev = Counter(i.severity for i in incidents)
    fam = Counter(i.check_family for i in incidents)
    return [date, len(incidents), sev["critical"], sev["warn"], sev["info"],
            *(fam[f] for f in FAMILIES)]


def replay_table(per_day: dict) -> str:
    headers = ["date", "total", "crit", "warn", "info", *FAMILIES]
    rows = [day_row(d, inc) for d, inc in per_day.items()]
    total = [sum(r[i] for r in rows) for i in range(1, len(headers))]
    return table(headers, rows + [["TOTAL", *total]])


def print_incidents(rows: list) -> None:
    """Pretty-print stored incident rows (sqlite3.Row or dict-like)."""
    if not rows:
        print("no incidents")
        return
    for r in sorted(rows, key=lambda r: SEVERITY_ORDER[r["severity"]]):
        files = json.loads(r["affected_files_json"])
        print(f"[{r['severity'].upper():8}] {r['incident_id']}  {r['pipeline_id']} / "
              f"{r['source_id']}\n    check    : {r['check_id']}  ({r['check_family']})\n"
              f"    expected : {r['expected_json']}\n    observed : {r['observed_json'][:600]}\n"
              f"    files    : {len(files)} {files[:3]}{' ...' if len(files) > 3 else ''}\n")
