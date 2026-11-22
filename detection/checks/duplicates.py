"""Duplicates family: repeated primary-key values within one day's data for a source.

Keys come from the manifest's ``unique`` validation checks (see manifest._unique_keys).
The day's data is every file the calendar expects for that date, so a row delivered twice
across two hourly pages is caught as well as one repeated inside a single file. With no
declared key the check falls back to fully identical rows.
"""
from __future__ import annotations

import json
from collections import defaultdict

from ..manifest import SourceSpec
from ..readers import FileProfile
from .common import emit
from .context import Context


def check_duplicates(spec: SourceSpec, profiles: list[FileProfile], ctx: Context) -> list:
    cfg = ctx.config["duplicates"]
    tolerance = cfg["overrides"].get(spec.source_id, cfg["max_duplicate_keys"])
    keys = list(spec.unique_keys) or ([("*",)] if cfg["fallback_full_row"] else [])
    findings = []
    readable = [p for p in profiles if p.readable and p.records]
    for key in keys:
        ctx.stats.evaluate("duplicates.key")
        seen: dict = {}
        dup_values: dict = defaultdict(int)
        files: dict = defaultdict(int)
        for p in readable:
            for rec in p.records:
                if key == ("*",):
                    ident = json.dumps(rec, sort_keys=True, default=str)
                else:
                    vals = tuple(rec.get(c) for c in key)
                    if any(v is None or v == "" for v in vals):
                        continue
                    ident = vals
                if ident in seen:
                    dup_values[ident if key != ("*",) else "row"] += 1
                    files[p.rel] += 1
                    files[seen[ident]] += 0     # the first copy's file is affected too
                else:
                    seen[ident] = p.rel
        n = len(dup_values) if key != ("*",) else sum(dup_values.values())
        ctx.stats.utilisation("duplicates.key", n, tolerance + 1)
        if n > tolerance:
            cid = "duplicates.row" if key == ("*",) else f"duplicates.key:{'+'.join(key)}"
            examples = [str(k)[:60] for k in list(dup_values)[:3]]
            findings.append(emit(
                ctx, spec, "duplicates", cid, "duplicates.key",
                key != ("*",) and any(c in spec.key_columns for c in key),
                {"max_duplicate_keys": tolerance},
                {r: {"duplicate_keys": n, "extra_copies": c, "examples": examples}
                 for r, c in sorted(files.items())}))
    return findings
