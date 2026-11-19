"""Volume/null family: row counts and per-column null rates vs build-time baselines."""
from __future__ import annotations

import math
from collections import defaultdict

from ..config import resolve_severity
from ..incidents import Finding
from ..manifest import SourceSpec
from ..readers import FileProfile
from .context import Context


def row_count_band(spec: SourceSpec, cfg: dict) -> tuple[int, int]:
    """Allowed rows per file: learned [min,max] widened by margin and slack.

    Falls back to the manifest's mean rows/file (+-margin*4) if no baseline learned.
    """
    vcfg = cfg["volume"]
    learned = cfg["baselines"].get("row_count_bands", {}).get(spec.source_id)
    if learned:
        lo, hi = learned["min"], learned["max"]
    else:
        mean = spec.baseline_rows / spec.baseline_files
        lo, hi = mean * (1 - 4 * vcfg["row_count_margin"]), mean * (1 + 4 * vcfg["row_count_margin"])
    slack = vcfg["row_count_abs_slack"]
    return (max(0, math.floor(lo * (1 - vcfg["row_count_margin"])) - slack),
            math.ceil(hi * (1 + vcfg["row_count_margin"])) + slack)


def null_threshold(baseline: float, n: int, cfg: dict, source_id: str, col: str) -> float:
    """Highest acceptable null rate for a file of n rows."""
    ncfg = cfg["nulls"]
    delta = ncfg["overrides"].get(source_id, {}).get(col, ncfg["abs_delta"])
    stderr = math.sqrt(baseline * (1 - baseline) / n) if n else 0.0
    return baseline + max(delta, ncfg["z"] * stderr)


def check_volume_nulls(spec: SourceSpec, profiles: list[FileProfile],
                       ctx: Context) -> list[Finding]:
    """One finding per out-of-band row count and per column with a null-rate spike."""
    cfg = ctx.config
    lo, hi = row_count_band(spec, cfg)
    row_hits = {}
    null_hits = defaultdict(dict)
    thresholds = {}
    for p in profiles:
        if not p.readable:
            continue
        ctx.stats.evaluate("volume.row_count")
        if p.row_count < lo or p.row_count > hi:
            row_hits[p.rel] = p.row_count
        else:
            ctx.stats.utilisation("volume.row_count(hi)", p.row_count, hi)
            if lo > 0:
                ctx.stats.utilisation("volume.row_count(lo)", lo, max(p.row_count, 1))
        if not p.row_count:
            continue
        for col in p.columns:
            if col not in spec.expected_schema:
                continue
            base = spec.null_baselines.get(col, 0.0)
            thr = null_threshold(base, p.row_count, cfg, spec.source_id, col)
            rate = p.null_counts.get(col, 0) / p.row_count
            ctx.stats.evaluate("nulls.spike")
            if rate > thr:
                null_hits[col][p.rel] = {"null_rate": round(rate, 4), "rows": p.row_count}
                thresholds[col] = (base, thr)
            elif thr > base:
                ctx.stats.utilisation("nulls.spike", max(rate - base, 0.0), thr - base)

    findings = []
    if row_hits:
        ctx.stats.fire("volume.row_count")
        findings.append(Finding(
            spec.source_id, "volume.row_count", "volume_nulls",
            resolve_severity(cfg, "volume.row_count"),
            {"rows_per_file": {"min": lo, "max": hi}},
            {"files_affected": len(row_hits), "per_file_rows": row_hits}, list(row_hits)))
    for col, per_file in sorted(null_hits.items()):
        base, thr = thresholds[col]
        ctx.stats.fire("nulls.spike")
        findings.append(Finding(
            spec.source_id, f"nulls.spike:{col}", "volume_nulls",
            resolve_severity(cfg, "nulls.spike", col in spec.key_columns),
            {"baseline_null_rate": base, "max_null_rate": round(thr, 4)},
            {"files_affected": len(per_file), "per_file": per_file}, list(per_file)))
    return findings
