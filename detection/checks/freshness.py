"""Freshness family: expected files present, non-empty, intact and on time."""
from __future__ import annotations

import datetime as dt

from ..config import resolve_severity
from ..incidents import Finding
from ..manifest import SourceSpec
from ..readers import FileProfile
from .context import Context

_KIND = {"missing": "freshness.missing_file", "empty": "freshness.empty_file",
         "truncated": "freshness.truncated_file", "unreadable": "freshness.unreadable_file"}


def check_freshness(spec: SourceSpec, profiles: list[FileProfile], ctx: Context) -> list[Finding]:
    """Calendar-driven: absence is detected because the *schedule* says a file is due."""
    cfg = ctx.config
    tol = dt.timedelta(hours=cfg["freshness"]["late_tolerance_hours"])
    groups: dict[str, dict] = {}
    late: dict = {}
    for p in profiles:
        ctx.stats.evaluate("freshness.file")
        if p.status != "ok":
            groups.setdefault(_KIND[p.status], {})[p.rel] = (
                {"detail": p.detail or "file absent (calendar says it is due)"})
            continue
        avail = p.slot.available_at
        if avail is None or p.mtime is None:
            continue
        # Use the later of file mtime and the envelope's own timestamp (if any).
        arrived = max(t for t in (p.mtime, p.generated_at) if t is not None)
        delay = arrived - avail
        ctx.stats.utilisation("freshness.late_file", max(delay.total_seconds(), 0.0),
                              tol.total_seconds())
        if delay > tol:
            late[p.rel] = {"expected_by": avail.isoformat(timespec="minutes"),
                           "arrived": arrived.isoformat(timespec="minutes"),
                           "hours_late": round(delay.total_seconds() / 3600, 2)}
    if late:
        groups["freshness.late_file"] = late

    findings = []
    for kind, per_file in sorted(groups.items()):
        ctx.stats.fire(kind)
        findings.append(Finding(
            spec.source_id, kind, "freshness", resolve_severity(cfg, kind),
            {"files_expected": len(profiles),
             **({"late_tolerance_hours": cfg["freshness"]["late_tolerance_hours"]}
                if kind.endswith("late_file") else {})},
            {"files_affected": len(per_file), "per_file": per_file}, list(per_file)))
    return findings
