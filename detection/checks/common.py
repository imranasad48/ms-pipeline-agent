"""Small helpers shared by the content-level checks."""
from __future__ import annotations

import datetime as dt

from ..config import resolve_severity
from ..incidents import Finding
from ..manifest import SourceSpec
from .context import Context


def emit(ctx: Context, spec: SourceSpec, family: str, check_id: str, kind: str,
         is_key: bool, expected: dict, per_file: dict) -> Finding:
    """Build a Finding for one check on one source, listing every affected file."""
    ctx.stats.fire(check_id)
    return Finding(spec.source_id, check_id, family,
                   resolve_severity(ctx.config, kind, is_key), expected,
                   {"files_affected": len(per_file), "per_file": per_file}, list(per_file))


def parse_ts(value) -> dt.datetime | None:
    """ISO timestamp -> naive wall-clock datetime (offset dropped), or None if unparsable."""
    if not isinstance(value, str):
        return None
    try:
        return dt.datetime.fromisoformat(value.strip()).replace(tzinfo=None)
    except ValueError:
        return None


def as_number(value) -> float | None:
    """Numeric value of a cell (native or text), None for null/bool/non-numeric."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if value == value else None
    if isinstance(value, str):
        try:
            f = float(value.strip())
            return f if f == f else None
        except ValueError:
            return None
    return None
