"""Calendar logic: which files SHOULD exist for a business date, and when."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

from .manifest import SourceSpec

DATA_ROOT_VAR = "${DATA_ROOT}"


@dataclass(frozen=True)
class ExpectedFile:
    """One file the calendar says must exist (whether or not it does)."""

    path: Path
    rel: str                         # path relative to data root, used in incidents
    table: str | None                # sqlite table, if the pattern has '#table'
    available_at: dt.datetime | None  # naive local time, like file mtimes
    start: dt.datetime | None = None   # first instant the file covers (naive local)
    end: dt.datetime | None = None     # first instant it no longer covers


def split_pattern(spec: SourceSpec) -> tuple[str, str | None]:
    """Return (relative file pattern, sqlite table or None)."""
    pat = spec.path_pattern.replace(DATA_ROOT_VAR, "").lstrip("/")
    if "#" in pat:
        pat, table = pat.split("#", 1)
        return pat, table
    return pat, None


def expected_files(spec: SourceSpec, business_date: dt.date, data_root: Path,
                   cfg: dict) -> list[ExpectedFile]:
    """Expand the source's path_pattern into the concrete files due on a date."""
    pattern, table = split_pattern(spec)
    subdir = pattern.split("/", 1)[0]
    cad = cfg["cadence"].get(subdir, {"kind": "daily", "available_after_hours": 24})
    kind = cad["kind"]
    midnight = dt.datetime.combine(business_date, dt.time.min)
    offset = dt.timedelta(hours=float(cad.get("available_after_hours", 0)))

    span = {"hourly": dt.timedelta(hours=1), "daily": dt.timedelta(days=1),
            "weekly": dt.timedelta(days=7)}.get(kind)

    def make(token: str | None, slot_start: dt.datetime | None):
        rel = pattern if token is None else pattern.replace("*", token)
        avail = None if slot_start is None else slot_start + offset
        end = None if slot_start is None or span is None else slot_start + span
        return ExpectedFile(Path(data_root) / rel, rel + (f"#{table}" if table else ""),
                            table, avail, slot_start, end)

    if "*" not in pattern or kind == "static":
        return [make(None, None)]
    if kind == "daily":
        return [make(business_date.isoformat(), midnight)]
    if kind == "hourly":
        return [make(f"{business_date.isoformat()}T{h:02d}", midnight + dt.timedelta(hours=h))
                for h in range(24)]
    if kind == "weekly":
        if business_date.weekday() != int(cad.get("weekday", 4)):
            return []
        year, week, _ = business_date.isocalendar()
        return [make(f"{year}-W{week:02d}", midnight)]
    raise ValueError(f"unknown cadence kind {kind!r} for {subdir}")


def infer_date_range(data_root: Path) -> tuple[dt.date, dt.date]:
    """Min/max date found in file names under the data root (for calibrate)."""
    import re
    dates = set()
    for p in Path(data_root).rglob("*"):
        m = re.search(r"(\d{4}-\d{2}-\d{2})", p.name)
        if m:
            dates.add(dt.date.fromisoformat(m.group(1)))
    if not dates:
        raise SystemExit(f"no dated files under {data_root}")
    return min(dates), max(dates)


def source_dir(spec: SourceSpec) -> str:
    """First directory of the path pattern ('crm', '3pl', ...): how manifests name sources."""
    return split_pattern(spec)[0].split("/", 1)[0]


def preceding_files(spec: SourceSpec, business_date: dt.date, data_root: Path,
                    cfg: dict) -> list:
    """For each file expected on ``business_date``: (file, the file one period before it).

    Hourly -> the previous hour (hour 0 looks at yesterday's hour 23); daily -> yesterday;
    weekly -> last week. Static sources have no previous period.
    """
    today = expected_files(spec, business_date, data_root, cfg)
    cad = cfg["cadence"].get(source_dir(spec), {"kind": "daily"})
    if cad["kind"] == "static" or not today:
        return []
    step = dt.timedelta(days=7 if cad["kind"] == "weekly" else 1)
    before = expected_files(spec, business_date - step, data_root, cfg)
    combined = before + today
    return [(f, combined[len(before) + i - 1]) for i, f in enumerate(today)
            if len(before) + i - 1 >= 0]
