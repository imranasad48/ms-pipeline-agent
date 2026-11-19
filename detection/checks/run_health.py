"""Run-health family: the warehouse table exists, has rows, and was loaded recently."""
from __future__ import annotations

import datetime as dt
import sqlite3

from ..config import resolve_severity
from ..incidents import Finding
from ..manifest import Manifest
from .context import Context


def _parse_ts(text) -> dt.datetime | None:
    try:
        t = dt.datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)


def check_run_health(manifest: Manifest, business_date: dt.date, ctx: Context) -> list[Finding]:
    """Simple version: table present, non-empty, MAX(loaded_at) not too far before day end.

    (Airflow run-state integration is a later step.) Tables without a loaded_at
    column only get the existence/non-empty checks.
    """
    cfg, table = ctx.config, manifest.dest_table
    sid = f"warehouse.{table}"
    ctx.stats.evaluate("run_health.table")

    def finding(kind, expected, observed):
        ctx.stats.fire(kind)
        return [Finding(sid, kind, "run_health", resolve_severity(cfg, kind),
                        expected, observed, [str(ctx.warehouse_db.name)])]

    if not ctx.warehouse_db.exists():
        return finding("run_health.table_missing", {"table": table},
                       {"reason": "warehouse db not found"})
    con = sqlite3.connect(f"file:{ctx.warehouse_db}?mode=ro", uri=True)
    try:
        cols = [r[1] for r in con.execute(f'PRAGMA table_info("{table}")')]
        if not cols:
            return finding("run_health.table_missing", {"table": table},
                           {"reason": "table not found"})
        n = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        if n == 0:
            return finding("run_health.table_empty", {"min_rows": 1}, {"rows": 0})
        if "loaded_at" not in cols:
            return []
        latest = _parse_ts(con.execute(f'SELECT MAX(loaded_at) FROM "{table}"').fetchone()[0])
    finally:
        con.close()
    ctx.stats.evaluate("run_health.load_stale")
    day_end = dt.datetime.combine(business_date + dt.timedelta(days=1), dt.time.min,
                                  tzinfo=dt.timezone.utc)
    max_lag = cfg["run_health"]["max_load_lag_hours"]
    lag_h = (day_end - latest).total_seconds() / 3600 if latest else float("inf")
    ctx.stats.utilisation("run_health.load_stale", max(lag_h, 0.0), max_lag)
    if lag_h > max_lag:
        return finding("run_health.load_stale", {"max_lag_hours": max_lag},
                       {"latest_loaded_at": latest.isoformat() if latest else None,
                        "lag_hours": round(lag_h, 1)})
    return []
