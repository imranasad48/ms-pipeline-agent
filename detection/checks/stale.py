"""Stale-data family: a period's file whose *records* equal the previous period's file.

Compared on a hash of the parsed records, not the bytes: a re-sent 3PL page keeps its new
envelope (request id, generated_at) but carries the previous hour's events. Empty files
are never stale (an idle hour legitimately repeats "no events").

The manifest's ``delivery`` decides what "stale" means for a source (manifest.SourceSpec):
  incremental (default) - each period brings new records, so equal to the previous period = stale.
  snapshot              - each period re-delivers the full current state, so equal to the previous
                          period is normal; stale = the content has not changed for MORE than the
                          declared ``max_unchanged_periods`` consecutive periods.
"""
from __future__ import annotations

import datetime as dt

from ..readers import cached_profile
from ..schedule import expected_files, preceding_files, source_dir
from .common import emit
from .context import Context


def _history(spec, business_date: dt.date, periods: int, ctx: Context) -> list:
    """Expected files, oldest first, ending with ``business_date``'s, covering >= periods + 1 periods."""
    kind = ctx.config["cadence"].get(source_dir(spec), {"kind": "daily"})["kind"]
    step = dt.timedelta(days=7 if kind == "weekly" else 1)
    days = (periods // 24 + 2) if kind == "hourly" else periods + 1
    slots = []
    for k in range(days, -1, -1):
        slots += expected_files(spec, business_date - k * step, ctx.data_root, ctx.config)
    return slots


def unchanged_run(spec, business_date: dt.date, slot, ctx: Context, limit: int) -> int:
    """How many consecutive periods right before ``slot`` hold exactly its records (capped at
    limit + 1, enough to decide "more than limit")."""
    tokens = ctx.config["nulls"]["null_tokens"]
    min_rows = ctx.config["stale_data"]["min_rows"]
    slots = _history(spec, business_date, limit + 1, ctx)
    i = next(n for n, s in enumerate(slots) if s.rel == slot.rel)
    cur = cached_profile(slot, spec, tokens, ctx.cache)
    run = 0
    for prev_slot in reversed(slots[:i]):
        prev = cached_profile(prev_slot, spec, tokens, ctx.cache)
        if not prev.readable or prev.row_count < min_rows or prev.content_hash != cur.content_hash:
            break
        run += 1
        if run > limit:
            break
    return run


def check_stale(spec, business_date: dt.date, profiles: list, ctx: Context) -> list:
    cfg = ctx.config["stale_data"]
    by_rel = {p.rel: p for p in profiles}
    tokens = ctx.config["nulls"]["null_tokens"]
    snapshot = spec.delivery == "snapshot"
    hits = {}
    for slot, prev_slot in preceding_files(spec, business_date, ctx.data_root, ctx.config):
        cur = by_rel.get(slot.rel)
        if cur is None or not cur.readable or cur.row_count < cfg["min_rows"]:
            continue
        prev = cached_profile(prev_slot, spec, tokens, ctx.cache)
        if not prev.readable or prev.row_count < cfg["min_rows"]:
            continue
        ctx.stats.evaluate("stale.identical_to_previous")
        if not (cur.content_hash and cur.content_hash == prev.content_hash):
            continue
        if not snapshot:
            hits[cur.rel] = {"identical_to": prev.rel, "rows": cur.row_count}
            continue
        limit = spec.max_unchanged_periods
        run = unchanged_run(spec, business_date, slot, ctx, limit)
        ctx.stats.utilisation("stale.unchanged_periods", run, max(limit, 1))
        if run > limit:
            hits[cur.rel] = {"identical_to": prev.rel, "rows": cur.row_count,
                             "unchanged_periods": run, "max_unchanged_periods": limit}
    if not hits:
        return []
    expected = ({"content_differs_from_previous_period": True} if not snapshot else
                {"content_changes_within_periods": spec.max_unchanged_periods})
    return [emit(ctx, spec, "stale_data", "stale.identical_to_previous",
                 "stale.identical_to_previous", False, expected, hits)]
