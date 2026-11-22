"""Stale-data family: a period's file whose *records* equal the previous period's file.

Compared on a hash of the parsed records, not the bytes: a re-sent 3PL page keeps its new
envelope (request id, generated_at) but carries the previous hour's events. Empty files
are never stale (an idle hour legitimately repeats "no events").
"""
from __future__ import annotations

import datetime as dt

from ..readers import cached_profile
from ..schedule import preceding_files
from .common import emit
from .context import Context


def check_stale(spec, business_date: dt.date, profiles: list, ctx: Context) -> list:
    cfg = ctx.config["stale_data"]
    by_rel = {p.rel: p for p in profiles}
    tokens = ctx.config["nulls"]["null_tokens"]
    hits = {}
    for slot, prev_slot in preceding_files(spec, business_date, ctx.data_root, ctx.config):
        cur = by_rel.get(slot.rel)
        if cur is None or not cur.readable or cur.row_count < cfg["min_rows"]:
            continue
        prev = cached_profile(prev_slot, spec, tokens, ctx.cache)
        if not prev.readable or prev.row_count < cfg["min_rows"]:
            continue
        ctx.stats.evaluate("stale.identical_to_previous")
        if cur.content_hash and cur.content_hash == prev.content_hash:
            hits[cur.rel] = {"identical_to": prev.rel, "rows": cur.row_count}
    if not hits:
        return []
    return [emit(ctx, spec, "stale_data", "stale.identical_to_previous",
                 "stale.identical_to_previous", False,
                 {"content_differs_from_previous_period": True}, hits)]
