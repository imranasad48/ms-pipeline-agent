"""Out-of-order family: timestamps outside the file's declared window / not chronological.

Window: a 3PL page declares ``window_start``/``window_end``; every other file covers the
calendar period it is named for (an IoT daily file = that day). Order: inside one file the
time column must not go backwards (``max_descents`` allowed, 0 on clean data).
"""
from __future__ import annotations

import datetime as dt

from ..manifest import SourceSpec
from ..readers import FileProfile
from .common import emit, parse_ts
from .context import Context


def _window(p: FileProfile):
    env = p.envelope
    lo, hi = parse_ts(env.get("window_start")), parse_ts(env.get("window_end"))
    if lo and hi:
        return lo, hi
    return p.slot.start, p.slot.end


def check_ordering(spec: SourceSpec, profiles: list[FileProfile], ctx: Context) -> list:
    cfg = ctx.config["out_of_order"]
    col = cfg["time_columns"].get(spec.source_id)
    if not col or col not in spec.expected_schema:
        return []
    slack = dt.timedelta(minutes=cfg["window_slack_minutes"])
    outside, descending = {}, {}
    for p in profiles:
        if not p.readable or not p.records:
            continue
        ctx.stats.evaluate("out_of_order.file")
        times = [parse_ts(r.get(col)) for r in p.records]
        times = [t for t in times if t is not None]
        lo, hi = _window(p)
        if lo and hi:
            bad = [t for t in times if t < lo - slack or t >= hi + slack]
            if bad:
                outside[p.rel] = {"outside_window": len(bad), "of": len(times),
                                  "window": [lo.isoformat(), hi.isoformat()],
                                  "example": bad[0].isoformat()}
        descents = sum(b < a for a, b in zip(times, times[1:]))
        if descents > cfg["max_descents"]:
            descending[p.rel] = {"descents": descents, "of": len(times)}
    out = []
    if outside:
        out.append(emit(ctx, spec, "out_of_order", f"ordering.out_of_window:{col}",
                        "ordering.out_of_window", False,
                        {"within": "declared file window", "slack_minutes": cfg["window_slack_minutes"]},
                        outside))
    if descending:
        out.append(emit(ctx, spec, "out_of_order", f"ordering.non_monotonic:{col}",
                        "ordering.non_monotonic", False,
                        {"max_descents_per_file": cfg["max_descents"]}, descending))
    return out
