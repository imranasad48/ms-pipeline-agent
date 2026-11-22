"""IoT behavioural family: per-device cadence gaps and flat-lined sensors.

Cadence: the clean reporting interval is learned per source (baselines.yaml). A device whose
consecutive readings are further apart than ``gap_multiple`` x interval has dropped out. Devices
learned as *always-on* (reported all day on every clean day) are also checked at the edges of
the day, so a dead battery or a device missing from the file entirely shows up as silence.
Flatline: ``min_run`` or more consecutive identical values of the watched column from one device.
"""
from __future__ import annotations

import datetime as dt
from collections import defaultdict

from ..manifest import SourceSpec
from ..readers import FileProfile
from .common import emit, parse_ts
from .context import Context


def check_iot_behavior(spec: SourceSpec, profiles: list[FileProfile], ctx: Context) -> list:
    cfg = ctx.config["iot_behavior"]
    scfg = cfg["sources"].get(spec.source_id)
    if not scfg:
        return []
    learned = ctx.config["baselines"].get("iot_behavior", {}).get(spec.source_id, {})
    interval = learned.get("interval_minutes", scfg.get("default_interval_minutes", 5))
    limit = dt.timedelta(minutes=interval * cfg["gap_multiple"])
    always_on = set(learned.get("always_on_devices", []))
    dev_col, time_col = scfg["device_column"], scfg["time_column"]
    fcfg = cfg["flatline"]
    gaps, flats = {}, {}
    for p in profiles:
        if not p.readable or not p.records or p.slot.start is None:
            continue
        per_dev: dict = defaultdict(list)
        for r in p.records:
            t = parse_ts(r.get(time_col))
            if t is not None and r.get(dev_col) is not None:
                per_dev[r[dev_col]].append((t, r.get(fcfg["column"])))
        for dev in sorted(always_on - set(per_dev)):
            gaps.setdefault(p.rel, {})[dev] = {"no_readings_all_day": True}
        for dev, rows in sorted(per_dev.items()):
            rows.sort(key=lambda x: x[0])
            ctx.stats.evaluate("iot.cadence_gap")
            ctx.stats.evaluate("iot.flatline")
            times = [t for t, _ in rows]
            span = [(a, b) for a, b in zip(times, times[1:])]
            if dev in always_on:
                # the last on-time reading sits one interval before midnight, so measure the
                # closing silence from the moment the *next* reading was due
                span = ([(p.slot.start, times[0])] + span
                        + [(times[-1] + dt.timedelta(minutes=interval), p.slot.end)])
            worst = max(((b - a) for a, b in span), default=dt.timedelta(0))
            ctx.stats.utilisation("iot.cadence_gap", worst.total_seconds(), limit.total_seconds())
            if worst > limit:
                a, b = max(span, key=lambda ab: ab[1] - ab[0])
                gaps.setdefault(p.rel, {})[dev] = {
                    "gap_minutes": round(worst.total_seconds() / 60),
                    "from": a.isoformat(), "to": b.isoformat()}
            # longest run of identical non-null values
            run = best = 0
            prev = object()
            for _, v in rows:
                run = run + 1 if (v is not None and v == prev) else 1
                prev = v
                best = max(best, run)
            ctx.stats.utilisation("iot.flatline", best, fcfg["min_run"])
            if best >= fcfg["min_run"]:
                flats.setdefault(p.rel, {})[dev] = {"identical_readings_in_a_row": best}
    out = []
    if gaps:
        out.append(emit(ctx, spec, "iot_behavior", "iot.cadence_gap", "iot.cadence_gap", False,
                        {"max_gap_minutes": round(limit.total_seconds() / 60),
                         "interval_minutes": interval},
                        {f: {"devices": d} for f, d in gaps.items()}))
    if flats:
        out.append(emit(ctx, spec, "iot_behavior", "iot.flatline", "iot.flatline", False,
                        {"min_run_flagged": fcfg["min_run"], "column": fcfg["column"]},
                        {f: {"devices": d} for f, d in flats.items()}))
    return out
