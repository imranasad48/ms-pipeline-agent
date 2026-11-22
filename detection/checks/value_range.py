"""Value-range family: numeric columns outside manifest-/baseline-derived bounds.

Bounds per column = (clean-data observed [min, max] widened by ``margin`` of the value)
intersected with the manifest's ``range`` validation params, unless ``overrides`` pins
explicit numbers (documented in config.yaml). One value outside is enough to trip
(``max_violation_rate`` 0): range violations are physically/commercially impossible, not noisy.
A silent unit change (C->F, PKR->USD) shifts every value out of the envelope.
"""
from __future__ import annotations

from collections import defaultdict

from ..manifest import SourceSpec
from ..readers import FileProfile
from .common import as_number, emit
from .context import Context

NUMERIC = ("int64", "float64")


def column_bounds(spec: SourceSpec, col: str, cfg: dict, baselines: dict):
    """(lo, hi, origin) for one column, or None if nothing bounds it."""
    vcfg = cfg["value_range"]
    lo = hi = None
    origin = []
    learned = baselines.get("value_ranges", {}).get(spec.source_id, {}).get(col)
    if learned:
        m = vcfg["margin"]
        lo, hi = learned["min"] - m * abs(learned["min"]), learned["max"] + m * abs(learned["max"])
        origin.append("baseline")
    ov = vcfg["overrides"].get(spec.source_id, {}).get(col)
    if ov:
        lo = ov.get("min", lo)
        hi = ov.get("max", hi)
        origin.append("override")
    mlo, mhi = spec.range_bounds.get(col, (None, None))
    if mlo is not None:
        lo = mlo if lo is None else max(lo, mlo)
        origin.append("manifest")
    if mhi is not None:
        hi = mhi if hi is None else min(hi, mhi)
        origin.append("manifest")
    if lo is None and hi is None:
        return None
    return lo, hi, "+".join(dict.fromkeys(origin))


def check_value_range(spec: SourceSpec, profiles: list[FileProfile], ctx: Context) -> list:
    cfg = ctx.config
    max_rate = cfg["value_range"]["max_violation_rate"]
    bounds = {c: b for c in spec.expected_schema
              if spec.expected_schema[c] in NUMERIC
              and (b := column_bounds(spec, c, cfg, cfg["baselines"]))}
    hits: dict = defaultdict(dict)
    for p in profiles:
        if not p.readable or not p.records:
            continue
        for col, (lo, hi, _) in bounds.items():
            if col not in p.columns:
                continue
            learned = cfg["baselines"].get("value_ranges", {}).get(spec.source_id, {}).get(col)
            ctx.stats.evaluate("value_range.column")
            n = bad = 0
            examples = []
            worst = 0.0
            for r in p.records:
                v = as_number(r.get(col))
                if v is None:
                    continue
                n += 1
                if (lo is not None and v < lo) or (hi is not None and v > hi):
                    bad += 1
                    if len(examples) < 3:
                        examples.append(v)
                elif learned:
                    # share of the widening margin consumed (0 = inside the clean envelope,
                    # 1.0 = on the trip point)
                    if v > learned["max"] and hi > learned["max"]:
                        worst = max(worst, (v - learned["max"]) / (hi - learned["max"]))
                    elif v < learned["min"] and learned["min"] > lo:
                        worst = max(worst, (learned["min"] - v) / (learned["min"] - lo))
                    else:
                        worst = max(worst, 0.0)
            if n and bad / n > max_rate:
                hits[col][p.rel] = {"out_of_bounds": bad, "of": n, "examples": examples}
            elif learned:
                ctx.stats.utilisation("value_range(margin used)", worst, 1.0)
    return [emit(ctx, spec, "value_range", f"value_range.out_of_bounds:{col}",
                 "value_range.out_of_bounds", col in spec.key_columns,
                 {"min": bounds[col][0], "max": bounds[col][1], "derived_from": bounds[col][2]},
                 per_file)
            for col, per_file in sorted(hits.items())]
