"""Run every compiled check for a business date and turn findings into incidents."""
from __future__ import annotations

import datetime as dt

from .checks.common import as_number
from .checks.context import Context
from .checks.duplicates import check_duplicates
from .checks.freshness import check_freshness
from .checks.iot_behavior import check_iot_behavior
from .checks.ordering import check_ordering
from .checks.referential import check_referential
from .checks.run_health import check_run_health
from .checks.schema import check_schema
from .checks.stale import check_stale
from .checks.value_range import NUMERIC
from .checks.value_range import check_value_range
from .checks.volume_nulls import check_volume_nulls
from .checks.common import parse_ts
from .incidents import Incident, to_incident
from .manifest import Manifest, SourceSpec
from .readers import CONTENT_FORMATS, cached_profile
from .schedule import expected_files


def profile_source(spec: SourceSpec, business_date: dt.date, ctx: Context) -> list:
    """Profile every file the calendar expects for this source and date."""
    return [cached_profile(slot, spec, ctx.config["nulls"]["null_tokens"], ctx.cache)
            for slot in expected_files(spec, business_date, ctx.data_root, ctx.config)]


def check_source(spec: SourceSpec, business_date: dt.date, ctx: Context,
                 profiles: list | None = None) -> list:
    """All source-level findings for one date: freshness plus, for parsed formats, the
    schema, volume/null, duplicate, stale, ordering, value-range and IoT behaviour checks."""
    if profiles is None:
        profiles = profile_source(spec, business_date, ctx)
    findings = check_freshness(spec, profiles, ctx)
    if spec.format in CONTENT_FORMATS:
        findings += check_schema(spec, profiles, ctx)
        findings += check_volume_nulls(spec, profiles, ctx)
        findings += check_duplicates(spec, profiles, ctx)
        findings += check_stale(spec, business_date, profiles, ctx)
        findings += check_ordering(spec, profiles, ctx)
        findings += check_value_range(spec, profiles, ctx)
        findings += check_iot_behavior(spec, profiles, ctx)
    return findings


def run_date(manifests: list[Manifest], business_date: dt.date, ctx: Context) -> list[Incident]:
    """Run all checks for all manifests on one date; deterministic and sorted.

    A source shared by two manifests is checked once, then reported per pipeline.
    """
    detected_at = ctx.now.isoformat(timespec="seconds")
    cache: dict = {}
    profiles: dict = {}

    def profiles_of(spec: SourceSpec) -> list:
        if spec.fingerprint not in profiles:
            profiles[spec.fingerprint] = profile_source(spec, business_date, ctx)
        return profiles[spec.fingerprint]

    incidents: list[Incident] = []
    cross = check_referential(manifests, business_date, ctx, profiles_of)
    for m in manifests:
        findings = []
        for spec in m.sources:
            if spec.fingerprint not in cache:
                cache[spec.fingerprint] = (check_source(spec, business_date, ctx, profiles_of(spec))
                                           + cross.get(spec.fingerprint, []))
            findings += cache[spec.fingerprint]
        findings += check_run_health(m, business_date, ctx)
        incidents += [to_incident(f, m.pipeline_id, business_date, detected_at)
                      for f in findings]
    incidents.sort(key=lambda i: (i.pipeline_id, i.source_id, i.check_family, i.check_id))
    return incidents


def date_range(start: dt.date, end: dt.date):
    """Inclusive day iterator."""
    for n in range((end - start).days + 1):
        yield start + dt.timedelta(days=n)


def _numeric_columns(spec: SourceSpec) -> list:
    return [c for c, t in spec.expected_schema.items() if t in NUMERIC]


def learn_baselines(manifests: list[Manifest], dates, ctx: Context) -> dict:
    """Everything learned from CLEAN data, in baselines.yaml shape:

    row_count_bands, value_ranges (observed min/max of numeric columns), iot_behavior
    (reporting interval + always-on devices) and known_values (clean domains of
    ``referential.known_values`` columns).
    """
    icfg = ctx.config["iot_behavior"]["sources"]
    known_cols = ctx.config["referential"]["known_values"]
    bands: dict = {}
    ranges: dict = {}
    known: dict = {}
    diffs: dict = {}
    days_seen: dict = {}
    on_days: dict = {}
    for d in dates:
        done = set()
        for m in manifests:
            for spec in m.sources:
                if spec.fingerprint in done or spec.format not in CONTENT_FORMATS:
                    continue
                done.add(spec.fingerprint)
                for p in profile_source(spec, d, ctx):
                    if not p.readable:
                        continue
                    lo, hi = bands.get(spec.source_id, (p.row_count, p.row_count))
                    bands[spec.source_id] = (min(lo, p.row_count), max(hi, p.row_count))
                    for col in _numeric_columns(spec):
                        for r in p.records:
                            v = as_number(r.get(col))
                            if v is not None:
                                a, b = ranges.setdefault(spec.source_id, {}).get(col, (v, v))
                                ranges[spec.source_id][col] = (min(a, v), max(b, v))
                    for col in known_cols.get(spec.source_id, []):
                        known.setdefault(spec.source_id, {}).setdefault(col, set()).update(
                            r[col] for r in p.records if r.get(col) not in (None, ""))
                    if spec.source_id in icfg and p.slot.start is not None:
                        sc = icfg[spec.source_id]
                        per: dict = {}
                        for r in p.records:
                            t = parse_ts(r.get(sc["time_column"]))
                            if t is not None:
                                per.setdefault(r[sc["device_column"]], []).append(t)
                        days_seen[spec.source_id] = days_seen.get(spec.source_id, 0) + 1
                        for dev, ts in per.items():
                            ts.sort()
                            diffs.setdefault(spec.source_id, []).extend(
                                (b - a).total_seconds() / 60 for a, b in zip(ts, ts[1:]))
                            on_days.setdefault((spec.source_id, dev), []).append(
                                ((ts[0] - p.slot.start).total_seconds() / 60,
                                 (p.slot.end - ts[-1]).total_seconds() / 60))
    out: dict = {"row_count_bands": {k: {"min": lo, "max": hi} for k, (lo, hi) in sorted(bands.items())}}
    # A column that never varied in clean data (excursion_flag, is_late: always 0) has no
    # envelope to learn -- bounding it would flag the genuine events that set the flag.
    out["value_ranges"] = {sid: {c: {"min": round(lo, 6), "max": round(hi, 6)}
                                 for c, (lo, hi) in sorted(cols.items()) if lo != hi}
                           for sid, cols in sorted(ranges.items())}
    iot: dict = {}
    for sid, dd in diffs.items():
        dd.sort()
        interval = dd[len(dd) // 2]
        always = sorted(dev for (s2, dev), edges in on_days.items()
                        if s2 == sid and len(edges) == days_seen[sid]
                        and all(a <= interval and b <= 2 * interval for a, b in edges))
        iot[sid] = {"interval_minutes": round(interval, 3), "always_on_devices": always}
    if iot:
        out["iot_behavior"] = iot
    if known:
        out["known_values"] = {sid: {c: sorted(vs) for c, vs in cols.items()}
                               for sid, cols in sorted(known.items())}
    return out
