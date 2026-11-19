"""Run every compiled check for a business date and turn findings into incidents."""
from __future__ import annotations

import datetime as dt

from .checks.context import Context
from .checks.freshness import check_freshness
from .checks.run_health import check_run_health
from .checks.schema import check_schema
from .checks.volume_nulls import check_volume_nulls
from .incidents import Incident, to_incident
from .manifest import Manifest, SourceSpec
from .readers import CONTENT_FORMATS, profile_file
from .schedule import expected_files


def profile_source(spec: SourceSpec, business_date: dt.date, ctx: Context) -> list:
    """Profile every file the calendar expects for this source and date."""
    return [profile_file(slot, spec, ctx.config["nulls"]["null_tokens"])
            for slot in expected_files(spec, business_date, ctx.data_root, ctx.config)]


def check_source(spec: SourceSpec, business_date: dt.date, ctx: Context) -> list:
    """All source-level findings (schema, volume/nulls, freshness) for one date."""
    profiles = profile_source(spec, business_date, ctx)
    findings = check_freshness(spec, profiles, ctx)
    if spec.format in CONTENT_FORMATS:
        findings += check_schema(spec, profiles, ctx)
        findings += check_volume_nulls(spec, profiles, ctx)
    return findings


def run_date(manifests: list[Manifest], business_date: dt.date, ctx: Context) -> list[Incident]:
    """Run all checks for all manifests on one date; deterministic and sorted.

    A source shared by two manifests is checked once, then reported per pipeline.
    """
    detected_at = ctx.now.isoformat(timespec="seconds")
    cache: dict = {}
    incidents: list[Incident] = []
    for m in manifests:
        findings = []
        for spec in m.sources:
            if spec.fingerprint not in cache:
                cache[spec.fingerprint] = check_source(spec, business_date, ctx)
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


def learn_row_count_bands(manifests: list[Manifest], dates, ctx: Context) -> dict:
    """Observe per-file row counts over (clean) dates -> {source_id: {min, max}}."""
    seen: dict = {}
    for d in dates:
        done = set()
        for m in manifests:
            for spec in m.sources:
                if spec.fingerprint in done or spec.format not in CONTENT_FORMATS:
                    continue
                done.add(spec.fingerprint)
                for p in profile_source(spec, d, ctx):
                    if p.readable:
                        lo, hi = seen.get(spec.source_id, (p.row_count, p.row_count))
                        seen[spec.source_id] = (min(lo, p.row_count), max(hi, p.row_count))
    return {k: {"min": lo, "max": hi} for k, (lo, hi) in sorted(seen.items())}
