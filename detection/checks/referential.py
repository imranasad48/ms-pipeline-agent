"""Referential family: child keys present in the parent source, at daily granularity.

Pairs come from the manifest (``referential_integrity``: child "3pl.sales_order_id", parent
"crm.order_id") plus ``config.yaml -> referential.extra``. "3pl"/"crm" are the first directory
of a source's path (the same name the calendar uses); a ref "crm.order_id" means every source in
that directory whose contract has the column (a directory can hold several sources: the CRM
order lines and the CRM customer master both live in crm/). Child side: each such source is
checked. Parent side: the keys of all of them together, reliable only if each is reliable. For business date D the child side is
D's files; the parent side is the parent's whole history up to D (a delayed shipment can
reference an order placed many days earlier). The check is skipped for a day when the parent's
recent files are missing or stale (freshness / stale checks already report those). ``known_values`` pairs a column
with the set of values seen in clean data (small domains such as device/location ids).
"""
from __future__ import annotations

import datetime as dt
from collections import defaultdict

from ..readers import profile_file
from ..schedule import expected_files, source_dir
from .common import emit
from .context import Context


def _parse(ref: str) -> tuple[str, str]:
    d, _, col = ref.partition(".")
    return d, col


def _file_keys(spec, slot, col: str, ctx: Context) -> tuple:
    """(readable, frozenset of the column's values, content hash) for one parent file, cached
    by path+mtime+size so each file is parsed once for the whole replay."""
    try:
        st = slot.path.stat()
        key = ("pk", spec.fingerprint, str(slot.path), st.st_mtime_ns, st.st_size, col)
    except OSError:
        return False, frozenset(), ""
    if key not in ctx.cache:
        p = profile_file(slot, spec, ctx.config["nulls"]["null_tokens"])
        ctx.cache[key] = (p.readable, frozenset(r.get(col) for r in p.records
                                                if r.get(col) is not None), p.content_hash)
    return ctx.cache[key]


def _parent_keys(spec, col: str, business_date: dt.date, ctx: Context):
    """(keys, reliable). keys = the column's values over the parent's whole history up to the
    date (a delayed shipment may reference an order many days old; a genuine orphan is in no
    file at all). reliable = every parent file due in the last ``required_days`` is readable
    and none is a stale re-send of its predecessor -- otherwise absence proves nothing."""
    cfg = ctx.config["referential"]
    seen_rel, keys, reliable, prev_hash, started = set(), set(), True, None, False
    for back in range(cfg["history_days"], -1, -1):
        for slot in expected_files(spec, business_date - dt.timedelta(days=back),
                                   ctx.data_root, ctx.config):
            if slot.rel in seen_rel:
                continue
            seen_rel.add(slot.rel)
            ok, vals, h = _file_keys(spec, slot, col, ctx)
            keys |= vals
            started = started or ok
            # a file absent *before the parent's history began* is not a gap (cold start);
            # a repeat of the previous period is a stale re-send, except for a snapshot source,
            # which legitimately re-delivers the same state (its own stale check judges that)
            if back <= cfg["required_days"] and ((not ok and started)
                                                 or (not ok and slot.path.exists())
                                                 or (h and h == prev_hash and spec.delivery != "snapshot")):
                reliable = False
            prev_hash = h if ok else None
    return keys, reliable


def referential_pairs(manifests, ctx: Context) -> list:
    """Unique (child ref, parent ref) pairs: manifest-declared first, then config extras."""
    pairs = []
    for m in manifests:
        pairs += [tuple(x) for x in m.referential]
    pairs += [(e["child"], e["parent"]) for e in ctx.config["referential"]["extra"]]
    return list(dict.fromkeys(pairs))


def check_referential(manifests, business_date: dt.date, ctx: Context, profiles_of) -> dict:
    """Return {child source fingerprint: [Finding]}. ``profiles_of(spec)`` -> this date's profiles."""
    cfg = ctx.config["referential"]
    by_dir: dict = {}
    for m in manifests:
        for s in m.sources:
            group = by_dir.setdefault(source_dir(s), [])
            if all(s.fingerprint != x.fingerprint for x in group):
                group.append(s)

    def having(d: str, col: str) -> list:
        return [s for s in by_dir.get(d, []) if col in s.expected_schema]

    out: dict = defaultdict(list)
    for child_ref, parent_ref in referential_pairs(manifests, ctx):
        (cdir, ccol), (pdir, pcol) = _parse(child_ref), _parse(parent_ref)
        parents = having(pdir, pcol)
        if not parents:
            continue
        for child in having(cdir, ccol):
            _check_pair(child, ccol, parents, pdir, pcol, business_date, ctx, profiles_of, cfg, out)
    _check_known_values(manifests, ctx, profiles_of, cfg, out)
    return out


def _union_parent_keys(parents: list, col: str, business_date: dt.date, ctx: Context):
    keys, reliable = set(), True
    for spec in parents:
        k, ok = _parent_keys(spec, col, business_date, ctx)
        keys |= k
        reliable = reliable and ok
    return keys, reliable


def _check_pair(child, ccol, parents, pdir, pcol, business_date, ctx, profiles_of, cfg, out) -> None:
    profiles = [p for p in profiles_of(child) if p.readable and p.records]
    if not profiles:
        return
    parent_keys = None
    orphans = {}
    for p in profiles:
        ctx.stats.evaluate("referential.orphan")
        vals = [r.get(ccol) for r in p.records if r.get(ccol) not in (None, "")]
        if not vals:
            continue
        if parent_keys is None:
            parent_keys, reliable = _union_parent_keys(parents, pcol, business_date, ctx)
            if not reliable:            # parent file missing/stale: cannot judge orphans
                ctx.stats.evaluate("referential.skipped_unreliable_parent")
                break
        bad = [v for v in vals if v not in parent_keys]
        ctx.stats.utilisation("referential.orphan", len(bad) / len(vals),
                              max(cfg["max_orphan_rate"], 1e-9))
        if bad and len(bad) / len(vals) > cfg["max_orphan_rate"]:
            orphans[p.rel] = {"orphans": len(bad), "of": len(vals),
                              "examples": sorted(set(map(str, bad)))[:3]}
    if orphans:
        out[child.fingerprint].append(emit(
            ctx, child, "referential", f"referential.orphan:{ccol}->{pdir}.{pcol}",
            "referential.orphan", ccol in child.key_columns,
            {"child_keys_subset_of": "|".join(f"{x.source_id}.{pcol}" for x in parents),
             "parent_required_days": cfg["required_days"]}, orphans))


def _check_known_values(manifests, ctx: Context, profiles_of, cfg, out) -> None:
    """known-value domains learned from clean data"""
    known = ctx.config["baselines"].get("known_values", {})
    seen = set()
    for m in manifests:
        for spec in m.sources:
            if spec.fingerprint in seen:
                continue
            seen.add(spec.fingerprint)
            for col in cfg["known_values"].get(spec.source_id, []):
                domain = set(known.get(spec.source_id, {}).get(col, []))
                if not domain:
                    continue
                orphans = {}
                for p in profiles_of(spec):
                    if not p.readable or not p.records:
                        continue
                    ctx.stats.evaluate("referential.orphan")
                    vals = [r.get(col) for r in p.records if r.get(col) not in (None, "")]
                    bad = [v for v in vals if v not in domain]
                    if bad:
                        orphans[p.rel] = {"orphans": len(bad), "of": len(vals),
                                          "examples": sorted(set(map(str, bad)))[:3]}
                if orphans:
                    out[spec.fingerprint].append(emit(
                        ctx, spec, "referential", f"referential.unknown_value:{col}",
                        "referential.orphan", col in spec.key_columns,
                        {"values_subset_of": "values seen in clean data"}, orphans))
