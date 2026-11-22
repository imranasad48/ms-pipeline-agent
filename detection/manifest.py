"""Read pipeline manifests and compile them into per-source check specs."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field, replace
from pathlib import Path

import yaml


@dataclass(frozen=True)
class SourceSpec:
    """Everything the checks need to know about one manifest source."""

    source_id: str
    format: str
    path_pattern: str
    expected_schema: dict          # column -> dtype string, in manifest order
    baseline_rows: int             # total_rows seen at build
    baseline_files: int
    null_baselines: dict           # column -> null rate seen at build
    key_columns: frozenset
    formats: dict = field(default_factory=dict, hash=False)   # column -> regex
    unique_keys: tuple = ()        # tuple of column-tuples declared unique (validation_checks)
    range_bounds: dict = field(default_factory=dict, hash=False)  # col -> (min, max) hard bounds
    sheet: str | None = None       # expected worksheet name (xlsx), from observed.layout
    header_row: int | None = None  # expected 1-based header row (xlsx), from observed.layout

    @property
    def fingerprint(self) -> str:
        """Identity of the *checks* (same source in two manifests -> same value)."""
        blob = json.dumps([self.source_id, self.format, self.path_pattern,
                           list(self.expected_schema.items()),
                           sorted(self.key_columns), self.formats,
                           [list(k) for k in self.unique_keys],
                           sorted(self.range_bounds.items(), key=str)], sort_keys=True)
        return hashlib.sha1(blob.encode()).hexdigest()[:12]


@dataclass(frozen=True)
class Manifest:
    pipeline_id: str
    sources: tuple
    dest_table: str
    dest_path: str
    path: Path | None = None
    referential: tuple = ()     # (child "dir.col", parent "dir.col") pairs the manifest declares


def _key_columns(raw_source: dict, manifest: dict, cfg: dict) -> frozenset:
    """Key columns: config wins, else columns of the manifest's `unique` checks."""
    sid = raw_source["id"]
    schema = raw_source.get("expected_schema", {})
    if sid in cfg.get("key_columns", {}):
        return frozenset(cfg["key_columns"][sid])
    cols = set()
    for chk in manifest.get("validation_checks", []):
        if chk.get("type") == "unique":
            cols.update(c for c in chk.get("params", {}).get("columns", []) if c in schema)
    return frozenset(cols)


def _unique_keys(raw_source: dict, manifest: dict) -> tuple:
    """Uniqueness constraints that apply to this *source*.

    A ``unique`` check applies when it names this source (``params.source``) or when the
    manifest has a single source (the check is then about that source's rows). In a
    multi-source manifest an unnamed ``unique`` is about the destination table, not any one
    source, so it is ignored here.
    """
    schema = raw_source.get("expected_schema", {})
    single = len(manifest.get("sources", [])) == 1
    keys = []
    for chk in manifest.get("validation_checks", []):
        if chk.get("type") != "unique":
            continue
        params = chk.get("params", {})
        if params.get("source", raw_source["id"] if single else None) != raw_source["id"]:
            continue
        # Derived columns (e.g. stock_counts' week/source_file, taken from the filename)
        # are constant inside one file, so dropping them keeps the key meaningful per file.
        cols = tuple(c for c in (params.get("columns") or params.get("key") or ())
                     if c in schema)
        if cols and cols not in keys:
            keys.append(cols)
    return tuple(keys)


def _range_bounds(raw_source: dict, manifest: dict) -> dict:
    """Hard [min, max] bounds from ``range`` checks whose columns belong to this source."""
    schema = raw_source.get("expected_schema", {})
    single = len(manifest.get("sources", [])) == 1
    out: dict = {}
    for chk in manifest.get("validation_checks", []):
        if chk.get("type") != "range":
            continue
        params = chk.get("params", {})
        if params.get("source", raw_source["id"] if single else None) != raw_source["id"]:
            continue
        cols = params.get("columns") or [params.get("column")]
        lo = params.get("min", params.get("min_exclusive"))
        hi = params.get("max", params.get("max_exclusive"))
        for c in cols:
            if c in schema:
                out[c] = (lo, hi)
    return out


def load_manifest(path: Path, cfg: dict) -> Manifest:
    """Parse one manifest YAML file."""
    raw = yaml.safe_load(Path(path).read_text())
    sources = []
    for s in raw.get("sources", []):
        obs = s.get("observed", {})
        sources.append(SourceSpec(
            source_id=s["id"], format=s["format"], path_pattern=s["path_pattern"],
            expected_schema=dict(s.get("expected_schema", {})),
            baseline_rows=int(obs.get("total_rows", 0)),
            baseline_files=int(obs.get("files", 1) or 1),
            null_baselines=dict(obs.get("null_rates", {})),
            key_columns=_key_columns(s, raw, cfg),
            formats=dict(cfg.get("formats", {}).get(s["id"], {})),
            unique_keys=_unique_keys(s, raw),
            range_bounds=_range_bounds(s, raw),
            sheet=(re.search(r"sheet '([^']+)'", obs.get("layout", "")) or [None, None])[1],
            header_row=(int(m.group(1)) if (m := re.search(r"header on row (\d+)",
                                                            obs.get("layout", ""))) else None),
        ))
    dest = raw.get("destination", {})
    refs = tuple((c["params"]["child"], c["params"]["parent"])
                 for c in raw.get("validation_checks", [])
                 if c.get("type") == "referential_integrity"
                 and {"child", "parent"} <= set(c.get("params", {})))
    return Manifest(raw["pipeline_id"], tuple(sources), dest.get("table", ""),
                    dest.get("path", ""), Path(path), refs)


def load_manifests(directory: Path, cfg: dict) -> list[Manifest]:
    """Load every ``*.yaml`` manifest in a directory (sorted by name).

    A source shared by several manifests is checked once, so its declared uniqueness and
    range constraints are merged (union) across them -- otherwise the result would depend
    on which manifest happened to be loaded first.
    """
    manifests = [load_manifest(p, cfg) for p in sorted(Path(directory).glob("*.yaml"))]
    keys: dict = {}
    bounds: dict = {}
    for m in manifests:
        for s in m.sources:
            ks = keys.setdefault(s.source_id, [])
            ks += [k for k in s.unique_keys if k not in ks]
            bounds.setdefault(s.source_id, {}).update(s.range_bounds)
    return [replace(m, sources=tuple(
        replace(s, unique_keys=tuple(keys[s.source_id]), range_bounds=dict(bounds[s.source_id]))
        for s in m.sources)) for m in manifests]
