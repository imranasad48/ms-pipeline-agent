"""Read pipeline manifests and compile them into per-source check specs."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
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

    @property
    def fingerprint(self) -> str:
        """Identity of the *checks* (same source in two manifests -> same value)."""
        blob = json.dumps([self.source_id, self.format, self.path_pattern,
                           list(self.expected_schema.items()),
                           sorted(self.key_columns), self.formats], sort_keys=True)
        return hashlib.sha1(blob.encode()).hexdigest()[:12]


@dataclass(frozen=True)
class Manifest:
    pipeline_id: str
    sources: tuple
    dest_table: str
    dest_path: str
    path: Path | None = None


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
        ))
    dest = raw.get("destination", {})
    return Manifest(raw["pipeline_id"], tuple(sources), dest.get("table", ""),
                    dest.get("path", ""), Path(path))


def load_manifests(directory: Path, cfg: dict) -> list[Manifest]:
    """Load every ``*.yaml`` manifest in a directory (sorted by name)."""
    return [load_manifest(p, cfg) for p in sorted(Path(directory).glob("*.yaml"))]
