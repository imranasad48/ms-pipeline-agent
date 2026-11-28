"""Source catalog for "New pipeline": every source under the data root, and which manifests use it.

A source is either a *file group* (files in one folder whose names differ only by a date/week
part, e.g. ``crm/customers_*.csv``) or a *SQLite table* (``erp/erp.db#products``). Catalog keys are
the only thing a generation request may name, so no free path ever reaches the job runner.

Files named ``error_manifest.*`` (the evaluation ground truth) are dropped by name before
anything else looks at them: never opened, never counted, never shown.
"""
from __future__ import annotations

import fnmatch
import re
import sqlite3
from pathlib import Path

import yaml

HIDDEN_PREFIX = "error_manifest."
SQLITE_EXT = {".db", ".sqlite", ".sqlite3"}
# 2026-08-14, 2026-08-14T05, 2026-08-14T05:00, 2026-W33, 20260814
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}(?:T\d{2}(?::?\d{2})?)?|\d{4}-W\d{2}|(?<!\d)\d{8}(?!\d)")


def withheld(name: str) -> bool:
    return name.startswith(HIDDEN_PREFIX) or name.startswith(".")


def _files(root: Path):
    """Relative file paths under root, skipping withheld names and hidden dirs (no symlink escape)."""
    root = root.resolve()
    if not root.is_dir():
        return
    stack = [root]
    while stack:
        d = stack.pop()
        try:
            entries = sorted(d.iterdir())
        except OSError:
            continue
        for p in entries:
            if withheld(p.name) or p.is_symlink():
                continue
            if p.is_dir():
                stack.append(p)
            elif p.is_file():
                yield p.relative_to(root).as_posix()


def _sqlite_tables(path: Path) -> list[tuple[str, int]]:
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            names = [r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
            return [(n, len(con.execute(f'PRAGMA table_info("{n}")').fetchall())) for n in names]
        finally:
            con.close()
    except sqlite3.Error:
        return []


def manifest_patterns(manifests_dir: Path) -> list[tuple[str, str]]:
    """[(pipeline_id, data-root-relative path_pattern)] from every manifest's sources."""
    out = []
    for p in sorted(Path(manifests_dir).glob("*.yaml")):
        try:
            m = yaml.safe_load(p.read_text())
        except (OSError, yaml.YAMLError):
            continue
        if not isinstance(m, dict):
            continue
        for s in m.get("sources") or []:
            if not isinstance(s, dict):
                continue
            pat = str(s.get("path_pattern") or "")
            pat = re.sub(r"^\$\{?DATA_ROOT\}?/?", "", pat)
            if pat:
                out.append((m.get("pipeline_id", p.stem), pat))
    return out


def build_catalog(data_root: Path, manifests_dir: Path) -> list[dict]:
    """Every source, uncovered first, then by key."""
    root = Path(data_root)
    groups: dict[str, dict] = {}
    tables = []
    for rel in _files(root):
        folder, name = rel.rsplit("/", 1) if "/" in rel else ("", rel)
        if Path(name).suffix.lower() in SQLITE_EXT:
            for t, ncols in _sqlite_tables(root / rel):
                tables.append({"key": f"{rel}#{t}", "kind": "table", "folder": folder, "name": t,
                               "file": rel, "columns": ncols, "files": [rel]})
            continue
        dates = _DATE.findall(name)
        pattern = _DATE.sub("*", name) if dates else name
        key = f"{folder}/{pattern}" if folder else pattern
        g = groups.setdefault(key, {"key": key, "kind": "files", "folder": folder, "name": pattern,
                                    "files": [], "dates": []})
        g["files"].append(rel)
        g["dates"] += dates
    out = []
    for g in groups.values():
        d = sorted(g.pop("dates"))
        g["count"] = len(g["files"])
        g["date_range"] = (d[0], d[-1]) if d else None
        out.append(g)
    out += tables
    pats = manifest_patterns(manifests_dir)
    for s in out:
        s["covered_by"] = sorted({pid for pid, pat in pats
                                  if pat == s["key"] or any(fnmatch.fnmatchcase(f, pat) for f in s["files"])
                                  or ("#" in pat and fnmatch.fnmatchcase(s["key"], pat))})
        s["suggest"] = suggest_id(s)
    out.sort(key=lambda s: (bool(s["covered_by"]), s["key"]))
    return out


def suggest_id(s: dict) -> str:
    base = s["name"] if s["kind"] == "table" else re.sub(r"\.[^.]*$", "", s["name"])
    base = re.sub(r"[^a-z0-9]+", "_", base.lower().replace("*", "")).strip("_")
    if s["kind"] == "table" and s["folder"]:
        base = f"{re.sub(r'[^a-z0-9]+', '_', s['folder'].lower()).strip('_')}_{base}"
    if not base or not base[0].isalpha():
        base = "p_" + base
    return base[:48]
