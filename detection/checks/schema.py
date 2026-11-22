"""Schema family: missing_column / new_column / renamed / type_change / column_order."""
from __future__ import annotations

import re
from collections import defaultdict
from difflib import SequenceMatcher

from ..config import resolve_severity
from ..incidents import Finding
from ..manifest import SourceSpec
from ..readers import FileProfile, value_conforms
from .context import Context


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def pair_renames(spec: SourceSpec, prof: FileProfile, missing: list, new: list,
                 min_sim: float) -> dict:
    """Match missing columns to new ones that look like renames (old -> new).

    A pair needs a column whose values still fit the old dtype, and either the same
    position in the header or a similar name. Best-scoring pairs win.
    """
    exp_cols = list(spec.expected_schema)
    scored = []
    for m in missing:
        for n in new:
            vals = [r.get(n) for r in prof.records if r.get(n) not in (None, "")]
            if not all(value_conforms(v, spec.expected_schema[m]) for v in vals):
                continue
            sim = SequenceMatcher(None, _norm(m), _norm(n)).ratio()
            same_pos = (n in prof.columns and prof.columns.index(n) == exp_cols.index(m))
            if same_pos or sim >= min_sim:
                scored.append((int(same_pos) + sim, m, n))
    pairs, used_m, used_n = {}, set(), set()
    for _, m, n in sorted(scored, key=lambda t: (-t[0], t[1], t[2])):
        if m not in used_m and n not in used_n:
            pairs[m] = n
            used_m.add(m)
            used_n.add(n)
    return pairs


def check_schema(spec: SourceSpec, profiles: list[FileProfile], ctx: Context) -> list[Finding]:
    """Compare each readable file's header/types to the manifest's expected_schema."""
    cfg = ctx.config
    exp = list(spec.expected_schema)
    hits = defaultdict(dict)       # check_id -> {file: detail}
    meta = {}                      # check_id -> (kind, is_key, expected)

    def hit(check_id, kind, is_key, rel, detail, expected):
        hits[check_id][rel] = detail
        meta[check_id] = (kind, is_key, expected)

    for p in profiles:
        if not p.readable or not p.columns:
            continue          # unreadable files are the freshness family's problem
        ctx.stats.evaluate("schema.header")
        if p.corrupt_lines:
            hit("schema.corrupt_line", "schema.corrupt_line", False, p.rel,
                {"corrupt_lines": len(p.corrupt_lines), "of": p.row_count + len(p.corrupt_lines),
                 "first_line_numbers": p.corrupt_lines[:10]}, {"corrupt_lines": 0})
        if spec.format == "xlsx":
            if spec.sheet and spec.sheet not in p.sheet_names:
                hit("schema.sheet_renamed", "schema.sheet_renamed", False, p.rel,
                    {"sheets": p.sheet_names}, {"sheet": spec.sheet})
            if spec.header_row and p.header_row != spec.header_row:
                hit("schema.header_row_shift", "schema.header_row_shift", False, p.rel,
                    {"header_row": p.header_row}, {"header_row": spec.header_row})
        obs = p.columns
        missing = [c for c in exp if c not in obs]
        new = [c for c in obs if c not in spec.expected_schema]
        renames = pair_renames(spec, p, missing, new,
                               cfg["schema"]["rename_min_name_similarity"])
        for old, nw in renames.items():
            hit(f"schema.renamed:{old}->{nw}", "schema.renamed", old in spec.key_columns,
                p.rel, {"column": nw}, {"column": old})
        for c in missing:
            if c not in renames:
                hit(f"schema.missing_column:{c}", "schema.missing_column",
                    c in spec.key_columns, p.rel, {"present": False}, {"present": True})
        renamed_new = set(renames.values())
        for c in new:
            if c not in renamed_new:
                hit(f"schema.new_column:{c}", "schema.new_column", False, p.rel,
                    {"present": True}, {"present": False})
        if spec.format != "json" and not renames:       # JSON object key order is not a contract
            common_exp = [c for c in exp if c in obs]
            common_obs = [c for c in obs if c in spec.expected_schema]
            if common_exp != common_obs:
                hit("schema.column_order", "schema.column_order", False, p.rel,
                    {"order": common_obs}, {"order": common_exp})
        tol = cfg["schema"]["type_bad_rate_tolerance"]
        for c, b in p.bad_values.items():
            if c in renames or b["non_null"] == 0:
                continue
            ctx.stats.evaluate("schema.type_change")
            if b["bad"] / b["non_null"] > tol:
                fmt = spec.formats.get(c)
                hit(f"schema.type_change:{c}", "schema.type_change", c in spec.key_columns,
                    p.rel, {"bad_values": b["bad"], "non_null": b["non_null"],
                            "examples": b["examples"]},
                    {"dtype": spec.expected_schema[c], **({"format": fmt} if fmt else {})})

    findings = []
    for check_id, per_file in sorted(hits.items()):
        kind, is_key, expected = meta[check_id]
        ctx.stats.fire(check_id)
        findings.append(Finding(
            spec.source_id, check_id, "schema", resolve_severity(cfg, kind, is_key),
            {"column_contract": expected},
            {"files_affected": len(per_file), "per_file": per_file}, list(per_file)))
    return findings
