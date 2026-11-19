"""Read one source file into a FileProfile: columns, row count, nulls, bad values."""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import math
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from .manifest import SourceSpec
from .schedule import ExpectedFile

_INT_RE = re.compile(r"^[+-]?\d+$")
CONTENT_FORMATS = {"csv", "json", "sqlite"}   # others get freshness checks only


# --------------------------------------------------------------------- types
def _is_iso(text: str) -> bool:
    try:
        dt.datetime.fromisoformat(text)
        return True
    except ValueError:
        return False


def value_conforms(value, dtype: str) -> bool:
    """Does a non-null value fit the declared dtype? Strings are parsed, natives type-checked."""
    if isinstance(value, str) and dtype != "string":
        v = value.strip()
        if dtype == "int64":
            return bool(_INT_RE.match(v))
        if dtype == "float64":
            if "_" in v:
                return False
            try:
                return math.isfinite(float(v))
            except ValueError:
                return False
        if dtype in ("datetime", "date"):
            return _is_iso(v)
        if dtype == "bool":
            return v.lower() in {"true", "false", "0", "1"}
        return True
    if dtype == "int64":
        return (isinstance(value, int) and not isinstance(value, bool)) or \
               (isinstance(value, float) and value.is_integer())
    if dtype == "float64":
        return isinstance(value, (int, float)) and not isinstance(value, bool) \
            and math.isfinite(value)
    if dtype == "bool":
        return isinstance(value, bool)
    if dtype in ("datetime", "date"):
        return isinstance(value, str) and _is_iso(value)
    return isinstance(value, str)       # "string" and unknown dtypes


# ------------------------------------------------------------------ profile
@dataclass
class FileProfile:
    """What we learned by reading one expected file."""

    slot: ExpectedFile
    status: str = "ok"              # ok | missing | empty | truncated | unreadable
    detail: str = ""
    size: int = 0
    mtime: dt.datetime | None = None
    generated_at: dt.datetime | None = None    # 3PL-style envelope timestamp
    columns: list = field(default_factory=list)
    records: list = field(default_factory=list, repr=False)
    row_count: int = 0
    null_counts: dict = field(default_factory=dict)
    bad_values: dict = field(default_factory=dict)  # col -> {"bad": n, "non_null": n, "examples": []}

    @property
    def rel(self) -> str:
        return self.slot.rel

    @property
    def readable(self) -> bool:
        return self.status == "ok"


def _is_null(v, tokens: frozenset) -> bool:
    return v is None or (isinstance(v, str) and v.strip() in tokens)


def _read_csv(raw: bytes) -> tuple[list, list, str, str]:
    """Return (columns, records, status, detail) for CSV bytes."""
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as e:
        return [], [], "unreadable", f"not utf-8: {e}"
    rows = list(csv.reader(io.StringIO(text, newline="")))
    if not rows:
        return [], [], "empty", "no header"
    header, body = rows[0], rows[1:]
    ragged = [i for i, r in enumerate(body) if len(r) != len(header)]
    if ragged:
        if ragged == [len(body) - 1] or not raw.endswith(b"\n"):
            return header, [], "truncated", "last record cut mid-row"
        return header, [], "unreadable", f"{len(ragged)} ragged rows"
    if not raw.endswith(b"\n"):
        return header, [], "truncated", "no trailing newline"
    return header, [dict(zip(header, r)) for r in body], "ok", ""


def _read_json(raw: bytes) -> tuple[list, list, str, str, dict]:
    try:
        doc = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        return [], [], "truncated", f"JSON parse error: {e}", {}
    env: dict = {}
    if isinstance(doc, list):
        records = doc
    elif isinstance(doc, dict):
        key = "events" if "events" in doc else next(
            (k for k, v in doc.items() if isinstance(v, list)), None)
        if key is None:
            return [], [], "unreadable", "no records array", {}
        records, env = doc[key], doc
    else:
        return [], [], "unreadable", "unexpected JSON root", {}
    if not all(isinstance(r, dict) for r in records):
        return [], [], "unreadable", "non-object records", {}
    declared = env.get("record_count")
    if declared is not None and declared != len(records):
        return [], [], "truncated", f"record_count {declared} != {len(records)} events", env
    cols: list = []
    for r in records:
        for k in r:
            if k not in cols:
                cols.append(k)
    return cols, records, "ok", "", env


def _read_sqlite(path: Path, table: str) -> tuple[list, list, str, str]:
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            cur = con.execute(f'SELECT * FROM "{table}"')
            cols = [d[0] for d in cur.description]
            return cols, [dict(zip(cols, r)) for r in cur.fetchall()], "ok", ""
        finally:
            con.close()
    except sqlite3.Error as e:
        return [], [], "unreadable", f"sqlite error: {e}"


def profile_file(slot: ExpectedFile, spec: SourceSpec, null_tokens) -> FileProfile:
    """Stat and (for csv/json/sqlite) parse one expected file."""
    tokens = frozenset(null_tokens)
    p = FileProfile(slot)
    if not slot.path.exists():
        p.status = "missing"
        return p
    st = slot.path.stat()
    p.size, p.mtime = st.st_size, dt.datetime.fromtimestamp(st.st_mtime)
    if st.st_size == 0:
        p.status, p.detail = "empty", "zero bytes"
        return p
    if spec.format not in CONTENT_FORMATS:
        return p                                   # existence/size/mtime only
    env: dict = {}
    if spec.format == "csv":
        cols, recs, p.status, p.detail = _read_csv(slot.path.read_bytes())
    elif spec.format == "json":
        cols, recs, p.status, p.detail, env = _read_json(slot.path.read_bytes())
    else:
        cols, recs, p.status, p.detail = _read_sqlite(slot.path, slot.table)
    if env.get("generated_at"):
        try:
            p.generated_at = dt.datetime.fromisoformat(env["generated_at"]).replace(tzinfo=None)
        except (TypeError, ValueError):
            pass
    if p.status != "ok":
        return p
    p.columns, p.records, p.row_count = cols, recs, len(recs)
    for col in cols:
        vals = [r.get(col) for r in recs]
        p.null_counts[col] = sum(_is_null(v, tokens) for v in vals)
        dtype = spec.expected_schema.get(col)
        if dtype is None:
            continue
        rx = re.compile(spec.formats[col]) if col in spec.formats else None
        bad, examples, non_null = 0, [], 0
        for v in vals:
            if _is_null(v, tokens):
                continue
            non_null += 1
            ok = value_conforms(v, dtype) and (rx is None or not isinstance(v, str)
                                               or rx.match(v.strip()) is not None)
            if not ok:
                bad += 1
                if len(examples) < 3 and repr(v)[:40] not in examples:
                    examples.append(repr(v)[:40])
        p.bad_values[col] = {"bad": bad, "non_null": non_null, "examples": examples}
    return p
