"""Read one source file into a FileProfile: columns, row count, nulls, bad values."""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import json
import math
import re
import sqlite3
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

from .manifest import SourceSpec
from .schedule import ExpectedFile

_INT_RE = re.compile(r"^[+-]?\d+$")
CONTENT_FORMATS = {"csv", "json", "jsonl", "xlsx", "sqlite"}   # others get freshness checks only
# Formats whose values arrive with a native type (number, bool, string). There a string where
# the contract says number is a real type change; CSV is all text, so it is parsed instead.
NATIVE_FORMATS = {"json", "jsonl", "xlsx", "sqlite"}


# --------------------------------------------------------------------- types
def _is_iso(text: str) -> bool:
    try:
        dt.datetime.fromisoformat(text)
        return True
    except ValueError:
        return False


def value_conforms(value, dtype: str, strict: bool = False) -> bool:
    """Does a non-null value fit the declared dtype?

    Default: strings are parsed (right for CSV, where every value is text). ``strict``
    (native-typed formats: JSON/JSONL/XLSX/SQLite): a string is never a number or a bool,
    so ``"16.73"`` in a float column is a type change even though it would parse.
    """
    if strict and isinstance(value, str) and dtype in ("int64", "float64", "bool"):
        return False
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
    envelope: dict = field(default_factory=dict, repr=False)   # JSON page metadata (window_*, ...)
    corrupt_lines: list = field(default_factory=list)  # JSONL: 1-based numbers of unparsable lines
    sheet: str | None = None            # XLSX: worksheet actually read
    sheet_names: list = field(default_factory=list)
    header_row: int | None = None       # XLSX: 1-based row the header was found on
    content_hash: str = ""              # sha1 of the records (envelope/bytes ignored)

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
    # Fewer events than the page declares = cut short. MORE events than declared is not a
    # truncation (e.g. rows re-delivered/appended): the page stays readable so the content
    # checks (duplicates, ordering, window, ...) can see what is actually in it.
    if declared is not None and declared > len(records):
        return [], [], "truncated", f"record_count {declared} != {len(records)} events", env
    cols: list = []
    for r in records:
        for k in r:
            if k not in cols:
                cols.append(k)
    return cols, records, "ok", "", env


def _read_jsonl(raw: bytes) -> tuple[list, list, str, str, list]:
    """Line-delimited JSON. Unparsable lines are *counted*, not silently skipped."""
    text = raw.decode("utf-8", errors="replace")
    records, bad, last_bad = [], [], False
    lines = text.split("\n")
    for n, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            if not isinstance(obj, dict):
                raise ValueError("not an object")
            records.append(obj)
            last_bad = False
        except ValueError:
            bad.append(n)
            last_bad = True
    if not records:
        return [], [], "unreadable", f"no parsable JSON lines ({len(bad)} bad)", bad
    if last_bad and not raw.endswith(b"\n"):
        return [], [], "truncated", "last line cut mid-record", bad
    cols: list = []
    seen: set = set()
    for r in records:
        for k in r:
            if k not in seen:
                seen.add(k)
                cols.append(k)
    return cols, records, "ok", "", bad


def _norm_header(v) -> str:
    return re.sub(r"[^a-z0-9]", "", str(v).lower()) if v is not None else ""


def _xl_value(v):
    """Native cell value -> JSON-like value (dates become ISO strings, blanks None)."""
    if isinstance(v, dt.datetime):
        return v.isoformat(timespec="seconds") if v.time() != dt.time.min else v.date().isoformat()
    if isinstance(v, dt.date):
        return v.isoformat()
    if isinstance(v, str) and v == "":
        return None
    return v


def _read_xlsx(path: Path, spec: SourceSpec) -> tuple[dict, str, str]:
    """Open a workbook, pick the worksheet, find the header row *by content*.

    The header row is the first row in the top 30 whose cells match >= 3 expected column names
    after case/space/punctuation folding ("Product Code" == product_code). Headers that fold
    to an expected name are renamed to it; anything else keeps its raw text.
    """
    import openpyxl
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    except (zipfile.BadZipFile, OSError, KeyError, ValueError) as e:
        return {}, "unreadable", f"not a readable workbook: {e}"
    info: dict = {"sheet_names": list(wb.sheetnames)}
    try:
        name = spec.sheet if spec.sheet in wb.sheetnames else wb.sheetnames[0]
        info["sheet"] = name
        rows = list(wb[name].iter_rows(values_only=True))
    finally:
        wb.close()
    canon = {_norm_header(c): c for c in spec.expected_schema}
    best, best_i = 0, None
    for i, row in enumerate(rows[:30]):
        hits = sum(_norm_header(c) in canon for c in row if c is not None)
        if hits > best:
            best, best_i = hits, i
    if best_i is None or best < 3:
        return info, "unreadable", f"header row not found (best match {best} columns)"
    header = [canon.get(_norm_header(c), str(c).strip()) if c is not None else None
              for c in rows[best_i]]
    cols = [c for c in header if c is not None]
    records = []
    for row in rows[best_i + 1:]:
        rec = {h: _xl_value(v) for h, v in zip(header, row) if h is not None}
        if any(v is not None for v in rec.values()):
            records.append(rec)
    info.update(header_row=best_i + 1, columns=cols, records=records)
    return info, "ok", ""


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
    elif spec.format == "jsonl":
        cols, recs, p.status, p.detail, p.corrupt_lines = _read_jsonl(slot.path.read_bytes())
    elif spec.format == "xlsx":
        info, p.status, p.detail = _read_xlsx(slot.path, spec)
        p.sheet, p.sheet_names = info.get("sheet"), info.get("sheet_names", [])
        p.header_row = info.get("header_row")
        cols, recs = info.get("columns", []), info.get("records", [])
    else:
        cols, recs, p.status, p.detail = _read_sqlite(slot.path, slot.table)
    p.envelope = env
    if env.get("generated_at"):
        try:
            p.generated_at = dt.datetime.fromisoformat(env["generated_at"]).replace(tzinfo=None)
        except (TypeError, ValueError):
            pass
    if p.status != "ok":
        return p
    p.columns, p.records, p.row_count = cols, recs, len(recs)
    p.content_hash = hashlib.sha1(
        json.dumps(recs, sort_keys=True, default=str).encode()).hexdigest() if recs else ""
    strict = spec.format in NATIVE_FORMATS
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
            ok = value_conforms(v, dtype, strict) and (rx is None or not isinstance(v, str)
                                               or rx.match(v.strip()) is not None)
            if not ok:
                bad += 1
                if len(examples) < 3 and repr(v)[:40] not in examples:
                    examples.append(repr(v)[:40])
        p.bad_values[col] = {"bad": bad, "non_null": non_null, "examples": examples}
    return p


def cached_profile(slot: ExpectedFile, spec: SourceSpec, null_tokens, cache: dict,
                   keep: int = 200) -> FileProfile:
    """profile_file with a small FIFO cache keyed by path+mtime+size (stale/referential re-read
    yesterday's and last-hour's files; this avoids parsing each file several times)."""
    try:
        st = slot.path.stat()
        key = (spec.fingerprint, str(slot.path), st.st_mtime_ns, st.st_size)
    except OSError:
        return profile_file(slot, spec, null_tokens)
    if key not in cache:
        while len(cache) >= keep:
            cache.pop(next(iter(cache)))
        cache[key] = profile_file(slot, spec, null_tokens)
    return cache[key]
