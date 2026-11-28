"""Weekly vendor price sheets (manual xlsx) -> warehouse table `vendor_prices` (full reload).

Price history is preserved: one row per price line per weekly sheet, tagged with
`week` (ISO week from the filename) and `source_file`, plus `loaded_at` (UTC
timestamp of the load run). Defensive parsing: banner rows skipped (header row
located by content), header aliases normalised, numerics coerced (thousand
separators / currency text), dates parsed from strings or Excel serials, text
trimmed, fully blank / footer rows dropped. Duplicates are NOT removed.
Pure functions, stdlib + pandas only. Steps hand over via pickles in STAGING_DIR.
"""
import glob
import os
import re
import sqlite3
from datetime import datetime, timezone

import pandas as pd

PIPELINE_ID = "vendor_prices"
TABLE = "vendor_prices"
GLOB = "manual/vendor_price_sheet_*.xlsx"

# canonical column -> (type, accepted normalised header aliases)
COLUMNS = {
    "supplier_code": ("string", ["suppliercode", "supplierid", "vendorcode", "vendorid", "supplier"]),
    "supplier_name": ("string", ["suppliername", "vendorname", "vendor"]),
    "product_code": ("string", ["productcode", "productid", "sku", "itemcode"]),
    "product_name": ("string", ["productname", "description", "product", "itemname"]),
    "pack_size": ("string", ["packsize", "pack", "packing"]),
    "unit_price": ("float", ["unitprice", "price", "unitcost", "rate"]),
    "currency": ("string", ["currency", "ccy", "cur"]),
    "moq": ("int64", ["moq", "minorderqty", "minimumorderquantity", "minqty"]),
    "effective_from": ("date", ["effectivefrom", "effectivedate", "validfrom", "from"]),
    "valid_until": ("date", ["validuntil", "validto", "expirydate", "until", "to"]),
    "cold_chain": ("bool", ["coldchain", "coldchainrequired", "cold"]),
    "notes": ("string", ["notes", "note", "comments", "remarks"]),
}
META = {"week": "string", "source_file": "string", "loaded_at": "string"}
OUT_COLS = list(COLUMNS) + list(META)
SQL_TYPES = {"string": "TEXT", "date": "TEXT", "int64": "INTEGER", "float": "REAL", "bool": "INTEGER"}
REQUIRED = [c for c in OUT_COLS if c != "notes"]
KEY = ["week", "supplier_code", "product_code"]
_TRUE = {"yes", "y", "true", "t", "1", "x"}
_FALSE = {"no", "n", "false", "f", "0", ""}


def data_root() -> str:
    return os.environ.get("DATA_ROOT", "/opt/testbed/data_clean")


def warehouse_db() -> str:
    return os.environ.get("WAREHOUSE_DB", "/opt/warehouse/warehouse.db")


def _stage(name: str) -> str:
    d = os.environ.get("STAGING_DIR", os.path.join("/tmp", PIPELINE_ID))
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, name)


def _norm(v) -> str:
    return re.sub(r"[^a-z0-9]", "", str(v).lower()) if v is not None and v == v else ""


_ALIAS = {a: c for c, (_, al) in COLUMNS.items() for a in al}


def _files() -> list:
    return sorted(glob.glob(os.path.join(data_root(), GLOB)))


def _week(path: str):
    m = re.search(r"(\d{4})-?W(\d{1,2})", os.path.basename(path), re.I)
    return f"{m.group(1)}-W{int(m.group(2)):02d}" if m else None


def _find_header(raw: pd.DataFrame) -> int:
    best, best_i = 0, -1
    for i in range(min(len(raw), 30)):
        hits = len({_ALIAS[_norm(v)] for v in raw.iloc[i] if _norm(v) in _ALIAS})
        if hits > best:
            best, best_i = hits, i
    if best < 6:
        raise ValueError(f"header row not found (best match {best} columns)")
    return best_i


def _is_data_row(df: pd.DataFrame) -> pd.Series:
    """A price line has a supplier or product code (excludes blank/footer/total rows)."""
    def present(v):
        return v is not None and v == v and str(v).strip() != ""
    return df["supplier_code"].map(present) | df["product_code"].map(present)


def read_workbook(path: str) -> pd.DataFrame:
    """Read every data sheet of one workbook into canonical columns (raw values)."""
    parts = []
    for _, raw in pd.read_excel(path, sheet_name=None, header=None, dtype=object).items():
        raw = raw.dropna(how="all").reset_index(drop=True)
        if raw.empty:
            continue
        try:
            h = _find_header(raw)
        except ValueError:
            continue  # non-data sheet (e.g. Instructions)
        names = [_ALIAS.get(_norm(v)) for v in raw.iloc[h]]
        body = raw.iloc[h + 1:].copy()
        body.columns = [n or f"_x{i}" for i, n in enumerate(names)]
        body = body.loc[:, ~body.columns.duplicated()]
        for c in COLUMNS:
            if c not in body.columns:
                body[c] = None
        body = body[list(COLUMNS)]
        parts.append(body[_is_data_row(body)])
    if not parts:
        raise ValueError(f"no data sheet found in {path}")
    df = pd.concat(parts, ignore_index=True)
    df["week"] = _week(path)
    df["source_file"] = os.path.basename(path)
    return df


def extract() -> dict:
    files = _files()
    if not files:
        raise FileNotFoundError(f"no files match {os.path.join(data_root(), GLOB)}")
    df = pd.concat([read_workbook(f) for f in files], ignore_index=True)
    df.to_pickle(_stage("raw.pkl"))
    return {"files": len(files), "rows": len(df)}


def _to_num(s: pd.Series) -> pd.Series:
    def clean(v):
        if isinstance(v, str):
            v = re.sub(r"[^0-9.\-]", "", v.replace(",", ""))
        return v
    return pd.to_numeric(s.map(clean), errors="coerce")


def _to_date(s: pd.Series) -> pd.Series:
    def one(v):
        if v is None or v != v or (isinstance(v, str) and not v.strip()):
            return None
        if isinstance(v, (int, float)) and not isinstance(v, bool):  # Excel serial
            d = pd.to_datetime(v, unit="D", origin="1899-12-30", errors="coerce")
        else:
            d = pd.to_datetime(str(v).strip(), errors="coerce")
        return None if pd.isna(d) else d.strftime("%Y-%m-%d")
    return s.map(one).astype("string")


def _to_bool(s: pd.Series) -> pd.Series:
    def one(v):
        if v is None or v != v:
            return None
        k = str(v).strip().lower()
        return 1 if k in _TRUE else 0 if k in _FALSE else None
    return s.map(one).astype("Int64")


def transform() -> dict:
    df = pd.read_pickle(_stage("raw.pkl"))
    for col, (t, _) in COLUMNS.items():
        if t == "string":
            df[col] = df[col].map(lambda v: None if v is None or v != v else str(v).strip() or None).astype("string")
        elif t == "int64":
            df[col] = _to_num(df[col]).round().astype("Int64")
        elif t == "float":
            df[col] = _to_num(df[col]).astype("float64")
        elif t == "bool":
            df[col] = _to_bool(df[col])
        else:
            df[col] = _to_date(df[col])
    for c in ("supplier_code", "product_code", "currency"):
        df[c] = df[c].str.upper()
    df = df.reset_index(drop=True)  # no de-duplication: one row per price line per sheet
    df.to_pickle(_stage("clean.pkl"))
    return {"rows": len(df)}


def load() -> dict:
    df = pd.read_pickle(_stage("clean.pkl"))
    df["loaded_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    df = df[OUT_COLS]
    types = {**{c: t for c, (t, _) in COLUMNS.items()}, **META}
    db = warehouse_db()
    os.makedirs(os.path.dirname(os.path.abspath(db)), exist_ok=True)
    cols = ", ".join(f'"{c}" {SQL_TYPES[types[c]]}' for c in OUT_COLS)
    con = sqlite3.connect(db)
    try:
        with con:
            con.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
            con.execute(f'CREATE TABLE "{TABLE}" ({cols})')
            rows = df.astype(object).where(df.notna(), None).values.tolist()
            con.executemany(f'INSERT INTO "{TABLE}" VALUES ({",".join("?" * len(OUT_COLS))})', rows)
    finally:
        con.close()
    return {"rows": len(df)}


def validate() -> dict:
    errors = []
    files = _files()
    expected = {os.path.basename(f): len(read_workbook(f)) for f in files}
    con = sqlite3.connect(warehouse_db())
    q = lambda sql: con.execute(sql).fetchall()
    try:
        n = q(f'SELECT COUNT(*) FROM "{TABLE}"')[0][0]
        if n == 0 or n != sum(expected.values()):
            errors.append(f"row count {n} != source price lines {sum(expected.values())}")
        got = dict(q(f'SELECT source_file, COUNT(*) FROM "{TABLE}" GROUP BY source_file'))
        if got != expected:
            errors.append(f"per-file row counts differ: {sorted(set(got.items()) ^ set(expected.items()))}")
        have = [r[1] for r in q(f'PRAGMA table_info("{TABLE}")')]
        if have != OUT_COLS:
            errors.append(f"columns mismatch: {have}")
        for c in REQUIRED:
            k = q(f'SELECT COUNT(*) FROM "{TABLE}" WHERE "{c}" IS NULL')[0][0]
            if k:
                errors.append(f"{k} NULLs in {c}")
        k = q(f'SELECT COUNT(*) FROM (SELECT 1 FROM "{TABLE}" GROUP BY {", ".join(KEY)} HAVING COUNT(*)>1)')[0][0]
        if k:
            errors.append(f"{k} duplicate keys on {KEY}")
        k = q(f'SELECT COUNT(DISTINCT week), COUNT(DISTINCT source_file) FROM "{TABLE}"')[0]
        if k != (len(files), len(files)):
            errors.append(f"distinct weeks/files {k} != {len(files)} files")
        k = q(f"SELECT COUNT(*) FROM \"{TABLE}\" WHERE instr(source_file, week) = 0")[0][0]
        if k:
            errors.append(f"{k} rows where week does not match source_file")
        k = q(f'SELECT COUNT(*) FROM "{TABLE}" WHERE unit_price <= 0 OR moq <= 0')[0][0]
        if k:
            errors.append(f"{k} rows with non-positive unit_price/moq")
        iso = "'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'"
        k = q(f'SELECT COUNT(*) FROM "{TABLE}" WHERE effective_from NOT GLOB {iso} OR valid_until NOT GLOB {iso}')[0][0]
        if k:
            errors.append(f"{k} malformed dates")
        k = q(f'SELECT COUNT(*) FROM "{TABLE}" WHERE valid_until < effective_from')[0][0]
        if k:
            errors.append(f"{k} rows with valid_until < effective_from")
        k = q(f'SELECT COUNT(*) FROM "{TABLE}" WHERE cold_chain NOT IN (0, 1)')[0][0]
        if k:
            errors.append(f"{k} rows with cold_chain not 0/1")
        la = [r[0] for r in q(f'SELECT DISTINCT loaded_at FROM "{TABLE}"')]
        if len(la) != 1 or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00", str(la[0])):
            errors.append(f"loaded_at not a single UTC ISO timestamp: {la[:3]}")
    finally:
        con.close()
    if errors:
        raise AssertionError("; ".join(errors))
    return {"rows": n}


def run_all() -> dict:
    extract(); transform(); load()
    return validate()
