"""Manual weekly stock-count workbooks -> warehouse table `stock_counts` (full reload).

Defensive parsing: banner rows above the header are skipped (header row is
located by content), header names are normalised (case/space/punctuation),
numerics coerced (thousand separators, stray text -> NULL), dates parsed from
several formats / Excel serials, text trimmed, blank rows dropped.
Pure functions, stdlib + pandas only. Steps hand over via pickles in STAGING_DIR.
"""
import glob
import os
import re
import sqlite3

import pandas as pd

PIPELINE_ID = "stock_counts"
TABLE = "stock_counts"
GLOB = "manual/stock_count_*.xlsx"

# canonical column -> (type, accepted normalised header aliases)
COLUMNS = {
    "warehouse": ("string", ["warehouse", "warehouseid", "wh"]),
    "zone": ("string", ["zone", "zoneid"]),
    "product_code": ("string", ["productcode", "productid", "sku", "product"]),
    "product_name": ("string", ["productname", "description", "name"]),
    "batch_no": ("string", ["batchno", "batch", "batchnumber", "lot"]),
    "expiry_date": ("date", ["expirydate", "expiry", "expdate"]),
    "system_qty": ("int64", ["systemqty", "systemquantity", "sysqty"]),
    "counted_qty": ("int64", ["countedqty", "countedquantity", "count"]),
    "variance": ("int64", ["variance", "var", "diff"]),
    "uom": ("string", ["uom", "unit", "unitofmeasure"]),
    "count_date": ("date", ["countdate", "dateofcount", "date"]),
    "counted_by": ("string", ["countedby", "counter"]),
    "notes": ("string", ["notes", "note", "comments", "remarks"]),
}
OUT_COLS = list(COLUMNS) + ["week", "source_file"]
SQL_TYPES = {"string": "TEXT", "date": "TEXT", "int64": "INTEGER"}
REQUIRED = ["warehouse", "product_code", "counted_qty", "count_date", "week"]


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


def _find_header(raw: pd.DataFrame) -> int:
    best, best_i = 0, -1
    for i in range(min(len(raw), 30)):
        hits = len({_ALIAS[_norm(v)] for v in raw.iloc[i] if _norm(v) in _ALIAS})
        if hits > best:
            best, best_i = hits, i
    if best < 5:
        raise ValueError(f"header row not found (best match {best} columns)")
    return best_i


def _files() -> list:
    return sorted(glob.glob(os.path.join(data_root(), GLOB)))


def _week(path: str):
    m = re.search(r"(\d{4})-?W(\d{1,2})", os.path.basename(path), re.I)
    return f"{m.group(1)}-W{int(m.group(2)):02d}" if m else None


def read_workbook(path: str) -> pd.DataFrame:
    """Read every sheet of one workbook into canonical columns (all raw values)."""
    parts = []
    for _, raw in pd.read_excel(path, sheet_name=None, header=None, dtype=object).items():
        raw = raw.dropna(how="all")
        if raw.empty:
            continue
        raw = raw.reset_index(drop=True)
        try:
            h = _find_header(raw)
        except ValueError:
            continue  # non-data sheet
        names = [_ALIAS.get(_norm(v)) for v in raw.iloc[h]]
        body = raw.iloc[h + 1:].copy()
        body.columns = [n or f"_x{i}" for i, n in enumerate(names)]
        body = body.loc[:, ~body.columns.duplicated()]
        for c in COLUMNS:
            if c not in body.columns:
                body[c] = None
        parts.append(body[list(COLUMNS)])
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
    frames = [read_workbook(f) for f in files]
    df = pd.concat(frames, ignore_index=True)
    df.to_pickle(_stage("raw.pkl"))
    return {"files": len(files), "rows": len(df)}


def _to_int(s: pd.Series) -> pd.Series:
    s = s.map(lambda v: re.sub(r"[,\s]", "", v) if isinstance(v, str) else v)
    return pd.to_numeric(s, errors="coerce").round().astype("Int64")


def _to_date(s: pd.Series) -> pd.Series:
    def one(v):
        if v is None or v != v or (isinstance(v, str) and not v.strip()):
            return None
        if isinstance(v, (int, float)) and not isinstance(v, bool):  # Excel serial
            d = pd.to_datetime(v, unit="D", origin="1899-12-30", errors="coerce")
        else:
            d = pd.to_datetime(str(v).strip(), errors="coerce", dayfirst=False)
        return None if pd.isna(d) else d.strftime("%Y-%m-%d")
    return s.map(one).astype("string")


def transform() -> dict:
    df = pd.read_pickle(_stage("raw.pkl"))
    for col, (t, _) in COLUMNS.items():
        if t == "string":
            df[col] = df[col].map(lambda v: None if v is None or v != v else str(v).strip() or None).astype("string")
        elif t == "int64":
            df[col] = _to_int(df[col])
        else:
            df[col] = _to_date(df[col])
    df["warehouse"] = df["warehouse"].str.upper()
    df["product_code"] = df["product_code"].str.upper()
    # drop rows with no identifying data (footers, totals, blanks)
    df = df[df["product_code"].notna() | df["counted_qty"].notna()]
    df = df[df["product_code"].notna()]
    # fill variance when missing; counted qty is authoritative
    miss = df["variance"].isna() & df["counted_qty"].notna() & df["system_qty"].notna()
    df.loc[miss, "variance"] = df.loc[miss, "counted_qty"] - df.loc[miss, "system_qty"]
    df = df.drop_duplicates().reset_index(drop=True)
    df.to_pickle(_stage("clean.pkl"))
    return {"rows": len(df)}


def load() -> dict:
    df = pd.read_pickle(_stage("clean.pkl"))[OUT_COLS]
    types = {**{c: t for c, (t, _) in COLUMNS.items()}, "week": "string", "source_file": "string"}
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
    expected = sum(len(read_workbook(f).dropna(subset=["product_code"])) for f in _files())
    con = sqlite3.connect(warehouse_db())
    try:
        n = con.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()[0]
        if n == 0 or n != expected:
            errors.append(f"row count {n} != source data rows {expected}")
        have = [r[1] for r in con.execute(f'PRAGMA table_info("{TABLE}")')]
        if have != OUT_COLS:
            errors.append(f"columns mismatch: {have}")
        for c in REQUIRED:
            k = con.execute(f'SELECT COUNT(*) FROM "{TABLE}" WHERE "{c}" IS NULL').fetchone()[0]
            if k:
                errors.append(f"{k} NULLs in {c}")
        k = con.execute(f'SELECT COUNT(*) FROM (SELECT 1 FROM "{TABLE}" GROUP BY week, warehouse, zone, product_code, batch_no, system_qty, counted_qty, count_date, counted_by HAVING COUNT(*)>1)').fetchone()[0]
        if k:
            errors.append(f"{k} fully duplicated count lines")
        k = con.execute(f'SELECT COUNT(*) FROM "{TABLE}" WHERE variance IS NOT NULL AND system_qty IS NOT NULL AND variance != counted_qty - system_qty').fetchone()[0]
        if k:
            errors.append(f"{k} rows where variance != counted_qty - system_qty")
        k = con.execute(f'SELECT COUNT(*) FROM "{TABLE}" WHERE counted_qty < 0 OR system_qty < 0').fetchone()[0]
        if k:
            errors.append(f"{k} rows with negative quantities")
        k = con.execute(f'SELECT COUNT(DISTINCT week) FROM "{TABLE}"').fetchone()[0]
        if k != len(_files()):
            errors.append(f"{k} weeks loaded != {len(_files())} files")
        k = con.execute(f"SELECT COUNT(*) FROM \"{TABLE}\" WHERE count_date NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'").fetchone()[0]
        if k:
            errors.append(f"{k} malformed count_date values")
    finally:
        con.close()
    if errors:
        raise AssertionError("; ".join(errors))
    return {"rows": n}


def run_all() -> dict:
    extract(); transform(); load()
    return validate()
