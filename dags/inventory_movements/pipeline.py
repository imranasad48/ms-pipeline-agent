"""ERP inventory_movements -> warehouse `inventory_movements` fact table (full reload).

One row per movement (movement_id). All 15 source columns are kept with
enforced types (ISO dates/timestamps, integer qty, real unit_cost). Empty
reference_id strings become NULL. If the source ever holds duplicate
movement_id rows, exactly one is kept (latest created_at, then last seen),
so each movement appears exactly once. A `loaded_at` column holds the UTC
load timestamp. Stdlib + pandas only; steps hand data over via pickle files
in STAGING_DIR.
"""
import os
import sqlite3
from datetime import datetime, timezone

import pandas as pd

PIPELINE_ID = "inventory_movements"
TABLE = "inventory_movements"
SOURCE_TABLE = "inventory_movements"
ERP_PATH = "erp/erp.db"
KEY = "movement_id"

SOURCE_SCHEMA = {
    "movement_id": "string", "movement_date": "date", "movement_timestamp": "timestamp",
    "movement_type": "string", "batch_no": "string", "product_id": "string",
    "warehouse_id": "string", "zone_id": "string", "qty": "int64", "uom": "string",
    "reference_type": "string", "reference_id": "string", "unit_cost": "float64",
    "created_by": "string", "created_at": "timestamp",
}
SCHEMA = dict(SOURCE_SCHEMA, loaded_at="timestamp")
SQL_TYPES = {"string": "TEXT", "int64": "INTEGER", "float64": "REAL",
             "date": "TEXT", "timestamp": "TEXT"}
NULLABLE = {"reference_id"}
REQUIRED = [c for c in SCHEMA if c not in NULLABLE]
MOVEMENT_TYPES = {"adjustment", "goods_receipt", "pick", "return", "ship", "transfer"}


def data_root() -> str:
    return os.environ.get("DATA_ROOT", "/opt/testbed/data_clean")


def warehouse_db() -> str:
    return os.environ.get("WAREHOUSE_DB", "/opt/warehouse/warehouse.db")


def _erp_path() -> str:
    return os.path.join(data_root(), ERP_PATH)


def _stage(name: str) -> str:
    d = os.environ.get("STAGING_DIR", os.path.join("/tmp", PIPELINE_ID))
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, name)


def _src():
    return sqlite3.connect(f"file:{_erp_path()}?mode=ro", uri=True)


def _cast(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    df = df[list(schema)].copy()
    for col, t in schema.items():
        if t == "string":
            s = df[col].astype("string").str.strip()
            df[col] = s.mask(s == "")
        elif t == "int64":
            df[col] = pd.to_numeric(df[col]).astype("int64")
        elif t == "float64":
            df[col] = pd.to_numeric(df[col]).astype("float64")
        elif t == "date":
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d").dt.strftime("%Y-%m-%d")
        elif t == "timestamp":
            df[col] = pd.to_datetime(df[col], format="ISO8601").dt.strftime("%Y-%m-%dT%H:%M:%S")
    return df


def extract() -> dict:
    con = _src()
    try:
        df = pd.read_sql_query(f'SELECT * FROM "{SOURCE_TABLE}"', con)
    finally:
        con.close()
    missing = set(SOURCE_SCHEMA) - set(df.columns)
    if missing:
        raise ValueError(f"ERP {SOURCE_TABLE} missing columns: {sorted(missing)}")
    df.to_pickle(_stage("raw.pkl"))
    return {"rows": len(df)}


def transform() -> dict:
    raw = pd.read_pickle(_stage("raw.pkl"))
    df = _cast(raw, SOURCE_SCHEMA)
    if df[KEY].isna().any():
        raise ValueError(f"{int(df[KEY].isna().sum())} rows with NULL {KEY}")
    n_in = len(df)
    # exactly one row per movement: drop exact duplicates, then keep latest version
    df = df.drop_duplicates()
    df["_ord"] = range(len(df))
    df = (df.sort_values(["created_at", "_ord"], na_position="first")
            .drop_duplicates(KEY, keep="last").drop(columns="_ord"))
    df["loaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df = df[list(SCHEMA)].sort_values(["movement_timestamp", KEY]).reset_index(drop=True)
    df.to_pickle(_stage("clean.pkl"))
    return {"rows_in": n_in, "rows_out": len(df), "duplicates_removed": n_in - len(df)}


def load() -> dict:
    df = pd.read_pickle(_stage("clean.pkl"))
    db = warehouse_db()
    os.makedirs(os.path.dirname(os.path.abspath(db)), exist_ok=True)
    cols = ", ".join(
        f'"{c}" {SQL_TYPES[t]}' + (" PRIMARY KEY" if c == KEY else "")
        + (" NOT NULL" if c in REQUIRED and c != KEY else "")
        for c, t in SCHEMA.items())
    con = sqlite3.connect(db)
    try:
        with con:
            con.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
            con.execute(f'CREATE TABLE "{TABLE}" ({cols})')
            rows = df.astype(object).where(df.notna(), None).values.tolist()
            con.executemany(
                f'INSERT INTO "{TABLE}" VALUES ({",".join("?" * len(SCHEMA))})', rows)
    finally:
        con.close()
    return {"rows": len(df)}


def validate() -> dict:
    src = _src()
    con = sqlite3.connect(warehouse_db())
    errors = []
    one = lambda c, q: c.execute(q).fetchone()
    try:
        src_n = one(src, f'SELECT COUNT(DISTINCT TRIM(movement_id)) FROM "{SOURCE_TABLE}"')[0]
        n = one(con, f'SELECT COUNT(*) FROM "{TABLE}"')[0]
        if n == 0 or n != src_n:
            errors.append(f"row count {n} != distinct source movements {src_n}")
        if one(con, f'SELECT COUNT(DISTINCT {KEY}) FROM "{TABLE}"')[0] != n:
            errors.append(f"{KEY} not unique")
        src_ids = {r[0] for r in src.execute(f'SELECT DISTINCT TRIM(movement_id) FROM "{SOURCE_TABLE}"')}
        dst_ids = {r[0] for r in con.execute(f'SELECT {KEY} FROM "{TABLE}"')}
        if src_ids != dst_ids:
            errors.append(f"movement_id sets differ: {len(src_ids - dst_ids)} missing, "
                          f"{len(dst_ids - src_ids)} extra")
        have = [r[1] for r in con.execute(f'PRAGMA table_info("{TABLE}")')]
        if have != list(SCHEMA):
            errors.append(f"columns mismatch: {have}")
        for c in REQUIRED:
            k = one(con, f'SELECT COUNT(*) FROM "{TABLE}" WHERE "{c}" IS NULL')[0]
            if k:
                errors.append(f"{k} NULLs in {c}")
        for c, t in SCHEMA.items():
            want = {"int64": "integer", "float64": "real"}.get(t, "text")
            k = one(con, f'SELECT COUNT(*) FROM "{TABLE}" WHERE "{c}" IS NOT NULL AND typeof("{c}") <> \'{want}\'')[0]
            if k:
                errors.append(f"{k} values in {c} not stored as {want}")
        k = one(con, f'SELECT COUNT(*) FROM "{TABLE}" WHERE date(movement_date) IS NOT movement_date')[0]
        if k:
            errors.append(f"{k} invalid movement_date")
        for c in ("movement_timestamp", "created_at"):
            k = one(con, f'SELECT COUNT(*) FROM "{TABLE}" WHERE datetime("{c}") IS NOT replace("{c}", \'T\', \' \')')[0]
            if k:
                errors.append(f"{k} invalid timestamps in {c}")
        k = one(con, f'SELECT COUNT(*) FROM "{TABLE}" WHERE substr(movement_timestamp, 1, 10) <> movement_date')[0]
        if k:
            errors.append(f"{k} rows where movement_date != date(movement_timestamp)")
        bad = [r[0] for r in con.execute(f'SELECT DISTINCT movement_type FROM "{TABLE}"')
               if r[0] not in MOVEMENT_TYPES]
        if bad:
            errors.append(f"unexpected movement_type values: {bad}")
        # aggregate equality with source when source has no duplicate keys
        if one(src, f'SELECT COUNT(*) FROM "{SOURCE_TABLE}"')[0] == src_n:
            s = one(src, f'SELECT SUM(qty), SUM(unit_cost) FROM "{SOURCE_TABLE}"')
            d = one(con, f'SELECT SUM(qty), SUM(unit_cost) FROM "{TABLE}"')
            if s[0] != d[0] or abs((s[1] or 0) - (d[1] or 0)) > 0.01:
                errors.append(f"qty/unit_cost sums {d} != source {s}")
        la = [r[0] for r in con.execute(f'SELECT DISTINCT loaded_at FROM "{TABLE}"')]
        ok = len(la) == 1 and la[0] is not None and str(la[0]).endswith("Z") and \
            con.execute("SELECT datetime(replace(?, 'Z', ''))", (la[0],)).fetchone()[0] is not None
        if not ok:
            errors.append(f"loaded_at not a single valid UTC timestamp: {la[:3]}")
    finally:
        src.close()
        con.close()
    if errors:
        raise AssertionError("; ".join(errors))
    return {"rows": n}


def run_all() -> dict:
    extract(); transform(); load()
    return validate()
