"""ERP products -> warehouse table `dim_products` (full reload, cold-chain flag and storage requirements kept).

Pure functions, stdlib + pandas only. Steps hand data over via pickle files in STAGING_DIR.
"""
import os
import sqlite3

import pandas as pd

PIPELINE_ID = "dim_products"
TABLE = "dim_products"
SOURCE_REL = "erp/erp.db"
SOURCE_TABLE = "products"

COLUMNS = {
    "product_id": "TEXT", "sku": "TEXT", "product_name": "TEXT", "brand_name": "TEXT",
    "dosage_form": "TEXT", "strength": "TEXT", "pack_size": "TEXT",
    "is_cold_chain": "INTEGER", "storage_requirement": "TEXT",
    "storage_min_c": "REAL", "storage_max_c": "REAL", "unit_price": "REAL",
    "currency": "TEXT", "supplier_id": "TEXT", "atc_code": "TEXT",
    "requires_prescription": "INTEGER", "shelf_life_days": "INTEGER", "is_active": "INTEGER",
}
NOT_NULL = ["product_id", "sku", "product_name", "is_cold_chain", "storage_requirement"]


def data_root() -> str:
    return os.environ.get("DATA_ROOT", "/opt/testbed/data_clean")


def warehouse_db() -> str:
    return os.environ.get("WAREHOUSE_DB", "/opt/warehouse/warehouse.db")


def _stage(name: str) -> str:
    d = os.environ.get("STAGING_DIR", os.path.join("/tmp", PIPELINE_ID))
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, f"{name}.pkl")


def extract() -> dict:
    path = os.path.join(data_root(), SOURCE_REL)
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        df = pd.read_sql_query(f'SELECT * FROM "{SOURCE_TABLE}"', con)
    finally:
        con.close()
    missing = [c for c in COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Source missing columns: {missing}")
    df.to_pickle(_stage("raw"))
    print(f"[extract] {len(df)} rows from {path}")
    return {"rows": len(df)}


def transform() -> dict:
    df = pd.read_pickle(_stage("raw"))[list(COLUMNS)].copy()
    for c in ("product_id", "sku", "product_name", "storage_requirement"):
        df[c] = df[c].astype("string").str.strip()
    for c, t in COLUMNS.items():
        if t == "INTEGER":
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        elif t == "REAL":
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df.to_pickle(_stage("transformed"))
    print(f"[transform] {len(df)} rows")
    return {"rows": len(df)}


def load() -> dict:
    df = pd.read_pickle(_stage("transformed"))
    cols = list(COLUMNS)
    ddl = ", ".join(
        f'"{c}" {t}' + (" NOT NULL" if c in NOT_NULL else "") + (" PRIMARY KEY" if c == "product_id" else "")
        for c, t in COLUMNS.items())
    out = df[cols].astype(object).where(df[cols].notna(), None)
    db = warehouse_db()
    os.makedirs(os.path.dirname(os.path.abspath(db)), exist_ok=True)
    tmp = f"{TABLE}__new"
    con = sqlite3.connect(db)
    try:
        with con:
            con.execute(f'DROP TABLE IF EXISTS "{tmp}"')
            con.execute(f'CREATE TABLE "{tmp}" ({ddl})')
            con.executemany(f'INSERT INTO "{tmp}" VALUES ({", ".join("?" * len(cols))})',
                            out.itertuples(index=False, name=None))
            con.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
            con.execute(f'ALTER TABLE "{tmp}" RENAME TO "{TABLE}"')
        n = con.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()[0]
    finally:
        con.close()
    print(f"[load] {n} rows -> {db}:{TABLE}")
    return {"rows": n}


def validate() -> dict:
    raw = pd.read_pickle(_stage("raw"))
    con = sqlite3.connect(warehouse_db())
    q = lambda s: con.execute(s).fetchone()[0]
    try:
        r = {
            "row_count_matches_source": q(f'SELECT COUNT(*) FROM "{TABLE}"') == len(raw),
            "product_id_unique": q(f'SELECT COUNT(*)-COUNT(DISTINCT product_id) FROM "{TABLE}"') == 0,
            "required_non_null": all(
                q(f'SELECT COUNT(*) FROM "{TABLE}" WHERE "{c}" IS NULL') == 0 for c in NOT_NULL),
            "cold_chain_flag_binary": q(
                f'SELECT COUNT(*) FROM "{TABLE}" WHERE is_cold_chain NOT IN (0,1)') == 0,
            "cold_chain_count_matches_source": q(
                f'SELECT COUNT(*) FROM "{TABLE}" WHERE is_cold_chain=1') == int((raw["is_cold_chain"] == 1).sum()),
            "storage_columns_present": {"is_cold_chain", "storage_requirement", "storage_min_c", "storage_max_c"}
                <= {row[1] for row in con.execute(f'PRAGMA table_info("{TABLE}")')},
            "storage_temp_range_valid": q(
                f'SELECT COUNT(*) FROM "{TABLE}" WHERE storage_min_c > storage_max_c') == 0,
        }
    finally:
        con.close()
    for k, ok in r.items():
        print(f"[validate] {'PASS' if ok else 'FAIL'} {k}")
    failed = [k for k, ok in r.items() if not ok]
    if failed:
        raise AssertionError(f"Validation failed: {failed}")
    return r


def run_all() -> dict:
    extract(); transform(); load()
    return validate()


if __name__ == "__main__":
    run_all()
