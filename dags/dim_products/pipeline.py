"""ERP products -> warehouse table `dim_products` (full reload).

One row per product (product_id), all ERP product columns kept, including the
cold-chain flag (is_cold_chain) and storage requirements (storage_requirement,
storage_min_c, storage_max_c). Pure functions, stdlib + pandas only. Steps
hand data over via pickle files in STAGING_DIR.
"""
import os
import sqlite3

import pandas as pd

PIPELINE_ID = "dim_products"
TABLE = "dim_products"
ERP_PATH = "erp/erp.db"

SCHEMA = {
    "product_id": "string", "sku": "string", "product_name": "string",
    "brand_name": "string", "dosage_form": "string", "strength": "string",
    "pack_size": "string", "is_cold_chain": "int64", "storage_requirement": "string",
    "storage_min_c": "float64", "storage_max_c": "float64", "unit_price": "float64",
    "currency": "string", "supplier_id": "string", "atc_code": "string",
    "requires_prescription": "int64", "shelf_life_days": "int64", "is_active": "int64",
}
SQL_TYPES = {"string": "TEXT", "int64": "INTEGER", "float64": "REAL"}
REQUIRED = ["product_id", "sku", "product_name", "is_cold_chain", "storage_requirement"]


def data_root() -> str:
    return os.environ.get("DATA_ROOT", "/opt/testbed/data_clean")


def warehouse_db() -> str:
    return os.environ.get("WAREHOUSE_DB", "/opt/warehouse/warehouse.db")


def _stage(name: str) -> str:
    d = os.environ.get("STAGING_DIR", os.path.join("/tmp", PIPELINE_ID))
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, name)


def extract() -> dict:
    path = os.path.join(data_root(), ERP_PATH)
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        df = pd.read_sql_query("SELECT * FROM products", con)
    finally:
        con.close()
    missing = set(SCHEMA) - set(df.columns)
    if missing:
        raise ValueError(f"ERP products missing columns: {sorted(missing)}")
    df.to_pickle(_stage("raw.pkl"))
    return {"rows": len(df)}


def transform() -> dict:
    df = pd.read_pickle(_stage("raw.pkl"))[list(SCHEMA)].copy()
    for col, t in SCHEMA.items():
        if t == "string":
            df[col] = df[col].astype("string").str.strip()
        elif t == "int64":
            df[col] = pd.to_numeric(df[col]).astype("int64")
        else:
            df[col] = pd.to_numeric(df[col]).astype("float64")
    df = df.drop_duplicates(subset=["product_id"], keep="last").sort_values("product_id")
    df.to_pickle(_stage("clean.pkl"))
    return {"rows": len(df)}


def load() -> dict:
    df = pd.read_pickle(_stage("clean.pkl"))
    db = warehouse_db()
    os.makedirs(os.path.dirname(os.path.abspath(db)), exist_ok=True)
    cols = ", ".join(
        f'"{c}" {SQL_TYPES[t]}' + (" PRIMARY KEY" if c == "product_id" else "")
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
    src = sqlite3.connect(f"file:{os.path.join(data_root(), ERP_PATH)}?mode=ro", uri=True)
    con = sqlite3.connect(warehouse_db())
    errors = []
    try:
        src_n = src.execute("SELECT COUNT(DISTINCT product_id) FROM products").fetchone()[0]
        n = con.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()[0]
        if n == 0 or n != src_n:
            errors.append(f"row count {n} != distinct source product_id {src_n}")
        if con.execute(f'SELECT COUNT(DISTINCT product_id) FROM "{TABLE}"').fetchone()[0] != n:
            errors.append("product_id not unique")
        have = [r[1] for r in con.execute(f'PRAGMA table_info("{TABLE}")')]
        if have != list(SCHEMA):
            errors.append(f"columns mismatch: {have}")
        for c in REQUIRED:
            k = con.execute(f'SELECT COUNT(*) FROM "{TABLE}" WHERE "{c}" IS NULL').fetchone()[0]
            if k:
                errors.append(f"{k} NULLs in {c}")
        bad = con.execute(f'SELECT COUNT(*) FROM "{TABLE}" WHERE is_cold_chain NOT IN (0,1)').fetchone()[0]
        if bad:
            errors.append(f"{bad} rows with is_cold_chain not in (0,1)")
        s_cold = src.execute("SELECT SUM(is_cold_chain) FROM products").fetchone()[0]
        d_cold = con.execute(f'SELECT SUM(is_cold_chain) FROM "{TABLE}"').fetchone()[0]
        if s_cold != d_cold:
            errors.append(f"cold-chain count {d_cold} != source {s_cold}")
    finally:
        src.close()
        con.close()
    if errors:
        raise AssertionError("; ".join(errors))
    return {"rows": n, "cold_chain": d_cold}


def run_all() -> dict:
    extract(); transform(); load()
    return validate()
