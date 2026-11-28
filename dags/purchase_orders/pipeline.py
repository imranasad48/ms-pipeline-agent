"""ERP purchase_orders + purchase_order_lines -> warehouse `purchase_order_lines` (full reload).

One row per purchase order line (po_line_id). Order-level fields (supplier,
warehouse, dates, status, ...) are carried onto each line. Every line must
match exactly one purchase order; orphans or duplicate POs fail the run.
The order-level `currency` is renamed `po_currency` (the line keeps `currency`).
A `loaded_at` column holds the UTC load timestamp. Stdlib + pandas only;
steps hand data over via pickle files in STAGING_DIR.
"""
import os
import sqlite3
from datetime import datetime, timezone

import pandas as pd

PIPELINE_ID = "purchase_orders"
TABLE = "purchase_order_lines"
ERP_PATH = "erp/erp.db"

LINE_SCHEMA = {
    "po_line_id": "string", "po_id": "string", "line_no": "int64",
    "product_id": "string", "ordered_qty": "int64", "received_qty": "int64",
    "unit_cost": "float64", "currency": "string", "line_status": "string",
}
PO_SCHEMA = {
    "po_id": "string", "supplier_id": "string", "warehouse_id": "string",
    "order_date": "date", "expected_date": "date", "status": "string",
    "total_value": "float64", "currency": "string", "buyer": "string",
    "incoterm": "string", "created_at": "timestamp",
}
PO_RENAME = {"currency": "po_currency"}

# Output schema (column order of the warehouse table).
SCHEMA = dict(LINE_SCHEMA)
for _c, _t in PO_SCHEMA.items():
    if _c != "po_id":
        SCHEMA[PO_RENAME.get(_c, _c)] = _t
SCHEMA["loaded_at"] = "timestamp"

SQL_TYPES = {"string": "TEXT", "int64": "INTEGER", "float64": "REAL",
             "date": "TEXT", "timestamp": "TEXT"}
REQUIRED = ["po_line_id", "po_id", "line_no", "product_id", "ordered_qty",
            "supplier_id", "warehouse_id", "order_date", "status", "loaded_at"]


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


def _cast(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    df = df[list(schema)].copy()
    for col, t in schema.items():
        if t == "string":
            df[col] = df[col].astype("string").str.strip()
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
    con = sqlite3.connect(f"file:{_erp_path()}?mode=ro", uri=True)
    try:
        lines = pd.read_sql_query("SELECT * FROM purchase_order_lines", con)
        pos = pd.read_sql_query("SELECT * FROM purchase_orders", con)
    finally:
        con.close()
    for name, df, schema in (("purchase_order_lines", lines, LINE_SCHEMA),
                             ("purchase_orders", pos, PO_SCHEMA)):
        missing = set(schema) - set(df.columns)
        if missing:
            raise ValueError(f"ERP {name} missing columns: {sorted(missing)}")
    lines.to_pickle(_stage("raw_lines.pkl"))
    pos.to_pickle(_stage("raw_pos.pkl"))
    return {"lines": len(lines), "purchase_orders": len(pos)}


def transform() -> dict:
    lines = _cast(pd.read_pickle(_stage("raw_lines.pkl")), LINE_SCHEMA)
    pos = _cast(pd.read_pickle(_stage("raw_pos.pkl")), PO_SCHEMA).rename(columns=PO_RENAME)

    dup_po = pos["po_id"][pos["po_id"].duplicated()].unique().tolist()
    if dup_po:
        raise ValueError(f"{len(dup_po)} duplicate po_id in purchase_orders, e.g. {dup_po[:5]}")
    dup_line = lines["po_line_id"][lines["po_line_id"].duplicated()].unique().tolist()
    if dup_line:
        raise ValueError(f"{len(dup_line)} duplicate po_line_id, e.g. {dup_line[:5]}")

    df = lines.merge(pos, on="po_id", how="left", validate="many_to_one", indicator=True)
    orphans = df.loc[df["_merge"] != "both", "po_line_id"].tolist()
    if orphans:
        raise ValueError(f"{len(orphans)} lines without a purchase order, e.g. {orphans[:5]}")
    df = df.drop(columns="_merge")
    df["loaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df = df[list(SCHEMA)].sort_values(["po_id", "line_no"]).reset_index(drop=True)
    df.to_pickle(_stage("clean.pkl"))
    return {"rows": len(df)}


def load() -> dict:
    df = pd.read_pickle(_stage("clean.pkl"))
    db = warehouse_db()
    os.makedirs(os.path.dirname(os.path.abspath(db)), exist_ok=True)
    cols = ", ".join(
        f'"{c}" {SQL_TYPES[t]}' + (" PRIMARY KEY" if c == "po_line_id" else "")
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
    src = sqlite3.connect(f"file:{_erp_path()}?mode=ro", uri=True)
    con = sqlite3.connect(warehouse_db())
    errors = []
    one = lambda c, q: c.execute(q).fetchone()
    try:
        src_n = one(src, "SELECT COUNT(*) FROM purchase_order_lines")[0]
        n = one(con, f'SELECT COUNT(*) FROM "{TABLE}"')[0]
        if n == 0 or n != src_n:
            errors.append(f"row count {n} != source lines {src_n}")
        if one(con, f'SELECT COUNT(DISTINCT po_line_id) FROM "{TABLE}"')[0] != n:
            errors.append("po_line_id not unique")
        if one(con, f'SELECT COUNT(*) FROM (SELECT 1 FROM "{TABLE}" GROUP BY po_id, line_no HAVING COUNT(*) > 1)')[0]:
            errors.append("(po_id, line_no) not unique")
        # every source line matches exactly one source PO
        bad = one(src, """SELECT COUNT(*) FROM purchase_order_lines l
                          WHERE (SELECT COUNT(*) FROM purchase_orders p WHERE p.po_id = l.po_id) <> 1""")[0]
        if bad:
            errors.append(f"{bad} source lines not matching exactly one purchase order")
        have = [r[1] for r in con.execute(f'PRAGMA table_info("{TABLE}")')]
        if have != list(SCHEMA):
            errors.append(f"columns mismatch: {have}")
        for c in REQUIRED:
            k = one(con, f'SELECT COUNT(*) FROM "{TABLE}" WHERE "{c}" IS NULL')[0]
            if k:
                errors.append(f"{k} NULLs in {c}")
        for c in ("order_date", "expected_date"):
            k = one(con, f'SELECT COUNT(*) FROM "{TABLE}" WHERE "{c}" IS NOT NULL AND date("{c}") IS NOT "{c}"')[0]
            if k:
                errors.append(f"{k} invalid dates in {c}")
        k = one(con, f'SELECT COUNT(*) FROM "{TABLE}" WHERE created_at IS NOT NULL AND datetime(created_at) IS NULL')[0]
        if k:
            errors.append(f"{k} invalid timestamps in created_at")
        s = one(src, "SELECT SUM(ordered_qty), SUM(received_qty) FROM purchase_order_lines")
        d = one(con, f'SELECT SUM(ordered_qty), SUM(received_qty) FROM "{TABLE}"')
        if tuple(s) != tuple(d):
            errors.append(f"qty sums {d} != source {s}")
        la = con.execute(f'SELECT DISTINCT loaded_at FROM "{TABLE}"').fetchall()
        if len(la) != 1 or la[0][0] is None or not str(la[0][0]).endswith("Z") or \
                one(con, f"SELECT datetime(replace('{la[0][0]}','Z',''))")[0] is None:
            errors.append(f"loaded_at not a single valid UTC timestamp: {la[:3]}")
        # order-level fields carried correctly
        po_src = {r[0]: r[1:] for r in src.execute(
            "SELECT po_id, supplier_id, status, total_value FROM purchase_orders")}
        mism = sum(1 for r in con.execute(
            f'SELECT po_id, supplier_id, status, total_value FROM "{TABLE}"')
            if po_src.get(r[0]) != tuple(r[1:]))
        if mism:
            errors.append(f"{mism} lines with order-level fields not matching source PO")
    finally:
        src.close()
        con.close()
    if errors:
        raise AssertionError("; ".join(errors))
    return {"rows": n}


def run_all() -> dict:
    extract(); transform(); load()
    return validate()
