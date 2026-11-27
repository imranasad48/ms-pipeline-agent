"""CRM sales orders -> warehouse table `sales_orders`.

Pure pipeline functions (stdlib + pandas only). Full-reload semantics: the
target table is rebuilt from all source files on every run and swapped in
atomically.

Steps hand data to each other through pickle files in STAGING_DIR so that each
step can run as an independent Airflow task.
"""
import glob
import os
import re
import sqlite3
from datetime import datetime, timezone

import pandas as pd

PIPELINE_ID = "crm_sales_orders"
TABLE = "sales_orders"

# Expected source schema: column -> pandas dtype
EXPECTED_SCHEMA = {
    "order_line_id": "string",
    "order_id": "string",
    "order_date": "string",
    "order_datetime": "string",
    "customer_id": "string",
    "warehouse_id": "string",
    "line_no": "int64",
    "product_id": "string",
    "sku": "string",
    "quantity": "int64",
    "uom": "string",
    "unit_price": "float64",
    "discount_pct": "float64",
    "line_total": "float64",
    "currency": "string",
    "is_cold_chain": "int64",
    "requested_delivery_date": "string",
    "order_status": "string",
    "priority": "string",
    "sales_rep": "string",
    "channel": "string",
    "payment_terms_days": "int64",
    "has_cold_chain_items": "int64",
}
SQLITE_TYPES = {"string": "TEXT", "int64": "INTEGER", "float64": "REAL"}
LINEAGE_COLS = {"source_file": "TEXT", "loaded_at": "TEXT"}

ACCEPTED_VALUES = {
    "order_status": {"confirmed", "picked", "shipped", "delivered", "invoiced"},
    "priority": {"standard", "urgent"},
    "channel": {"edi", "field_rep", "phone", "portal"},
    "currency": {"PKR"},
}
FILE_PATTERN = "crm/sales_orders_*.csv"
FILE_DATE_RE = re.compile(r"sales_orders_(\d{4}-\d{2}-\d{2})\.csv$")


# --------------------------------------------------------------------------- config
def data_root() -> str:
    return os.environ.get("DATA_ROOT", "/opt/testbed/data_clean")


def warehouse_db() -> str:
    return os.environ.get("WAREHOUSE_DB", "/opt/warehouse/warehouse.db")


def staging_dir() -> str:
    d = os.environ.get("STAGING_DIR", os.path.join("/tmp", PIPELINE_ID))
    os.makedirs(d, exist_ok=True)
    return d


def _stage_path(name: str) -> str:
    return os.path.join(staging_dir(), f"{name}.pkl")


def source_files() -> list:
    return sorted(glob.glob(os.path.join(data_root(), FILE_PATTERN)))


# --------------------------------------------------------------------------- steps
def extract() -> dict:
    """Read every daily sales_orders CSV as raw strings; tag with source file."""
    files = source_files()
    if not files:
        raise FileNotFoundError(f"No files match {FILE_PATTERN} under {data_root()}")
    frames, per_file = [], {}
    for f in files:
        df = pd.read_csv(f, dtype=str, keep_default_na=False, na_values=[""])
        missing = set(EXPECTED_SCHEMA) - set(df.columns)
        if missing:
            raise ValueError(f"{os.path.basename(f)} missing columns: {sorted(missing)}")
        df["source_file"] = os.path.basename(f)
        per_file[os.path.basename(f)] = len(df)
        frames.append(df)
    raw = pd.concat(frames, ignore_index=True)
    raw.to_pickle(_stage_path("extracted"))
    stats = {"files": len(files), "rows": len(raw), "per_file": per_file}
    pd.to_pickle(stats, _stage_path("extract_stats"))
    print(f"[extract] {stats['files']} files, {stats['rows']} rows")
    return {"files": stats["files"], "rows": stats["rows"]}


def transform() -> dict:
    """Cast to expected types, trim strings, add lineage columns."""
    raw = pd.read_pickle(_stage_path("extracted"))
    df = pd.DataFrame(index=raw.index)
    for col, typ in EXPECTED_SCHEMA.items():
        s = raw[col]
        if typ == "string":
            df[col] = s.str.strip()
        elif typ == "int64":
            df[col] = pd.to_numeric(s, errors="raise").astype("int64")
        else:
            df[col] = pd.to_numeric(s, errors="raise").astype("float64")
    df["source_file"] = raw["source_file"]
    df["loaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if len(df) != len(raw):
        raise AssertionError("row count changed during transform")
    df.to_pickle(_stage_path("transformed"))
    print(f"[transform] {len(df)} rows typed")
    return {"rows": len(df)}


def load() -> dict:
    """Full reload: build a new table, then atomically swap it in."""
    df = pd.read_pickle(_stage_path("transformed"))
    db = warehouse_db()
    os.makedirs(os.path.dirname(os.path.abspath(db)), exist_ok=True)
    cols = {**{c: SQLITE_TYPES[t] for c, t in EXPECTED_SCHEMA.items()}, **LINEAGE_COLS}
    tmp = f"{TABLE}__new"
    ddl = ", ".join(
        f'"{c}" {t} NOT NULL' + (" PRIMARY KEY" if c == "order_line_id" else "")
        for c, t in cols.items()
    )
    con = sqlite3.connect(db)
    try:
        with con:
            con.execute(f'DROP TABLE IF EXISTS "{tmp}"')
            con.execute(f'CREATE TABLE "{tmp}" ({ddl})')
            placeholders = ", ".join("?" for _ in cols)
            rows = df[list(cols)].astype(object).itertuples(index=False, name=None)
            con.executemany(f'INSERT INTO "{tmp}" VALUES ({placeholders})', rows)
            con.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
            con.execute(f'ALTER TABLE "{tmp}" RENAME TO "{TABLE}"')
            con.execute(f'CREATE INDEX IF NOT EXISTS ix_{TABLE}_order_id ON "{TABLE}"(order_id)')
            con.execute(f'CREATE INDEX IF NOT EXISTS ix_{TABLE}_order_date ON "{TABLE}"(order_date)')
        n = con.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()[0]
    finally:
        con.close()
    print(f"[load] {n} rows -> {db}:{TABLE}")
    return {"rows": n, "db": db, "table": TABLE}


def validate() -> dict:
    """Run all validation checks against the loaded table. Raises on failure."""
    stats = pd.read_pickle(_stage_path("extract_stats"))
    con = sqlite3.connect(warehouse_db())
    q = lambda sql: con.execute(sql).fetchone()[0]  # noqa: E731
    results = {}
    try:
        n = q(f"SELECT COUNT(*) FROM {TABLE}")
        results["row_count_match"] = n == stats["rows"] and n > 0
        per_file = dict(con.execute(f"SELECT source_file, COUNT(*) FROM {TABLE} GROUP BY 1").fetchall())
        results["row_count_per_file"] = per_file == stats["per_file"]
        results["pk_unique_order_line_id"] = q(
            f"SELECT COUNT(*) - COUNT(DISTINCT order_line_id) FROM {TABLE}") == 0
        results["unique_order_id_line_no"] = q(
            f"SELECT COUNT(*) FROM (SELECT order_id, line_no FROM {TABLE} "
            f"GROUP BY 1,2 HAVING COUNT(*) > 1)") == 0
        table_cols = [r[1] for r in con.execute(f"PRAGMA table_info({TABLE})")]
        results["schema_columns_present"] = set(EXPECTED_SCHEMA) <= set(table_cols)
        nn = " OR ".join(f'"{c}" IS NULL' for c in EXPECTED_SCHEMA)
        results["not_null_required"] = q(f"SELECT COUNT(*) FROM {TABLE} WHERE {nn}") == 0
        for col, vals in ACCEPTED_VALUES.items():
            lst = ",".join(f"'{v}'" for v in sorted(vals))
            results[f"accepted_values_{col}"] = q(
                f"SELECT COUNT(*) FROM {TABLE} WHERE {col} NOT IN ({lst})") == 0
        results["range_quantity_positive"] = q(f"SELECT COUNT(*) FROM {TABLE} WHERE quantity <= 0") == 0
        results["range_unit_price_positive"] = q(f"SELECT COUNT(*) FROM {TABLE} WHERE unit_price <= 0") == 0
        results["range_discount_pct"] = q(
            f"SELECT COUNT(*) FROM {TABLE} WHERE discount_pct < 0 OR discount_pct > 1") == 0
        results["line_total_consistent"] = q(
            f"SELECT COUNT(*) FROM {TABLE} WHERE "
            f"ABS(ROUND(quantity*unit_price*(1-discount_pct),2) - line_total) > 0.02") == 0
        bad_dates = 0
        for fname, d in con.execute(f"SELECT DISTINCT source_file, order_date FROM {TABLE}"):
            m = FILE_DATE_RE.search(fname)
            if not m or m.group(1) != d:
                bad_dates += 1
        results["order_date_matches_file"] = bad_dates == 0
    finally:
        con.close()
    failed = [k for k, ok in results.items() if not ok]
    for k, ok in results.items():
        print(f"[validate] {'PASS' if ok else 'FAIL'} {k}")
    if failed:
        raise AssertionError(f"Validation failed: {failed}")
    return results


def run_all() -> dict:
    extract(); transform(); load()
    return validate()


if __name__ == "__main__":
    run_all()
