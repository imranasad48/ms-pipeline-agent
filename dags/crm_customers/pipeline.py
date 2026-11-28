"""CRM daily customer files -> warehouse table `dim_customers` (full reload).

One row per customer (customer_id). When a customer appears in several daily
files the version from the most recent file (by the date in the file name) is
kept. All 16 source columns are kept with typed columns, plus `source_file`
(file name the row came from) and `source_date` (that file's date).
Pure functions, stdlib + pandas only. Steps hand data over via pickle files
in STAGING_DIR.
"""
import glob
import os
import re
import sqlite3

import pandas as pd

PIPELINE_ID = "crm_customers"
TABLE = "dim_customers"
SOURCE_GLOB = "crm/customers_*.csv"
FILE_RE = re.compile(r"^customers_(\d{4}-\d{2}-\d{2})\.csv$")

SOURCE_SCHEMA = {
    "customer_id": "string", "customer_name": "string", "customer_type": "string",
    "city": "string", "address": "string", "postal_code": "string",
    "contact_person": "string", "email": "string", "phone": "string",
    "drug_licence_no": "string", "cold_chain_capable": "int64",
    "credit_limit_pkr": "int64", "payment_terms_days": "int64",
    "assigned_warehouse_id": "string", "registered_on": "date", "is_active": "int64",
}
SCHEMA = {**SOURCE_SCHEMA, "source_file": "string", "source_date": "date"}
SQL_TYPES = {"string": "TEXT", "int64": "INTEGER", "date": "TEXT"}
FLAGS = ["cold_chain_capable", "is_active"]


def data_root() -> str:
    return os.environ.get("DATA_ROOT", "/opt/testbed/data_clean")


def warehouse_db() -> str:
    return os.environ.get("WAREHOUSE_DB", "/opt/warehouse/warehouse.db")


def _stage(name: str) -> str:
    d = os.environ.get("STAGING_DIR", os.path.join("/tmp", PIPELINE_ID))
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, name)


def source_files() -> list:
    """Return [(file_date, path)] sorted oldest -> newest."""
    out = []
    for p in glob.glob(os.path.join(data_root(), SOURCE_GLOB)):
        m = FILE_RE.match(os.path.basename(p))
        if m:
            out.append((m.group(1), p))
    if not out:
        raise FileNotFoundError(f"no files match {SOURCE_GLOB} under {data_root()}")
    return sorted(out)


def read_file(path: str) -> pd.DataFrame:
    # Read everything as text so IDs / postal codes keep leading zeros.
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


def extract() -> dict:
    frames = []
    for fdate, path in source_files():
        df = read_file(path)
        missing = set(SOURCE_SCHEMA) - set(df.columns)
        if missing:
            raise ValueError(f"{os.path.basename(path)} missing columns: {sorted(missing)}")
        df = df[list(SOURCE_SCHEMA)].copy()
        df["source_file"] = os.path.basename(path)
        df["source_date"] = fdate
        df["_row"] = range(len(df))
        frames.append(df)
    raw = pd.concat(frames, ignore_index=True)
    raw.to_pickle(_stage("raw.pkl"))
    return {"files": len(frames), "rows": len(raw)}


def transform() -> dict:
    df = pd.read_pickle(_stage("raw.pkl"))
    for col, t in SOURCE_SCHEMA.items():
        s = df[col].astype("string").str.strip()
        if t == "int64":
            df[col] = pd.to_numeric(s, errors="coerce").round().astype("Int64")
        elif t == "date":
            df[col] = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce").dt.strftime("%Y-%m-%d")
        else:
            df[col] = s
    df = df[df["customer_id"].notna() & (df["customer_id"] != "")]
    # Most recent version wins: newest file, then last row within that file.
    df = (df.sort_values(["source_date", "_row"])
            .drop_duplicates(subset=["customer_id"], keep="last")
            .sort_values("customer_id")[list(SCHEMA)]
            .reset_index(drop=True))
    df.to_pickle(_stage("clean.pkl"))
    return {"rows": len(df)}


def load() -> dict:
    df = pd.read_pickle(_stage("clean.pkl"))
    db = warehouse_db()
    os.makedirs(os.path.dirname(os.path.abspath(db)), exist_ok=True)
    cols = ", ".join(
        f'"{c}" {SQL_TYPES[t]}' + (" PRIMARY KEY" if c == "customer_id" else "")
        for c, t in SCHEMA.items())
    rows = [[None if pd.isna(v) else (int(v) if SCHEMA[c] == "int64" else v)
             for c, v in zip(SCHEMA, r)] for r in df.itertuples(index=False)]
    con = sqlite3.connect(db)
    try:
        with con:
            con.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
            con.execute(f'CREATE TABLE "{TABLE}" ({cols})')
            con.executemany(
                f'INSERT INTO "{TABLE}" VALUES ({",".join("?" * len(SCHEMA))})', rows)
    finally:
        con.close()
    return {"rows": len(rows)}


def validate() -> dict:
    files = source_files()
    latest = {}  # customer_id -> (file_name, raw row dict) from newest file containing it
    for fdate, path in files:
        for r in read_file(path).to_dict("records"):
            cid = r.get("customer_id")
            if isinstance(cid, str) and cid.strip():
                latest[cid.strip()] = (os.path.basename(path), r)

    con = sqlite3.connect(warehouse_db())
    errors = []
    try:
        n = con.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()[0]
        if n == 0 or n != len(latest):
            errors.append(f"row count {n} != distinct source customer_id {len(latest)}")
        if con.execute(f'SELECT COUNT(DISTINCT customer_id) FROM "{TABLE}"').fetchone()[0] != n:
            errors.append("customer_id not unique")
        have = [r[1] for r in con.execute(f'PRAGMA table_info("{TABLE}")')]
        if have != list(SCHEMA):
            errors.append(f"columns mismatch: {have}")
        for c in SCHEMA:
            k = con.execute(f'SELECT COUNT(*) FROM "{TABLE}" WHERE "{c}" IS NULL').fetchone()[0]
            if k:
                errors.append(f"{k} NULLs in {c}")
        for c in FLAGS:
            k = con.execute(f'SELECT COUNT(*) FROM "{TABLE}" WHERE "{c}" NOT IN (0,1)').fetchone()[0]
            if k:
                errors.append(f"{k} rows with {c} not in (0,1)")
        k = con.execute(f'SELECT COUNT(*) FROM "{TABLE}" WHERE typeof(postal_code) != \'text\' '
                        f'OR length(postal_code) != 5').fetchone()[0]
        if k:
            errors.append(f"{k} rows with postal_code not a 5-char string")
        k = con.execute(f'SELECT COUNT(*) FROM "{TABLE}" WHERE date(registered_on) IS NULL '
                        f'OR date(registered_on) != registered_on').fetchone()[0]
        if k:
            errors.append(f"{k} rows with invalid registered_on")
        # Latest-version check: source_file is newest file for that customer, values match it.
        mism = 0
        cur = con.execute(f'SELECT * FROM "{TABLE}"')
        names = [d[0] for d in cur.description]
        for row in cur:
            rec = dict(zip(names, row))
            exp = latest.get(rec["customer_id"])
            if exp is None or rec["source_file"] != exp[0]:
                mism += 1
                continue
            for c in SOURCE_SCHEMA:
                if str(rec[c]).strip() != str(exp[1][c]).strip():
                    mism += 1
                    break
        if mism:
            errors.append(f"{mism} rows not matching the most recent source version")
    finally:
        con.close()
    if errors:
        raise AssertionError("; ".join(errors))
    return {"rows": n, "files": len(files)}


def run_all() -> dict:
    extract(); transform(); load()
    return validate()
