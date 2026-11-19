"""IoT cold-chain JSONL readings -> warehouse table `sensor_readings` (full reload).

Keeps temperature, humidity, battery, device/location identifiers and reading
timestamps. Pure functions, stdlib + pandas only; steps hand over via pickles.
"""
import glob
import json
import os
import sqlite3

import pandas as pd

PIPELINE_ID = "sensor_readings"
TABLE = "sensor_readings"
GLOB = "iot/sensor_readings_*.jsonl"

SCHEMA = {
    "reading_id": "string", "device_id": "string", "device_type": "string",
    "location_type": "string", "location_id": "string", "warehouse_id": "string",
    "timestamp": "string", "temperature_c": "float64", "temperature_unit": "string",
    "humidity_pct": "float64", "battery_level": "float64",
    "signal_strength_dbm": "int64", "firmware": "string",
    "excursion_flag": "int64", "seq": "int64",
}
SQL_TYPES = {"string": "TEXT", "int64": "INTEGER", "float64": "REAL"}
REQUIRED = ["reading_id", "device_id", "location_id", "timestamp",
            "temperature_c", "humidity_pct", "battery_level"]


def data_root() -> str:
    return os.environ.get("DATA_ROOT", "/opt/testbed/data_clean")


def warehouse_db() -> str:
    return os.environ.get("WAREHOUSE_DB", "/opt/warehouse/warehouse.db")


def _stage(name: str) -> str:
    d = os.environ.get("STAGING_DIR", os.path.join("/tmp", PIPELINE_ID))
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, name)


def _files() -> list:
    return sorted(glob.glob(os.path.join(data_root(), GLOB)))


def _read_all() -> list:
    rows = []
    for f in _files():
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    return rows


def extract() -> dict:
    files = _files()
    if not files:
        raise FileNotFoundError(f"no files match {GLOB} under {data_root()}")
    df = pd.DataFrame(_read_all())
    missing = set(SCHEMA) - set(df.columns)
    if missing:
        raise ValueError(f"IoT readings missing columns: {sorted(missing)}")
    df.to_pickle(_stage("raw.pkl"))
    return {"files": len(files), "rows": len(df)}


def transform() -> dict:
    df = pd.read_pickle(_stage("raw.pkl"))[list(SCHEMA)].copy()
    for col, t in SCHEMA.items():
        if t == "string":
            df[col] = df[col].astype("string").str.strip()
        elif t == "int64":
            df[col] = pd.to_numeric(df[col]).astype("int64")
        else:
            df[col] = pd.to_numeric(df[col]).astype("float64")
    ts = pd.to_datetime(df["timestamp"], errors="raise")
    df["timestamp"] = ts.dt.strftime("%Y-%m-%dT%H:%M:%S")
    df = df.drop_duplicates(subset=["reading_id"], keep="last")
    df = df.sort_values(["timestamp", "device_id"])
    df.to_pickle(_stage("clean.pkl"))
    return {"rows": len(df)}


def load() -> dict:
    df = pd.read_pickle(_stage("clean.pkl"))
    db = warehouse_db()
    os.makedirs(os.path.dirname(os.path.abspath(db)), exist_ok=True)
    cols = ", ".join(
        f'"{c}" {SQL_TYPES[t]}' + (" PRIMARY KEY" if c == "reading_id" else "")
        for c, t in SCHEMA.items())
    con = sqlite3.connect(db)
    try:
        with con:
            con.execute(f'DROP TABLE IF EXISTS "{TABLE}"')
            con.execute(f'CREATE TABLE "{TABLE}" ({cols})')
            rows = df.astype(object).where(df.notna(), None).values.tolist()
            con.executemany(
                f'INSERT INTO "{TABLE}" VALUES ({",".join("?" * len(SCHEMA))})', rows)
            con.execute(f'CREATE INDEX IF NOT EXISTS idx_{TABLE}_device_ts '
                        f'ON "{TABLE}" (device_id, timestamp)')
    finally:
        con.close()
    return {"rows": len(df)}


def validate() -> dict:
    src_ids = {r["reading_id"] for r in _read_all()}
    con = sqlite3.connect(warehouse_db())
    errors = []
    try:
        q = lambda s: con.execute(s).fetchone()[0]
        n = q(f'SELECT COUNT(*) FROM "{TABLE}"')
        if n == 0 or n != len(src_ids):
            errors.append(f"row count {n} != distinct source reading_id {len(src_ids)}")
        if q(f'SELECT COUNT(DISTINCT reading_id) FROM "{TABLE}"') != n:
            errors.append("reading_id not unique")
        have = [r[1] for r in con.execute(f'PRAGMA table_info("{TABLE}")')]
        if have != list(SCHEMA):
            errors.append(f"columns mismatch: {have}")
        for c in REQUIRED:
            k = q(f'SELECT COUNT(*) FROM "{TABLE}" WHERE "{c}" IS NULL')
            if k:
                errors.append(f"{k} NULLs in {c}")
        k = q(f'SELECT COUNT(*) FROM "{TABLE}" WHERE humidity_pct < 0 OR humidity_pct > 100')
        if k:
            errors.append(f"{k} rows humidity_pct outside 0-100")
        k = q(f'SELECT COUNT(*) FROM "{TABLE}" WHERE battery_level < 0 OR battery_level > 100')
        if k:
            errors.append(f"{k} rows battery_level outside 0-100")
        k = q(f"SELECT COUNT(*) FROM \"{TABLE}\" WHERE temperature_unit <> 'C'")
        if k:
            errors.append(f"{k} rows with temperature_unit not C")
        k = q(f'SELECT COUNT(*) FROM "{TABLE}" WHERE temperature_c < -60 OR temperature_c > 60')
        if k:
            errors.append(f"{k} rows temperature_c outside -60..60")
        k = q(f"SELECT COUNT(*) FROM \"{TABLE}\" WHERE timestamp NOT GLOB "
              "'[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]'")
        if k:
            errors.append(f"{k} rows with malformed timestamp")
    finally:
        con.close()
    if errors:
        raise AssertionError("; ".join(errors))
    return {"rows": n}


def run_all() -> dict:
    extract(); transform(); load()
    return validate()
