"""CRM sales orders + 3PL shipment events -> warehouse table `delivery_performance`.

One row per CRM sales order (order_id). Order header attributes and line
aggregates come from CRM; shipment attributes come from the 3PL tracking
events (latest status, pickup / delivery timestamps, promised delivery time).

is_late is DERIVED here (delivered_at > promised_delivery_at) because the 3PL
feed's own `is_late` field is not reliable (observed constant 0, even for
shipments delivered after their promise). is_late is NULL when the shipment
has not been delivered (in transit, returned, or no shipment yet).

Pure functions, stdlib + pandas only. Full-reload semantics: the table is
rebuilt from all source files every run and swapped in atomically. Steps hand
data to each other through pickle files in STAGING_DIR so each step can run as
an independent Airflow task.
"""
import glob
import json
import os
import sqlite3
from datetime import datetime, timezone

import pandas as pd

PIPELINE_ID = "delivery_performance"
TABLE = "delivery_performance"

CRM_PATTERN = "crm/sales_orders_*.csv"
TPL_PATTERN = "3pl/shipment_events_*.json"

CRM_SCHEMA = {
    "order_line_id": "string", "order_id": "string", "order_date": "string",
    "order_datetime": "string", "customer_id": "string", "warehouse_id": "string",
    "line_no": "int64", "product_id": "string", "sku": "string", "quantity": "int64",
    "uom": "string", "unit_price": "float64", "discount_pct": "float64",
    "line_total": "float64", "currency": "string", "is_cold_chain": "int64",
    "requested_delivery_date": "string", "order_status": "string", "priority": "string",
    "sales_rep": "string", "channel": "string", "payment_terms_days": "int64",
    "has_cold_chain_items": "int64",
}
EVENT_SCHEMA = {
    "event_id": "string", "tracking_id": "string", "sales_order_id": "string",
    "customer_id": "string", "status": "string", "event_timestamp": "datetime",
    "recorded_at": "datetime", "vehicle_id": "string", "driver_id": "string",
    "hub_code": "string", "origin_warehouse_id": "string", "destination_city": "string",
    "temperature_controlled": "bool", "service_level": "string",
    "promised_delivery_at": "datetime", "pieces": "int64", "weight_kg": "float64",
    "notes": "string", "is_late": "int64",
}
EVENT_KEY_ALIASES = {"vehicleId": "vehicle_id", "driverID": "driver_id",
                     "hubCode": "hub_code", "trackingId": "tracking_id",
                     "originWarehouseId": "origin_warehouse_id",
                     "eventId": "event_id", "salesOrderId": "sales_order_id"}
EVENT_STATUSES ={"picked_up", "in_transit", "delivered", "returned"}
LOCAL_TZ = "+05:00"  # source feeds are Pakistan time; outputs normalised to it

# Output table: column -> SQLite type ; NOT NULL columns listed separately.
OUTPUT_COLUMNS = {
    "order_id": "TEXT",
    "order_date": "TEXT",
    "order_datetime": "TEXT",
    "customer_id": "TEXT",
    "warehouse_id": "TEXT",
    "order_status": "TEXT",
    "priority": "TEXT",
    "channel": "TEXT",
    "has_cold_chain_items": "INTEGER",
    "line_count": "INTEGER",
    "total_quantity": "INTEGER",
    "order_total": "REAL",
    "currency": "TEXT",
    "requested_delivery_date": "TEXT",
    "tracking_id": "TEXT",
    "service_level": "TEXT",
    "destination_city": "TEXT",
    "latest_status": "TEXT",
    "latest_status_at": "TEXT",
    "pickup_at": "TEXT",
    "delivered_at": "TEXT",
    "promised_delivery_at": "TEXT",
    "is_late": "INTEGER",
    "delivery_delay_hours": "REAL",
    "event_count": "INTEGER",
    "source_event_count": "INTEGER",
    "loaded_at": "TEXT",
}
NOT_NULL = [
    "order_id", "order_date", "order_datetime", "customer_id", "warehouse_id",
    "order_status", "priority", "channel", "has_cold_chain_items", "line_count",
    "total_quantity", "order_total", "currency", "requested_delivery_date",
    "event_count", "loaded_at",
]
ORDER_LEVEL_COLS = [
    "order_date", "order_datetime", "customer_id", "warehouse_id", "order_status",
    "priority", "channel", "has_cold_chain_items", "currency", "requested_delivery_date",
]


# --------------------------------------------------------------------------- config
def data_root() -> str:
    return os.environ.get("DATA_ROOT", "/opt/testbed/data_clean")


def warehouse_db() -> str:
    return os.environ.get("WAREHOUSE_DB", "/opt/warehouse/warehouse.db")


def staging_dir() -> str:
    d = os.environ.get("STAGING_DIR", os.path.join("/tmp", PIPELINE_ID))
    os.makedirs(d, exist_ok=True)
    return d


def _stage(name: str) -> str:
    return os.path.join(staging_dir(), f"{name}.pkl")


def _files(pattern: str) -> list:
    files = sorted(glob.glob(os.path.join(data_root(), pattern)))
    if not files:
        raise FileNotFoundError(f"No files match {pattern} under {data_root()}")
    return files


def _to_local_iso(s: pd.Series) -> pd.Series:
    """Datetime series (tz-aware) -> ISO-8601 strings in +05:00; NaT -> None."""
    out = s.dt.tz_convert(LOCAL_TZ).dt.strftime("%Y-%m-%dT%H:%M:%S") + LOCAL_TZ
    return out.where(s.notna(), None)


# --------------------------------------------------------------------------- steps
def extract_orders() -> dict:
    """Read all CRM sales_orders CSVs as raw strings, verify columns, stage."""
    frames, per_file = [], {}
    for f in _files(CRM_PATTERN):
        df = pd.read_csv(f, dtype=str, keep_default_na=False, na_values=[""])
        missing = set(CRM_SCHEMA) - set(df.columns)
        if missing:
            raise ValueError(f"{os.path.basename(f)} missing columns: {sorted(missing)}")
        df["source_file"] = os.path.basename(f)
        per_file[os.path.basename(f)] = len(df)
        frames.append(df)
    raw = pd.concat(frames, ignore_index=True)
    raw.to_pickle(_stage("orders_raw"))
    stats = {"files": len(per_file), "rows": len(raw), "orders": raw["order_id"].nunique()}
    pd.to_pickle(stats, _stage("orders_stats"))
    print(f"[extract_orders] {stats}")
    return stats


def extract_shipments() -> dict:
    """Read all 3PL hourly event pages, check envelope integrity, flatten events."""
    rows, n_files, n_empty = [], 0, 0
    for f in _files(TPL_PATTERN):
        with open(f, encoding="utf-8") as fh:
            payload = json.load(fh)
        events = payload.get("events")
        if not isinstance(events, list):
            raise ValueError(f"{os.path.basename(f)}: 'events' array missing")
        rc = payload.get("record_count")
        if rc is not None and rc != len(events):
            raise ValueError(f"{os.path.basename(f)}: record_count={rc} but {len(events)} events")
        n_files += 1
        n_empty += not events
        for e in events:
            # schema drift: accept renamed keys (e.g. vehicleId) and map to canonical names
            for old, new in EVENT_KEY_ALIASES.items():
                if old in e and new not in e:
                    e[new] = e.pop(old)
            missing = set(EVENT_SCHEMA) - set(e)
            if missing:
                raise ValueError(f"{os.path.basename(f)} event {e.get('event_id')} "
                                 f"missing fields: {sorted(missing)}")
            rows.append({**e, "source_file": os.path.basename(f)})
    if not rows:
        raise ValueError("3PL feed contains no events")
    raw = pd.DataFrame(rows)
    raw.to_pickle(_stage("events_raw"))
    stats = {"files": n_files, "empty_files": n_empty, "events": len(raw),
             "distinct_event_ids": raw["event_id"].nunique()}
    pd.to_pickle(stats, _stage("events_stats"))
    print(f"[extract_shipments] {stats}")
    return stats


def transform() -> dict:
    """Aggregate CRM lines to orders, collapse events to one shipment per order, join."""
    lines = pd.read_pickle(_stage("orders_raw"))
    ev = pd.read_pickle(_stage("events_raw"))

    # ---- CRM: order lines -> one row per order
    for c in ("quantity", "line_no", "has_cold_chain_items"):
        lines[c] = pd.to_numeric(lines[c], errors="raise").astype("int64")
    lines["line_total"] = pd.to_numeric(lines["line_total"], errors="raise").astype("float64")
    for c in ORDER_LEVEL_COLS + ["order_id"]:
        if lines[c].dtype == object:
            lines[c] = lines[c].str.strip()
    inconsistent = (lines.groupby("order_id")[ORDER_LEVEL_COLS].nunique() > 1).any()
    if inconsistent.any():
        raise ValueError(f"order-level columns vary within an order: "
                         f"{list(inconsistent[inconsistent].index)}")
    orders = (lines.groupby("order_id", sort=True)
              .agg(**{c: (c, "first") for c in ORDER_LEVEL_COLS},
                   line_count=("order_line_id", "nunique"),
                   total_quantity=("quantity", "sum"),
                   order_total=("line_total", "sum"))
              .reset_index())
    orders["order_total"] = orders["order_total"].round(2)

    # ---- 3PL: typed events, deduped by event_id (pages may overlap)
    source_event_count = len(ev)
    ev = ev.drop_duplicates("event_id", keep="last").copy()
    for c in ("event_timestamp", "recorded_at", "promised_delivery_at"):
        ev[c] = pd.to_datetime(ev[c], utc=True, errors="raise", format="ISO8601")
    ev["status"] = ev["status"].str.strip().str.lower()
    ev = ev.sort_values(["sales_order_id", "event_timestamp", "recorded_at", "event_id"])
    g = ev.groupby("sales_order_id", sort=False)
    last = g.tail(1).set_index("sales_order_id")
    first_ts = lambda status: (ev[ev["status"] == status]  # noqa: E731
                               .groupby("sales_order_id")["event_timestamp"].min())
    ship = pd.DataFrame({
        "tracking_id": last["tracking_id"],
        "service_level": last["service_level"],
        "destination_city": last["destination_city"],
        "ship_customer_id": last["customer_id"],
        "latest_status": last["status"],
        "latest_status_at": last["event_timestamp"],
        # promise: latest value the carrier reported (constant per order in practice)
        "promised_delivery_at": last["promised_delivery_at"],
        "event_count": g.size(),
    })
    ship["pickup_at"] = first_ts("picked_up")
    # delivered_at only when the shipment's latest state is delivered
    ship["delivered_at"] = first_ts("delivered").where(ship["latest_status"] == "delivered")
    ship.index.name = "order_id"
    ship = ship.reset_index()

    orphans = sorted(set(ship["order_id"]) - set(orders["order_id"]))
    df = orders.merge(ship, on="order_id", how="left", validate="one_to_one")
    df["event_count"] = df["event_count"].fillna(0).astype("int64")

    delay = (df["delivered_at"] - df["promised_delivery_at"]).dt.total_seconds() / 3600.0
    df["delivery_delay_hours"] = delay.round(2)
    df["is_late"] = pd.array([None if pd.isna(d) else int(d > 0) for d in delay], dtype="Int64")

    for c in ("latest_status_at", "pickup_at", "delivered_at", "promised_delivery_at"):
        df[c] = _to_local_iso(pd.to_datetime(df[c], utc=True))
    df["source_event_count"] = source_event_count
    df["loaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    df.to_pickle(_stage("transformed"))
    stats = {"rows": len(df), "orders_with_shipment": int(df["tracking_id"].notna().sum()),
             "orphan_event_orders": orphans, "events_after_dedup": len(ev),
             "customer_mismatches": int((df["ship_customer_id"].notna()
                                         & (df["ship_customer_id"] != df["customer_id"])).sum())}
    pd.to_pickle(stats, _stage("transform_stats"))
    print(f"[transform] rows={stats['rows']} with_shipment={stats['orders_with_shipment']} "
          f"orphans={len(orphans)} late={int((df['is_late'] == 1).sum())}")
    return {k: (len(v) if isinstance(v, list) else v) for k, v in stats.items()}


def load() -> dict:
    """Full reload: build a new table, then atomically swap it in."""
    df = pd.read_pickle(_stage("transformed"))
    cols = list(OUTPUT_COLUMNS)
    ddl = ", ".join(
        f'"{c}" {t}' + (" NOT NULL" if c in NOT_NULL else "")
        + (" PRIMARY KEY" if c == "order_id" else "")
        for c, t in OUTPUT_COLUMNS.items()
    )
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
            for c in ("order_date", "latest_status", "tracking_id"):
                con.execute(f'CREATE INDEX IF NOT EXISTS ix_{TABLE}_{c} ON "{TABLE}"({c})')
        n = con.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()[0]
    finally:
        con.close()
    print(f"[load] {n} rows -> {db}:{TABLE}")
    return {"rows": n, "db": db, "table": TABLE}


def validate() -> dict:
    """Run all validation checks against the loaded table. Raises on failure."""
    o_stats = pd.read_pickle(_stage("orders_stats"))
    e_stats = pd.read_pickle(_stage("events_stats"))
    t_stats = pd.read_pickle(_stage("transform_stats"))
    lines = pd.read_pickle(_stage("orders_raw"))
    con = sqlite3.connect(warehouse_db())
    q = lambda sql: con.execute(sql).fetchone()[0]  # noqa: E731
    T, r = TABLE, {}
    try:
        n = q(f"SELECT COUNT(*) FROM {T}")
        r["row_count_equals_crm_orders"] = n == o_stats["orders"] and n > 0
        r["pk_unique_order_id"] = q(f"SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM {T}") == 0
        cols = {row[1] for row in con.execute(f"PRAGMA table_info({T})")}
        r["schema_columns_present"] = set(OUTPUT_COLUMNS) <= cols
        nn = " OR ".join(f'"{c}" IS NULL' for c in NOT_NULL)
        r["not_null_required"] = q(f"SELECT COUNT(*) FROM {T} WHERE {nn}") == 0
        r["event_orders_exist_in_crm"] = len(t_stats["orphan_event_orders"]) == 0
        r["all_events_accounted"] = q(f"SELECT SUM(event_count) FROM {T}") == e_stats["distinct_event_ids"]
        r["event_id_unique_in_source"] = e_stats["distinct_event_ids"] == e_stats["events"]
        sts = ",".join(f"'{s}'" for s in sorted(EVENT_STATUSES))
        r["accepted_values_latest_status"] = q(
            f"SELECT COUNT(*) FROM {T} WHERE latest_status IS NOT NULL AND latest_status NOT IN ({sts})") == 0
        r["shipment_fields_complete"] = q(
            f"SELECT COUNT(*) FROM {T} WHERE tracking_id IS NOT NULL AND (latest_status IS NULL "
            f"OR latest_status_at IS NULL OR pickup_at IS NULL OR promised_delivery_at IS NULL)") == 0
        r["no_shipment_means_no_events"] = q(
            f"SELECT COUNT(*) FROM {T} WHERE (tracking_id IS NULL) <> (event_count = 0)") == 0
        r["delivered_at_iff_delivered"] = q(
            f"SELECT COUNT(*) FROM {T} WHERE (delivered_at IS NOT NULL) <> "
            f"(COALESCE(latest_status,'') = 'delivered')") == 0
        r["pickup_before_delivery"] = q(
            f"SELECT COUNT(*) FROM {T} WHERE delivered_at IS NOT NULL AND pickup_at > delivered_at") == 0
        r["is_late_consistent"] = q(
            f"SELECT COUNT(*) FROM {T} WHERE NOT ("
            f"(delivered_at IS NULL AND is_late IS NULL AND delivery_delay_hours IS NULL) OR "
            f"(delivered_at IS NOT NULL AND is_late = (delivered_at > promised_delivery_at) "
            f" AND is_late = (delivery_delay_hours > 0)))") == 0
        r["accepted_values_is_late"] = q(
            f"SELECT COUNT(*) FROM {T} WHERE is_late IS NOT NULL AND is_late NOT IN (0,1)") == 0
        r["promised_date_matches_requested"] = q(
            f"SELECT COUNT(*) FROM {T} WHERE promised_delivery_at IS NOT NULL "
            f"AND substr(promised_delivery_at,1,10) <> requested_delivery_date") == 0
        r["customer_id_consistent_across_sources"] = t_stats["customer_mismatches"] == 0
        crm_total = round(pd.to_numeric(lines["line_total"]).sum(), 2)
        r["order_total_reconciles"] = abs(q(f"SELECT SUM(order_total) FROM {T}") - crm_total) < 1.0
        r["line_count_reconciles"] = q(f"SELECT SUM(line_count) FROM {T}") == o_stats["rows"]
    finally:
        con.close()
    for k, ok in r.items():
        print(f"[validate] {'PASS' if ok else 'FAIL'} {k}")
    failed = [k for k, ok in r.items() if not ok]
    if failed:
        raise AssertionError(f"Validation failed: {failed}")
    return r


def run_all() -> dict:
    extract_orders(); extract_shipments(); transform(); load()
    return validate()


if __name__ == "__main__":
    run_all()
