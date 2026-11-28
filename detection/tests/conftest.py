"""Tiny synthetic world: one CSV (daily), 24 JSON pages (hourly), one SQLite table."""
from __future__ import annotations

import csv
import datetime as dt
import json
import os
import sqlite3
from pathlib import Path

import pytest
import yaml

from detection import config as C
from detection.checks.context import Context
from detection.manifest import load_manifest

DAY = dt.date(2026, 8, 14)


@pytest.fixture(autouse=True)
def _no_real_airflow_snapshot(tmp_path_factory, monkeypatch):
    """Detection tests never see the live Airflow snapshot: they use snapshot fixtures only."""
    monkeypatch.setenv("DETECTION_AIRFLOW_SNAPSHOT",
                       str(tmp_path_factory.mktemp("no_airflow") / "absent.json"))
CSV_COLS = ["order_line_id", "order_id", "quantity", "unit_price", "order_date"]
JSON_COLS = ["event_id", "tracking_id", "status", "weight_kg"]
ERP_COLS = ["product_id", "sku", "is_cold_chain"]

MANIFEST = {
    "manifest_version": 1, "pipeline_id": "p1",
    "sources": [
        {"id": "crm_sales_orders_csv", "format": "csv",
         "path_pattern": "${DATA_ROOT}/crm/sales_orders_*.csv",
         "expected_schema": {"order_line_id": "string", "order_id": "string",
                             "quantity": "int64", "unit_price": "float64",
                             "order_date": "string"},
         "observed": {"files": 1, "total_rows": 10,
                      "null_rates": dict.fromkeys(CSV_COLS, 0.0)}},
        {"id": "tpl_shipment_events_json", "format": "json",
         "path_pattern": "${DATA_ROOT}/3pl/shipment_events_*.json",
         "expected_schema": {"event_id": "string", "tracking_id": "string",
                             "status": "string", "weight_kg": "float64"},
         "observed": {"files": 24, "total_rows": 48,
                      "null_rates": dict.fromkeys(JSON_COLS, 0.0)}},
        {"id": "erp_products", "format": "sqlite",
         "path_pattern": "${DATA_ROOT}/erp/erp.db#products",
         "expected_schema": {"product_id": "string", "sku": "string", "is_cold_chain": "int64"},
         "observed": {"files": 1, "total_rows": 3,
                      "null_rates": dict.fromkeys(ERP_COLS, 0.0)}},
    ],
    "destination": {"type": "sqlite", "table": "sales_orders", "path": "${WAREHOUSE_DB}"},
}


class World:
    """Paths + helpers to build and mutate the fixture."""

    def __init__(self, root: Path):
        self.root, self.day = root, DAY
        self.csv = root / "crm" / f"sales_orders_{DAY}.csv"
        self.erp = root / "erp" / "erp.db"
        self.warehouse = root / "warehouse.db"
        cfg = C.load_config()
        cfg["baselines"] = {"row_count_bands": {
            "crm_sales_orders_csv": {"min": 8, "max": 12},
            "tpl_shipment_events_json": {"min": 1, "max": 4},
            "erp_products": {"min": 3, "max": 3}}}
        self.cfg = cfg
        mpath = root / "manifest.yaml"
        mpath.write_text(yaml.safe_dump(MANIFEST, sort_keys=False))
        self.manifest = load_manifest(mpath, cfg)
        self.ctx = Context(root, self.warehouse, cfg,
                           now=dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc))
        self.write_csv()
        for h in range(24):
            self.write_json(h)
        self.write_erp()
        self.write_warehouse()

    # ---- builders
    def stamp(self, path: Path, available_at: dt.datetime, extra_hours: float = 0) -> None:
        t = (available_at + dt.timedelta(hours=extra_hours)).timestamp()
        os.utime(path, (t, t))

    def csv_rows(self, n=10):
        return [[f"L{i}", f"O{i}", str(i + 1), f"{10 + i}.5", str(DAY)] for i in range(n)]

    def write_csv(self, header=None, rows=None):
        self.csv.parent.mkdir(exist_ok=True)
        with open(self.csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(header or CSV_COLS)
            w.writerows(rows if rows is not None else self.csv_rows())
        self.stamp(self.csv, dt.datetime.combine(DAY, dt.time.min) + dt.timedelta(hours=26))

    def json_path(self, h):
        return self.root / "3pl" / f"shipment_events_{DAY}T{h:02d}.json"

    def write_json(self, h, events=None, record_count=None):
        p = self.json_path(h)
        p.parent.mkdir(exist_ok=True)
        events = events if events is not None else [
            {"event_id": f"E{h}-{k}", "tracking_id": f"T{h}", "status": "in_transit",
             "weight_kg": 2.5} for k in range(2)]
        p.write_text(json.dumps({"record_count": len(events) if record_count is None
                                 else record_count, "events": events}))
        self.stamp(p, dt.datetime.combine(DAY, dt.time.min) + dt.timedelta(hours=h + 1.25))

    def write_erp(self, cols=None):
        self.erp.parent.mkdir(exist_ok=True)
        self.erp.unlink(missing_ok=True)
        cols = cols or ERP_COLS
        con = sqlite3.connect(self.erp)
        con.execute(f"CREATE TABLE products ({', '.join(cols)})")
        for i in range(3):
            con.execute(f"INSERT INTO products VALUES ({','.join('?' * len(cols))})",
                        [f"P{i}", f"S{i}", 0][:len(cols)] if cols == ERP_COLS
                        else [f"P{i}"] * len(cols))
        con.commit()
        con.close()

    def write_warehouse(self, loaded_at="2026-08-15T01:00:00Z", with_table=True):
        self.warehouse.unlink(missing_ok=True)
        con = sqlite3.connect(self.warehouse)
        if with_table:
            con.execute("CREATE TABLE sales_orders (order_id TEXT, loaded_at TEXT)")
            con.execute("INSERT INTO sales_orders VALUES ('O1', ?)", (loaded_at,))
        con.commit()
        con.close()

    # ---- run
    def check_ids(self, business_date=DAY):
        from detection.engine import run_date
        return {i.check_id for i in run_date([self.manifest], business_date, self.ctx)}

    def incidents(self, business_date=DAY):
        from detection.engine import run_date
        return run_date([self.manifest], business_date, self.ctx)


@pytest.fixture
def world(tmp_path) -> World:
    return World(tmp_path)
