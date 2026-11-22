"""Detection v2: jsonl/xlsx parsing, strict native typing and the six new check families.

One synthetic world per test (Friday 2026-08-14, so the weekly workbook is due): a CRM CSV,
24 hourly 3PL pages, an IoT jsonl and a stock-count workbook, with the manifests declaring the
unique / range / referential constraints the checks are compiled from.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import os
import sqlite3
from pathlib import Path

import openpyxl
import pytest
import yaml

from detection import config as C
from detection.checks.context import Context
from detection.engine import run_date
from detection.manifest import load_manifest, load_manifests
from detection.schedule import expected_files

DAY = dt.date(2026, 8, 14)
MIDNIGHT = dt.datetime.combine(DAY, dt.time.min)
CRM_COLS = ["order_line_id", "order_id", "quantity", "unit_price"]
TPL_COLS = ["event_id", "sales_order_id", "event_timestamp", "weight_kg"]
IOT_COLS = ["reading_id", "device_id", "timestamp", "temperature_c"]
XL_COLS = ["warehouse", "product_code", "counted_qty"]

MANIFEST = {
    "manifest_version": 1, "pipeline_id": "p2",
    "sources": [
        {"id": "crm_sales_orders_csv", "format": "csv",
         "path_pattern": "${DATA_ROOT}/crm/sales_orders_*.csv",
         "expected_schema": {"order_line_id": "string", "order_id": "string",
                             "quantity": "int64", "unit_price": "float64"},
         "observed": {"files": 1, "total_rows": 10, "null_rates": dict.fromkeys(CRM_COLS, 0.0)}},
        {"id": "tpl_shipment_events_json", "format": "json",
         "path_pattern": "${DATA_ROOT}/3pl/shipment_events_*.json",
         "expected_schema": {"event_id": "string", "sales_order_id": "string",
                             "event_timestamp": "string", "weight_kg": "float64"},
         "observed": {"files": 24, "total_rows": 48, "null_rates": dict.fromkeys(TPL_COLS, 0.0)}},
        {"id": "iot_readings", "format": "jsonl",
         "path_pattern": "${DATA_ROOT}/iot/sensor_readings_*.jsonl",
         "expected_schema": {"reading_id": "string", "device_id": "string",
                             "timestamp": "string", "temperature_c": "float64"},
         "observed": {"files": 1, "total_rows": 388, "null_rates": dict.fromkeys(IOT_COLS, 0.0)}},
        {"id": "manual_stock_count", "format": "xlsx",
         "path_pattern": "${DATA_ROOT}/manual/stock_count_*.xlsx",
         "expected_schema": {"warehouse": "string", "product_code": "string",
                             "counted_qty": "int64"},
         "observed": {"files": 1, "total_rows": 3,
                      "layout": "sheet 'Stock Count'; 3 banner rows then header on row 4",
                      "null_rates": dict.fromkeys(XL_COLS, 0.0)}},
    ],
    "validation_checks": [
        {"id": "pk", "type": "unique", "params": {"source": "crm_sales_orders_csv",
                                                  "columns": ["order_line_id"]}},
        {"id": "ev", "type": "unique", "params": {"source": "tpl_shipment_events_json",
                                                  "columns": ["event_id"]}},
        {"id": "rd", "type": "unique", "params": {"source": "iot_readings",
                                                  "columns": ["reading_id"]}},
        {"id": "q", "type": "range", "params": {"source": "crm_sales_orders_csv",
                                                "column": "quantity", "min": 0}},
        {"id": "t", "type": "range", "params": {"source": "iot_readings",
                                                "column": "temperature_c", "min": -60, "max": 60}},
        {"id": "ref", "type": "referential_integrity",
         "params": {"child": "3pl.sales_order_id", "parent": "crm.order_id"}},
    ],
    "destination": {"type": "sqlite", "table": "t", "path": "${WAREHOUSE_DB}"},
}


class World2:
    def __init__(self, root: Path):
        self.root = root
        cfg = C.load_config()
        cfg["baselines"] = {
            "row_count_bands": {"crm_sales_orders_csv": {"min": 8, "max": 12},
                                "tpl_shipment_events_json": {"min": 0, "max": 4},
                                "iot_readings": {"min": 380, "max": 400},
                                "manual_stock_count": {"min": 3, "max": 3}},
            "value_ranges": {"crm_sales_orders_csv": {"quantity": {"min": 1, "max": 20},
                                                     "unit_price": {"min": 10, "max": 100}},
                             "manual_stock_count": {"counted_qty": {"min": 5, "max": 50}}},
            "iot_behavior": {"iot_readings": {"interval_minutes": 5,
                                              "always_on_devices": ["D1"]}},
            "known_values": {"iot_readings": {"device_id": ["D1", "D2"],
                                              "location_id": ["L1"]}},
        }
        cfg["referential"]["known_values"] = {"iot_readings": ["device_id"]}
        cfg["referential"]["extra"] = []
        self.cfg = cfg
        (root / "m").mkdir()
        (root / "m" / "m.yaml").write_text(yaml.safe_dump(MANIFEST, sort_keys=False))
        self.manifest = load_manifest(root / "m" / "m.yaml", cfg)
        self.ctx = Context(root, root / "wh.db", cfg,
                           now=dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc))
        self.write_crm()
        for h in range(24):
            self.write_tpl(h)
        self.write_iot()
        self.write_xlsx()
        con = sqlite3.connect(root / "wh.db")
        con.execute("CREATE TABLE t (id TEXT, loaded_at TEXT)")
        con.execute("INSERT INTO t VALUES ('x', '2026-08-15T01:00:00Z')")
        con.commit()
        con.close()
        self.stamp_all()

    # ------------------------------------------------------------- builders
    def crm_rows(self):
        return [[f"L{i}", f"O{i}", str(i + 1), f"{20 + i}.5"] for i in range(10)]

    def write_crm(self, rows=None, day=DAY, header=None):
        p = self.root / "crm" / f"sales_orders_{day}.csv"
        p.parent.mkdir(exist_ok=True)
        with open(p, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(header or CRM_COLS)
            w.writerows(self.crm_rows() if rows is None else rows)

    def tpl_path(self, h, day=DAY):
        return self.root / "3pl" / f"shipment_events_{day}T{h:02d}.json"

    def tpl_events(self, h):
        return [{"event_id": f"E{h}-{k}", "sales_order_id": f"O{(h + k) % 10}",
                 "event_timestamp": f"{DAY}T{h:02d}:{5 + 10 * k:02d}:00+05:00",
                 "weight_kg": 2.5} for k in range(2)]

    def write_tpl(self, h, events=None):
        p = self.tpl_path(h)
        p.parent.mkdir(exist_ok=True)
        ev = self.tpl_events(h) if events is None else events
        p.write_text(json.dumps({
            "window_start": f"{DAY}T{h:02d}:00:00+05:00",
            "window_end": (MIDNIGHT + dt.timedelta(hours=h + 1)).isoformat() + "+05:00",
            "record_count": len(ev), "events": ev}))

    def iot_rows(self):
        rows = []
        for i in range(288):                          # D1: always on, every 5 minutes
            t = MIDNIGHT + dt.timedelta(minutes=5 * i)
            rows.append({"reading_id": f"D1-{i}", "device_id": "D1", "timestamp": t.isoformat(),
                         "temperature_c": round(4.0 + (i % 7) * 0.1, 2)})
        for i in range(100):                          # D2: a shift, 10:00..18:15
            t = MIDNIGHT + dt.timedelta(hours=10, minutes=5 * i)
            rows.append({"reading_id": f"D2-{i}", "device_id": "D2", "timestamp": t.isoformat(),
                         "temperature_c": round(5.0 + (i % 5) * 0.1, 2)})
        rows.sort(key=lambda r: r["timestamp"])
        return rows

    def write_iot(self, rows=None, raw_lines=None, trailing_newline=True):
        p = self.root / "iot" / f"sensor_readings_{DAY}.jsonl"
        p.parent.mkdir(exist_ok=True)
        lines = raw_lines if raw_lines is not None else [
            json.dumps(r) for r in (self.iot_rows() if rows is None else rows)]
        p.write_text("\n".join(lines) + ("\n" if trailing_newline else ""))

    def write_xlsx(self, rows=None, banner=3, sheet="Stock Count", header=None):
        p = self.root / "manual" / "stock_count_2026-W33.xlsx"
        p.parent.mkdir(exist_ok=True)
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = sheet
        for i in range(banner):
            ws.append([f"banner {i}", None, None])
        ws.append(header or ["Warehouse", "Product Code", "Counted Qty"])
        for r in (rows if rows is not None else
                  [["WH-01", "PRD-1", 10], ["WH-01", "PRD-2", 20], ["WH-02", "PRD-3", 30]]):
            ws.append(r)
        wb.save(p)

    def stamp_all(self):
        """Stamp every file as arriving exactly on time (no freshness noise)."""
        for spec in self.manifest.sources:
            for slot in expected_files(spec, DAY, self.root, self.cfg):
                if slot.path.exists() and slot.available_at:
                    t = slot.available_at.timestamp()
                    os.utime(slot.path, (t, t))

    # ------------------------------------------------------------------ run
    def incidents(self, day=DAY):
        self.stamp_all()
        return run_date([self.manifest], day, self.ctx)

    def ids(self, day=DAY):
        return {i.check_id for i in self.incidents(day)}


@pytest.fixture
def w2(tmp_path) -> World2:
    return World2(tmp_path)


def test_v2_clean_world_is_silent(w2):
    assert w2.ids() == set()


# ------------------------------------------------------------ manifest compile
def test_unique_and_range_are_compiled_per_source(w2):
    specs = {s.source_id: s for s in w2.manifest.sources}
    assert specs["crm_sales_orders_csv"].unique_keys == (("order_line_id",),)
    assert specs["iot_readings"].range_bounds == {"temperature_c": (-60, 60)}
    assert specs["manual_stock_count"].sheet == "Stock Count"
    assert specs["manual_stock_count"].header_row == 4
    assert w2.manifest.referential == (("3pl.sales_order_id", "crm.order_id"),)


def test_unnamed_unique_in_multi_source_manifest_is_not_a_source_key(tmp_path):
    raw = yaml.safe_load(yaml.safe_dump(MANIFEST))
    raw["validation_checks"] = [{"id": "x", "type": "unique", "params": {"columns": ["order_id"]}}]
    (tmp_path / "a.yaml").write_text(yaml.safe_dump(raw))
    m = load_manifests(tmp_path, C.load_config())[0]
    assert all(s.unique_keys == () for s in m.sources)


# ------------------------------------------------------------------- parsers
def test_jsonl_corrupt_line_is_its_own_incident(w2):
    lines = [json.dumps(r) for r in w2.iot_rows()]
    lines[5] = lines[5][:-10]                      # cut mid-record
    lines[9] = lines[9] + ","                      # trailing comma
    w2.write_iot(raw_lines=lines)
    inc = w2.incidents()
    ids = {i.check_id for i in inc}
    assert "schema.corrupt_line" in ids
    bad = next(i for i in inc if i.check_id == "schema.corrupt_line")
    assert json.loads(bad.observed_json)["per_file"]
    assert not {i for i in ids if i.startswith("freshness.")}


def test_jsonl_truncated_last_line_is_a_truncated_file(w2):
    lines = [json.dumps(r) for r in w2.iot_rows()]
    lines[-1] = lines[-1][:20]
    w2.write_iot(raw_lines=lines, trailing_newline=False)
    assert w2.ids() == {"freshness.truncated_file"}


def test_xlsx_header_found_by_content_and_aliases_mapped(w2):
    # clean world already uses 'Product Code' for product_code behind 3 banner rows
    assert w2.ids() == set()


def test_xlsx_header_row_shift(w2):
    w2.write_xlsx(banner=4)
    assert w2.ids() == {"schema.header_row_shift"}


def test_xlsx_renamed_sheet(w2):
    w2.write_xlsx(sheet="final v2")
    assert w2.ids() == {"schema.sheet_renamed"}


def test_xlsx_text_in_numeric_column_is_a_type_change(w2):
    w2.write_xlsx(rows=[["WH-01", "PRD-1", 10], ["WH-01", "PRD-2", "twenty"],
                        ["WH-02", "PRD-3", 30]])
    assert "schema.type_change:counted_qty" in w2.ids()


def test_xlsx_negative_count_is_out_of_range(w2):
    w2.write_xlsx(rows=[["WH-01", "PRD-1", 10], ["WH-01", "PRD-2", -4], ["WH-02", "PRD-3", 30]])
    assert w2.ids() == {"value_range.out_of_bounds:counted_qty"}


def test_json_numeric_string_is_a_type_change(w2):
    ev = w2.tpl_events(3)
    ev[0]["weight_kg"] = "16.73"                    # parses as a float, but is text
    w2.write_tpl(3, ev)
    assert w2.ids() == {"schema.type_change:weight_kg"}


def test_csv_numeric_text_is_not_a_type_change(w2):
    assert w2.ids() == set()                        # CSV values are all text: still parsed


# ---------------------------------------------------------------- duplicates
def test_duplicate_primary_key_in_a_file(w2):
    rows = w2.crm_rows()
    w2.write_crm(rows=rows + [rows[3]])
    assert w2.ids() == {"duplicates.key:order_line_id"}


def test_duplicate_event_redelivered_in_another_hourly_page(w2):
    w2.write_tpl(7, w2.tpl_events(7) + w2.tpl_events(2)[:1])
    ids = w2.ids()
    assert "duplicates.key:event_id" in ids


def test_duplicate_tolerance_is_configurable(w2):
    rows = w2.crm_rows()
    w2.write_crm(rows=rows + [rows[3]])
    w2.cfg["duplicates"]["max_duplicate_keys"] = 1
    assert w2.ids() == set()


def test_duplicate_fallback_to_full_rows_without_key(w2):
    w2.write_xlsx(rows=[["WH-01", "PRD-1", 10], ["WH-01", "PRD-1", 10], ["WH-02", "PRD-3", 30]])
    assert w2.ids() == {"duplicates.row"}


# --------------------------------------------------------------------- stale
def test_stale_3pl_page_equals_previous_hour(w2):
    prev = json.loads(w2.tpl_path(4).read_text())["events"]
    w2.write_tpl(5, prev)                           # new envelope, last hour's events
    ids = w2.ids()
    assert "stale.identical_to_previous" in ids
    assert "ordering.out_of_window:event_timestamp" in ids     # collateral, by construction


def test_stale_crm_day_equals_previous_day(w2):
    w2.write_crm(day=DAY - dt.timedelta(days=1))    # yesterday's file, same rows
    ids = w2.ids()
    assert "stale.identical_to_previous" in ids


def test_empty_hourly_pages_are_never_stale(w2):
    w2.write_tpl(4, [])
    w2.write_tpl(5, [])
    assert w2.ids() == set()


# ------------------------------------------------------------------ ordering
def test_events_out_of_chronological_order(w2):
    ev = w2.tpl_events(6)
    w2.write_tpl(6, ev[::-1])
    assert w2.ids() == {"ordering.non_monotonic:event_timestamp"}


def test_event_outside_the_pages_window(w2):
    ev = w2.tpl_events(6)
    ev[1]["event_timestamp"] = f"{DAY}T09:15:00+05:00"
    w2.write_tpl(6, ev)
    assert "ordering.out_of_window:event_timestamp" in w2.ids()


def test_iot_reading_outside_the_day(w2):
    rows = w2.iot_rows()
    rows[-1]["timestamp"] = f"{DAY + dt.timedelta(days=1)}T00:00:00"
    w2.write_iot(rows=rows)
    assert "ordering.out_of_window:timestamp" in w2.ids()


def test_ordering_tolerance_is_configurable(w2):
    w2.write_tpl(6, w2.tpl_events(6)[::-1])
    w2.cfg["out_of_order"]["max_descents"] = 1
    assert w2.ids() == set()


# --------------------------------------------------------------- value range
def test_negative_quantity(w2):
    rows = w2.crm_rows()
    rows[2][2] = "-7"
    w2.write_crm(rows=rows)
    assert w2.ids() == {"value_range.out_of_bounds:quantity"}


def test_fahrenheit_scale_temperatures_are_out_of_range(w2):
    rows = w2.iot_rows()
    for r in rows:
        if r["device_id"] == "D1":
            r["temperature_c"] = round(r["temperature_c"] * 9 / 5 + 32, 2)
    w2.write_iot(rows=rows)
    assert w2.ids() == {"value_range.out_of_bounds:temperature_c"}


def test_genuine_excursion_is_not_a_range_violation(w2):
    rows = w2.iot_rows()
    for k, r in enumerate(rows[200:230]):
        if r["device_id"] == "D1":
            r["temperature_c"] = round(13.0 + (k % 5) * 0.1, 2)   # warm but plausible, and it jitters
    w2.write_iot(rows=rows)
    assert w2.ids() == set()


def test_sensor_fault_value(w2):
    rows = w2.iot_rows()
    rows[10]["temperature_c"] = 999.9
    w2.write_iot(rows=rows)
    assert w2.ids() == {"value_range.out_of_bounds:temperature_c"}


def test_manifest_bound_is_a_hard_limit_even_without_baseline(w2):
    w2.cfg["baselines"]["value_ranges"] = {}
    w2.cfg["value_range"]["overrides"] = {}
    rows = w2.crm_rows()
    rows[0][2] = "-1"
    w2.write_crm(rows=rows)
    assert w2.ids() == {"value_range.out_of_bounds:quantity"}


# ----------------------------------------------------------------- referential
def test_orphan_sales_order_id(w2):
    ev = w2.tpl_events(8)
    ev[0]["sales_order_id"] = "SO-NOPE"
    w2.write_tpl(8, ev)
    assert w2.ids() == {"referential.orphan:sales_order_id->crm.order_id"}


def test_parent_history_covers_old_orders(w2):
    older = DAY - dt.timedelta(days=9)     # a delayed shipment: order placed over a week ago
    w2.write_crm(rows=[["LX", "OLD-1", "3", "30.5"]], day=older)
    ev = w2.tpl_events(8)
    ev[0]["sales_order_id"] = "OLD-1"
    w2.write_tpl(8, ev)
    assert "referential.orphan:sales_order_id->crm.order_id" not in w2.ids()


def test_unknown_device_is_flagged_from_known_values(w2):
    rows = w2.iot_rows()
    rows[0]["device_id"] = "ROGUE"
    w2.write_iot(rows=rows)
    assert "referential.unknown_value:device_id" in w2.ids()


# -------------------------------------------------------------------- IoT
def test_cadence_gap_for_one_device(w2):
    rows = [r for r in w2.iot_rows() if not (r["device_id"] == "D2"
                                            and "12:00" <= r["timestamp"][11:16] < "13:30")]
    w2.write_iot(rows=rows)
    assert "iot.cadence_gap" in w2.ids()


def test_always_on_device_dying_midday_is_silence(w2):
    rows = [r for r in w2.iot_rows() if not (r["device_id"] == "D1"
                                            and r["timestamp"][11:13] >= "15")]
    w2.write_iot(rows=rows)
    inc = {i.check_id: i for i in w2.incidents()}
    assert "iot.cadence_gap" in inc


def test_device_missing_all_day_is_silence(w2):
    w2.write_iot(rows=[r for r in w2.iot_rows() if r["device_id"] != "D1"])
    assert "iot.cadence_gap" in w2.ids()


def test_flatline_run(w2):
    rows = w2.iot_rows()
    n = 0
    for r in rows:
        if r["device_id"] == "D2" and n < 30:
            r["temperature_c"] = 4.66
            n += 1
    w2.write_iot(rows=rows)
    assert w2.ids() == {"iot.flatline"}


def test_flatline_threshold_is_configurable(w2):
    rows = w2.iot_rows()
    n = 0
    for r in rows:
        if r["device_id"] == "D2" and n < 9:
            r["temperature_c"] = 4.66
            n += 1
    w2.write_iot(rows=rows)
    assert w2.ids() == {"iot.flatline"}
    w2.cfg["iot_behavior"]["flatline"]["min_run"] = 12
    assert w2.ids() == set()


def test_baseline_learning_ignores_flag_columns_and_finds_always_on(tmp_path):
    """learn_baselines: constant columns get no envelope; only all-day devices are always-on."""
    from detection.engine import learn_baselines
    w = World2(tmp_path)
    got = learn_baselines([w.manifest], [DAY], w.ctx)
    assert got["iot_behavior"]["iot_readings"]["always_on_devices"] == ["D1"]
    assert got["iot_behavior"]["iot_readings"]["interval_minutes"] == 5.0
    assert got["value_ranges"]["crm_sales_orders_csv"]["quantity"] == {"min": 1.0, "max": 10.0}


def test_page_with_more_events_than_declared_is_readable_not_truncated(w2):
    """Rows appended without bumping record_count (a retry re-delivery) must reach the content
    checks; only a page with FEWER events than declared is truncated."""
    ev = w2.tpl_events(9)
    p = w2.tpl_path(9)
    doc = {"window_start": f"{DAY}T09:00:00+05:00", "window_end": f"{DAY}T10:00:00+05:00",
           "record_count": len(ev), "events": ev + ev[:1]}
    p.write_text(json.dumps(doc))
    ids = w2.ids()
    assert "duplicates.key:event_id" in ids
    assert "freshness.truncated_file" not in ids


def test_page_with_fewer_events_than_declared_is_truncated(w2):
    ev = w2.tpl_events(9)
    w2.tpl_path(9).write_text(json.dumps({
        "window_start": f"{DAY}T09:00:00+05:00", "window_end": f"{DAY}T10:00:00+05:00",
        "record_count": len(ev) + 3, "events": ev}))
    assert w2.ids() == {"freshness.truncated_file"}


def test_referential_skipped_when_a_recent_parent_file_is_missing(w2):
    """With the parent CRM files of the last days missing we cannot call an order an orphan."""
    for back in (1, 2):
        w2.write_crm(day=DAY - dt.timedelta(days=back), rows=[["LZ", "OTHER", "3", "30.5"]])
    w2.root.joinpath("crm", f"sales_orders_{DAY}.csv").unlink()
    ev = w2.tpl_events(8)
    ev[0]["sales_order_id"] = "SO-NOPE"
    w2.write_tpl(8, ev)
    ids = w2.ids()
    assert not {i for i in ids if i.startswith("referential.orphan")}
    assert "freshness.missing_file" in ids


def test_referential_skipped_when_parent_file_is_a_stale_resend(w2):
    w2.write_crm(day=DAY - dt.timedelta(days=1))          # yesterday == today
    ev = w2.tpl_events(8)
    ev[0]["sales_order_id"] = "SO-NOPE"
    w2.write_tpl(8, ev)
    ids = w2.ids()
    assert "stale.identical_to_previous" in ids
    assert not {i for i in ids if i.startswith("referential.orphan")}
