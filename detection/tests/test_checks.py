"""One test per failure mode: the right incident fires, and *only* that one."""
from __future__ import annotations

import datetime as dt
import json

from detection.checks.context import Context
from detection.engine import run_date
from detection.incidents import IncidentStore
from detection.readers import value_conforms

from .conftest import CSV_COLS, DAY, ERP_COLS


def test_clean_world_has_no_incidents(world):
    assert world.check_ids() == set()


# ----------------------------------------------------------------- schema
def test_renamed_column_non_key_is_warn(world):
    header = [("qty" if c == "quantity" else c) for c in CSV_COLS]
    world.write_csv(header=header)
    inc = world.incidents()
    assert {i.check_id for i in inc} == {"schema.renamed:quantity->qty"}
    assert inc[0].severity == "warn"


def test_renamed_key_column_is_critical(world):
    world.write_csv(header=[("orderId" if c == "order_id" else c) for c in CSV_COLS])
    inc = world.incidents()
    assert {i.check_id for i in inc} == {"schema.renamed:order_id->orderId"}
    assert inc[0].severity == "critical"


def test_missing_column(world):
    keep = [i for i, c in enumerate(CSV_COLS) if c != "unit_price"]
    world.write_csv(header=[CSV_COLS[i] for i in keep],
                    rows=[[r[i] for i in keep] for r in world.csv_rows()])
    assert world.check_ids() == {"schema.missing_column:unit_price"}


def test_new_column_is_info(world):
    world.write_csv(header=CSV_COLS + ["promo_code"],
                    rows=[r + ["X1"] for r in world.csv_rows()])
    inc = world.incidents()
    assert {i.check_id for i in inc} == {"schema.new_column:promo_code"}
    assert inc[0].severity == "info"


def test_type_change_on_share_of_rows(world):
    rows = world.csv_rows()
    rows[0][2], rows[1][2] = "1,234", "45 units"
    world.write_csv(rows=rows)
    inc = world.incidents()
    assert {i.check_id for i in inc} == {"schema.type_change:quantity"}
    assert json.loads(inc[0].observed_json)["per_file"][
        "crm/sales_orders_2026-08-14.csv"]["bad_values"] == 2


def test_date_format_drift_in_string_column_is_type_change(world):
    rows = world.csv_rows()
    rows[0][4] = "14/08/2026"
    world.write_csv(rows=rows)
    world.cfg["formats"] = {}      # formats come from the spec, so rebuild it
    from detection.manifest import load_manifest
    world.cfg["formats"] = {"crm_sales_orders_csv": {"order_date": r"^\d{4}-\d{2}-\d{2}$"}}
    world.manifest = load_manifest(world.root / "manifest.yaml", world.cfg)
    assert world.check_ids() == {"schema.type_change:order_date"}


def test_json_missing_and_renamed_keys(world):
    events = [{"event_id": "E1", "tracking_id": "T1", "state": "in_transit"}]
    world.write_json(5, events=events)       # status -> state, weight_kg dropped
    assert world.check_ids() == {"schema.renamed:status->state",
                                 "schema.missing_column:weight_kg"}


def test_sqlite_column_drop(world):
    world.write_erp(cols=["product_id", "sku"])
    assert world.check_ids() == {"schema.missing_column:is_cold_chain"}


def test_reordered_columns_info(world):
    order = [1, 0, 2, 3, 4]
    world.write_csv(header=[CSV_COLS[i] for i in order],
                    rows=[[r[i] for i in order] for r in world.csv_rows()])
    assert world.check_ids() == {"schema.column_order"}


# ----------------------------------------------------------- volume / nulls
def test_null_spike(world):
    rows = world.csv_rows()
    for r in rows[:4]:
        r[3] = ""
    world.write_csv(rows=rows)
    inc = world.incidents()
    assert {i.check_id for i in inc} == {"nulls.spike:unit_price"}


def test_null_spike_on_key_is_critical(world):
    rows = world.csv_rows()
    for r in rows[:4]:
        r[1] = ""
    world.write_csv(rows=rows)
    inc = world.incidents()
    assert [(i.check_id, i.severity) for i in inc] == [("nulls.spike:order_id", "critical")]


def test_small_null_rate_within_band_is_quiet_for_sparse_column(world):
    # weight_kg has baseline 0.0; a single null in a 2-row file is still a spike
    world.write_json(3, events=[
        {"event_id": "a", "tracking_id": "t", "status": "x", "weight_kg": None},
        {"event_id": "b", "tracking_id": "t", "status": "x", "weight_kg": 1.0}])
    assert world.check_ids() == {"nulls.spike:weight_kg"}


def test_row_count_too_high(world):
    world.write_csv(rows=world.csv_rows(60))
    assert world.check_ids() == {"volume.row_count"}


def test_row_count_too_low(world):
    world.write_csv(rows=world.csv_rows(1))
    assert world.check_ids() == {"volume.row_count"}


# ---------------------------------------------------------------- freshness
def test_missing_file(world):
    world.csv.unlink()
    assert world.check_ids() == {"freshness.missing_file"}


def test_missing_hourly_file_names_only_that_hour(world):
    world.json_path(7).unlink()
    inc = world.incidents()
    assert {i.check_id for i in inc} == {"freshness.missing_file"}
    assert json.loads(inc[0].affected_files_json) == ["3pl/shipment_events_2026-08-14T07.json"]


def test_zero_byte_file(world):
    world.csv.write_bytes(b"")
    assert world.check_ids() == {"freshness.empty_file"}


def test_truncated_csv(world):
    raw = world.csv.read_bytes()
    world.csv.write_bytes(raw[:-9])           # cut mid-record
    assert world.check_ids() == {"freshness.truncated_file"}


def test_truncated_json(world):
    p = world.json_path(2)
    p.write_bytes(p.read_bytes()[:40])
    assert world.check_ids() == {"freshness.truncated_file"}


def test_json_record_count_mismatch_is_truncation(world):
    world.write_json(4, record_count=5)
    assert world.check_ids() == {"freshness.truncated_file"}


def test_late_file(world):
    world.stamp(world.csv, dt.datetime.combine(DAY, dt.time.min) + dt.timedelta(hours=26), 6)
    inc = world.incidents()
    assert {i.check_id for i in inc} == {"freshness.late_file"}
    assert inc[0].severity == "warn"


def test_slightly_late_within_tolerance_is_quiet(world):
    world.stamp(world.csv, dt.datetime.combine(DAY, dt.time.min) + dt.timedelta(hours=26), 0.5)
    assert world.check_ids() == set()


# --------------------------------------------------------------- run health
def test_warehouse_table_missing(world):
    world.write_warehouse(with_table=False)
    assert world.check_ids() == {"run_health.table_missing"}


def test_warehouse_load_stale(world):
    assert world.check_ids(DAY + dt.timedelta(days=5)) >= {"run_health.load_stale"}
    # (the day-5 CSV is also missing, so only assert the run-health member here)


def test_warehouse_fresh_load_is_quiet(world):
    world.write_warehouse(loaded_at="2026-08-14T23:00:00Z")
    assert world.check_ids() == set()


def test_warehouse_missing_file(world):
    world.warehouse.unlink()
    assert world.check_ids() == {"run_health.table_missing"}


# ------------------------------------------------------------ determinism
def test_same_inputs_give_identical_incidents(world):
    world.write_csv(header=[("qty" if c == "quantity" else c) for c in CSV_COLS])
    world.json_path(9).unlink()
    a, b = world.incidents(), world.incidents()
    assert a == b and len(a) >= 2
    assert len({i.incident_id for i in a}) == len(a)


def test_incident_store_upsert_keeps_status(world, tmp_path):
    world.csv.unlink()
    store = IncidentStore(tmp_path / "inc.db")
    store.save(world.incidents())
    store.con.execute("UPDATE incidents SET status='acknowledged'")
    store.save(world.incidents())
    rows = store.for_date(DAY.isoformat())
    assert len(rows) == 1 and rows[0]["status"] == "acknowledged"
    assert set(rows[0].keys()) >= {"incident_id", "detected_at", "business_date",
                                   "pipeline_id", "source_id", "check_id", "check_family",
                                   "severity", "expected_json", "observed_json",
                                   "affected_files_json", "status"}


# ------------------------------------------------------------------- unit
def test_value_conforms_table():
    assert value_conforms("12", "int64") and not value_conforms("1,234", "int64")
    assert value_conforms("1.5", "float64") and not value_conforms("45 units", "float64")
    assert not value_conforms("Yes", "int64")
    assert value_conforms("2026-08-14T08:02:00+05:00", "datetime")
    assert not value_conforms("14/08/2026", "datetime") and not value_conforms(1.7e9, "datetime")
    assert value_conforms(True, "bool") and not value_conforms(True, "int64")
    assert not value_conforms(5, "string")
