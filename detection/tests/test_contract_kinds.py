"""Fixes for the clean-data false positives on the four generated pipelines, and proof that every
changed check still fires on a genuinely bad input of the same kind.

  - stale_data:   `delivery: snapshot` + `max_unchanged_periods` (manifest property; default
                  `incremental` keeps the old rule: equal to the previous period = stale)
  - referential:  a "dir.col" ref resolves to every source in that directory with the column
                  (it used to take whichever source of the directory loaded first)
  - the corrected generated manifests (vendor_prices dtypes, inventory_movements null baseline,
    crm_customers snapshot) still catch real type changes, null spikes and stale feeds.
"""
from __future__ import annotations

import copy
import csv
import datetime as dt
import os
import sqlite3

import openpyxl
import pytest
import yaml

from detection import config as C
from detection.checks.context import Context
from detection.engine import run_date
from detection.manifest import load_manifest
from detection.schedule import expected_files
from detection.tests.test_v2_checks import DAY, World2

CUST = {
    "manifest_version": 1, "pipeline_id": "a_customers",       # sorts before the order manifest
    "sources": [{
        "id": "crm_customers_csv", "format": "csv",
        "path_pattern": "${DATA_ROOT}/crm/customers_*.csv",
        "delivery": "snapshot", "max_unchanged_periods": 3,
        "expected_schema": {"customer_id": "string", "customer_name": "string"},
        "observed": {"files": 1, "total_rows": 3,
                     "null_rates": {"customer_id": 0.0, "customer_name": 0.0}}}],
    "validation_checks": [{"id": "u", "type": "unique", "params": {"columns": ["customer_id"]}}],
    "destination": {"type": "sqlite", "table": "t", "path": "${WAREHOUSE_DB}"},
}
CUST_ROWS = [["C1", "Alpha"], ["C2", "Beta"], ["C3", "Gamma"]]


class WithCustomers:
    """World2 plus a CRM customer-master snapshot source in the same crm/ directory."""

    def __init__(self, root, raw=None):
        self.w = World2(root)
        (root / "m" / "a.yaml").write_text(yaml.safe_dump(raw or CUST, sort_keys=False))
        self.cust = load_manifest(root / "m" / "a.yaml", self.w.cfg)

    def write_customers(self, day, rows=CUST_ROWS):
        p = self.w.root / "crm" / f"customers_{day}.csv"
        with open(p, "w", newline="") as f:
            csv.writer(f).writerows([["customer_id", "customer_name"], *rows])
        slot = expected_files(self.cust.sources[0], day, self.w.root, self.w.cfg)[0]
        t = slot.available_at.timestamp()
        os.utime(p, (t, t))

    def history(self, days, rows=CUST_ROWS):
        """Identical snapshots for DAY-days .. DAY (days+1 files)."""
        for k in range(days, -1, -1):
            self.write_customers(DAY - dt.timedelta(days=k), rows)

    def ids(self):
        self.w.stamp_all()
        return {(i.pipeline_id, i.check_id) for i in run_date([self.cust, self.w.manifest], DAY, self.w.ctx)}


@pytest.fixture
def wc(tmp_path):
    return WithCustomers(tmp_path)


# ------------------------------------------------------------------ stale: snapshot delivery
def test_snapshot_unchanged_within_its_declared_window_is_not_stale(wc):
    wc.history(3)                                     # today + 3 identical previous days
    assert ("a_customers", "stale.identical_to_previous") not in wc.ids()


def test_snapshot_unchanged_longer_than_declared_is_stale(wc):
    wc.history(4)                                     # 4 identical previous days > 3
    assert ("a_customers", "stale.identical_to_previous") in wc.ids()


def test_snapshot_change_resets_the_unchanged_run(wc):
    wc.history(6)
    wc.write_customers(DAY - dt.timedelta(days=2), CUST_ROWS + [["C4", "Delta"]])
    assert ("a_customers", "stale.identical_to_previous") not in wc.ids()   # only 1 day unchanged


def test_incremental_default_still_flags_any_repeat(tmp_path):
    raw = copy.deepcopy(CUST)
    del raw["sources"][0]["delivery"], raw["sources"][0]["max_unchanged_periods"]
    wc = WithCustomers(tmp_path, raw)
    assert wc.cust.sources[0].delivery == "incremental"
    wc.history(1)
    assert ("a_customers", "stale.identical_to_previous") in wc.ids()


def test_snapshot_without_a_window_is_a_manifest_error(tmp_path):
    raw = copy.deepcopy(CUST)
    del raw["sources"][0]["max_unchanged_periods"]
    (tmp_path / "x.yaml").write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="max_unchanged_periods"):
        load_manifest(tmp_path / "x.yaml", C.load_config())
    raw["sources"][0]["delivery"] = "daily-ish"
    (tmp_path / "x.yaml").write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="delivery must be one of"):
        load_manifest(tmp_path / "x.yaml", C.load_config())


def test_default_delivery_keeps_existing_fingerprints(tmp_path):
    raw = copy.deepcopy(CUST)
    del raw["sources"][0]["delivery"], raw["sources"][0]["max_unchanged_periods"]
    (tmp_path / "x.yaml").write_text(yaml.safe_dump(raw))
    plain = load_manifest(tmp_path / "x.yaml", C.load_config()).sources[0]
    raw["sources"][0]["delivery"] = "incremental"
    (tmp_path / "y.yaml").write_text(yaml.safe_dump(raw))
    explicit = load_manifest(tmp_path / "y.yaml", C.load_config()).sources[0]
    assert plain.fingerprint == explicit.fingerprint


# ------------------------------------------------------------------ referential: several sources per dir
def test_orphan_still_found_when_another_crm_source_loads_first(wc):
    """The regression: crm/ now also holds the customer master (no order_id). The ref
    3pl.sales_order_id -> crm.order_id must still be checked against the order lines."""
    wc.history(2)
    assert wc.ids() == set()                         # clean: no orphan, no stale
    ev = wc.w.tpl_events(8)
    ev[0]["sales_order_id"] = "SO-NOPE"
    wc.w.write_tpl(8, ev)
    assert ("p2", "referential.orphan:sales_order_id->crm.order_id") in wc.ids()


def test_child_ref_checks_the_source_that_has_the_column(wc):
    wc.w.cfg["referential"]["extra"] = [{"child": "crm.order_id", "parent": "3pl.sales_order_id"}]
    wc.history(2)
    rows = wc.w.crm_rows()
    rows[0][1] = "NOT-SHIPPED"
    wc.w.write_crm(rows=rows)
    assert ("p2", "referential.orphan:order_id->3pl.sales_order_id") in wc.ids()


def test_snapshot_parent_repeating_itself_does_not_disable_the_check(wc):
    """customer_id is in both crm sources; the master repeats daily (snapshot), which must not
    make the parent 'unreliable' -- a real orphan customer is still reported."""
    raw = yaml.safe_load((wc.w.root / "m" / "m.yaml").read_text())
    raw["sources"][1]["expected_schema"]["customer_id"] = "string"
    raw["sources"][1]["observed"]["null_rates"]["customer_id"] = 0.0
    (wc.w.root / "m" / "m.yaml").write_text(yaml.safe_dump(raw, sort_keys=False))
    wc.w.manifest = load_manifest(wc.w.root / "m" / "m.yaml", wc.w.cfg)
    wc.w.cfg["referential"]["extra"] = [{"child": "3pl.customer_id", "parent": "crm.customer_id"}]
    wc.history(3)
    for h in range(24):
        ev = wc.w.tpl_events(h)
        for e in ev:
            e["customer_id"] = "C1"
        wc.w.write_tpl(h, ev)
    assert not {c for _, c in wc.ids() if c.startswith("referential")}
    ev = wc.w.tpl_events(9)
    for e in ev:
        e["customer_id"] = "C-GHOST"
    wc.w.write_tpl(9, ev)
    assert ("p2", "referential.orphan:customer_id->crm.customer_id") in wc.ids()


def test_incremental_parent_resend_still_disables_the_check(wc):
    """Unchanged rule for incremental parents: a stale re-send proves nothing about orphans."""
    wc.history(2)
    wc.w.write_crm(day=DAY - dt.timedelta(days=1))
    ev = wc.w.tpl_events(8)
    ev[0]["sales_order_id"] = "SO-NOPE"
    wc.w.write_tpl(8, ev)
    ids = wc.ids()
    assert ("p2", "stale.identical_to_previous") in ids
    assert not {c for _, c in ids if c.startswith("referential.orphan")}


# ------------------------------------------------------------------ the corrected generated manifests
def _real(name, root):
    cfg = C.load_config()
    m = load_manifest(C.DEFAULT_MANIFESTS / f"{name}.yaml", cfg)
    return m, Context(root, root / "wh.db", cfg, now=dt.datetime(2026, 12, 1, tzinfo=dt.timezone.utc))


def _check_ids(m, ctx, day):
    return {i.check_id for i in run_date([m], day, ctx)}


VP_HEADER = ["Supplier Code", "Supplier Name", "Product Code", "Product Name", "Pack Size", "Unit Price",
             "Currency", "MOQ", "Effective From", "Valid Until", "Cold Chain", "Notes"]


def _vendor_sheet(root, unit_price=1060.79, cold_chain="Yes"):
    p = root / "manual" / "vendor_price_sheet_2026-W33.xlsx"
    p.parent.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Price Sheet"
    ws.append(["Vendor price sheet"])
    ws.append(["week commencing 2026-08-10"])
    ws.append([])
    ws.append(VP_HEADER)
    for i in range(45):
        ws.append([f"SUP-{i % 5}", "Supplier", f"PRD-{i:04d}", "Product", "10x1", unit_price if i == 0 else 99.5,
                   "PKR", 10, dt.date(2026, 8, 10), dt.date(2026, 8, 16), cold_chain if i == 0 else "No", None])
    wb.save(p)


@pytest.mark.parametrize("unit_price,cold_chain,expect", [
    (1060.79, "Yes", set()),
    ("1,060.79", "Yes", {"schema.type_change:unit_price"}),     # a real type change: price as text
    (1060.79, True, {"schema.type_change:cold_chain"}),          # Yes/No column turned into booleans
])
def test_vendor_prices_contract_catches_real_type_changes(tmp_path, unit_price, cold_chain, expect):
    m, ctx = _real("vendor_prices", tmp_path)
    _vendor_sheet(tmp_path, unit_price, cold_chain)
    got = {c for c in _check_ids(m, ctx, DAY) if c.startswith("schema.type_change")}
    assert got == expect


def _movements(root, empty_refs: int, n=100):
    p = root / "erp" / "erp.db"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.unlink(missing_ok=True)
    m = yaml.safe_load((C.DEFAULT_MANIFESTS / "inventory_movements.yaml").read_text())
    cols = list(m["sources"][0]["expected_schema"])
    con = sqlite3.connect(p)
    con.execute(f"CREATE TABLE inventory_movements ({', '.join(cols)})")
    for i in range(n):
        row = {"movement_id": f"MV{i}", "movement_date": "2026-08-14", "movement_timestamp": "2026-08-14T10:00:00",
               "movement_type": "receipt", "batch_no": "B1", "product_id": "P1", "warehouse_id": "W1",
               "zone_id": "Z1", "qty": 5, "uom": "EA", "reference_type": "po",
               "reference_id": "" if i < empty_refs else f"PO{i}", "unit_cost": 1.5, "created_by": "u",
               "created_at": "2026-08-14T10:05:00"}
        con.execute(f"INSERT INTO inventory_movements VALUES ({', '.join('?' * len(cols))})", [row[c] for c in cols])
    con.commit()
    con.close()


@pytest.mark.parametrize("empty_refs,fires", [(35, False), (70, True)])
def test_inventory_movements_baseline_still_catches_a_real_null_spike(tmp_path, empty_refs, fires):
    m, ctx = _real("inventory_movements", tmp_path)
    _movements(tmp_path, empty_refs)
    assert ("nulls.spike:reference_id" in _check_ids(m, ctx, DAY)) is fires


CUST_COLS = ["customer_id", "customer_name", "customer_type", "city", "address", "postal_code", "contact_person",
             "email", "phone", "drug_licence_no", "cold_chain_capable", "credit_limit_pkr", "payment_terms_days",
             "assigned_warehouse_id", "registered_on", "is_active"]


@pytest.mark.parametrize("previous_days,fires", [(90, False), (91, True)])
def test_crm_customers_snapshot_still_catches_a_truly_stale_feed(tmp_path, previous_days, fires):
    """The master may legitimately stay unchanged for 90 days (it did in all of the clean data);
    a feed stuck for longer is stale."""
    m, ctx = _real("crm_customers", tmp_path)
    (tmp_path / "crm").mkdir()
    rows = [[f"CUST-{i:04d}", "Name", "pharmacy", "Karachi", "addr", "75500", "p", "e@x", "021", "DL1",
             1, 500000, 30, "WH-01", "2025-01-01", 1] for i in range(200)]
    for k in range(previous_days, -1, -1):
        with open(tmp_path / "crm" / f"customers_{DAY - dt.timedelta(days=k)}.csv", "w", newline="") as f:
            csv.writer(f).writerows([CUST_COLS, *rows])
    assert ("stale.identical_to_previous" in _check_ids(m, ctx, DAY)) is fires
