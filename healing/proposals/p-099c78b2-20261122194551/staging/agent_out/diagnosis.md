# inc_099c78b2cee6d9ab – duplicate (order_id, line_no) in crm/sales_orders_2026-11-02.csv

**Root cause:** The CRM file contains 4 byte-identical repeated rows (lines 132-135 repeat lines 128-131: orders SO-20261102-0051 and -0052, lines 1 and 2). Re-delivered/copied rows; no content differs.

**Classification: FIX.** Exact duplicate rows are a plumbing defect and carry no new meaning.

**Change:** `extract_orders` in `dags/delivery_performance/pipeline.py` now drops fully identical rows per file (`drop_duplicates(keep="first")`). Rows that share a key but differ in content are NOT dropped, so conflicting data still surfaces. No validation checks were changed; the manifest is unchanged.

**Self-test:** `validate:line_count_reconciles` (failed in baseline because of the duplicates) now passes. Remaining failures (`event_orders_exist_in_crm`, `all_events_accounted`, `shipment_fields_complete`) come from the quarantined 3PL hour-11 file (the separate `originWarehouseId` incident, `scope.json`), not from this incident.
