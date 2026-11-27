# Diagnosis: duplicate order lines in crm/sales_orders_2026-11-02.csv

**Root cause:** the CRM export contains 4 byte-identical repeated rows (lines L1/L2 of SO-20261102-0051 and SO-20261102-0052 appear twice, rows 132-135). The same rows trip both `order_id+line_no` and `order_line_id` key checks. Values are unchanged, so this is a replayed export, not a business event.

**Classification:** FIX (duplicates; the data's meaning is unchanged).

**Change:** `extract_orders` in `dags/delivery_performance/pipeline.py` now calls `drop_duplicates(keep="first")` on each CRM file before staging. Only exact full-row duplicates are removed, so conflicting rows for the same key are still kept and surface in the existing checks. No validation checks were changed and the manifest is untouched.

**Self-test:** `line_count_reconciles` now passes (it failed in the baseline). The remaining failures (`event_orders_exist_in_crm`, `all_events_accounted`, `shipment_fields_complete`) are unrelated to this incident. They come from the partial data slice and the separately handled 3PL file `shipment_events_2026-11-02T11.json`, which is quarantined out of data_root. I did not touch them.
