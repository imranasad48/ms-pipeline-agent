# inc_099c78b2cee6d9ab – duplicates on order_id+line_no (crm sales_orders 2026-11-02)

**Root cause:** the CRM file contains a replayed block: rows for orders SO-20261102-0051 and -0052 (4 lines) appear twice, byte-for-byte identical (lines 128-131 repeated at 132-135). This is a duplicate-load defect, not a business event. It inflated line_count (line_count_reconciles failed in baseline).

**Classification:** FIX – exact-row duplicates are plumbing; meaning of the data is unchanged.

**Change:** in `extract_orders` (dags/delivery_performance/pipeline.py), drop rows that are identical across all CRM schema columns per file (keep first). Rows sharing a key but differing in content are NOT dropped, so validation still flags genuine conflicts. No validation checks were changed; manifest untouched.

**Self-test (1 attempt):** extract/transform/load pass; `line_count_reconciles` now passes. Remaining failures (`event_orders_exist_in_crm`, `all_events_accounted`, `shipment_fields_complete`) are caused by the quarantined shipment_events_2026-11-02T11.json (a separate incident, tpl schema rename) being absent from the case data slice, not by this change.
