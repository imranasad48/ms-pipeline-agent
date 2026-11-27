# Incident inc_099c78b2cee6d9ab – duplicates on order_id+line_no (CRM 2026-11-02)

**Root cause:** crm/sales_orders_2026-11-02.csv contains 4 re-sent rows (lines L1/L2 of SO-20261102-0051 and 0052 appear again at the end of the file), byte-for-byte identical to the originals. The same rows also trigger the sibling incident on order_line_id.

**Classification: FIX.** Rows are fully identical in every column, so dropping them changes no meaning (no conflicting values, no business signal).

**Change:** `extract_orders` in dags/delivery_performance/pipeline.py now does `drop_duplicates(keep="first")` on exact whole-row duplicates per file. No validation check was touched; manifest unchanged.

**Self-test:** `line_count_reconciles` (failed in baseline) now passes. Remaining failures (`event_orders_exist_in_crm`, `all_events_accounted`, `shipment_fields_complete`) were already failing in baseline and stem from 3PL events referencing orders absent from the partial data slice (other-day CRM files) – unrelated to this incident and not altered. The 3PL originWarehouseId rename is handled by the existing alias map.
