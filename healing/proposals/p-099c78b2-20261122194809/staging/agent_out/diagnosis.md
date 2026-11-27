# Incident inc_099c78b2cee6d9ab – duplicates on order_id+line_no

**Root cause:** crm/sales_orders_2026-11-02.csv contains 4 lines (orders 0051 L1/L2, 0052 L1/L2) repeated verbatim at the end of the file (rows 132-135 repeat rows 128-131). Every column is identical, so this is a replay/duplicate-delivery defect, not new business data. The same defect raised inc_b5d7d93f59429715 (key order_line_id).

**Impact in the unpatched pipeline:** `line_count_reconciles` failed. The order totals would also have been double counted (total_quantity and order_total inflated for orders 0051/0052).

**Classification:** FIX. Exact duplicate rows are plumbing, and their meaning is unchanged.

**Change:** in `extract_orders` (dags/delivery_performance/pipeline.py), `drop_duplicates(keep="first")` on the full row of each CRM file, before row counts are computed. Only fully identical rows are removed. Rows that share a key but differ in content are kept, so the existing consistency checks still catch them. No validation check was changed or removed. The manifest is unchanged.

**Self-test (1 attempt):** extract, transform and load pass. `line_count_reconciles` now passes, along with the other validate checks except three. `event_orders_exist_in_crm`, `all_events_accounted` and `shipment_fields_complete` still fail. They also failed in the baseline, and they come from the partial data slice. The hour-11 events file is quarantined, and the sliced events reference orders and events outside the slice. They are not caused by this incident or by my patch, and I left them alone.
