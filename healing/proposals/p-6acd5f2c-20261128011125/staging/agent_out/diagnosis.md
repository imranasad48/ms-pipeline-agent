# inc_6acd5f2c073a6b87 — delivery_performance, 2026-11-03 — duplicates.key:order_id+line_no

## Root cause
`crm/sales_orders_2026-11-03.csv` has three order lines that appear twice: SO-20261103-0049 lines L2, L3 and L4 show up again at the end of the file (rows 129–131). The copies match the originals in every column. The CRM export re-sent part of an order. This is a delivery/plumbing duplicate. It is not a new business fact.

## Impact before the patch
- `line_count_reconciles` FAILED: the order has 4 distinct lines but the source has 7 rows.
- `order_total` for SO-20261103-0049 was quietly inflated by about 2.24M PKR. `transform` sums line_total over all rows, so the copies were counted twice. `order_total_reconciles` still passed only because it compares against the same inflated raw sum.

## Classification: FIX
The copies are identical in every column, including order_line_id. The data's meaning is unchanged, so dropping the copies is safe and loses no signal.

## Change (dags/delivery_performance/pipeline.py, extract_orders)
- After the CRM files are combined, drop rows that are exact copies across every source column (source_file is ignored). The number dropped is logged.
- Safeguard: if rows still share a line key (`order_id`+`line_no` or `order_line_id`) but differ in content, the step raises an error. A conflict like that needs a human. It is never resolved silently.
- No validation checks were changed. The manifest is unchanged because the contract did not change.

## Self-test (attempt 1)
- All steps up to load pass. `line_count_reconciles` now passes, along with all other checks except the two below.
- `event_orders_exist_in_crm` and `all_events_accounted` still fail. Both come from the partial data slice, not from this incident. All 32 orphan orders are SO-20261101-* or SO-20261102-*, and their CRM files are not in the case data_root. These failures were present in the baseline before the patch, and the control run shows that validate fails on this slice anyway.
