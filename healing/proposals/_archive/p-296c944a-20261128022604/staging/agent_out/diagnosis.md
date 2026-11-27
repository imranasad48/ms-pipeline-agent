# inc_296c944a6c7abd1d — delivery_performance, 2026-11-04 — duplicates.key:order_id+line_no

## Root cause
`crm/sales_orders_2026-11-04.csv` contains the 3 lines of order SO-20261104-0052 (L1–L3) twice:
rows 138–140 are repeated verbatim as rows 141–143 (identical in every column). This is a
re-delivered / re-appended block, a plumbing defect: the order's meaning is unchanged.

## Impact on the unpatched pipeline
- `line_count_reconciles` FAILED (SUM(line_count) uses distinct order_line_id = 3, but CRM rows read = 6 for that order).
- Silent corruption: `total_quantity` and `order_total` for SO-20261104-0052 were doubled
  (order_total_reconciles still passed only because the CRM-side sum was inflated by the same duplicates).
- Related same-day incident inc_bc9715657fa040ae (duplicates.key:order_line_id) has the same cause and the same fix.

## Classification: FIX
These are exact duplicate rows with no conflicting values, so there's nothing for a business user to decide.

## Change (dags/delivery_performance/pipeline.py, extract_orders)
- After concatenating the CRM files, drop rows that are identical across all contract (CRM_SCHEMA) columns.
- Then require uniqueness on `order_line_id` and on (`order_id`,`line_no`). If rows share a key but differ
  in content, the step raises instead of picking one, so real conflicts still go to a human.
- The count of dropped duplicates is logged and recorded in orders_stats (`duplicate_lines_dropped`).
- No validation checks were changed. The manifest is unchanged because the contract is the same.

## Self-test (1 attempt)
All steps pass through load, and `line_count_reconciles` now passes. The only failures are `event_orders_exist_in_crm` and
`all_events_accounted`. These are listed in scope_artifacts and fail the same way in the control run: they come from the
partial data slice (3PL events for orders from other dates).
