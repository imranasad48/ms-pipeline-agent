# Incident inc_bc9715657fa040ae — delivery_performance, 2026-11-04

## Root cause
`crm/sales_orders_2026-11-04.csv` contains the 3 lines of order SO-20261104-0052
(L1, L2, L3; file rows 138-140) written a second time (rows 141-143). The copies match
the originals in every field, so this is a re-emitted or double-written block in the
CRM export, not new business activity. The detector also flagged `duplicates.row`
(whole-row duplicates) and the same-day incident `order_id+line_no`, which is the
same defect.

## Impact on the unpatched pipeline
`extract_orders` staged 3 extra rows. The aggregation counts lines with
`nunique(order_line_id)`, but `line_count_reconciles` compares against the raw row
count, so it FAILED. `order_total` and the CRM line_total sum were both inflated by
the same 406,113.45 PKR. That reconciliation still passed, but it was comparing two
wrong numbers: the order total for SO-20261104-0052 was doubled.

## Classification: FIX
The duplicates are exact copies, so the meaning of the data is unchanged. Removing
them is lossless and has only one correct result.

## Change (dags/delivery_performance/pipeline.py, extract_orders)
- After concatenating the CRM files, drop exact duplicate rows. All columns are
  compared except `source_file`, and the first copy is kept. The number dropped is
  logged and recorded as `duplicate_rows_dropped` in orders_stats.
- Guard: if an `order_line_id` still repeats after that, its copies differ in content.
  That case is ambiguous, so the step raises an error instead of choosing a copy.
- Existing behaviour is unchanged for clean files. No validation checks were touched,
  and the manifest was not changed (the contract is the same).

## Self-test (attempt 1)
All steps passed. `line_count_reconciles` now passes, and order_total is correct.
Still failing: `event_orders_exist_in_crm` and `all_events_accounted`. These are the
listed scope artifacts and fail identically in the control run, because 3PL events
reference orders from dates outside the case data slice. They are not caused by
this incident or this patch.
