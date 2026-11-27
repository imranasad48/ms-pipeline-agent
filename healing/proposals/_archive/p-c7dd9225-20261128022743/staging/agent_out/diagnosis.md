# Incident inc_c7dd92259d08f902 — crm_sales_orders, 2026-11-04

## Root cause
`crm/sales_orders_2026-11-04.csv` has 142 data rows. The last three (file lines 141–143) are
byte-identical copies of lines 138–140, the three lines of order `SO-20261104-0052`.
Every field is the same: ids, timestamp, quantities, prices, totals and status. This is a
re-delivery or double-append in the export. It is not three new or conflicting order lines.
The unpatched `load` step failed with `UNIQUE constraint failed: order_line_id`, and validation was skipped.
The sibling incident `duplicates.key:order_id+line_no` (inc_0e86fb7397df1bac) has the same cause.

## Classification: FIX
Exact full-row duplicates are a plumbing defect. Dropping the extra copies does not change what
the data means: there is still one order with 3 lines. If the copies had differed in any value,
the right version would be unclear, so that case would need a human.

## Change (dags/crm_sales_orders/pipeline.py, `extract`)
- In each file, drop rows that are exact copies of an earlier row in the same file
  (`drop_duplicates()` over all columns, on the raw strings, keep the first). The count dropped
  is printed and stored in the extract stats as `exact_duplicates_dropped`.
- Rows that share a key but differ in any value are **not** removed. They still fail the PK at
  load and the `pk_unique_order_line_id` / `unique_order_id_line_no` checks.
- No validation check was changed. Row-count checks now compare against the de-duplicated
  count from extract. The manifest is unchanged because the contract has not changed.

## Self-test
Attempt 1: all steps and all 15 validate checks pass, including the row-count checks that
failed in the control run only because of the partial data slice.
