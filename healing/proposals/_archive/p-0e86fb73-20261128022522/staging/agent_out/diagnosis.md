# Incident inc_0e86fb7397df1bac — crm_sales_orders, 2026-11-04

**Check:** duplicates.key:order_id+line_no (along with the same-day incident duplicates.key:order_line_id)

## Root cause
`crm/sales_orders_2026-11-04.csv` has 3 lines from order SO-20261104-0052 (L1–L3, file lines 138–140)
written a second time at the end of the file (lines 141–143). Each copy matches its original
in every column: ids, timestamps, quantities, prices, totals, status and rep.
This is a re-emit/append defect in the export, not a second order. The unpatched load failed with
`UNIQUE constraint failed: sales_orders__new.order_line_id`.

## Classification: FIX
The data means the same thing as before. The copies are exact, so dropping them loses nothing and
does not hide anything real. If a key were repeated with *different* values (a real conflict that
needs a decision), that would be an escalation. The patch does not drop those rows.

## Change
`dags/crm_sales_orders/pipeline.py` / `extract()`: for each file, drop rows that match an earlier row
in every source column (keep the first), and log how many were dropped. Per-file and total row
counts are taken after this step, so the row-count checks compare the table against the de-duplicated
source. Rows that share a key but differ in content are untouched. They still fail the PK
constraint in load and the uniqueness checks. None of the validation checks were changed, and the
manifest is unchanged because the contract stays the same.

## Self-test
Attempt 1: all steps and all 15 validate checks pass.
The control-run row_count failures were artifacts of the partial data slice and do not occur here.
