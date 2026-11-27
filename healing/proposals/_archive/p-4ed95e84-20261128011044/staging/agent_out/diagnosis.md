# Incident inc_4ed95e84a9df8e7b — crm_sales_orders, 2026-11-03

## Root cause
`crm/sales_orders_2026-11-03.csv` has 130 data rows. The last 3 rows (file lines 129–131) are
exact copies of the lines above them for order SO-20261103-0049, lines L2, L3 and L4. Every
column is identical, including order_datetime, quantity, price and line_total. This looks
like a re-sent or appended block from the CRM export, not new orders. The load step failed
with `UNIQUE constraint failed: sales_orders__new.order_line_id`. The same root cause also
explains the same-day incident `duplicates.key:order_id+line_no` (inc_baad7757bea19f87).

## Classification: FIX
These are exact duplicate rows, which is a plumbing defect. They add no information and the
business meaning is unchanged: order 0049 really has 4 lines (L1–L4), and the duplicated
lines are byte-identical. Removing them loses nothing.

## Change (dags/crm_sales_orders/pipeline.py, extract step)
- Each file is read and its required columns are checked as before. Then rows that are
  **identical in every column** within that file are dropped, keeping the first copy.
- Rows that share a key (order_line_id or order_id+line_no) but differ in any value are
  **not** dropped. They still fail the PK constraint and the uniqueness checks, so a real
  conflict still needs a human.
- The number of rows dropped per file is printed and recorded in `extract_stats`
  (`exact_duplicates_dropped`) for auditing. Row-count checks now compare against the
  deduplicated counts.
- No validation checks were changed. The manifest was not changed because the contract
  itself is the same.

## Self-test (attempt 1)
All 4 steps and all 15 validate checks pass. The table has 127 rows for this date
(130 minus 3 exact duplicates).

## Recommendation
Approve. Also ask the CRM export owner why order 0049's lines were sent twice.
