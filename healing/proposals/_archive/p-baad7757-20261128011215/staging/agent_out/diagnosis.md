# Incident inc_baad7757bea19f87 — crm_sales_orders, 2026-11-03

## What happened
`crm/sales_orders_2026-11-03.csv` has 130 data rows. Rows 129–131 repeat rows 126–128 exactly
(order SO-20261103-0049, lines L2, L3, L4). Every column matches, including order_line_id,
quantity, price and timestamps. This trips both duplicate detectors (order_id+line_no and
order_line_id). In the unpatched run, `load` failed with
`UNIQUE constraint failed: sales_orders__new.order_line_id`.

## Classification: FIX
These are exact full-row copies: the source sent the same lines twice, which is a delivery/plumbing
defect. They are not new or conflicting order lines, so removing the copies does not change what
the data means. Order 0049 still has its 4 lines, each exactly once.

## Change
`dags/crm_sales_orders/pipeline.py`, `extract()`: each file is passed through
`drop_duplicates()` across all source columns before it is staged. The number of copies dropped per file
is logged and saved in extract_stats (`exact_duplicates_dropped`). Row counts used by validation
are taken after this step.
Safety: rows that share a key but differ in any value are NOT dropped. They still fail the PK
constraint and the `pk_unique_order_line_id` / `unique_order_id_line_no` checks, so a real
key conflict will still escalate. No validation check was changed. The manifest is unchanged
because the contract is the same.

## Self-test
First attempt: all steps and all 15 validation checks pass, with 127 rows loaded.
