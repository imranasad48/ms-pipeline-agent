# Diagnosis: duplicates on order_id+line_no (crm/sales_orders_2026-11-02.csv)

**Root cause:** The CRM file contains 4 repeated lines (SO-20261102-0051 L1/L2, SO-20261102-0052 L1/L2). They are byte-identical copies appended at the end of the file (rows 130-133). 134 rows, 4 exact duplicates.

**Classification: FIX.** This is a duplicate-delivery defect and the data's meaning is unchanged. The same rows also trigger the order_line_id duplicate incident (inc_b5d7d93f59429715).

**Change:** `extract_orders` in `dags/delivery_performance/pipeline.py` now drops fully identical rows (all contract columns) before staging. Rows that share a key but differ in content are not dropped, so real conflicts still surface. No validation checks were changed and the manifest is untouched.

**Self-test:** `line_count_reconciles` now passes. Without the patch it failed because it counted the duplicated lines. Three validate checks still fail: `event_orders_exist_in_crm`, `all_events_accounted` and `shipment_fields_complete`. They were already failing in the baseline. Their cause is the 3PL file for hour T11, which is quarantined in this data slice (the origin_warehouse_id rename incident). They are not caused by this incident or by this patch.
