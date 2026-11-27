# Diagnosis: duplicates.key:order_line_id (delivery_performance, 2026-11-03)

**Root cause:** crm/sales_orders_2026-11-03.csv contains lines L2, L3, L4 of order SO-20261103-0049 twice. The repeats are byte-identical to the originals, so this is a replay and the data's meaning is unchanged. It also tripped the sibling check order_id+line_no.

**Effect:** `line_count_reconciles` failed in the baseline because the SUM of line_count (distinct lines) did not equal the raw row count.

**Classification:** FIX. This is a plumbing duplicate, not a business signal.

**Change:** `extract_orders` in dags/delivery_performance/pipeline.py now calls `drop_duplicates(keep="first")` on fully identical rows per file. Rows that share a key but differ in content are kept, so real conflicts are still surfaced. No validation checks were changed and the manifest is untouched.

**Self-test:** `line_count_reconciles` now passes. The remaining failures, `event_orders_exist_in_crm` and `all_events_accounted`, come from the partial data slice. Only this day's CRM file is present, so events for other days' orders look orphaned. They also failed in the baseline and are unrelated to this incident.
