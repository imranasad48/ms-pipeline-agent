# Proposal p-296c944a-20261128022604

**Verdict: READY_FOR_APPROVAL**

- Incident: `inc_296c944a6c7abd1d`  (critical)  `duplicates.key:order_id+line_no`
- Pipeline: `delivery_performance`   Source: `crm_sales_orders_csv`   Business date: 2026-11-04
- Affected files: crm/sales_orders_2026-11-04.csv
- Agent decision: FIX  (attempts: 1)


## Diagnosis

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

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -141,8 +141,21 @@
         per_file[os.path.basename(f)] = len(df)
         frames.append(df)
     raw = pd.concat(frames, ignore_index=True)
+    # duplicates: drop exact re-deliveries of the same order line (identical in every
+    # CRM column). Conflicting rows sharing a line key are NOT silently resolved.
+    n_before = len(raw)
+    raw = raw.drop_duplicates(subset=list(CRM_SCHEMA), keep="first").reset_index(drop=True)
+    n_exact_dupes = n_before - len(raw)
+    for key in (["order_line_id"], ["order_id", "line_no"]):
+        conflict = raw[raw.duplicated(key, keep=False)]
+        if not conflict.empty:
+            raise ValueError(f"conflicting CRM rows share key {key}: "
+                             f"{conflict[key].drop_duplicates().head(5).values.tolist()}")
+    if n_exact_dupes:
+        print(f"[extract_orders] dropped {n_exact_dupes} exact duplicate order line(s)")
     raw.to_pickle(_stage("orders_raw"))
-    stats = {"files": len(per_file), "rows": len(raw), "orders": raw["order_id"].nunique()}
+    stats = {"files": len(per_file), "rows": len(raw), "orders": raw["order_id"].nunique(),
+             "duplicate_lines_dropped": n_exact_dupes}
     pd.to_pickle(stats, _stage("orders_stats"))
     print(f"[extract_orders] {stats}")
     return stats
```

## Validation (deterministic, same case data, before vs after)

| check | before | after | note |
|---|---|---|---|
| detect:crm_sales_orders_csv:duplicates.row | fail | fail | source file still dirty (expected: code-only fix) |
| step:validate | fail | fail | scope artifact (also fails without the incident's files) |
| validate:all_events_accounted | fail | fail | scope artifact (also fails without the incident's files) |
| validate:event_orders_exist_in_crm | fail | fail | scope artifact (also fails without the incident's files) |
| validate:line_count_reconciles | fail | pass | TARGET |
| _20 other checks_ | | pass | _unchanged or newly passing; full list in validation.json_ |

Targets: validate:line_count_reconciles  |  regressions: none  |  unmasked failures: none

_Scope note: the case runs on one business date's files with other incidents' files removed. Checks that also fail on a control run without this incident's files are marked 'scope artifact' and do not block approval._

## Decision

Approve:  `python -m healing apply p-296c944a-20261128022604`
Decline:  `python -m healing reject p-296c944a-20261128022604 --reason "..."`
