# Proposal p-6acd5f2c-20261128011125

**Verdict: FAILED_VALIDATION**

- Incident: `inc_6acd5f2c073a6b87`  (critical)  `duplicates.key:order_id+line_no`
- Pipeline: `delivery_performance`   Source: `crm_sales_orders_csv`   Business date: 2026-11-03
- Affected files: crm/sales_orders_2026-11-03.csv
- Agent decision: FIX  (attempts: 1)


## Diagnosis

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

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -141,6 +141,18 @@
         per_file[os.path.basename(f)] = len(df)
         frames.append(df)
     raw = pd.concat(frames, ignore_index=True)
+    # Duplicates: the CRM export occasionally re-emits identical order lines.
+    # Drop exact copies (all source columns identical); any remaining duplicate
+    # line key with differing content is a genuine conflict -> fail loudly.
+    src_cols = [c for c in raw.columns if c != "source_file"]
+    n_before = len(raw)
+    raw = raw.drop_duplicates(subset=src_cols, keep="first").reset_index(drop=True)
+    if len(raw) != n_before:
+        print(f"[extract_orders] dropped {n_before - len(raw)} exact duplicate order lines")
+    conflict = raw.duplicated(["order_id", "line_no"], keep=False) | raw.duplicated("order_line_id", keep=False)
+    if conflict.any():
+        raise ValueError(f"conflicting duplicate order lines (same key, different values): "
+                         f"{sorted(raw.loc[conflict, 'order_line_id'].unique())[:10]}")
     raw.to_pickle(_stage("orders_raw"))
     stats = {"files": len(per_file), "rows": len(raw), "orders": raw["order_id"].nunique()}
     pd.to_pickle(stats, _stage("orders_stats"))
```

## Validation (deterministic, same case data, before vs after)

| check | before | after | note |
|---|---|---|---|
| detect:crm_sales_orders_csv:duplicates.row | fail | fail | source file still dirty (expected: code-only fix) |
| step:validate | fail | fail | scope artifact (also fails without the incident's files) |
| validate:all_events_accounted | fail | fail | TARGET |
| validate:event_orders_exist_in_crm | fail | fail | TARGET |
| validate:line_count_reconciles | fail | pass | TARGET |
| _20 other checks_ | | pass | _unchanged or newly passing; full list in validation.json_ |

Targets: validate:all_events_accounted, validate:event_orders_exist_in_crm, validate:line_count_reconciles  |  regressions: none  |  unmasked failures: none

_Scope note: the case runs on one business date's files with other incidents' files removed. Checks that also fail on a control run without this incident's files are marked 'scope artifact' and do not block approval._

## Decision

Do NOT apply. Validation failed; see table. Archive with: `python -m healing reject p-6acd5f2c-20261128011125 --reason "..."`
