# Proposal p-099c78b2-20261122194809

**Verdict: FAILED_VALIDATION**

- Incident: `inc_099c78b2cee6d9ab`  (critical)  `duplicates.key:order_id+line_no`
- Pipeline: `delivery_performance`   Source: `crm_sales_orders_csv`   Business date: 2026-11-02
- Affected files: crm/sales_orders_2026-11-02.csv
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Incident inc_099c78b2cee6d9ab – duplicates on order_id+line_no

**Root cause:** crm/sales_orders_2026-11-02.csv contains 4 lines (orders 0051 L1/L2, 0052 L1/L2) repeated verbatim at the end of the file (rows 132-135 repeat rows 128-131). Every column is identical, so this is a replay/duplicate-delivery defect, not new business data. The same defect raised inc_b5d7d93f59429715 (key order_line_id).

**Impact in the unpatched pipeline:** `line_count_reconciles` failed. The order totals would also have been double counted (total_quantity and order_total inflated for orders 0051/0052).

**Classification:** FIX. Exact duplicate rows are plumbing, and their meaning is unchanged.

**Change:** in `extract_orders` (dags/delivery_performance/pipeline.py), `drop_duplicates(keep="first")` on the full row of each CRM file, before row counts are computed. Only fully identical rows are removed. Rows that share a key but differ in content are kept, so the existing consistency checks still catch them. No validation check was changed or removed. The manifest is unchanged.

**Self-test (1 attempt):** extract, transform and load pass. `line_count_reconciles` now passes, along with the other validate checks except three. `event_orders_exist_in_crm`, `all_events_accounted` and `shipment_fields_complete` still fail. They also failed in the baseline, and they come from the partial data slice. The hour-11 events file is quarantined, and the sliced events reference orders and events outside the slice. They are not caused by this incident or by my patch, and I left them alone.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -136,6 +136,8 @@
         missing = set(CRM_SCHEMA) - set(df.columns)
         if missing:
             raise ValueError(f"{os.path.basename(f)} missing columns: {sorted(missing)}")
+        # exact repeated rows (replayed lines) carry no new information: drop them
+        df = df.drop_duplicates(keep="first").reset_index(drop=True)
         df["source_file"] = os.path.basename(f)
         per_file[os.path.basename(f)] = len(df)
         frames.append(df)
```

## Validation (deterministic, same case data, before vs after)

| check | before | after | note |
|---|---|---|---|
| detect:crm_sales_orders_csv:duplicates.row | fail | fail | source file still dirty (expected: code-only fix) |
| detect:tpl_shipment_events_json:schema.renamed:origin_warehouse_id->originWarehouseId | fail | fail | source file still dirty (expected: code-only fix) |
| step:validate | fail | fail | TARGET |
| validate:all_events_accounted | fail | fail | TARGET |
| validate:event_orders_exist_in_crm | fail | fail | TARGET |
| validate:line_count_reconciles | fail | pass | TARGET |
| validate:shipment_fields_complete | fail | fail | TARGET |
| _19 other checks_ | | pass | _unchanged or newly passing; full list in validation.json_ |

Targets: step:validate, validate:all_events_accounted, validate:event_orders_exist_in_crm, validate:line_count_reconciles, validate:shipment_fields_complete  |  regressions: none  |  unmasked failures: none

_Scope note: the case runs on one business date's files with other incidents' files removed. Checks that also fail on a control run without this incident's files are marked 'scope artifact' and do not block approval._

## Decision

Do NOT apply. Validation failed; see table. Archive with: `python -m healing reject p-099c78b2-20261122194809 --reason "..."`
