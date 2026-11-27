# Proposal p-099c78b2-20261122194658

**Verdict: FAILED_VALIDATION**

- Incident: `inc_099c78b2cee6d9ab`  (critical)  `duplicates.key:order_id+line_no`
- Pipeline: `delivery_performance`   Source: `crm_sales_orders_csv`   Business date: 2026-11-02
- Affected files: crm/sales_orders_2026-11-02.csv
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Diagnosis inc_099c78b2cee6d9ab (duplicates.key:order_id+line_no)
**Root cause:** crm/sales_orders_2026-11-02.csv contains a replayed block: 4 rows (orders 0051 and 0052, lines 1-2) appear twice, byte-identical in every column. This inflated line_count (validate `line_count_reconciles` failed in baseline).
**Classification:** FIX. Exact duplicate rows are a plumbing defect; the data's meaning is unchanged.
**Change:** `extract_orders` in dags/delivery_performance/pipeline.py now drops fully identical rows per file (`drop_duplicates(keep="first")`). Rows that share a key but differ in content are kept, so real conflicts still surface. No validation checks were changed and the manifest is untouched.
**Self-test:** `line_count_reconciles` now passes. The remaining failures (`event_orders_exist_in_crm`, `all_events_accounted`, `shipment_fields_complete`) were failing before the patch. They look related to the 3PL hour-11 file, which is not in the case data_root (it belongs to the separate originWarehouseId incident). They are not related to this duplicate incident. The same-day incident inc_b5d7d93f59429715 (order_line_id key) is the same duplicates and is also fixed by this patch.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -136,6 +136,9 @@
         missing = set(CRM_SCHEMA) - set(df.columns)
         if missing:
             raise ValueError(f"{os.path.basename(f)} missing columns: {sorted(missing)}")
+        # duplicates: drop exact replayed rows (all columns identical); conflicting
+        # rows sharing a key are kept so real data problems still surface downstream
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

Do NOT apply. Validation failed; see table. Archive with: `python -m healing reject p-099c78b2-20261122194658 --reason "..."`
