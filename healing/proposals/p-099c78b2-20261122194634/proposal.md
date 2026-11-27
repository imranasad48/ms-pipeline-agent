# Proposal p-099c78b2-20261122194634

**Verdict: FAILED_VALIDATION**

- Incident: `inc_099c78b2cee6d9ab`  (critical)  `duplicates.key:order_id+line_no`
- Pipeline: `delivery_performance`   Source: `crm_sales_orders_csv`   Business date: 2026-11-02
- Affected files: crm/sales_orders_2026-11-02.csv
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Diagnosis: duplicates on order_id+line_no (crm/sales_orders_2026-11-02.csv)

**Root cause:** The CRM file contains 4 repeated lines (SO-20261102-0051 L1/L2, SO-20261102-0052 L1/L2). They are byte-identical copies appended at the end of the file (rows 130-133). 134 rows, 4 exact duplicates.

**Classification: FIX.** This is a duplicate-delivery defect and the data's meaning is unchanged. The same rows also trigger the order_line_id duplicate incident (inc_b5d7d93f59429715).

**Change:** `extract_orders` in `dags/delivery_performance/pipeline.py` now drops fully identical rows (all contract columns) before staging. Rows that share a key but differ in content are not dropped, so real conflicts still surface. No validation checks were changed and the manifest is untouched.

**Self-test:** `line_count_reconciles` now passes. Without the patch it failed because it counted the duplicated lines. Three validate checks still fail: `event_orders_exist_in_crm`, `all_events_accounted` and `shipment_fields_complete`. They were already failing in the baseline. Their cause is the 3PL file for hour T11, which is quarantined in this data slice (the origin_warehouse_id rename incident). They are not caused by this incident or by this patch.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -140,6 +140,9 @@
         per_file[os.path.basename(f)] = len(df)
         frames.append(df)
     raw = pd.concat(frames, ignore_index=True)
+    # duplicates: drop byte-identical repeated lines (same order_id+line_no, same content).
+    # Conflicting duplicates (same key, different content) are kept so validation still flags them.
+    raw = raw.drop_duplicates(subset=list(CRM_SCHEMA), keep="first").reset_index(drop=True)
     raw.to_pickle(_stage("orders_raw"))
     stats = {"files": len(per_file), "rows": len(raw), "orders": raw["order_id"].nunique()}
     pd.to_pickle(stats, _stage("orders_stats"))
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

Do NOT apply. Validation failed; see table. Archive with: `python -m healing reject p-099c78b2-20261122194634 --reason "..."`
