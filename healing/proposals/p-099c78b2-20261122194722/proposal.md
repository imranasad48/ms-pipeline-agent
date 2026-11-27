# Proposal p-099c78b2-20261122194722

**Verdict: FAILED_VALIDATION**

- Incident: `inc_099c78b2cee6d9ab`  (critical)  `duplicates.key:order_id+line_no`
- Pipeline: `delivery_performance`   Source: `crm_sales_orders_csv`   Business date: 2026-11-02
- Affected files: crm/sales_orders_2026-11-02.csv
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Incident inc_099c78b2cee6d9ab – duplicates on order_id+line_no (CRM 2026-11-02)

**Root cause:** crm/sales_orders_2026-11-02.csv contains 4 re-sent rows (lines L1/L2 of SO-20261102-0051 and 0052 appear again at the end of the file), byte-for-byte identical to the originals. The same rows also trigger the sibling incident on order_line_id.

**Classification: FIX.** Rows are fully identical in every column, so dropping them changes no meaning (no conflicting values, no business signal).

**Change:** `extract_orders` in dags/delivery_performance/pipeline.py now does `drop_duplicates(keep="first")` on exact whole-row duplicates per file. No validation check was touched; manifest unchanged.

**Self-test:** `line_count_reconciles` (failed in baseline) now passes. Remaining failures (`event_orders_exist_in_crm`, `all_events_accounted`, `shipment_fields_complete`) were already failing in baseline and stem from 3PL events referencing orders absent from the partial data slice (other-day CRM files) – unrelated to this incident and not altered. The 3PL originWarehouseId rename is handled by the existing alias map.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -136,6 +136,8 @@
         missing = set(CRM_SCHEMA) - set(df.columns)
         if missing:
             raise ValueError(f"{os.path.basename(f)} missing columns: {sorted(missing)}")
+        # exact re-sent rows (every column identical) carry no new information: drop them
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

Do NOT apply. Validation failed; see table. Archive with: `python -m healing reject p-099c78b2-20261122194722 --reason "..."`
