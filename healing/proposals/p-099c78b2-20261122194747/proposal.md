# Proposal p-099c78b2-20261122194747

**Verdict: FAILED_VALIDATION**

- Incident: `inc_099c78b2cee6d9ab`  (critical)  `duplicates.key:order_id+line_no`
- Pipeline: `delivery_performance`   Source: `crm_sales_orders_csv`   Business date: 2026-11-02
- Affected files: crm/sales_orders_2026-11-02.csv
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# inc_099c78b2cee6d9ab – duplicates on order_id+line_no (crm sales_orders 2026-11-02)

**Root cause:** the CRM file contains a replayed block: rows for orders SO-20261102-0051 and -0052 (4 lines) appear twice, byte-for-byte identical (lines 128-131 repeated at 132-135). This is a duplicate-load defect, not a business event. It inflated line_count (line_count_reconciles failed in baseline).

**Classification:** FIX – exact-row duplicates are plumbing; meaning of the data is unchanged.

**Change:** in `extract_orders` (dags/delivery_performance/pipeline.py), drop rows that are identical across all CRM schema columns per file (keep first). Rows sharing a key but differing in content are NOT dropped, so validation still flags genuine conflicts. No validation checks were changed; manifest untouched.

**Self-test (1 attempt):** extract/transform/load pass; `line_count_reconciles` now passes. Remaining failures (`event_orders_exist_in_crm`, `all_events_accounted`, `shipment_fields_complete`) are caused by the quarantined shipment_events_2026-11-02T11.json (a separate incident, tpl schema rename) being absent from the case data slice, not by this change.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -136,6 +136,9 @@
         missing = set(CRM_SCHEMA) - set(df.columns)
         if missing:
             raise ValueError(f"{os.path.basename(f)} missing columns: {sorted(missing)}")
+        # exact duplicate rows (whole-row replays) are plumbing noise: drop them.
+        # Rows sharing a key but differing in content are kept so validation still flags them.
+        df = df.drop_duplicates(subset=list(CRM_SCHEMA), keep="first").reset_index(drop=True)
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

Do NOT apply. Validation failed; see table. Archive with: `python -m healing reject p-099c78b2-20261122194747 --reason "..."`
