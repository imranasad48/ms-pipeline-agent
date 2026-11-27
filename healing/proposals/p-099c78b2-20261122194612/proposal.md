# Proposal p-099c78b2-20261122194612

**Verdict: FAILED_VALIDATION**

- Incident: `inc_099c78b2cee6d9ab`  (critical)  `duplicates.key:order_id+line_no`
- Pipeline: `delivery_performance`   Source: `crm_sales_orders_csv`   Business date: 2026-11-02
- Affected files: crm/sales_orders_2026-11-02.csv
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Diagnosis: duplicate order lines in crm/sales_orders_2026-11-02.csv

**Root cause:** the CRM export contains 4 byte-identical repeated rows (lines L1/L2 of SO-20261102-0051 and SO-20261102-0052 appear twice, rows 132-135). The same rows trip both `order_id+line_no` and `order_line_id` key checks. Values are unchanged, so this is a replayed export, not a business event.

**Classification:** FIX (duplicates; the data's meaning is unchanged).

**Change:** `extract_orders` in `dags/delivery_performance/pipeline.py` now calls `drop_duplicates(keep="first")` on each CRM file before staging. Only exact full-row duplicates are removed, so conflicting rows for the same key are still kept and surface in the existing checks. No validation checks were changed and the manifest is untouched.

**Self-test:** `line_count_reconciles` now passes (it failed in the baseline). The remaining failures (`event_orders_exist_in_crm`, `all_events_accounted`, `shipment_fields_complete`) are unrelated to this incident. They come from the partial data slice and the separately handled 3PL file `shipment_events_2026-11-02T11.json`, which is quarantined out of data_root. I did not touch them.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -136,6 +136,8 @@
         missing = set(CRM_SCHEMA) - set(df.columns)
         if missing:
             raise ValueError(f"{os.path.basename(f)} missing columns: {sorted(missing)}")
+        # exact duplicate line rows (replayed export) carry no new information: drop them
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

Do NOT apply. Validation failed; see table. Archive with: `python -m healing reject p-099c78b2-20261122194612 --reason "..."`
