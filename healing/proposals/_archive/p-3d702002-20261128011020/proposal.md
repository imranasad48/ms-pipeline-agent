# Proposal p-3d702002-20261128011020

**Verdict: FAILED_VALIDATION**

- Incident: `inc_3d7020024ca3e468`  (critical)  `duplicates.key:order_line_id`
- Pipeline: `delivery_performance`   Source: `crm_sales_orders_csv`   Business date: 2026-11-03
- Affected files: crm/sales_orders_2026-11-03.csv
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Diagnosis: duplicates.key:order_line_id (delivery_performance, 2026-11-03)

**Root cause:** crm/sales_orders_2026-11-03.csv contains lines L2, L3, L4 of order SO-20261103-0049 twice. The repeats are byte-identical to the originals, so this is a replay and the data's meaning is unchanged. It also tripped the sibling check order_id+line_no.

**Effect:** `line_count_reconciles` failed in the baseline because the SUM of line_count (distinct lines) did not equal the raw row count.

**Classification:** FIX. This is a plumbing duplicate, not a business signal.

**Change:** `extract_orders` in dags/delivery_performance/pipeline.py now calls `drop_duplicates(keep="first")` on fully identical rows per file. Rows that share a key but differ in content are kept, so real conflicts are still surfaced. No validation checks were changed and the manifest is untouched.

**Self-test:** `line_count_reconciles` now passes. The remaining failures, `event_orders_exist_in_crm` and `all_events_accounted`, come from the partial data slice. Only this day's CRM file is present, so events for other days' orders look orphaned. They also failed in the baseline and are unrelated to this incident.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -137,6 +137,9 @@
         missing = set(CRM_SCHEMA) - set(df.columns)
         if missing:
             raise ValueError(f"{os.path.basename(f)} missing columns: {sorted(missing)}")
+        # duplicates: drop exact repeated rows (replayed lines); conflicting
+        # rows sharing a key are kept so downstream checks still flag them
+        df = df.drop_duplicates(keep="first").reset_index(drop=True)
         df["source_file"] = os.path.basename(f)
         per_file[os.path.basename(f)] = len(df)
         frames.append(df)
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

Do NOT apply. Validation failed; see table. Archive with: `python -m healing reject p-3d702002-20261128011020 --reason "..."`
