# Proposal p-0e86fb73-20261128022522

**Verdict: READY_FOR_APPROVAL**

- Incident: `inc_0e86fb7397df1bac`  (critical)  `duplicates.key:order_id+line_no`
- Pipeline: `crm_sales_orders`   Source: `crm_sales_orders_csv`   Business date: 2026-11-04
- Affected files: crm/sales_orders_2026-11-04.csv
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Incident inc_0e86fb7397df1bac — crm_sales_orders, 2026-11-04

**Check:** duplicates.key:order_id+line_no (along with the same-day incident duplicates.key:order_line_id)

## Root cause
`crm/sales_orders_2026-11-04.csv` has 3 lines from order SO-20261104-0052 (L1–L3, file lines 138–140)
written a second time at the end of the file (lines 141–143). Each copy matches its original
in every column: ids, timestamps, quantities, prices, totals, status and rep.
This is a re-emit/append defect in the export, not a second order. The unpatched load failed with
`UNIQUE constraint failed: sales_orders__new.order_line_id`.

## Classification: FIX
The data means the same thing as before. The copies are exact, so dropping them loses nothing and
does not hide anything real. If a key were repeated with *different* values (a real conflict that
needs a decision), that would be an escalation. The patch does not drop those rows.

## Change
`dags/crm_sales_orders/pipeline.py` / `extract()`: for each file, drop rows that match an earlier row
in every source column (keep the first), and log how many were dropped. Per-file and total row
counts are taken after this step, so the row-count checks compare the table against the de-duplicated
source. Rows that share a key but differ in content are untouched. They still fail the PK
constraint in load and the uniqueness checks. None of the validation checks were changed, and the
manifest is unchanged because the contract stays the same.

## Self-test
Attempt 1: all steps and all 15 validate checks pass.
The control-run row_count failures were artifacts of the partial data slice and do not occur here.

## Proposed change

```diff
--- live/dags/crm_sales_orders/pipeline.py
+++ staged/dags/crm_sales_orders/pipeline.py
@@ -92,6 +92,15 @@
         missing = set(EXPECTED_SCHEMA) - set(df.columns)
         if missing:
             raise ValueError(f"{os.path.basename(f)} missing columns: {sorted(missing)}")
+        # Drop exact re-emitted rows (every source column identical). These carry
+        # no new information. Rows sharing a key but differing in content are
+        # NOT dropped here and still fail the load / uniqueness checks.
+        cols = list(df.columns)
+        dup_mask = df.duplicated(subset=cols, keep="first")
+        if dup_mask.any():
+            print(f"[extract] {os.path.basename(f)}: dropped {int(dup_mask.sum())} "
+                  f"exact duplicate row(s)")
+            df = df[~dup_mask].reset_index(drop=True)
         df["source_file"] = os.path.basename(f)
         per_file[os.path.basename(f)] = len(df)
         frames.append(df)
```

## Validation (deterministic, same case data, before vs after)

| check | before | after | note |
|---|---|---|---|
| detect:crm_sales_orders_csv:duplicates.key:order_id+line_no | fail | fail | source file still dirty (expected: code-only fix) |
| detect:crm_sales_orders_csv:duplicates.key:order_line_id | fail | fail | source file still dirty (expected: code-only fix) |
| step:load | fail | pass | TARGET |
| _19 other checks_ | | pass | _unchanged or newly passing; full list in validation.json_ |

Targets: step:load  |  regressions: none  |  unmasked failures: none

_Scope note: the case runs on one business date's files with other incidents' files removed. Checks that also fail on a control run without this incident's files are marked 'scope artifact' and do not block approval._

## Decision

Approve:  `python -m healing apply p-0e86fb73-20261128022522`
Decline:  `python -m healing reject p-0e86fb73-20261128022522 --reason "..."`
