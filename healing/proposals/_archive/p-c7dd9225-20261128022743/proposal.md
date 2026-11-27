# Proposal p-c7dd9225-20261128022743

**Verdict: READY_FOR_APPROVAL**

- Incident: `inc_c7dd92259d08f902`  (critical)  `duplicates.key:order_line_id`
- Pipeline: `crm_sales_orders`   Source: `crm_sales_orders_csv`   Business date: 2026-11-04
- Affected files: crm/sales_orders_2026-11-04.csv
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Incident inc_c7dd92259d08f902 — crm_sales_orders, 2026-11-04

## Root cause
`crm/sales_orders_2026-11-04.csv` has 142 data rows. The last three (file lines 141–143) are
byte-identical copies of lines 138–140, the three lines of order `SO-20261104-0052`.
Every field is the same: ids, timestamp, quantities, prices, totals and status. This is a
re-delivery or double-append in the export. It is not three new or conflicting order lines.
The unpatched `load` step failed with `UNIQUE constraint failed: order_line_id`, and validation was skipped.
The sibling incident `duplicates.key:order_id+line_no` (inc_0e86fb7397df1bac) has the same cause.

## Classification: FIX
Exact full-row duplicates are a plumbing defect. Dropping the extra copies does not change what
the data means: there is still one order with 3 lines. If the copies had differed in any value,
the right version would be unclear, so that case would need a human.

## Change (dags/crm_sales_orders/pipeline.py, `extract`)
- In each file, drop rows that are exact copies of an earlier row in the same file
  (`drop_duplicates()` over all columns, on the raw strings, keep the first). The count dropped
  is printed and stored in the extract stats as `exact_duplicates_dropped`.
- Rows that share a key but differ in any value are **not** removed. They still fail the PK at
  load and the `pk_unique_order_line_id` / `unique_order_id_line_no` checks.
- No validation check was changed. Row-count checks now compare against the de-duplicated
  count from extract. The manifest is unchanged because the contract has not changed.

## Self-test
Attempt 1: all steps and all 15 validate checks pass, including the row-count checks that
failed in the control run only because of the partial data slice.

## Proposed change

```diff
--- live/dags/crm_sales_orders/pipeline.py
+++ staged/dags/crm_sales_orders/pipeline.py
@@ -86,18 +86,27 @@
     files = source_files()
     if not files:
         raise FileNotFoundError(f"No files match {FILE_PATTERN} under {data_root()}")
-    frames, per_file = [], {}
+    frames, per_file, dups_dropped = [], {}, {}
     for f in files:
         df = pd.read_csv(f, dtype=str, keep_default_na=False, na_values=[""])
         missing = set(EXPECTED_SCHEMA) - set(df.columns)
         if missing:
             raise ValueError(f"{os.path.basename(f)} missing columns: {sorted(missing)}")
+        # Drop rows that are byte-identical copies of another row in the same file
+        # (re-delivered / double-appended lines). Rows sharing a key but differing in
+        # any value are NOT touched, so genuine key conflicts still fail the PK.
+        n_before = len(df)
+        df = df.drop_duplicates(keep="first").reset_index(drop=True)
+        if len(df) != n_before:
+            dups_dropped[os.path.basename(f)] = n_before - len(df)
+            print(f"[extract] {os.path.basename(f)}: dropped {n_before - len(df)} exact duplicate rows")
         df["source_file"] = os.path.basename(f)
         per_file[os.path.basename(f)] = len(df)
         frames.append(df)
     raw = pd.concat(frames, ignore_index=True)
     raw.to_pickle(_stage_path("extracted"))
-    stats = {"files": len(files), "rows": len(raw), "per_file": per_file}
+    stats = {"files": len(files), "rows": len(raw), "per_file": per_file,
+             "exact_duplicates_dropped": dups_dropped}
     pd.to_pickle(stats, _stage_path("extract_stats"))
     print(f"[extract] {stats['files']} files, {stats['rows']} rows")
     return {"files": stats["files"], "rows": stats["rows"]}
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

Approve:  `python -m healing apply p-c7dd9225-20261128022743`
Decline:  `python -m healing reject p-c7dd9225-20261128022743 --reason "..."`
