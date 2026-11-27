# Proposal p-4ed95e84-20261128011044

**Verdict: READY_FOR_APPROVAL**

- Incident: `inc_4ed95e84a9df8e7b`  (critical)  `duplicates.key:order_line_id`
- Pipeline: `crm_sales_orders`   Source: `crm_sales_orders_csv`   Business date: 2026-11-03
- Affected files: crm/sales_orders_2026-11-03.csv
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Incident inc_4ed95e84a9df8e7b — crm_sales_orders, 2026-11-03

## Root cause
`crm/sales_orders_2026-11-03.csv` has 130 data rows. The last 3 rows (file lines 129–131) are
exact copies of the lines above them for order SO-20261103-0049, lines L2, L3 and L4. Every
column is identical, including order_datetime, quantity, price and line_total. This looks
like a re-sent or appended block from the CRM export, not new orders. The load step failed
with `UNIQUE constraint failed: sales_orders__new.order_line_id`. The same root cause also
explains the same-day incident `duplicates.key:order_id+line_no` (inc_baad7757bea19f87).

## Classification: FIX
These are exact duplicate rows, which is a plumbing defect. They add no information and the
business meaning is unchanged: order 0049 really has 4 lines (L1–L4), and the duplicated
lines are byte-identical. Removing them loses nothing.

## Change (dags/crm_sales_orders/pipeline.py, extract step)
- Each file is read and its required columns are checked as before. Then rows that are
  **identical in every column** within that file are dropped, keeping the first copy.
- Rows that share a key (order_line_id or order_id+line_no) but differ in any value are
  **not** dropped. They still fail the PK constraint and the uniqueness checks, so a real
  conflict still needs a human.
- The number of rows dropped per file is printed and recorded in `extract_stats`
  (`exact_duplicates_dropped`) for auditing. Row-count checks now compare against the
  deduplicated counts.
- No validation checks were changed. The manifest was not changed because the contract
  itself is the same.

## Self-test (attempt 1)
All 4 steps and all 15 validate checks pass. The table has 127 rows for this date
(130 minus 3 exact duplicates).

## Recommendation
Approve. Also ask the CRM export owner why order 0049's lines were sent twice.

## Proposed change

```diff
--- live/dags/crm_sales_orders/pipeline.py
+++ staged/dags/crm_sales_orders/pipeline.py
@@ -86,18 +86,27 @@
     files = source_files()
     if not files:
         raise FileNotFoundError(f"No files match {FILE_PATTERN} under {data_root()}")
-    frames, per_file = [], {}
+    frames, per_file, dropped = [], {}, {}
     for f in files:
         df = pd.read_csv(f, dtype=str, keep_default_na=False, na_values=[""])
         missing = set(EXPECTED_SCHEMA) - set(df.columns)
         if missing:
             raise ValueError(f"{os.path.basename(f)} missing columns: {sorted(missing)}")
+        # Drop exact duplicate rows (identical in every source column) -- re-emitted
+        # lines carry no new information. Rows sharing a key but differing in any
+        # value are NOT dropped and will still fail the PK / uniqueness checks.
+        before = len(df)
+        df = df.drop_duplicates(keep="first").reset_index(drop=True)
+        if len(df) != before:
+            dropped[os.path.basename(f)] = before - len(df)
+            print(f"[extract] {os.path.basename(f)}: dropped {before - len(df)} exact duplicate rows")
         df["source_file"] = os.path.basename(f)
         per_file[os.path.basename(f)] = len(df)
         frames.append(df)
     raw = pd.concat(frames, ignore_index=True)
     raw.to_pickle(_stage_path("extracted"))
-    stats = {"files": len(files), "rows": len(raw), "per_file": per_file}
+    stats = {"files": len(files), "rows": len(raw), "per_file": per_file,
+             "exact_duplicates_dropped": dropped}
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

Approve:  `python -m healing apply p-4ed95e84-20261128011044`
Decline:  `python -m healing reject p-4ed95e84-20261128011044 --reason "..."`
