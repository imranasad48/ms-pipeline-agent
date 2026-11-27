# Proposal p-baad7757-20261128011215

**Verdict: READY_FOR_APPROVAL**

- Incident: `inc_baad7757bea19f87`  (critical)  `duplicates.key:order_id+line_no`
- Pipeline: `crm_sales_orders`   Source: `crm_sales_orders_csv`   Business date: 2026-11-03
- Affected files: crm/sales_orders_2026-11-03.csv
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Incident inc_baad7757bea19f87 — crm_sales_orders, 2026-11-03

## What happened
`crm/sales_orders_2026-11-03.csv` has 130 data rows. Rows 129–131 repeat rows 126–128 exactly
(order SO-20261103-0049, lines L2, L3, L4). Every column matches, including order_line_id,
quantity, price and timestamps. This trips both duplicate detectors (order_id+line_no and
order_line_id). In the unpatched run, `load` failed with
`UNIQUE constraint failed: sales_orders__new.order_line_id`.

## Classification: FIX
These are exact full-row copies: the source sent the same lines twice, which is a delivery/plumbing
defect. They are not new or conflicting order lines, so removing the copies does not change what
the data means. Order 0049 still has its 4 lines, each exactly once.

## Change
`dags/crm_sales_orders/pipeline.py`, `extract()`: each file is passed through
`drop_duplicates()` across all source columns before it is staged. The number of copies dropped per file
is logged and saved in extract_stats (`exact_duplicates_dropped`). Row counts used by validation
are taken after this step.
Safety: rows that share a key but differ in any value are NOT dropped. They still fail the PK
constraint and the `pk_unique_order_line_id` / `unique_order_id_line_no` checks, so a real
key conflict will still escalate. No validation check was changed. The manifest is unchanged
because the contract is the same.

## Self-test
First attempt: all steps and all 15 validation checks pass, with 127 rows loaded.

## Proposed change

```diff
--- live/dags/crm_sales_orders/pipeline.py
+++ staged/dags/crm_sales_orders/pipeline.py
@@ -86,18 +86,27 @@
     files = source_files()
     if not files:
         raise FileNotFoundError(f"No files match {FILE_PATTERN} under {data_root()}")
-    frames, per_file = [], {}
+    frames, per_file, dup_dropped = [], {}, {}
     for f in files:
         df = pd.read_csv(f, dtype=str, keep_default_na=False, na_values=[""])
         missing = set(EXPECTED_SCHEMA) - set(df.columns)
         if missing:
             raise ValueError(f"{os.path.basename(f)} missing columns: {sorted(missing)}")
+        # Drop EXACT full-row duplicates (source re-emitted identical lines).
+        # Rows sharing a key but differing in any value are kept, so the
+        # PK / uniqueness checks still catch genuine key conflicts.
+        before = len(df)
+        df = df.drop_duplicates(keep="first").reset_index(drop=True)
+        if len(df) != before:
+            dup_dropped[os.path.basename(f)] = before - len(df)
+            print(f"[extract] {os.path.basename(f)}: dropped {before - len(df)} exact duplicate rows")
         df["source_file"] = os.path.basename(f)
         per_file[os.path.basename(f)] = len(df)
         frames.append(df)
     raw = pd.concat(frames, ignore_index=True)
     raw.to_pickle(_stage_path("extracted"))
-    stats = {"files": len(files), "rows": len(raw), "per_file": per_file}
+    stats = {"files": len(files), "rows": len(raw), "per_file": per_file,
+             "exact_duplicates_dropped": dup_dropped}
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

Approve:  `python -m healing apply p-baad7757-20261128011215`
Decline:  `python -m healing reject p-baad7757-20261128011215 --reason "..."`
