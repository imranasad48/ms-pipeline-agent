# Proposal p-bc971565-20261128022652

**Verdict: READY_FOR_APPROVAL**

- Incident: `inc_bc9715657fa040ae`  (critical)  `duplicates.key:order_line_id`
- Pipeline: `delivery_performance`   Source: `crm_sales_orders_csv`   Business date: 2026-11-04
- Affected files: crm/sales_orders_2026-11-04.csv
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Incident inc_bc9715657fa040ae — delivery_performance, 2026-11-04

## Root cause
`crm/sales_orders_2026-11-04.csv` contains the 3 lines of order SO-20261104-0052
(L1, L2, L3; file rows 138-140) written a second time (rows 141-143). The copies match
the originals in every field, so this is a re-emitted or double-written block in the
CRM export, not new business activity. The detector also flagged `duplicates.row`
(whole-row duplicates) and the same-day incident `order_id+line_no`, which is the
same defect.

## Impact on the unpatched pipeline
`extract_orders` staged 3 extra rows. The aggregation counts lines with
`nunique(order_line_id)`, but `line_count_reconciles` compares against the raw row
count, so it FAILED. `order_total` and the CRM line_total sum were both inflated by
the same 406,113.45 PKR. That reconciliation still passed, but it was comparing two
wrong numbers: the order total for SO-20261104-0052 was doubled.

## Classification: FIX
The duplicates are exact copies, so the meaning of the data is unchanged. Removing
them is lossless and has only one correct result.

## Change (dags/delivery_performance/pipeline.py, extract_orders)
- After concatenating the CRM files, drop exact duplicate rows. All columns are
  compared except `source_file`, and the first copy is kept. The number dropped is
  logged and recorded as `duplicate_rows_dropped` in orders_stats.
- Guard: if an `order_line_id` still repeats after that, its copies differ in content.
  That case is ambiguous, so the step raises an error instead of choosing a copy.
- Existing behaviour is unchanged for clean files. No validation checks were touched,
  and the manifest was not changed (the contract is the same).

## Self-test (attempt 1)
All steps passed. `line_count_reconciles` now passes, and order_total is correct.
Still failing: `event_orders_exist_in_crm` and `all_events_accounted`. These are the
listed scope artifacts and fail identically in the control run, because 3PL events
reference orders from dates outside the case data slice. They are not caused by
this incident or this patch.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -141,8 +141,22 @@
         per_file[os.path.basename(f)] = len(df)
         frames.append(df)
     raw = pd.concat(frames, ignore_index=True)
+    # duplicates: the CRM export can re-emit identical line rows (same order_line_id,
+    # all fields equal). Drop exact copies (ignoring source_file); a line id that still
+    # repeats with DIFFERENT content is ambiguous and must fail loudly.
+    n_before = len(raw)
+    raw = raw.drop_duplicates(subset=[c for c in raw.columns if c != "source_file"],
+                              keep="first").reset_index(drop=True)
+    dropped = n_before - len(raw)
+    conflicting = raw.loc[raw["order_line_id"].duplicated(keep=False), "order_line_id"]
+    if not conflicting.empty:
+        raise ValueError(f"order_line_id repeated with differing content: "
+                         f"{sorted(conflicting.unique())[:10]}")
+    if dropped:
+        print(f"[extract_orders] dropped {dropped} exact duplicate CRM line row(s)")
     raw.to_pickle(_stage("orders_raw"))
-    stats = {"files": len(per_file), "rows": len(raw), "orders": raw["order_id"].nunique()}
+    stats = {"files": len(per_file), "rows": len(raw), "orders": raw["order_id"].nunique(),
+             "duplicate_rows_dropped": dropped}
     pd.to_pickle(stats, _stage("orders_stats"))
     print(f"[extract_orders] {stats}")
     return stats
```

## Validation (deterministic, same case data, before vs after)

| check | before | after | note |
|---|---|---|---|
| detect:crm_sales_orders_csv:duplicates.row | fail | fail | source file still dirty (expected: code-only fix) |
| step:validate | fail | fail | scope artifact (also fails without the incident's files) |
| validate:all_events_accounted | fail | fail | scope artifact (also fails without the incident's files) |
| validate:event_orders_exist_in_crm | fail | fail | scope artifact (also fails without the incident's files) |
| validate:line_count_reconciles | fail | pass | TARGET |
| _20 other checks_ | | pass | _unchanged or newly passing; full list in validation.json_ |

Targets: validate:line_count_reconciles  |  regressions: none  |  unmasked failures: none

_Scope note: the case runs on one business date's files with other incidents' files removed. Checks that also fail on a control run without this incident's files are marked 'scope artifact' and do not block approval._

## Decision

Approve:  `python -m healing apply p-bc971565-20261128022652`
Decline:  `python -m healing reject p-bc971565-20261128022652 --reason "..."`
