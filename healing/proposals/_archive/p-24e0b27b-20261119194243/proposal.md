# Proposal p-24e0b27b-20261119194243

**Verdict: FAILED_VALIDATION**

- Incident: `inc_24e0b27bf04ee5c7`  (warn)  `schema.renamed:vehicle_id->vehicleId`
- Pipeline: `delivery_performance`   Source: `tpl_shipment_events_json`   Business date: 2026-08-14
- Affected files: 3pl/shipment_events_2026-08-14T11.json
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Diagnosis: inc_24e0b27bf04ee5c7 (schema.renamed vehicle_id -> vehicleId)

**Root cause:** The 3PL file `shipment_events_2026-08-14T11.json` sends the key `vehicleId` instead of `vehicle_id`. `extract_shipments` requires every canonical field, so it raised `missing fields: ['vehicle_id']` and the DAG stopped. The values (VEH-001 etc.) are unchanged, so this is a pure rename.

**Classification: FIX.** It is schema drift with the same meaning.

**Change** (`dags/delivery_performance/pipeline.py`): added `EVENT_KEY_ALIASES = {"vehicleId": "vehicle_id"}`. Before the existing required-field check, `extract_shipments` now maps an aliased key back to the canonical name when the canonical key is absent. Old-format files behave as before. No validation was weakened, and the manifest is unchanged.

**Self-test (attempt 1):** all steps (extract_orders, extract_shipments, transform, load) pass, and 17 of 18 validate checks pass. `shipment_fields_complete` fails: 11 orders have no `pickup_at`.
- Cause: the case data_root deliberately quarantines the hourly files for 06, 07 and 20. Those files belong to other incidents (a truncated file, a null spike and a missing `hub_code`). The `picked_up` events for those orders are probably in the missing files.
- I could not confirm this, because the quarantined files are not in the case data.
- This is not caused by the rename patch. It is a data-completeness effect of the incomplete case data, and it is not fixable here without weakening a check.
- Expect it to clear on the full day's data once the other incidents are handled. A reviewer should re-check with the full data.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -47,7 +47,8 @@
     "promised_delivery_at": "datetime", "pieces": "int64", "weight_kg": "float64",
     "notes": "string", "is_late": "int64",
 }
-EVENT_STATUSES = {"picked_up", "in_transit", "delivered", "returned"}
+EVENT_KEY_ALIASES = {"vehicleId": "vehicle_id"}
+EVENT_STATUSES ={"picked_up", "in_transit", "delivered", "returned"}
 LOCAL_TZ = "+05:00"  # source feeds are Pakistan time; outputs normalised to it
 
 # Output table: column -> SQLite type ; NOT NULL columns listed separately.
@@ -159,6 +160,10 @@
         n_files += 1
         n_empty += not events
         for e in events:
+            # schema drift: accept renamed keys, map back to canonical names
+            for alias, canon in EVENT_KEY_ALIASES.items():
+                if canon not in e and alias in e:
+                    e[canon] = e.pop(alias)
             missing = set(EVENT_SCHEMA) - set(e)
             if missing:
                 raise ValueError(f"{os.path.basename(f)} event {e.get('event_id')} "
```

## Validation (deterministic, same case data, before vs after)

| check | before | after | note |
|---|---|---|---|
| detect:tpl_shipment_events_json:freshness.missing_file | fail | fail |  |
| detect:tpl_shipment_events_json:freshness.truncated_file | fail | fail |  |
| detect:tpl_shipment_events_json:nulls.spike:weight_kg | fail | fail |  |
| detect:tpl_shipment_events_json:schema.missing_column:hub_code | fail | fail |  |
| detect:tpl_shipment_events_json:schema.renamed:vehicle_id->vehicleId | fail | fail |  |
| policy:manifest_versioned | pass | pass |  |
| step:extract_orders | pass | pass |  |
| step:extract_shipments | fail | pass | TARGET |
| step:load | skipped | pass |  |
| step:transform | skipped | pass |  |
| step:validate | skipped | fail | UNMASKED FAILURE |
| validate | skipped | - |  |
| validate:accepted_values_is_late | - | pass |  |
| validate:accepted_values_latest_status | - | pass |  |
| validate:all_events_accounted | - | pass |  |
| validate:customer_id_consistent_across_sources | - | pass |  |
| validate:delivered_at_iff_delivered | - | pass |  |
| validate:event_id_unique_in_source | - | pass |  |
| validate:event_orders_exist_in_crm | - | pass |  |
| validate:is_late_consistent | - | pass |  |
| validate:line_count_reconciles | - | pass |  |
| validate:no_shipment_means_no_events | - | pass |  |
| validate:not_null_required | - | pass |  |
| validate:order_total_reconciles | - | pass |  |
| validate:pickup_before_delivery | - | pass |  |
| validate:pk_unique_order_id | - | pass |  |
| validate:promised_date_matches_requested | - | pass |  |
| validate:row_count_equals_crm_orders | - | pass |  |
| validate:schema_columns_present | - | pass |  |
| validate:shipment_fields_complete | - | fail |  |

Targets: step:extract_shipments  |  regressions: none  |  unmasked failures: step:validate

## Decision

Do NOT apply. Validation failed; see table. Archive with: `python -m healing reject p-24e0b27b-20261119194243 --reason "..."`
