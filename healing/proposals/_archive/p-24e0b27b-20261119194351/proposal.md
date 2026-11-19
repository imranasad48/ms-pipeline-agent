# Proposal p-24e0b27b-20261119194351

**Verdict: READY_FOR_APPROVAL**

- Incident: `inc_24e0b27bf04ee5c7`  (warn)  `schema.renamed:vehicle_id->vehicleId`
- Pipeline: `delivery_performance`   Source: `tpl_shipment_events_json`   Business date: 2026-08-14
- Affected files: 3pl/shipment_events_2026-08-14T11.json
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Diagnosis: vehicle_id -> vehicleId (inc_24e0b27bf04ee5c7)
**Root cause:** 3pl/shipment_events_2026-08-14T11.json emits `vehicleId` instead of `vehicle_id`; extract_shipments' schema check raised "missing fields: ['vehicle_id']".
**Classification:** FIX - pure key rename (schema drift), meaning unchanged.
**Change:** in dags/delivery_performance/pipeline.py extract_shipments, map `vehicleId` back to `vehicle_id` per event before the existing schema check (old form still works; check not weakened).
**Self-test:** all steps pass; only `step:validate` / `validate:shipment_fields_complete` fail, identical to the control's scope artifacts (partial data slice, not this incident). Other same-day incidents (missing/truncated file, hub_code, weight_kg nulls) are separate and untouched.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -159,6 +159,11 @@
         n_files += 1
         n_empty += not events
         for e in events:
+            # schema drift: upstream sometimes emits camelCase 'vehicleId'
+            if "vehicleId" in e:
+                if "vehicle_id" not in e:
+                    e["vehicle_id"] = e["vehicleId"]
+                del e["vehicleId"]
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
| step:validate | skipped | fail | scope artifact (also fails without the incident's files) |
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

Targets: step:extract_shipments  |  regressions: none  |  unmasked failures: none

_Scope note: the case runs on one business date's files with other incidents' files removed. Checks that also fail on a control run without this incident's files are marked 'scope artifact' and do not block approval._

## Decision

Approve:  `python -m healing apply p-24e0b27b-20261119194351`
Decline:  `python -m healing reject p-24e0b27b-20261119194351 --reason "..."`
