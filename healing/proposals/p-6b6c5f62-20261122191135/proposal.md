# Proposal p-6b6c5f62-20261122191135

**Verdict: READY_FOR_APPROVAL**

- Incident: `inc_6b6c5f62dcdb1b99`  (critical)  `schema.renamed:tracking_id->trackingId`
- Pipeline: `delivery_performance`   Source: `tpl_shipment_events_json`   Business date: 2026-10-27
- Affected files: 3pl/shipment_events_2026-10-27T11.json
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Diagnosis inc_6b6c5f62dcdb1b99
**Root cause:** In 3pl/shipment_events_2026-10-27T11.json the event key `tracking_id` was renamed `trackingId`; extract_shipments failed on the missing field.
**Classification:** FIX. Pure schema rename, same meaning; the pipeline already has an alias map for similar drift (vehicleId etc.).
**Change:** added `"trackingId": "tracking_id"` to `EVENT_KEY_ALIASES` in dags/delivery_performance/pipeline.py. The old form is still accepted and the schema check is untouched.
**Self-test (1 attempt):** all extract/transform/load steps pass. Remaining validate failures (all_events_accounted, event_orders_exist_in_crm) match the control's scope_artifacts from the partial data slice, not this incident. shipment_fields_complete now passes.
The freshness.late_file incident on the same file is a separate arrival-time matter and is not addressed here.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -48,7 +48,7 @@
     "notes": "string", "is_late": "int64",
 }
 EVENT_KEY_ALIASES = {"vehicleId": "vehicle_id", "driverID": "driver_id",
-                     "hubCode": "hub_code"}
+                     "hubCode": "hub_code", "trackingId": "tracking_id"}
 EVENT_STATUSES ={"picked_up", "in_transit", "delivered", "returned"}
 LOCAL_TZ = "+05:00"  # source feeds are Pakistan time; outputs normalised to it
```

## Validation (deterministic, same case data, before vs after)

| check | before | after | note |
|---|---|---|---|
| detect:tpl_shipment_events_json:freshness.late_file | fail | fail | source file still dirty (expected: code-only fix) |
| detect:tpl_shipment_events_json:schema.renamed:tracking_id->trackingId | fail | fail | source file still dirty (expected: code-only fix) |
| step:extract_shipments | fail | pass | TARGET |
| step:validate | skipped | fail | scope artifact (also fails without the incident's files) |
| validate:all_events_accounted | - | fail | scope artifact (also fails without the incident's files) |
| validate:event_orders_exist_in_crm | - | fail | scope artifact (also fails without the incident's files) |
| _20 other checks_ | | pass | _unchanged or newly passing; full list in validation.json_ |

Targets: step:extract_shipments  |  regressions: none  |  unmasked failures: none

_Scope note: the case runs on one business date's files with other incidents' files removed. Checks that also fail on a control run without this incident's files are marked 'scope artifact' and do not block approval._

## Decision

Approve:  `python -m healing apply p-6b6c5f62-20261122191135`
Decline:  `python -m healing reject p-6b6c5f62-20261122191135 --reason "..."`
