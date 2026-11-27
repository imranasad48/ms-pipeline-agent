# Proposal p-6266f96f-20261122194415

**Verdict: READY_FOR_APPROVAL**

- Incident: `inc_6266f96feba1688b`  (warn)  `schema.renamed:origin_warehouse_id->originWarehouseId`
- Pipeline: `delivery_performance`   Source: `tpl_shipment_events_json`   Business date: 2026-11-02
- Affected files: 3pl/shipment_events_2026-11-02T11.json
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Diagnosis: inc_6266f96feba1688b
**Root cause:** In 3pl/shipment_events_2026-11-02T11.json the 3PL renamed `origin_warehouse_id` to `originWarehouseId`. extract_shipments failed on the missing field. The meaning of the data is unchanged.
**Classification:** FIX (schema drift, a rename).
**Change:** added `"originWarehouseId": "origin_warehouse_id"` to `EVENT_KEY_ALIASES` in dags/delivery_performance/pipeline.py. The existing alias mapping runs before the schema check, so both the old and new names are accepted. No validation was weakened and the manifest is unchanged.
**Self-test (1 patch attempt):** all steps pass. The remaining validate failures (`all_events_accounted`, `event_orders_exist_in_crm`) are scope artifacts from the partial data slice, and they also fail in the control run. `shipment_fields_complete` now passes.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -48,7 +48,8 @@
     "notes": "string", "is_late": "int64",
 }
 EVENT_KEY_ALIASES = {"vehicleId": "vehicle_id", "driverID": "driver_id",
-                     "hubCode": "hub_code", "trackingId": "tracking_id"}
+                     "hubCode": "hub_code", "trackingId": "tracking_id",
+                     "originWarehouseId": "origin_warehouse_id"}
 EVENT_STATUSES ={"picked_up", "in_transit", "delivered", "returned"}
 LOCAL_TZ = "+05:00"  # source feeds are Pakistan time; outputs normalised to it
```

## Validation (deterministic, same case data, before vs after)

| check | before | after | note |
|---|---|---|---|
| detect:tpl_shipment_events_json:schema.renamed:origin_warehouse_id->originWarehouseId | fail | fail | source file still dirty (expected: code-only fix) |
| step:extract_shipments | fail | pass | TARGET |
| step:validate | skipped | fail | scope artifact (also fails without the incident's files) |
| validate:all_events_accounted | - | fail | scope artifact (also fails without the incident's files) |
| validate:event_orders_exist_in_crm | - | fail | scope artifact (also fails without the incident's files) |
| _20 other checks_ | | pass | _unchanged or newly passing; full list in validation.json_ |

Targets: step:extract_shipments  |  regressions: none  |  unmasked failures: none

_Scope note: the case runs on one business date's files with other incidents' files removed. Checks that also fail on a control run without this incident's files are marked 'scope artifact' and do not block approval._

## Decision

Approve:  `python -m healing apply p-6266f96f-20261122194415`
Decline:  `python -m healing reject p-6266f96f-20261122194415 --reason "..."`
