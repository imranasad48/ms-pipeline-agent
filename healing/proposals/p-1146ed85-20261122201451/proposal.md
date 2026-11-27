# Proposal p-1146ed85-20261122201451

**Verdict: READY_FOR_APPROVAL**

- Incident: `inc_1146ed8547c30cba`  (critical)  `schema.renamed:event_id->eventId`
- Pipeline: `delivery_performance`   Source: `tpl_shipment_events_json`   Business date: 2026-11-09
- Affected files: 3pl/shipment_events_2026-11-09T11.json
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Diagnosis: delivery_performance, eventId rename
**Root cause:** the 3PL file `shipment_events_2026-11-09T11.json` uses the key `eventId` instead of `event_id`. This is schema drift (a rename), and the meaning of the data is unchanged. `extract_shipments` failed on the missing-field check.

**Classification:** FIX. It is a pure key rename with no change to the values.

**Change:** added `"eventId": "event_id"` to `EVENT_KEY_ALIASES` in `dags/delivery_performance/pipeline.py`. The existing alias mapping now normalises the key before the schema check. Files that already use `event_id` behave as before, and no validation check was weakened. The same-day `originWarehouseId` rename was already covered by an existing alias. The late-file incident is a freshness matter and is not touched.

**Self-test:** all steps pass. The only failing validations are `all_events_accounted` and `event_orders_exist_in_crm`. Both also fail in the baseline `control` and are listed in `scope_artifacts` (partial data slice), so I left them alone.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -49,7 +49,8 @@
 }
 EVENT_KEY_ALIASES = {"vehicleId": "vehicle_id", "driverID": "driver_id",
                      "hubCode": "hub_code", "trackingId": "tracking_id",
-                     "originWarehouseId": "origin_warehouse_id"}
+                     "originWarehouseId": "origin_warehouse_id",
+                     "eventId": "event_id"}
 EVENT_STATUSES ={"picked_up", "in_transit", "delivered", "returned"}
 LOCAL_TZ = "+05:00"  # source feeds are Pakistan time; outputs normalised to it
```

## Validation (deterministic, same case data, before vs after)

| check | before | after | note |
|---|---|---|---|
| detect:tpl_shipment_events_json:schema.renamed:event_id->eventId | fail | fail | source file still dirty (expected: code-only fix) |
| step:extract_shipments | fail | pass | TARGET |
| step:validate | skipped | fail | scope artifact (also fails without the incident's files) |
| validate:all_events_accounted | - | fail | scope artifact (also fails without the incident's files) |
| validate:event_orders_exist_in_crm | - | fail | scope artifact (also fails without the incident's files) |
| _20 other checks_ | | pass | _unchanged or newly passing; full list in validation.json_ |

Targets: step:extract_shipments  |  regressions: none  |  unmasked failures: none

_Scope note: the case runs on one business date's files with other incidents' files removed. Checks that also fail on a control run without this incident's files are marked 'scope artifact' and do not block approval._

## Decision

Approve:  `python -m healing apply p-1146ed85-20261122201451`
Decline:  `python -m healing reject p-1146ed85-20261122201451 --reason "..."`
