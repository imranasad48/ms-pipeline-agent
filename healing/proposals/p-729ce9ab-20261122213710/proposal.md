# Proposal p-729ce9ab-20261122213710

**Verdict: READY_FOR_APPROVAL**

- Incident: `inc_729ce9ab3496e060`  (critical)  `schema.renamed:sales_order_id->salesOrderId`
- Pipeline: `delivery_performance`   Source: `tpl_shipment_events_json`   Business date: 2026-11-06
- Affected files: 3pl/shipment_events_2026-11-06T11.json
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# inc_729ce9ab3496e060 – delivery_performance, 2026-11-06

**Root cause:** the 3PL file `shipment_events_2026-11-06T11.json` sends `salesOrderId` instead of `sales_order_id` on all 15 events (camelCase rename). `extract_shipments` therefore failed its field check ("missing fields: ['sales_order_id']") and the downstream steps were skipped.

**Classification: FIX.** Pure schema drift (rename); values and meaning are unchanged. All other keys in the file are canonical.

**Change:** in `dags/delivery_performance/pipeline.py`, added `"salesOrderId": "sales_order_id"` to `EVENT_KEY_ALIASES`, which the existing code already uses to map renamed keys back before the schema check. Old and new forms are both accepted; no validation was weakened; manifest unchanged.

**Self-test (1 attempt):** all steps pass. `validate:shipment_fields_complete` now passes. Remaining failures (`all_events_accounted`, `event_orders_exist_in_crm`) are the same partial-data-slice artifacts listed in the baseline control, not caused by this incident.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -50,7 +50,7 @@
 EVENT_KEY_ALIASES = {"vehicleId": "vehicle_id", "driverID": "driver_id",
                      "hubCode": "hub_code", "trackingId": "tracking_id",
                      "originWarehouseId": "origin_warehouse_id",
-                     "eventId": "event_id"}
+                     "eventId": "event_id", "salesOrderId": "sales_order_id"}
 EVENT_STATUSES ={"picked_up", "in_transit", "delivered", "returned"}
 LOCAL_TZ = "+05:00"  # source feeds are Pakistan time; outputs normalised to it
```

## Validation (deterministic, same case data, before vs after)

| check | before | after | note |
|---|---|---|---|
| detect:tpl_shipment_events_json:schema.renamed:sales_order_id->salesOrderId | fail | fail | source file still dirty (expected: code-only fix) |
| step:extract_shipments | fail | pass | TARGET |
| step:validate | skipped | fail | scope artifact (also fails without the incident's files) |
| validate:all_events_accounted | - | fail | scope artifact (also fails without the incident's files) |
| validate:event_orders_exist_in_crm | - | fail | scope artifact (also fails without the incident's files) |
| _20 other checks_ | | pass | _unchanged or newly passing; full list in validation.json_ |

Targets: step:extract_shipments  |  regressions: none  |  unmasked failures: none

_Scope note: the case runs on one business date's files with other incidents' files removed. Checks that also fail on a control run without this incident's files are marked 'scope artifact' and do not block approval._

## Decision

Approve:  `python -m healing apply p-729ce9ab-20261122213710`
Decline:  `python -m healing reject p-729ce9ab-20261122213710 --reason "..."`
