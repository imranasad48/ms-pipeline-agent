# Proposal p-420b5fbb-20261122183052

**Verdict: READY_FOR_APPROVAL**

- Incident: `inc_420b5fbb4c3a5b7f`  (warn)  `schema.renamed:driver_id->driverID`
- Pipeline: `delivery_performance`   Source: `tpl_shipment_events_json`   Business date: 2026-10-07
- Affected files: 3pl/shipment_events_2026-10-07T11.json
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Diagnosis: delivery_performance, 2026-10-07
**Root cause:** the 3PL file `shipment_events_2026-10-07T11.json` renamed `driver_id` to `driverID`. `extract_shipments` raised "missing fields: ['driver_id']".

**Classification:** FIX. This is a pure key rename and the data's meaning is unchanged.

**Change:** `EVENT_KEY_ALIASES` in `dags/delivery_performance/pipeline.py` now also maps `driverID` to `driver_id`, next to the existing `vehicleId` mapping. The old form is still accepted and the schema check is untouched.

**Self-test:** all steps pass. `validate:all_events_accounted` and `validate:event_orders_exist_in_crm` still fail, and `step:validate` fails with them. These are the same failures as in the baseline control, caused by the partial data slice (scope artifacts), so I left them alone. `validate:shipment_fields_complete` is also in the scope_artifacts list but passes now that the file extracts.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -47,7 +47,7 @@
     "promised_delivery_at": "datetime", "pieces": "int64", "weight_kg": "float64",
     "notes": "string", "is_late": "int64",
 }
-EVENT_KEY_ALIASES = {"vehicleId": "vehicle_id"}
+EVENT_KEY_ALIASES = {"vehicleId": "vehicle_id", "driverID": "driver_id"}
 EVENT_STATUSES ={"picked_up", "in_transit", "delivered", "returned"}
 LOCAL_TZ = "+05:00"  # source feeds are Pakistan time; outputs normalised to it
```

## Validation (deterministic, same case data, before vs after)

| check | before | after | note |
|---|---|---|---|
| detect:tpl_shipment_events_json:schema.renamed:driver_id->driverID | fail | fail | source file still dirty (expected: code-only fix) |
| step:extract_shipments | fail | pass | TARGET |
| step:validate | skipped | fail | scope artifact (also fails without the incident's files) |
| validate:all_events_accounted | - | fail | scope artifact (also fails without the incident's files) |
| validate:event_orders_exist_in_crm | - | fail | scope artifact (also fails without the incident's files) |
| _20 other checks_ | | pass | _unchanged or newly passing; full list in validation.json_ |

Targets: step:extract_shipments  |  regressions: none  |  unmasked failures: none

_Scope note: the case runs on one business date's files with other incidents' files removed. Checks that also fail on a control run without this incident's files are marked 'scope artifact' and do not block approval._

## Decision

Approve:  `python -m healing apply p-420b5fbb-20261122183052`
Decline:  `python -m healing reject p-420b5fbb-20261122183052 --reason "..."`
