# Proposal p-ffb5e01e-20261122185337

**Verdict: READY_FOR_APPROVAL**

- Incident: `inc_ffb5e01ef96f04cb`  (warn)  `schema.renamed:hub_code->hubCode`
- Pipeline: `delivery_performance`   Source: `tpl_shipment_events_json`   Business date: 2026-10-20
- Affected files: 3pl/shipment_events_2026-10-20T11.json
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# inc_ffb5e01ef96f04cb – hub_code renamed to hubCode

**Root cause:** The 3PL file shipment_events_2026-10-20T11.json sends `hubCode` instead of `hub_code`. extract_shipments failed on "missing fields: ['hub_code']". The values are unchanged (e.g. HUB-SIA), so this is pure schema drift.

**Classification:** FIX. It is a key rename with no change in meaning.

**Change:** Added `"hubCode": "hub_code"` to `EVENT_KEY_ALIASES` in dags/delivery_performance/pipeline.py. The existing alias mapping runs before the schema check, so both forms are accepted. No validation check was changed.

**Self-test:** All steps pass. Remaining validate failures (all_events_accounted, event_orders_exist_in_crm) are the partial-data-slice `scope_artifacts` seen in the control run. shipment_fields_complete, which failed in the control run, now passes. The freshness.late_file incident is a separate late-arrival signal and is not addressed here.

## Proposed change

```diff
--- live/dags/delivery_performance/pipeline.py
+++ staged/dags/delivery_performance/pipeline.py
@@ -47,7 +47,8 @@
     "promised_delivery_at": "datetime", "pieces": "int64", "weight_kg": "float64",
     "notes": "string", "is_late": "int64",
 }
-EVENT_KEY_ALIASES = {"vehicleId": "vehicle_id", "driverID": "driver_id"}
+EVENT_KEY_ALIASES = {"vehicleId": "vehicle_id", "driverID": "driver_id",
+                     "hubCode": "hub_code"}
 EVENT_STATUSES ={"picked_up", "in_transit", "delivered", "returned"}
 LOCAL_TZ = "+05:00"  # source feeds are Pakistan time; outputs normalised to it
```

## Validation (deterministic, same case data, before vs after)

| check | before | after | note |
|---|---|---|---|
| detect:tpl_shipment_events_json:freshness.late_file | fail | fail | source file still dirty (expected: code-only fix) |
| detect:tpl_shipment_events_json:schema.renamed:hub_code->hubCode | fail | fail | source file still dirty (expected: code-only fix) |
| step:extract_shipments | fail | pass | TARGET |
| step:validate | skipped | fail | scope artifact (also fails without the incident's files) |
| validate:all_events_accounted | - | fail | scope artifact (also fails without the incident's files) |
| validate:event_orders_exist_in_crm | - | fail | scope artifact (also fails without the incident's files) |
| _20 other checks_ | | pass | _unchanged or newly passing; full list in validation.json_ |

Targets: step:extract_shipments  |  regressions: none  |  unmasked failures: none

_Scope note: the case runs on one business date's files with other incidents' files removed. Checks that also fail on a control run without this incident's files are marked 'scope artifact' and do not block approval._

## Decision

Approve:  `python -m healing apply p-ffb5e01e-20261122185337`
Decline:  `python -m healing reject p-ffb5e01e-20261122185337 --reason "..."`
