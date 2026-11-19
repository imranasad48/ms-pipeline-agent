# Proposal p-24e0b27b-20261119194444

**Verdict: READY_FOR_APPROVAL**

- Incident: `inc_24e0b27bf04ee5c7`  (warn)  `schema.renamed:vehicle_id->vehicleId`
- Pipeline: `delivery_performance`   Source: `tpl_shipment_events_json`   Business date: 2026-08-14
- Affected files: 3pl/shipment_events_2026-08-14T11.json
- Agent decision: FIX  (attempts: 1)


## Diagnosis

# Diagnosis: vehicle_id -> vehicleId (delivery_performance, 2026-08-14)
**Root cause:** the 3PL file shipment_events_2026-08-14T11.json renamed `vehicle_id` to `vehicleId`. Same data, different key. extract_shipments failed with "missing fields: ['vehicle_id']".

**Classification:** FIX. This is schema drift (a rename) and the meaning of the data is unchanged.

**Change:** in dags/delivery_performance/pipeline.py I added `EVENT_KEY_ALIASES = {"vehicleId": "vehicle_id"}`. extract_shipments maps the alias back to the canonical name before the existing required-field check. Files with the old name still work, and the check itself is unchanged.

**Self-test:** all extract, transform and load steps pass. The remaining failures are `step:validate` and `validate:shipment_fields_complete`. The baseline control (run with this incident's files removed) shows the same two failures, so they come from the partial data slice. Related same-day incidents (a missing hub_code in the T20 file, a truncated file, an empty file, a weight_kg null spike) are separate and were not touched.

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
+            # schema drift: accept renamed keys (e.g. vehicleId) and map to canonical names
+            for old, new in EVENT_KEY_ALIASES.items():
+                if old in e and new not in e:
+                    e[new] = e.pop(old)
             missing = set(EVENT_SCHEMA) - set(e)
             if missing:
                 raise ValueError(f"{os.path.basename(f)} event {e.get('event_id')} "
```

## Validation (deterministic, same case data, before vs after)

| check | before | after | note |
|---|---|---|---|
| detect:tpl_shipment_events_json:freshness.missing_file | fail | fail | source file still dirty (expected: code-only fix) |
| detect:tpl_shipment_events_json:freshness.truncated_file | fail | fail | source file still dirty (expected: code-only fix) |
| detect:tpl_shipment_events_json:nulls.spike:weight_kg | fail | fail | source file still dirty (expected: code-only fix) |
| detect:tpl_shipment_events_json:schema.missing_column:hub_code | fail | fail | source file still dirty (expected: code-only fix) |
| detect:tpl_shipment_events_json:schema.renamed:vehicle_id->vehicleId | fail | fail | source file still dirty (expected: code-only fix) |
| step:extract_shipments | fail | pass | TARGET |
| step:validate | skipped | fail | scope artifact (also fails without the incident's files) |
| validate:shipment_fields_complete | - | fail | scope artifact (also fails without the incident's files) |
| _21 other checks_ | | pass | _unchanged or newly passing; full list in validation.json_ |

Targets: step:extract_shipments  |  regressions: none  |  unmasked failures: none

_Scope note: the case runs on one business date's files with other incidents' files removed. Checks that also fail on a control run without this incident's files are marked 'scope artifact' and do not block approval._

## Decision

Approve:  `python -m healing apply p-24e0b27b-20261119194444`
Decline:  `python -m healing reject p-24e0b27b-20261119194444 --reason "..."`
