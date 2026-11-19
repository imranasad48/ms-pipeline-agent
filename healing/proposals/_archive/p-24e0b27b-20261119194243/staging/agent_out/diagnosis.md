# Diagnosis: inc_24e0b27bf04ee5c7 (schema.renamed vehicle_id -> vehicleId)

**Root cause:** The 3PL file `shipment_events_2026-08-14T11.json` sends the key `vehicleId` instead of `vehicle_id`. `extract_shipments` requires every canonical field, so it raised `missing fields: ['vehicle_id']` and the DAG stopped. The values (VEH-001 etc.) are unchanged, so this is a pure rename.

**Classification: FIX.** It is schema drift with the same meaning.

**Change** (`dags/delivery_performance/pipeline.py`): added `EVENT_KEY_ALIASES = {"vehicleId": "vehicle_id"}`. Before the existing required-field check, `extract_shipments` now maps an aliased key back to the canonical name when the canonical key is absent. Old-format files behave as before. No validation was weakened, and the manifest is unchanged.

**Self-test (attempt 1):** all steps (extract_orders, extract_shipments, transform, load) pass, and 17 of 18 validate checks pass. `shipment_fields_complete` fails: 11 orders have no `pickup_at`.
- Cause: the case data_root deliberately quarantines the hourly files for 06, 07 and 20. Those files belong to other incidents (a truncated file, a null spike and a missing `hub_code`). The `picked_up` events for those orders are probably in the missing files.
- I could not confirm this, because the quarantined files are not in the case data.
- This is not caused by the rename patch. It is a data-completeness effect of the incomplete case data, and it is not fixable here without weakening a check.
- Expect it to clear on the full day's data once the other incidents are handled. A reviewer should re-check with the full data.
