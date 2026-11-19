# Diagnosis: vehicle_id -> vehicleId (delivery_performance, 2026-08-14)
**Root cause:** the 3PL file shipment_events_2026-08-14T11.json renamed `vehicle_id` to `vehicleId`. Same data, different key. extract_shipments failed with "missing fields: ['vehicle_id']".

**Classification:** FIX. This is schema drift (a rename) and the meaning of the data is unchanged.

**Change:** in dags/delivery_performance/pipeline.py I added `EVENT_KEY_ALIASES = {"vehicleId": "vehicle_id"}`. extract_shipments maps the alias back to the canonical name before the existing required-field check. Files with the old name still work, and the check itself is unchanged.

**Self-test:** all extract, transform and load steps pass. The remaining failures are `step:validate` and `validate:shipment_fields_complete`. The baseline control (run with this incident's files removed) shows the same two failures, so they come from the partial data slice. Related same-day incidents (a missing hub_code in the T20 file, a truncated file, an empty file, a weight_kg null spike) are separate and were not touched.
