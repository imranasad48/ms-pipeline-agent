# Diagnosis: vehicle_id -> vehicleId (inc_24e0b27bf04ee5c7)
**Root cause:** 3pl/shipment_events_2026-08-14T11.json emits `vehicleId` instead of `vehicle_id`; extract_shipments' schema check raised "missing fields: ['vehicle_id']".
**Classification:** FIX - pure key rename (schema drift), meaning unchanged.
**Change:** in dags/delivery_performance/pipeline.py extract_shipments, map `vehicleId` back to `vehicle_id` per event before the existing schema check (old form still works; check not weakened).
**Self-test:** all steps pass; only `step:validate` / `validate:shipment_fields_complete` fail, identical to the control's scope artifacts (partial data slice, not this incident). Other same-day incidents (missing/truncated file, hub_code, weight_kg nulls) are separate and untouched.
