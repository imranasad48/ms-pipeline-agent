# Diagnosis: delivery_performance, 2026-10-07
**Root cause:** the 3PL file `shipment_events_2026-10-07T11.json` renamed `driver_id` to `driverID`. `extract_shipments` raised "missing fields: ['driver_id']".

**Classification:** FIX. This is a pure key rename and the data's meaning is unchanged.

**Change:** `EVENT_KEY_ALIASES` in `dags/delivery_performance/pipeline.py` now also maps `driverID` to `driver_id`, next to the existing `vehicleId` mapping. The old form is still accepted and the schema check is untouched.

**Self-test:** all steps pass. `validate:all_events_accounted` and `validate:event_orders_exist_in_crm` still fail, and `step:validate` fails with them. These are the same failures as in the baseline control, caused by the partial data slice (scope artifacts), so I left them alone. `validate:shipment_fields_complete` is also in the scope_artifacts list but passes now that the file extracts.
