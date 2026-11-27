# Diagnosis inc_6b6c5f62dcdb1b99
**Root cause:** In 3pl/shipment_events_2026-10-27T11.json the event key `tracking_id` was renamed `trackingId`; extract_shipments failed on the missing field.
**Classification:** FIX. Pure schema rename, same meaning; the pipeline already has an alias map for similar drift (vehicleId etc.).
**Change:** added `"trackingId": "tracking_id"` to `EVENT_KEY_ALIASES` in dags/delivery_performance/pipeline.py. The old form is still accepted and the schema check is untouched.
**Self-test (1 attempt):** all extract/transform/load steps pass. Remaining validate failures (all_events_accounted, event_orders_exist_in_crm) match the control's scope_artifacts from the partial data slice, not this incident. shipment_fields_complete now passes.
The freshness.late_file incident on the same file is a separate arrival-time matter and is not addressed here.
