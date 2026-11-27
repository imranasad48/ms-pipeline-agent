# Diagnosis: inc_6266f96feba1688b
**Root cause:** In 3pl/shipment_events_2026-11-02T11.json the 3PL renamed `origin_warehouse_id` to `originWarehouseId`. extract_shipments failed on the missing field. The meaning of the data is unchanged.
**Classification:** FIX (schema drift, a rename).
**Change:** added `"originWarehouseId": "origin_warehouse_id"` to `EVENT_KEY_ALIASES` in dags/delivery_performance/pipeline.py. The existing alias mapping runs before the schema check, so both the old and new names are accepted. No validation was weakened and the manifest is unchanged.
**Self-test (1 patch attempt):** all steps pass. The remaining validate failures (`all_events_accounted`, `event_orders_exist_in_crm`) are scope artifacts from the partial data slice, and they also fail in the control run. `shipment_fields_complete` now passes.
