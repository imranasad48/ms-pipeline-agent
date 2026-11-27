# Diagnosis: delivery_performance, eventId rename
**Root cause:** the 3PL file `shipment_events_2026-11-09T11.json` uses the key `eventId` instead of `event_id`. This is schema drift (a rename), and the meaning of the data is unchanged. `extract_shipments` failed on the missing-field check.

**Classification:** FIX. It is a pure key rename with no change to the values.

**Change:** added `"eventId": "event_id"` to `EVENT_KEY_ALIASES` in `dags/delivery_performance/pipeline.py`. The existing alias mapping now normalises the key before the schema check. Files that already use `event_id` behave as before, and no validation check was weakened. The same-day `originWarehouseId` rename was already covered by an existing alias. The late-file incident is a freshness matter and is not touched.

**Self-test:** all steps pass. The only failing validations are `all_events_accounted` and `event_orders_exist_in_crm`. Both also fail in the baseline `control` and are listed in `scope_artifacts` (partial data slice), so I left them alone.
