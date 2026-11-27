# Diagnosis: inc_394117168708ab9e (delivery_performance, 2026-11-09)

**Root cause:** The 3PL file `3pl/shipment_events_2026-11-09T11.json` used the camelCase key `originWarehouseId` instead of `origin_warehouse_id`. This is pure schema drift and the data's meaning is unchanged.

**Classification: FIX.** It is a rename that the extract step can absorb safely.

**State of the code:** The staged `pipeline.py` already contains `"originWarehouseId": "origin_warehouse_id"` in `EVENT_KEY_ALIASES`. `extract_shipments` applies it before the `EVENT_SCHEMA` check. The staging copy came from the live working tree, which already differs from git HEAD. I made no code change.

**Self-test (1 attempt):**
- All extract, transform and load steps pass.
- `validate:shipment_fields_complete` now passes. It failed in the control run.
- `event_orders_exist_in_crm` and `all_events_accounted` still fail. They fail identically in the control run, so they are caused by the partial data slice (orphan events whose CRM orders are outside data_root). They are scope artifacts, not caused by this incident, and I did not touch them.

No validation checks were weakened. The manifest is unchanged.
