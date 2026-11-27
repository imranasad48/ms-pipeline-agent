# Incident inc_394117168708ab9e – originWarehouseId rename

**Root cause:** In `3pl/shipment_events_2026-11-09T11.json` the 3PL feed used `originWarehouseId` instead of `origin_warehouse_id` (camelCase schema drift). Meaning is unchanged (WH-03 style warehouse codes).

**Classification: FIX (plumbing / schema drift).** No code change was needed: `EVENT_KEY_ALIASES` in pipeline.py already maps `originWarehouseId -> origin_warehouse_id` before the schema check. The manifest contract (canonical `origin_warehouse_id`) is also still correct.

**Evidence:** The unpatched baseline and my self-test run both pass extract_orders, extract_shipments, transform and load with the renamed file, with no missing-field error. The only failing checks (`event_orders_exist_in_crm`, `all_events_accounted`) also fail in the baseline `control` run (with this incident's file removed) and are listed in `scope_artifacts`. They are caused by the partial data slice (events for orders absent from the CRM slice), not by this incident. `shipment_fields_complete` fails only in the control run and passes with the incident file.

**Changed:** nothing. The detector flags the raw-file drift; the pipeline already absorbs it. No validation was weakened.
