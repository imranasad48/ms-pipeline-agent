# inc_394117168708ab9e – originWarehouseId rename

**Root cause:** In `3pl/shipment_events_2026-11-09T11.json` the key `origin_warehouse_id` arrives as camelCase `originWarehouseId` (9 events). Pure schema drift; values (e.g. `WH-03`) are unchanged.

**Classification: FIX (plumbing).** Meaning of the data is unchanged.

**Finding:** The staged `pipeline.py` already contains `"originWarehouseId": "origin_warehouse_id"` in `EVENT_KEY_ALIASES`, which maps the key back before the schema check. The unpatched baseline already passed `extract_shipments`/`transform`/`load`, so no code change was needed. The detector flags the raw-file drift; the pipeline absorbs it.

**Self-test (1 attempt):** all steps pass except validate, where `event_orders_exist_in_crm` and `all_events_accounted` fail. These fail identically in the `control` run (incident files removed) and are partial-data-slice `scope_artifacts`, unrelated to this incident. I did not touch validation checks.

**Changes:** none. Recommendation: approve as no-op; optionally acknowledge the incident as already handled by the existing alias.
