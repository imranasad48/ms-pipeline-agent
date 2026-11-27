# inc_729ce9ab3496e060 – delivery_performance, 2026-11-06

**Root cause:** the 3PL file `shipment_events_2026-11-06T11.json` sends `salesOrderId` instead of `sales_order_id` on all 15 events (camelCase rename). `extract_shipments` therefore failed its field check ("missing fields: ['sales_order_id']") and the downstream steps were skipped.

**Classification: FIX.** Pure schema drift (rename); values and meaning are unchanged. All other keys in the file are canonical.

**Change:** in `dags/delivery_performance/pipeline.py`, added `"salesOrderId": "sales_order_id"` to `EVENT_KEY_ALIASES`, which the existing code already uses to map renamed keys back before the schema check. Old and new forms are both accepted; no validation was weakened; manifest unchanged.

**Self-test (1 attempt):** all steps pass. `validate:shipment_fields_complete` now passes. Remaining failures (`all_events_accounted`, `event_orders_exist_in_crm`) are the same partial-data-slice artifacts listed in the baseline control, not caused by this incident.
