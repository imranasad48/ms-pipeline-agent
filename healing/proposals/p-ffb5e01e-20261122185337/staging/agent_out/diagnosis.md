# inc_ffb5e01ef96f04cb – hub_code renamed to hubCode

**Root cause:** The 3PL file shipment_events_2026-10-20T11.json sends `hubCode` instead of `hub_code`. extract_shipments failed on "missing fields: ['hub_code']". The values are unchanged (e.g. HUB-SIA), so this is pure schema drift.

**Classification:** FIX. It is a key rename with no change in meaning.

**Change:** Added `"hubCode": "hub_code"` to `EVENT_KEY_ALIASES` in dags/delivery_performance/pipeline.py. The existing alias mapping runs before the schema check, so both forms are accepted. No validation check was changed.

**Self-test:** All steps pass. Remaining validate failures (all_events_accounted, event_orders_exist_in_crm) are the partial-data-slice `scope_artifacts` seen in the control run. shipment_fields_complete, which failed in the control run, now passes. The freshness.late_file incident is a separate late-arrival signal and is not addressed here.
