# Proposal p-39411716-20261122195904

**Verdict: FAILED_VALIDATION**

- Incident: `inc_394117168708ab9e`  (warn)  `schema.renamed:origin_warehouse_id->originWarehouseId`
- Pipeline: `delivery_performance`   Source: `tpl_shipment_events_json`   Business date: 2026-11-09
- Affected files: 3pl/shipment_events_2026-11-09T11.json
- Agent decision: FIX  (attempts: 1)

> Agent said FIX but staged files are identical to live.

## Diagnosis

# Diagnosis: inc_394117168708ab9e (delivery_performance, 2026-11-09)

**Root cause:** The 3PL file `3pl/shipment_events_2026-11-09T11.json` used the camelCase key `originWarehouseId` instead of `origin_warehouse_id`. This is pure schema drift and the data's meaning is unchanged.

**Classification: FIX.** It is a rename that the extract step can absorb safely.

**State of the code:** The staged `pipeline.py` already contains `"originWarehouseId": "origin_warehouse_id"` in `EVENT_KEY_ALIASES`. `extract_shipments` applies it before the `EVENT_SCHEMA` check. The staging copy came from the live working tree, which already differs from git HEAD. I made no code change.

**Self-test (1 attempt):**
- All extract, transform and load steps pass.
- `validate:shipment_fields_complete` now passes. It failed in the control run.
- `event_orders_exist_in_crm` and `all_events_accounted` still fail. They fail identically in the control run, so they are caused by the partial data slice (orphan events whose CRM orders are outside data_root). They are scope artifacts, not caused by this incident, and I did not touch them.

No validation checks were weakened. The manifest is unchanged.

## Proposed change

```diff
(no changes)
```

## Validation (deterministic, same case data, before vs after)

| check | before | after | note |
|---|---|---|---|
| detect:tpl_shipment_events_json:schema.renamed:origin_warehouse_id->originWarehouseId | fail | fail | source file still dirty (expected: code-only fix) |
| step:validate | fail | fail | scope artifact (also fails without the incident's files) |
| validate:all_events_accounted | fail | fail | scope artifact (also fails without the incident's files) |
| validate:event_orders_exist_in_crm | fail | fail | scope artifact (also fails without the incident's files) |
| _21 other checks_ | | pass | _unchanged or newly passing; full list in validation.json_ |

Targets: step:validate, validate:all_events_accounted, validate:event_orders_exist_in_crm  |  regressions: none  |  unmasked failures: none

_Scope note: the case runs on one business date's files with other incidents' files removed. Checks that also fail on a control run without this incident's files are marked 'scope artifact' and do not block approval._

## Decision

Do NOT apply. Validation failed; see table. Archive with: `python -m healing reject p-39411716-20261122195904 --reason "..."`
