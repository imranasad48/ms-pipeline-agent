# Proposal p-39411716-20261122200317

**Verdict: FAILED_VALIDATION**

- Incident: `inc_394117168708ab9e`  (warn)  `schema.renamed:origin_warehouse_id->originWarehouseId`
- Pipeline: `delivery_performance`   Source: `tpl_shipment_events_json`   Business date: 2026-11-09
- Affected files: 3pl/shipment_events_2026-11-09T11.json
- Agent decision: FIX  (attempts: 1)

> Agent said FIX but staged files are identical to live.

## Diagnosis

# inc_394117168708ab9e – originWarehouseId rename

**Root cause:** In `3pl/shipment_events_2026-11-09T11.json` the key `origin_warehouse_id` arrives as camelCase `originWarehouseId` (9 events). Pure schema drift; values (e.g. `WH-03`) are unchanged.

**Classification: FIX (plumbing).** Meaning of the data is unchanged.

**Finding:** The staged `pipeline.py` already contains `"originWarehouseId": "origin_warehouse_id"` in `EVENT_KEY_ALIASES`, which maps the key back before the schema check. The unpatched baseline already passed `extract_shipments`/`transform`/`load`, so no code change was needed. The detector flags the raw-file drift; the pipeline absorbs it.

**Self-test (1 attempt):** all steps pass except validate, where `event_orders_exist_in_crm` and `all_events_accounted` fail. These fail identically in the `control` run (incident files removed) and are partial-data-slice `scope_artifacts`, unrelated to this incident. I did not touch validation checks.

**Changes:** none. Recommendation: approve as no-op; optionally acknowledge the incident as already handled by the existing alias.

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

Do NOT apply. Validation failed; see table. Archive with: `python -m healing reject p-39411716-20261122200317 --reason "..."`
