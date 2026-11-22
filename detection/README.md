# detection/ — the pipeline watcher

A plain, rule-based Python package that watches the data feeding our pipelines
and writes a row to `incidents.db` whenever something looks wrong.
**No LLM calls anywhere.** Same inputs always give the same incidents.

> Research rule: this code never opens `../data_generation/data/error_manifest.json`
> (or `.xlsx`). That file is the answer key used to *score* detection later.
> Thresholds come only from the pipeline manifests and the CLEAN dataset.

## The idea in one paragraph

Each file in `manifests/` says which sources a pipeline reads and what they should
look like (columns, types, null rates). For one **business date** we work out
which files *should* exist (the calendar), read them, and compare them with the
contract. Anything that doesn't match becomes an **incident**.

## Check families

| Family | Looks at | Incident ids |
|---|---|---|
| `schema` | header/keys/types vs `expected_schema` | `schema.missing_column:<c>`, `.new_column:<c>`, `.renamed:<old>-><new>`, `.type_change:<c>`, `.column_order`, `.corrupt_line` (JSONL), `.sheet_renamed`, `.header_row_shift` (XLSX) |
| `volume_nulls` | rows per file, null rate per column | `volume.row_count`, `nulls.spike:<c>` |
| `freshness` | calendar says a file is due | `freshness.missing_file`, `.empty_file`, `.truncated_file`, `.unreadable_file`, `.late_file` |
| `run_health` | warehouse table | `run_health.table_missing`, `.table_empty`, `.load_stale` |
| `duplicates` | repeated keys within a day's data | `duplicates.key:<col+col>`, `duplicates.row` |
| `stale_data` | file records == previous period's records | `stale.identical_to_previous` |
| `out_of_order` | timestamps vs the file's window / chronology | `ordering.out_of_window:<col>`, `ordering.non_monotonic:<col>` |
| `value_range` | numeric columns vs derived bounds | `value_range.out_of_bounds:<col>` |
| `referential` | child keys present in the parent source | `referential.orphan:<col>-><dir>.<col>`, `referential.unknown_value:<col>` |
| `iot_behavior` | per-device cadence and variance | `iot.cadence_gap`, `iot.flatline` |

Formats parsed: CSV, JSON (paged envelope), **JSONL** and **XLSX** (v2), SQLite. JSONL: each line is
parsed on its own; unparsable lines are counted (`schema.corrupt_line`, with line numbers), never
silently skipped; a cut-off last line with no trailing newline is a truncated file. XLSX: the
worksheet is the manifest's (`observed.layout` -> `sheet '...'`, else the first), the **header row is
found by content** (first row with >= 3 cells matching expected column names after case/space/punctuation
folding, so `Product Code` == `product_code`), and its position is compared with the manifest's
`header on row N`.

Details worth knowing:
- **Renames** = a missing + a new column in the same file, paired by header position or name similarity (and the values must still fit the old type). The pair is reported once as `renamed`, not as two separate incidents.
- **Type change** parses every value against the declared type. In CSV every value is text, so `"1,234"`, `"45 units"`, `"Yes"`, epoch ints fail the parse. In **native-typed formats (JSON, JSONL, XLSX, SQLite) a string in a numeric/bool column is a type change even if it would parse** (`"16.73"` in `weight_kg`). Columns the manifest types as plain `string` can get a regex in `config.yaml -> formats`.
- **Null spike**: a file trips when its null rate exceeds the manifest baseline by `abs_delta` *or* by `z` binomial standard errors (so tiny hourly files don't false-alarm), whichever is larger.
- **Row counts**: per-file `[min,max]` learned from clean data (`baselines.yaml`, made by `python -m detection baseline --data-root <clean> --write`), widened by a margin.
- **Freshness** is driven by the calendar, not by the files that arrived, so a missing file is visible. "Late" = file mtime (or the 3PL envelope `generated_at`) is more than 1h after the expected arrival time. A JSON page with **fewer** events than its `record_count` is truncated; a page with *more* is still readable (re-delivered rows were appended) so the content checks can see it. Unreadable files skip the other checks so one broken file yields one incident, not five.
- **Run health** is the simple version: table exists, has rows, and `MAX(loaded_at)` is not more than 36h before the end of the checked day. Tables without `loaded_at` (e.g. `dim_products`) only get the first two. Airflow integration comes later.

### The v2 checks (all compiled from manifests + clean baselines; thresholds in `config.yaml`)

- **duplicates** - keys are the manifest's `unique` validation checks (a `unique` applies to a source if it names it via `params.source`, or if the manifest has a single source; derived columns such as `week` are dropped). "A day's data" = all of the date's files for the source, so a row re-delivered in another hourly page counts. No declared key -> fully identical rows. Tolerance: `duplicates.max_duplicate_keys` (0) / per-source `overrides`.
- **stale_data** - hash of the parsed *records* (not bytes: a re-sent 3PL page keeps its new envelope) vs. the previous file in the cadence (previous hour / day / week). Empty files are never stale (`min_rows`).
- **out_of_order** - `time_columns` per source. Window = the page's `window_start/end`, else the calendar period of the file. Order = timestamps must not decrease in file order (`max_descents`). Ties are invisible to this check: a shuffle that leaves timestamps non-decreasing cannot be seen.
- **value_range** - bounds per numeric column = clean-observed `[min,max]` widened by `margin` (25% of |value|) intersected with the manifest's `range` params; `max_violation_rate` 0. Columns constant in clean data (`excursion_flag`, `is_late`: always 0) get no learned envelope - bounding them would flag the genuine events that set the flag. This is what catches negative/impossible quantities, sensor-fault values, a battery falling below its envelope, and silent unit changes (C->F, PKR->USD shift every value out of range).
- **referential** - the manifest's `referential_integrity` pairs (3pl.sales_order_id in crm.order_id) plus `referential.extra` (3pl.customer_id in crm.customer_id, crm.product_id in erp.product_id), at daily granularity. The parent set is the parent's *whole history* (a genuinely delayed shipment references an old order; a real orphan is in no file). The day is skipped when a parent file due in the last `required_days` is missing/unreadable or a stale re-send - absence proves nothing then (freshness/stale checks report that file). `known_values` checks a column against the closed set seen in clean data (IoT device/location ids; CRM customer ids as a stand-in for a customer master - a genuinely new customer is flagged until baselines are re-learned).
- **iot_behavior** - per device: consecutive readings further apart than `gap_multiple` x the learned interval (5 min -> 15) is `iot.cadence_gap`; devices learned as *always-on* are also checked at the start/end of the day and for being absent from the file (a dead battery / missing logger). `iot.flatline`: >= `min_run` (8) identical consecutive values (clean data never exceeds 3).

### Why `schema_type_change` missed 27 injections in v1 (and what is still invisible)

The v1 parser decided whether a value "fits" a type by *parsing* it, for every format. In JSON the
generator re-encodes numbers as text (`"16.73"`, `"4"`, `"6.00"`), and numeric-looking text parses as a
float/int, so a column that had silently turned into strings passed as healthy; only the styles that
cannot parse (`"45 units"`, `"Yes"`, epoch/alt date formats) were caught. 23 of the 27 misses were
JSON pages in the `thousands_separator` / `quoted_decimal` styles (values under 1000 never gain a comma,
so the *only* change is int/float -> string). JSON values arrive with a native type, so the contract
check is now strict for native-typed formats: a string where the manifest says number/bool is a type
change. The remaining 4 misses are CRM CSV: CSV stores everything as text, so `"156"`,
`"0.12"` and `"5887.96"` are byte-identical to the clean file and no parser can tell them apart.

A source used by two manifests (CRM is in two) is checked once and reported under each `pipeline_id`.

## Incidents

`detection/incidents.db`, table `incidents`: `incident_id, detected_at, business_date,
pipeline_id, source_id, check_id, check_family, severity, expected_json, observed_json,
affected_files_json, status`. One incident = one check on one source for one date,
listing all affected files. `incident_id` is a hash of (date, pipeline, source, check), so re-running
a day updates rows instead of duplicating them and keeps `detected_at`/`status`.
Severity comes from `config.yaml → severity`; a schema change or null spike on a
**key column** (`config.yaml → key_columns`) is `critical`.

## Commands (run from the repo root)

```bash
python -m detection run       --business-date 2026-08-14 --data-root ../data_generation/data
python -m detection calibrate --data-root ../data_generation/data_clean   # must print 0 incidents
python -m detection replay    --from 2026-08-14 --to 2026-08-20 --data-root ../data_generation/data
python -m detection baseline  --data-root ../data_generation/data_clean --write   # re-learn baselines
python -m detection incidents --date 2026-08-14
python -m pytest detection -q
```

`calibrate` replays every day found in the clean data, never writes to the DB,
exits non-zero if anything fires, and prints how close each threshold came to
tripping (utilisation 1.00 = exactly at the trip point).

## Calibration notes

- `calibrate` over the 90 clean days must end at 0 incidents with every check enabled (it does; per-check
  evaluation counts are printed). Re-run `baseline --write` first if the clean set changes.
- 3PL `notes` is optional free text, entirely empty in some hourly files -> exempted via `nulls.overrides`.
- File mtimes are naive local time (the generator stamps them in PKT); run on a machine in the same zone.
- **Per-column overrides (v2):** `value_range.overrides.iot_readings.temperature_c = [-30, 30]`. The clean
  envelope is the 1-8 C operating range, but genuine cold-chain excursions (domain events that must *not*
  alert as data errors) reach ~16 C; the bound is the physically plausible reefer range instead. All
  F-scale readings (>= 34), -273, -99, 85 and 999.9 fall outside it.
- Other tolerances: `iot_behavior.flatline.min_run` 8 (clean max 3), `gap_multiple` 3 (clean gaps are exactly
  1x), `value_range.margin` 0.25, `referential.required_days` 3.

## Known limits

- Only sources referenced by a manifest are checked (CRM, 3PL, ERP products, IoT, stock counts). The vendor price sheets, CRM customers and the other ERP tables have no manifest, so they are invisible.
- A text-typed CSV cannot reveal that a number was re-encoded as text (see above).
- Out-of-order rows whose timestamps tie are not detectable by timestamps alone.
- Whitespace-padded cells and hand-keyed near-miss codes (`O004-B000208`) are not inspected.
- Row-count bands are per file; a 3PL hourly file can legitimately have 0 rows, so low-volume hours are not caught by volume (missing/truncated ones are, by freshness).
- No Airflow run state yet.
