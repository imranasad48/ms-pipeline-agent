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

## Four check families

| Family | Looks at | Incident ids |
|---|---|---|
| `schema` | header/keys vs `expected_schema` | `schema.missing_column:<c>`, `schema.new_column:<c>`, `schema.renamed:<old>-><new>`, `schema.type_change:<c>`, `schema.column_order` |
| `volume_nulls` | rows per file, null rate per column | `volume.row_count`, `nulls.spike:<c>` |
| `freshness` | calendar says a file is due | `freshness.missing_file`, `.empty_file`, `.truncated_file`, `.unreadable_file`, `.late_file` |
| `run_health` | warehouse table | `run_health.table_missing`, `.table_empty`, `.load_stale` |

Details worth knowing:
- **Renames** = a missing + a new column in the same file, paired by header position or name similarity (and the values must still fit the old type). The pair is reported once as `renamed`, not as two separate incidents.
- **Type change** parses every value against the declared type (`"1,234"`, `"45 units"`, `"Yes"`, epoch ints all fail). Columns the manifest types as plain `string` can get a regex in `config.yaml → formats` (used for date formats).
- **Null spike**: a file trips when its null rate exceeds the manifest baseline by `abs_delta` *or* by `z` binomial standard errors (so tiny hourly files don't false-alarm), whichever is larger.
- **Row counts**: per-file `[min,max]` learned from clean data (`baselines.yaml`, made by `python -m detection baseline --data-root <clean> --write`), widened by a margin.
- **Freshness** is driven by the calendar, not by the files that arrived, so a missing file is visible. "Late" = file mtime (or the 3PL envelope `generated_at`) is more than 1h after the expected arrival time. A JSON page whose `record_count` ≠ number of events counts as truncated. Unreadable files skip the other checks so one broken file yields one incident, not five.
- **Run health** is the simple version: table exists, has rows, and `MAX(loaded_at)` is not more than 36h before the end of the checked day. Tables without `loaded_at` (e.g. `dim_products`) only get the first two. Airflow integration comes later.

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
python -m detection incidents --date 2026-08-14
python -m pytest detection -q
```

`calibrate` replays every day found in the clean data, never writes to the DB,
exits non-zero if anything fires, and prints how close each threshold came to
tripping (utilisation 1.00 = exactly at the trip point).

## Calibration notes

- 3PL `notes` is optional free text, entirely empty in some hourly files → exempted via `nulls.overrides`.
- File mtimes are naive local time (the generator stamps them in PKT); run on a machine in the same zone.

## Known limits / next steps

- Only sources referenced by a manifest are checked (CRM, 3PL, ERP products today). IoT and manual xlsx cadences are configured but only get freshness checks once a manifest references them.
- Row-count bands are per file; a 3PL hourly file can legitimately have 0 rows, so low-volume hours are not caught by volume (missing/truncated ones are, by freshness).
- Not yet: duplicate keys, range/referential checks, stale re-sent payloads, out-of-order rows, Airflow run state.
