# Pipeline State

This directory holds parquet snapshots of the warehouse's **stateful** tables —
the CDC change log, pipeline run history, and the CDC history/dimension tables:

| File | Table | Purpose |
|---|---|---|
| `meta.change_log.parquet` | `meta.change_log` | Every detected data change (the dashboard audit log) |
| `meta.pipeline_runs.parquet` | `meta.pipeline_runs` | Run history across all pipeline executions |
| `warehouse.fuel_price_history.parquet` | `warehouse.fuel_price_history` | Weekly fuel price history (value-based CDC) |
| `warehouse.gdp_quarterly.parquet` | `warehouse.gdp_quarterly` | GDP by quarter (append CDC with revision detection) |
| `warehouse.dim_household_income.parquet` | `warehouse.dim_household_income` | SCD Type 2 income dimension |

## Why this exists

The DuckDB warehouse (`warehouse/pulse.duckdb`) is too large for GitHub, so it
is gitignored and rebuilt from scratch on every CI run. CDC, however, only
works if the previous run's data survives — otherwise every run is a cold load
where everything looks new and revisions can never be detected.

These files are small (raw data is *not* persisted, only CDC state), so they
are committed to git. The pipeline restores them at the start of each run
(`python ingestion/state.py import`) and exports them after CDC
(`python ingestion/state.py export`); CI commits the updated snapshots along
with the rendered dashboard.

Do not edit these files by hand — they are regenerated on every pipeline run.
