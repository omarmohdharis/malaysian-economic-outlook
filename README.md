# 🇲🇾 Malaysian Economic Pulse

An end-to-end data engineering project that tracks cost-of-living pressure across Malaysia — ingesting open government data, transforming it through a layered DuckDB warehouse, and publishing an interactive dashboard rebuilt automatically every month.

**[View the live dashboard →](https://omarmohdharis.github.io/malaysian-economic-outlook/dashboard/index.html)**

---

## What It Does

The project produces a **Pulse Score (0–100)** — a composite index measuring how much financial pressure Malaysian households are under. Higher = more pressure. It's grounded in actual spending shares from the DOSM Household Expenditure Survey 2024.

| Signal | Weight | Source |
|---|---|---|
| Food prices | 35% | PriceCatcher (50M+ records) |
| CPI — broad price level | 30% | DOSM national CPI |
| Fuel — RON95 | 15% | Weekly retail fuel prices |
| Public transport ridership | 10% | data.gov.my transport |
| EV adoption share | 10% | Vehicle registration data |

---

## Architecture

```
data.gov.my / DOSM
      │  (Parquet)
      ▼
┌─────────────────┐
│  extract.py     │  Stage 1: Load raw data into DuckDB (incremental, watermarked)
└────────┬────────┘
         │  raw.*
         ▼
┌─────────────────┐
│  cdc.py         │  Stage 2: Change Data Capture → meta.change_log
└────────┬────────┘           (value CDC, append CDC, SCD Type 2)
         │  warehouse.*
         ▼
┌─────────────────┐
│  dbt            │  Stage 3: staging views → mart tables + data tests
└────────┬────────┘
         │  main_marts.*
         ▼
┌─────────────────┐
│  Quarto + Plotly│  Dashboard rendered to docs/ → GitHub Pages
└─────────────────┘
```

### DuckDB Warehouse Schemas

| Schema | Contents |
|---|---|
| `raw` | Raw tables from source Parquet files |
| `warehouse` | CDC history tables (fuel, GDP, SCD income) |
| `meta` | Pipeline run logs, watermarks, change log |
| `main_staging` | dbt staging views |
| `main_marts` | dbt mart tables (dashboard-ready) |

The warehouse file (`warehouse/pulse.duckdb`) is gitignored and rebuilt from scratch on every CI run.

### Change Data Capture

Three CDC patterns run after each extraction:

- **Value-based CDC** (fuel prices) — detects week-over-week price changes and revisions
- **Append CDC** (GDP) — detects new quarters and preliminary-to-final revisions
- **SCD Type 2** (household income) — expires old rows and inserts new ones when survey figures change, preserving full history

All changes are logged to `meta.change_log` and surfaced in the dashboard's audit table.

---

## Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Full pipeline (extract → CDC → dbt run → dbt test)
python run_pipeline.py

# Individual stages
python run_pipeline.py --extract
python run_pipeline.py --cdc
python run_pipeline.py --dbt

# Render the dashboard
quarto render
```

---

## Project Structure

```
├── ingestion/
│   ├── config.py          # Source definitions and URLs
│   ├── extract.py         # Extraction into DuckDB raw schema
│   └── cdc.py             # Change Data Capture
├── transforms/
│   ├── models/
│   │   ├── staging/       # stg_cpi, stg_fuel, stg_prices, stg_transport, stg_vehicles, stg_gdp
│   │   └── marts/         # mart_fuel_timeline, mart_price_summary, mart_economic_pulse
│   └── tests/             # data quality tests
├── dashboard/
│   ├── index.qmd          # Main dashboard
│   └── about.qmd          # Infrastructure docs
├── docs/                  # Rendered HTML (served by GitHub Pages)
├── run_pipeline.py        # Local pipeline runner
└── _quarto.yml            # Quarto site config
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data format | Apache Parquet |
| Warehouse | DuckDB |
| Transforms | dbt |
| Dashboard | Quarto + Plotly |
| Hosting | GitHub Pages |
| CI/CD | GitHub Actions |
| Language | Python 3.11 |

---

*Data: [data.gov.my](https://data.gov.my) · [DOSM](https://dosm.gov.my)*
