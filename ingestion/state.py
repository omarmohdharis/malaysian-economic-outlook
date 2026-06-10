"""
State persistence layer.

The DuckDB warehouse is gitignored (too large for GitHub) and rebuilt from
scratch on every CI run. Without intervention that wipes the CDC state too:
every run would see all data as brand new, the change log would only ever
contain one run's INSERTs, and revision detection (UPDATE / SCD Type 2)
could never fire.

This module fixes that by exporting the small *stateful* tables to parquet
files in `state/` (which IS committed to git) and re-importing them at the
start of each pipeline run, so CDC always has the previous run's view of the
world to compare against. The big raw data stays out of git.

Deliberately NOT persisted:
  - raw.*                  — bulk data, refetched from source every CI run
  - meta.source_watermarks — watermarks describe what is in the *local* raw
    schema; restoring them into an empty warehouse would make the extractors
    skip fetching data that isn't actually there
  - dbt schemas            — fully derived, rebuilt by `dbt run`

Usage:
    python state.py import   # restore state/ parquet files into the warehouse
    python state.py export   # dump stateful tables to state/ parquet files
"""

import sys
from pathlib import Path

import duckdb

from config import PROJECT_ROOT, WAREHOUSE_PATH

STATE_DIR = PROJECT_ROOT / "state"

# Shared DDL — single source of truth for the CDC history tables.
# cdc.py executes these too, so import order doesn't matter.
WAREHOUSE_DDL = {
    "warehouse.fuel_price_history": """
        CREATE TABLE IF NOT EXISTS warehouse.fuel_price_history (
            date_effective  DATE NOT NULL,
            fuel_type       VARCHAR NOT NULL,
            price           DOUBLE NOT NULL,
            loaded_at       TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (date_effective, fuel_type)
        )
    """,
    "warehouse.gdp_quarterly": """
        CREATE TABLE IF NOT EXISTS warehouse.gdp_quarterly (
            quarter     VARCHAR NOT NULL,
            series      VARCHAR NOT NULL,
            value       DOUBLE,
            loaded_at   TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (quarter, series)
        )
    """,
    "warehouse.dim_household_income": """
        CREATE TABLE IF NOT EXISTS warehouse.dim_household_income (
            geography       VARCHAR NOT NULL,
            geography_type  VARCHAR NOT NULL,   -- 'state' | 'national'
            income_mean     DOUBLE,
            income_median   DOUBLE,
            survey_year     INTEGER NOT NULL,
            valid_from      DATE NOT NULL,
            valid_to        DATE,               -- NULL means current
            is_current      BOOLEAN NOT NULL DEFAULT TRUE
        )
    """,
}

# Every table that must survive between CI runs, mapped to its parquet file.
STATEFUL_TABLES = {
    "meta.pipeline_runs":              "meta.pipeline_runs.parquet",
    "meta.change_log":                 "meta.change_log.parquet",
    "warehouse.fuel_price_history":    "warehouse.fuel_price_history.parquet",
    "warehouse.gdp_quarterly":         "warehouse.gdp_quarterly.parquet",
    "warehouse.dim_household_income":  "warehouse.dim_household_income.parquet",
}


def _ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create every stateful table so import has a target to load into."""
    from extract import _ensure_metadata_tables
    _ensure_metadata_tables(con)
    con.execute("CREATE SCHEMA IF NOT EXISTS warehouse")
    for ddl in WAREHOUSE_DDL.values():
        con.execute(ddl)


def _table_row_count(con: duckdb.DuckDBPyConnection, table: str) -> int | None:
    """Row count, or None if the table doesn't exist."""
    schema, name = table.split(".")
    exists = con.execute("""
        SELECT count(*) FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
    """, [schema, name]).fetchone()[0]
    if not exists:
        return None
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def import_state(con: duckdb.DuckDBPyConnection | None = None) -> None:
    """
    Load state/ parquet files into the warehouse.

    Only loads into EMPTY tables — a local warehouse that already carries
    live state is authoritative and must not get duplicate rows.
    """
    own_con = con is None
    if own_con:
        Path(WAREHOUSE_PATH).parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(WAREHOUSE_PATH)

    _ensure_tables(con)
    print(f"\n  State import — {STATE_DIR}")

    for table, filename in STATEFUL_TABLES.items():
        path = STATE_DIR / filename
        if not path.exists():
            print(f"  [{table}] no state file -- skipping.")
            continue
        if _table_row_count(con, table):
            print(f"  [{table}] already has rows -- keeping local state.")
            continue
        con.execute(
            f"INSERT INTO {table} BY NAME SELECT * FROM read_parquet(?)",
            [path.as_posix()],
        )
        rows = _table_row_count(con, table)
        print(f"  [{table}] OK restored {rows:,} rows.")

    if own_con:
        con.close()


def export_state(con: duckdb.DuckDBPyConnection | None = None) -> None:
    """Dump every stateful table to state/ so the next run can restore it."""
    own_con = con is None
    if own_con:
        con = duckdb.connect(WAREHOUSE_PATH)

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\n  State export — {STATE_DIR}")

    for table, filename in STATEFUL_TABLES.items():
        rows = _table_row_count(con, table)
        if rows is None:
            print(f"  [{table}] does not exist -- skipping.")
            continue
        path = (STATE_DIR / filename).as_posix()
        con.execute(f"COPY (SELECT * FROM {table}) TO '{path}' (FORMAT PARQUET)")
        print(f"  [{table}] OK exported {rows:,} rows.")

    if own_con:
        con.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Persist/restore stateful warehouse tables")
    parser.add_argument("action", choices=["import", "export"])
    args = parser.parse_args()

    if args.action == "import":
        import_state()
    else:
        export_state()
