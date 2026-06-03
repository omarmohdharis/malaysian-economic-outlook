"""
Extraction layer — pulls raw data from data.gov.my into DuckDB staging tables.

Each source lands in a schema called `raw_<source_name>`.
A `pipeline_runs` metadata table tracks every run so the CDC layer
knows what's new vs already seen.
"""

import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from config import SOURCES, WAREHOUSE_PATH


def get_connection() -> duckdb.DuckDBPyConnection:
    Path(WAREHOUSE_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(WAREHOUSE_PATH)
    _ensure_metadata_tables(con)
    return con


def _ensure_metadata_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create bookkeeping tables if they don't exist yet."""
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS meta;

        CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
            run_id          VARCHAR PRIMARY KEY,
            source_name     VARCHAR NOT NULL,
            run_at          TIMESTAMPTZ NOT NULL,
            status          VARCHAR NOT NULL,   -- 'success' | 'failed' | 'skipped'
            rows_loaded     INTEGER,
            new_rows        INTEGER,
            error_message   VARCHAR,
            notes           VARCHAR
        );

        CREATE TABLE IF NOT EXISTS meta.source_watermarks (
            source_name     VARCHAR PRIMARY KEY,
            last_loaded_at  TIMESTAMPTZ NOT NULL,
            last_max_date   VARCHAR,            -- latest date/period seen in the data
            row_count       INTEGER
        );

        -- CDC changelog: every detected value change across any source
        CREATE TABLE IF NOT EXISTS meta.change_log (
            detected_at     TIMESTAMPTZ NOT NULL,
            source_name     VARCHAR NOT NULL,
            entity_key      VARCHAR NOT NULL,   -- e.g. 'RON95' or 'Q1-2026'
            field_name      VARCHAR NOT NULL,
            old_value       VARCHAR,
            new_value       VARCHAR,
            change_type     VARCHAR NOT NULL    -- 'INSERT' | 'UPDATE' | 'DELETE'
        );
    """)


def _log_run(
    con, source_name: str, status: str,
    rows_loaded: int = 0, new_rows: int = 0,
    error: str = None, notes: str = None
) -> None:
    run_id = f"{source_name}__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
    con.execute("""
        INSERT INTO meta.pipeline_runs
            (run_id, source_name, run_at, status, rows_loaded, new_rows, error_message, notes)
        VALUES (?, ?, now(), ?, ?, ?, ?, ?)
    """, [run_id, source_name, status, rows_loaded, new_rows, error, notes])


def _update_watermark(con, source_name: str, row_count: int, max_date: str = None) -> None:
    con.execute("""
        INSERT INTO meta.source_watermarks (source_name, last_loaded_at, last_max_date, row_count)
        VALUES (?, now(), ?, ?)
        ON CONFLICT (source_name) DO UPDATE SET
            last_loaded_at = excluded.last_loaded_at,
            last_max_date  = excluded.last_max_date,
            row_count      = excluded.row_count
    """, [source_name, max_date, row_count])


def _get_watermark(con, source_name: str) -> dict | None:
    result = con.execute(
        "SELECT last_loaded_at, last_max_date, row_count FROM meta.source_watermarks WHERE source_name = ?",
        [source_name]
    ).fetchone()
    if result:
        return {"last_loaded_at": result[0], "last_max_date": result[1], "row_count": result[2]}
    return None


# ── Per-source extractors ─────────────────────────────────────────────────────

def extract_static_parquet(con: duckdb.DuckDBPyConnection, source_name: str, config: dict) -> None:
    """Load a single parquet URL into raw.<source_name>. Skip if loaded today."""
    watermark = _get_watermark(con, source_name)
    if watermark:
        last = watermark["last_loaded_at"]
        if hasattr(last, "date") and last.date() == datetime.now(timezone.utc).date():
            print(f"  [{source_name}] Already loaded today — skipping.")
            _log_run(con, source_name, "skipped", notes="Already loaded today")
            return

    try:
        print(f"  [{source_name}] Fetching {config['url']} ...")
        df = pd.read_parquet(config["url"])
        con.execute(f"CREATE SCHEMA IF NOT EXISTS raw")
        con.execute(f"DROP TABLE IF EXISTS raw.{source_name}")
        con.execute(f"CREATE TABLE raw.{source_name} AS SELECT * FROM df")
        rows = len(df)
        _update_watermark(con, source_name, rows)
        _log_run(con, source_name, "success", rows_loaded=rows, new_rows=rows)
        print(f"  [{source_name}] ✓ {rows:,} rows loaded.")
    except Exception as e:
        _log_run(con, source_name, "failed", error=str(e))
        print(f"  [{source_name}] ✗ Failed: {e}", file=sys.stderr)


def extract_monthly_partitioned(con: duckdb.DuckDBPyConnection, source_name: str, config: dict) -> None:
    """
    Load monthly parquet partitions incrementally.
    Only fetches months not yet in the watermark.
    """
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    watermark = _get_watermark(con, source_name)
    already_loaded = watermark["last_max_date"] if watermark else None

    months_to_load = [
        m for m in config["months"]
        if already_loaded is None or m > already_loaded
    ]

    if not months_to_load:
        print(f"  [{source_name}] No new months to load (latest: {already_loaded}).")
        _log_run(con, source_name, "skipped", notes=f"No new months after {already_loaded}")
        return

    total_new = 0
    last_successful_month = already_loaded

    for month in months_to_load:
        url = config["url_template"].format(month=month)
        try:
            print(f"  [{source_name}] Loading {month} from {url} ...")
            df = pd.read_parquet(url)
            df["_month_partition"] = month

            # Append to table (create on first load)
            existing = con.execute(
                f"SELECT count(*) FROM information_schema.tables "
                f"WHERE table_schema='raw' AND table_name='{source_name}'"
            ).fetchone()[0]

            if existing == 0:
                con.execute(f"CREATE TABLE raw.{source_name} AS SELECT * FROM df")
            else:
                # Remove any existing rows for this month (idempotent re-runs)
                con.execute(f"DELETE FROM raw.{source_name} WHERE _month_partition = ?", [month])
                con.execute(f"INSERT INTO raw.{source_name} SELECT * FROM df")

            total_new += len(df)
            last_successful_month = month
            print(f"  [{source_name}] ✓ {month}: {len(df):,} rows.")
        except Exception as e:
            print(f"  [{source_name}] ✗ {month} failed: {e} — skipping.", file=sys.stderr)

    if last_successful_month:
        total_rows = con.execute(f"SELECT count(*) FROM raw.{source_name}").fetchone()[0]
        _update_watermark(con, source_name, total_rows, max_date=last_successful_month)
        _log_run(con, source_name, "success", rows_loaded=total_rows, new_rows=total_new,
                 notes=f"Loaded up to {last_successful_month}")


# ── Orchestrate all sources ───────────────────────────────────────────────────

def run_all(sources: list[str] | None = None) -> None:
    """Run extraction for all sources, or a named subset."""
    con = get_connection()
    targets = {k: v for k, v in SOURCES.items() if sources is None or k in sources}

    print(f"\n{'='*60}")
    print(f"  Extraction run — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Sources: {list(targets.keys())}")
    print(f"{'='*60}\n")

    for name, config in targets.items():
        if config["type"] == "static_parquet":
            extract_static_parquet(con, name, config)
        elif config["type"] == "monthly_partitioned_parquet":
            extract_monthly_partitioned(con, name, config)
        else:
            print(f"  [{name}] Unknown source type '{config['type']}' — skipping.")

    print(f"\n{'='*60}")
    print("  Extraction complete.")
    print(f"{'='*60}\n")
    con.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Extract data.gov.my sources into DuckDB")
    parser.add_argument("--sources", nargs="*", help="Specific source names to run (default: all)")
    args = parser.parse_args()
    run_all(sources=args.sources)
