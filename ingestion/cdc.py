"""
Change Data Capture layer.

Compares newly extracted raw data against what was previously
staged in the warehouse and writes detected changes to meta.change_log.

Supported CDC patterns:
  - fuel_prices:       value-based CDC — price changed this week?
  - gdp_quarterly:     append CDC — is there a new quarter?
  - labour_force:      append + revision CDC — same quarter, different value?
  - cpi_national:      append CDC — new month published?
  - household_income:  SCD Type 2 — income figure changed for a geography?
"""

from datetime import datetime, timezone

import duckdb
import pandas as pd

from config import WAREHOUSE_PATH
from state import WAREHOUSE_DDL


def get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(WAREHOUSE_PATH)


def _write_change(con, source: str, entity_key: str, field: str,
                  old_val, new_val, change_type: str) -> None:
    con.execute("""
        INSERT INTO meta.change_log
            (detected_at, source_name, entity_key, field_name, old_value, new_value, change_type)
        VALUES (now(), ?, ?, ?, ?, ?, ?)
    """, [source, entity_key, field, str(old_val) if old_val is not None else None,
          str(new_val) if new_val is not None else None, change_type])


# ── CDC: Fuel prices (value-based) ───────────────────────────────────────────

def cdc_fuel_prices(con: duckdb.DuckDBPyConnection) -> int:
    """
    Detect week-over-week fuel price changes and maintain a changelog table.
    This is the clearest CDC example in the project — every Wednesday price
    update in Malaysia becomes a discrete change event.
    """
    # Ensure the history table exists (DDL shared with state.py)
    con.execute(WAREHOUSE_DDL["warehouse.fuel_price_history"])

    # Get raw fuel prices
    try:
        raw = con.execute("SELECT * FROM raw.fuel_prices").df()
    except Exception:
        print("  [cdc_fuel] raw.fuel_prices not loaded yet — skipping.")
        return 0

    # The fuel dataset is wide format with a series_type filter column:
    #   series_type | date | ron95 | ron97 | diesel | diesel_eastmsia | ...
    # Keep only 'level' rows (actual prices, not weekly changes).
    if "series_type" in raw.columns:
        raw = raw[raw["series_type"] == "level"].copy()

    price_cols = [c for c in raw.columns
                  if c not in ("date", "series_type", "_month_partition")
                  and raw[c].dtype in ("float64", "float32", "Float64", "double")]
    if not price_cols:
        # Fallback: any numeric-ish column that isn't date/series_type
        price_cols = [c for c in raw.columns
                      if c not in ("date", "series_type", "_month_partition")]

    long = raw.melt(id_vars=["date"], value_vars=price_cols,
                    var_name="fuel_type", value_name="price").dropna(subset=["price"])
    long = long[long["price"].apply(lambda x: str(x).replace(".", "").isnumeric() or
                                    (isinstance(x, float)))]
    long["price"] = long["price"].astype(float)
    long = long[long["price"] > 0]
    long["date"] = long["date"].astype(str).str[:10]
    long["fuel_type"] = long["fuel_type"].str.upper()

    changes = 0
    for _, row in long.iterrows():
        existing = con.execute("""
            SELECT price FROM warehouse.fuel_price_history
            WHERE date_effective = ? AND fuel_type = ?
        """, [row["date"], row["fuel_type"]]).fetchone()

        if existing is None:
            # New date/fuel_type combination — INSERT event
            con.execute("""
                INSERT INTO warehouse.fuel_price_history (date_effective, fuel_type, price, loaded_at)
                VALUES (?, ?, ?, now())
            """, [row["date"], row["fuel_type"], row["price"]])
            _write_change(con, "fuel_prices", f"{row['fuel_type']}@{row['date']}",
                          "price", None, row["price"], "INSERT")
            changes += 1

        elif abs(float(existing[0]) - float(row["price"])) > 0.001:
            # Same date but price revised — UPDATE event (rare but possible)
            con.execute("""
                UPDATE warehouse.fuel_price_history
                SET price = ?, loaded_at = now()
                WHERE date_effective = ? AND fuel_type = ?
            """, [row["price"], row["date"], row["fuel_type"]])
            _write_change(con, "fuel_prices", f"{row['fuel_type']}@{row['date']}",
                          "price", existing[0], row["price"], "UPDATE")
            changes += 1

    print(f"  [cdc_fuel] {changes} change(s) detected.")
    return changes


# ── CDC: GDP quarterly (append-only) ─────────────────────────────────────────

def cdc_gdp(con: duckdb.DuckDBPyConnection) -> int:
    """Detect new quarters published in the GDP dataset."""
    con.execute(WAREHOUSE_DDL["warehouse.gdp_quarterly"])

    try:
        raw = con.execute("SELECT * FROM raw.gdp_quarterly").df()
    except Exception:
        print("  [cdc_gdp] raw.gdp_quarterly not loaded yet — skipping.")
        return 0

    # GDP data is already long format: series, date, value
    raw.columns = [c.lower() for c in raw.columns]
    if "series" in raw.columns and "value" in raw.columns:
        long = raw[["date", "series", "value"]].dropna(subset=["value"]).copy()
        long = long.rename(columns={"date": "quarter"})
        long["quarter"] = long["quarter"].astype(str).str[:7]
    else:
        print(f"  [cdc_gdp] Unexpected columns: {list(raw.columns)}")
        return 0

    changes = 0
    for _, row in long.iterrows():
        existing = con.execute("""
            SELECT value FROM warehouse.gdp_quarterly
            WHERE quarter = ? AND series = ?
        """, [row["quarter"], row["series"]]).fetchone()

        if existing is None:
            con.execute("""
                INSERT INTO warehouse.gdp_quarterly (quarter, series, value, loaded_at)
                VALUES (?, ?, ?, now())
            """, [row["quarter"], row["series"], row["value"]])
            _write_change(con, "gdp_quarterly", f"{row['series']}@{row['quarter']}",
                          "value", None, row["value"], "INSERT")
            changes += 1
        elif abs(float(existing[0]) - float(row["value"])) > 0.01:
            # Revised GDP figure (common — preliminary vs final estimates)
            con.execute("""
                UPDATE warehouse.gdp_quarterly SET value = ?, loaded_at = now()
                WHERE quarter = ? AND series = ?
            """, [row["value"], row["quarter"], row["series"]])
            _write_change(con, "gdp_quarterly", f"{row['series']}@{row['quarter']}",
                          "value", existing[0], row["value"], "UPDATE")
            changes += 1

    print(f"  [cdc_gdp] {changes} change(s) detected.")
    return changes


# ── CDC: Household income — SCD Type 2 ───────────────────────────────────────

def cdc_household_income(con: duckdb.DuckDBPyConnection) -> int:
    """
    SCD Type 2 on constituency/state-level income.
    When a survey release updates the median income for a geography,
    the old row is expired (valid_to set) and a new current row is inserted.
    This lets you query 'what was the income here in 2020?' correctly.
    """
    con.execute(WAREHOUSE_DDL["warehouse.dim_household_income"])

    try:
        raw = con.execute("SELECT * FROM raw.household_income").df()
    except Exception:
        print("  [cdc_income] raw.household_income not loaded yet — skipping.")
        return 0

    # Normalise column names
    raw.columns = [c.lower() for c in raw.columns]
    geo_col  = next((c for c in raw.columns if "state" in c), None)
    year_col = next((c for c in raw.columns if "year" in c or "date" in c), None)
    med_col  = next((c for c in raw.columns if "median" in c), None)
    mean_col = next((c for c in raw.columns if "mean" in c), None)

    if not all([geo_col, year_col, med_col]):
        print(f"  [cdc_income] Unexpected columns: {list(raw.columns)}")
        return 0

    # Process oldest survey first so the dim builds history in order, and a
    # historical row never expires a newer current row (idempotent re-runs).
    raw = raw.sort_values(year_col)

    changes = 0
    for _, row in raw.iterrows():
        geo   = str(row[geo_col])
        year  = int(str(row[year_col])[:4])
        med   = float(row[med_col])  if pd.notna(row[med_col])  else None
        mean  = float(row[mean_col]) if mean_col and pd.notna(row.get(mean_col)) else None

        current = con.execute("""
            SELECT income_median, income_mean, survey_year FROM warehouse.dim_household_income
            WHERE geography = ? AND is_current = TRUE
        """, [geo]).fetchone()

        if current is None:
            con.execute("""
                INSERT INTO warehouse.dim_household_income
                    (geography, geography_type, income_mean, income_median, survey_year, valid_from, is_current)
                VALUES (?, 'state', ?, ?, ?, CURRENT_DATE, TRUE)
            """, [geo, mean, med, year])
            _write_change(con, "household_income", geo, "income_median", None, med, "INSERT")
            changes += 1
            continue

        # Only a survey at least as new as the current row can change the dim;
        # older surveys are already represented in the expired history.
        if year < current[2]:
            continue

        if current[0] is not None and abs(float(current[0]) - (med or 0)) > 1.0:
            # New survey or revision — expire old row, insert new one (SCD Type 2)
            con.execute("""
                UPDATE warehouse.dim_household_income
                SET valid_to = CURRENT_DATE, is_current = FALSE
                WHERE geography = ? AND is_current = TRUE
            """, [geo])
            con.execute("""
                INSERT INTO warehouse.dim_household_income
                    (geography, geography_type, income_mean, income_median, survey_year, valid_from, is_current)
                VALUES (?, 'state', ?, ?, ?, CURRENT_DATE, TRUE)
            """, [geo, mean, med, year])
            _write_change(con, "household_income", geo, "income_median", current[0], med, "UPDATE")
            changes += 1

    print(f"  [cdc_income] {changes} change(s) detected.")
    return changes


# ── Orchestrate all CDC checks ────────────────────────────────────────────────

def run_all() -> None:
    con = get_connection()
    con.execute("CREATE SCHEMA IF NOT EXISTS warehouse")

    print(f"\n{'='*60}")
    print(f"  CDC run — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    total = 0
    total += cdc_fuel_prices(con)
    total += cdc_gdp(con)
    total += cdc_household_income(con)

    print(f"\n  Total changes detected: {total}")
    print(f"{'='*60}\n")
    con.close()


if __name__ == "__main__":
    run_all()
