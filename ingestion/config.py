"""
Central config for all data sources.
Each source declares its URL pattern, cadence, and how to detect new data.
"""

from datetime import date, datetime
from pathlib import Path

# Project root is one level up from this file (ingestion/)
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _last_n_months(n: int = 24) -> list[str]:
    """Return the last n completed months as YYYY-MM strings."""
    y, m = date.today().year, date.today().month - 1
    if m == 0:
        m, y = 12, y - 1
    months = []
    for _ in range(n):
        months.append(f"{y}-{m:02d}")
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    return list(reversed(months))


WAREHOUSE_PATH = str(PROJECT_ROOT / "warehouse" / "pulse.duckdb")

# ── Source definitions ────────────────────────────────────────────────────────

SOURCES = {

    "pricecatcher_items": {
        "url": "https://storage.data.gov.my/pricecatcher/lookup_item.parquet",
        "type": "static_parquet",
        "description": "PriceCatcher item reference table",
    },

    "pricecatcher_premises": {
        "url": "https://storage.data.gov.my/pricecatcher/lookup_premise.parquet",
        "type": "static_parquet",
        "description": "PriceCatcher premise reference table",
    },

    "pricecatcher_prices": {
        "url_template": "https://storage.data.gov.my/pricecatcher/pricecatcher_{month}.parquet",
        "type": "monthly_partitioned_parquet",
        "months": _last_n_months(24),
        "description": "PriceCatcher monthly transactional price records",
    },

    "cpi_state": {
        "url": "https://storage.dosm.gov.my/cpi/cpi_2d_state.parquet",
        "type": "static_parquet",
        "description": "Monthly CPI by state and division",
    },

    "cpi_national": {
        "url": "https://storage.dosm.gov.my/cpi/cpi_2d.parquet",
        "type": "static_parquet",
        "description": "Monthly national CPI by division",
    },

    "fuel_prices": {
        "url": "https://storage.data.gov.my/commodities/fuelprice.parquet",
        "type": "static_parquet",
        "description": "Weekly retail prices of RON95, RON97, and diesel",
    },

    "gdp_quarterly": {
        "url": "https://storage.dosm.gov.my/gdp/gdp_qtr_nominal.parquet",
        "type": "static_parquet",
        "description": "Quarterly nominal GDP — columns: series, date, value",
    },

    "labour_force": {
        "url": "https://storage.dosm.gov.my/labour/lfs_qtr.parquet",
        "type": "static_parquet",
        "description": "Quarterly Labour Force Survey — employment, unemployment, participation",
    },

    "vehicle_registrations": {
        "url": "https://storage.data.gov.my/transportation/registrations_type_fuel.parquet",
        "type": "static_parquet",
        "description": "Monthly vehicle registrations by vehicle type and fuel type",
    },

    "transport_ridership": {
        "url": "https://storage.data.gov.my/transportation/ridership_headline.parquet",
        "type": "static_parquet",
        "description": "Daily public transport ridership across all services",
    },

    "household_income": {
        "url": "https://storage.dosm.gov.my/hies/hh_income_state.parquet",
        "type": "static_parquet",
        "description": "Household income mean and median by state (annual survey)",
    },
}
