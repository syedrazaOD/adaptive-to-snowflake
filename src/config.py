"""
config.py — Connection settings + version filter config.
All credentials via environment variables.
"""

import os

# ── Adaptive Planning ─────────────────────────────────────────
ADAPTIVE_LOGIN    = os.environ["ADAPTIVE_LOGIN"]
ADAPTIVE_PASSWORD = os.environ["ADAPTIVE_PASSWORD"]
ADAPTIVE_BASE_URL = "https://api.adaptiveplanning.com/api/v43"

# ── Snowflake ─────────────────────────────────────────────────
SF_ACCOUNT        = os.environ["SF_ACCOUNT"]
SF_USER           = os.environ["SF_USER"]
SF_PRIVATE_KEY    = os.environ["SF_PRIVATE_KEY"]
SF_ROLE           = os.environ.get("SF_ROLE",      "FINANCETOOLS_SERVICE_ACCOUNT_V2")
SF_WAREHOUSE      = os.environ.get("SF_WAREHOUSE", "INTERACTIVE_WH")
SF_DATABASE       = os.environ.get("SF_DATABASE",  "DWH")
SF_SCHEMA         = os.environ.get("SF_SCHEMA",    "DATA_MART_ADAPTIVE")

# ── Date range ────────────────────────────────────────────────
DATE_START = os.environ.get("DATE_START", "01/2020")
DATE_END   = os.environ.get("DATE_END",   "12/2028")

# ── Version filter ────────────────────────────────────────────
# Versions are included automatically if their name contains
# the current year or previous year (e.g. "Q2 2026 Forecast",
# "2025 AOP"). Versions with no year (e.g. "Actuals",
# "Plan - Working") are always included.
# Increase VERSION_LOOKBACK_YEARS to include older versions.
VERSION_LOOKBACK_YEARS = 2

# ── Always include these specific versions ────────────────────
ALWAYS_INCLUDE_VERSIONS = [
    "Plan - Working",
]

# ── Exclude lists ─────────────────────────────────────────────
EXCLUDE_VERSIONS = [
    "Actuals",
    "2025 AOP - 1.31.25",
    "2025 AOP",
    "2025 April Forecast",
    "2025 AOP - 1.31.25",
    "2025 AOP",
    "2025 April Forecast",
    "Actuals",
    "2025 July Forecast",
    "Q4 2025 Forecast",
    "Q2 2026 Forecast",
]  # Specific versions to skip

EXCLUDE_SHEETS = [
    "Contractor - Personnel",
    "Personnel",
    "Raw_Data",
    "Cohort Toggle",
    "UEA_Projections",
    "GMV",
    "****HEADCOUNT****",
    "****OTHER OPEX****",
    "**** CAPITAL ****",
    "CAPEX by Line Item",
    "Title JV P&L",
    "*** Balance Sheet Models***",
    "****Volume, Revenue, Margins****",
    "****Adjacent Services****",
    "Inventory Actuals",
    "GMV_LwOD",
    "OSN Core P&L",
    "GMV_BwOD",
    "Home Reserve PNL Summary Data Ingestion",
    "Leases",
    "Bonus/Other",
]
