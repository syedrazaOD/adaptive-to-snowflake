"""
config.py — Connection settings + EXCLUDE lists only.
No hardcoded sheet or version lists.
All credentials via environment variables.
"""

import os

# ── Adaptive Planning ─────────────────────────────────────────
ADAPTIVE_LOGIN    = os.environ["ADAPTIVE_LOGIN"]
ADAPTIVE_PASSWORD = os.environ["ADAPTIVE_PASSWORD"]
ADAPTIVE_BASE_URL = "https://api.adaptiveplanning.com/api/v43"

# ── Snowflake ─────────────────────────────────────────────────
SF_ACCOUNT        = os.environ["SF_ACCOUNT"]          # e.g. RW35960-OPENDOOR
SF_USER           = os.environ["SF_USER"]              # e.g. FPA_SERVICE_ACCOUNT
SF_PRIVATE_KEY    = os.environ["SF_PRIVATE_KEY"]       # PEM private key contents
SF_ROLE           = os.environ.get("SF_ROLE",      "FINANCETOOLS_SERVICE_ACCOUNT_V2")
SF_WAREHOUSE      = os.environ.get("SF_WAREHOUSE", "INTERACTIVE_WH")
SF_DATABASE       = os.environ.get("SF_DATABASE",  "DWH")
SF_SCHEMA         = os.environ.get("SF_SCHEMA",    "DATA_MART_ADAPTIVE")

# ── Date range ────────────────────────────────────────────────
DATE_START        = os.environ.get("DATE_START", "01/2020")
DATE_END          = os.environ.get("DATE_END",   "12/2028")

# ── Exclude lists (items to skip even if discovered) ─────────
# Add archived or irrelevant versions here — everything else is exported automatically.
EXCLUDE_VERSIONS = [
    # Example: "FY2019 Archive",
]

# Add sheets to skip (e.g. test sheets, deprecated sheets).
EXCLUDE_SHEETS = [
    # Example: "Test Sheet",
]
