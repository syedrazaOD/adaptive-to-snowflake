"""
snowflake_loader.py
Snowflake writer. Key-pair auth via environment variable.
Full-replace strategy: DROP + CREATE + INSERT for dim tables.
Append + truncate strategy for fact/mod tables (version-scoped).
"""

import json
import logging
import re
from datetime import datetime, timezone

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from config import (
    SF_ACCOUNT, SF_USER, SF_PRIVATE_KEY,
    SF_ROLE, SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA,
)

log = logging.getLogger(__name__)


def get_connection():
    private_key = load_pem_private_key(
        SF_PRIVATE_KEY.encode("utf-8"),
        password=None,
    )
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        private_key=private_key_bytes,
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )


def sanitize_col(name):
    s = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip())
    if s and s[0].isdigit():
        s = "_" + s
    return s.upper()


class SnowflakeLoader:
    def __init__(self):
        self.conn = get_connection()
        log.info(f"Connected to Snowflake: {SF_ACCOUNT} / {SF_DATABASE}.{SF_SCHEMA}")

    def close(self):
        self.conn.close()

    # ── Dim table loader (full-replace) ───────────────────────

    def load_dim(self, table_name, rows):
        """Drop and recreate dimension table with fresh data."""
        if not rows:
            log.info(f"  {table_name}: 0 rows, skipping")
            return 0

        cur = self.conn.cursor()
        cols    = list(rows[0].keys())
        sf_cols = [sanitize_col(c) for c in cols]

        col_defs   = ", ".join(f'"{c}" VARCHAR' for c in sf_cols)
        col_list   = ", ".join(f'"{c}"' for c in sf_cols)
        placeholders = ", ".join(["%s"] * len(sf_cols))

        cur.execute(f'TRUNCATE TABLE IF EXISTS {SF_SCHEMA}.{table_name.upper()}')

        def clean(v):
            """Escape % signs to prevent Snowflake connector format string errors."""
            return str(v).replace("%", "%%") if v is not None else ""

        # Batch insert in chunks of 1000
        insert_sql = f'INSERT INTO {SF_SCHEMA}.{table_name.upper()} ({col_list}) VALUES ({placeholders})'
        batch_size = 1000
        total = 0
        for i in range(0, len(rows), batch_size):
            batch = [[clean(row.get(c, "")) for c in cols] for row in rows[i:i+batch_size]]
            cur.executemany(insert_sql, batch)
            total += len(batch)

        self.conn.commit()
        cur.close()
        log.info(f"  {table_name}: {total} rows loaded")
        return total

    # ── Fact table loader (version-scoped replace) ────────────

    def load_fact(self, version_name, raw_rows, sheet_lookup):
        """
        Transforms exportData CSV rows into fact_planning_data rows.
        Deletes existing rows for this version, then inserts fresh data.
        sheet_lookup: dict of account_code -> sheet_name (best effort)
        """
        if not raw_rows:
            log.info(f"  fact_planning_data [{version_name}]: 0 rows")
            return 0

        cur = self.conn.cursor()

        # Delete existing rows for this version
        cur.execute(
            f'DELETE FROM {SF_SCHEMA}.FACT_PLANNING_DATA WHERE version_name = %s',
            (version_name,)
        )

        # Identify time columns vs meta columns
        sample = raw_rows[:20]
        period_pattern = re.compile(r"^\d{2}/\d{4}$")

        def is_period_col(key):
            for row in sample:
                val = str(row.get(key, "")).strip()
                if val and period_pattern.match(val):
                    return True
            return False

        all_keys   = list(raw_rows[0].keys())
        # Time columns look like "01/2024"
        period_cols = [k for k in all_keys if period_pattern.match(k.strip())]
        # Known meta columns
        meta_keys  = {"Account Code", "Account Name", "Level Name", "Level",
                       "Account code", "account code", "level name"}
        dim_cols   = [k for k in all_keys
                      if k not in period_cols and k not in meta_keys
                      and k.strip() not in ("", "Account Code", "Account Name",
                                            "Level Name", "Level")]

        insert_sql = (
            f'INSERT INTO {SF_SCHEMA}.FACT_PLANNING_DATA '
            "(version_name, sheet_name, account_code, account_name, "
            "level_name, period_code, period_name, amount, dimensions) "
            "SELECT %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)"
        )

        batch, total = [], 0
        for row in raw_rows:
            acc_code  = row.get("Account Code", row.get("account code", ""))
            acc_name  = row.get("Account Name", row.get("account name", ""))
            level     = row.get("Level Name", row.get("Level", ""))
            sheet     = sheet_lookup.get(acc_code, "")
            dims      = {k: row.get(k, "") for k in dim_cols if row.get(k, "")}
            dims_json = json.dumps(dims) if dims else "null"

            for period_col in period_cols:
                val_str = row.get(period_col, "").strip()
                if not val_str:
                    continue
                try:
                    amount = float(val_str.replace(",", ""))
                except ValueError:
                    continue
                if amount == 0:
                    continue
                batch.append((
                    version_name, sheet,
                    str(acc_code).replace("%", "%%"),
                    str(acc_name).replace("%", "%%"),
                    str(level).replace("%", "%%"),
                    period_col, period_col,
                    amount, dims_json
                ))
                if len(batch) >= 1000:
                    cur.executemany(insert_sql, batch)
                    total += len(batch)
                    batch = []

        if batch:
            cur.executemany(insert_sql, batch)
            total += len(batch)

        self.conn.commit()
        cur.close()
        log.info(f"  fact_planning_data [{version_name}]: {total} rows loaded")
        return total

    # ── Modeled sheet loader ──────────────────────────────────

    def load_modeled(self, sheet_name, version_name, rows):
        """
        Stores modeled sheet rows as VARIANT in mod_generic.
        Deletes existing rows for this sheet+version, inserts fresh data.
        """
        if not rows:
            log.info(f"  mod_generic [{sheet_name}/{version_name}]: 0 rows")
            return 0

        cur = self.conn.cursor()
        cur.execute(
            f'DELETE FROM {SF_SCHEMA}.MOD_GENERIC '
            "WHERE sheet_name = %s AND version_name = %s",
            (sheet_name, version_name)
        )

        insert_sql = (
            f'INSERT INTO {SF_SCHEMA}.MOD_GENERIC '
            "(version_name, sheet_name, raw_data) "
            "SELECT %s, %s, PARSE_JSON(%s)"
        )

        batch, total = [], 0
        for row in rows:
            batch.append((version_name, sheet_name, json.dumps(row)))
            if len(batch) >= 1000:
                cur.executemany(insert_sql, batch)
                total += len(batch)
                batch = []
        if batch:
            cur.executemany(insert_sql, batch)
            total += len(batch)

        self.conn.commit()
        cur.close()
        log.info(f"  mod_generic [{sheet_name}/{version_name}]: {total} rows loaded")
        return total

    # ── Sync log ──────────────────────────────────────────────

    def log_sync(self, phase, version_name, sheet_name,
                 rows_written, status, error_message, started_at):
        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        cur = self.conn.cursor()
        safe_error = (error_message or "").replace("%", "%%")
        cur.execute(
            f'INSERT INTO {SF_SCHEMA}._SYNC_LOG '
            "(started_at, completed_at, phase, version_name, sheet_name, "
            "rows_written, status, error_message, duration_seconds) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (started_at, datetime.now(timezone.utc), phase,
             version_name or "", sheet_name or "",
             rows_written, status, safe_error, elapsed)
        )
        self.conn.commit()
        cur.close()
