"""
main.py — Pipeline orchestrator.
Phases 0–3 with CLI flags for partial runs.

Usage:
  python main.py                          # Full run (all phases)
  python main.py --phase metadata         # Phase 1 only
  python main.py --phase data             # Phase 2 only (standard+cube)
  python main.py --phase modeled          # Phase 3 only
  python main.py --version "Actuals"      # Specific version only
  python main.py --modeled-sheet "Personnel"  # Specific modeled sheet only
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

import adaptive_client as ac
from snowflake_loader import SnowflakeLoader


DDL_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS fact_planning_data (
        version_name VARCHAR, sheet_name VARCHAR, account_code VARCHAR,
        account_name VARCHAR, level_name VARCHAR, period_code VARCHAR,
        period_name VARCHAR, amount FLOAT, dimensions VARCHAR,
        _synced_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""",
    """CREATE TABLE IF NOT EXISTS mod_generic (
        version_name VARCHAR, sheet_name VARCHAR, raw_data VARCHAR(16777216),
        _synced_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""",
    """CREATE TABLE IF NOT EXISTS dim_accounts (
        account_id VARCHAR, account_code VARCHAR, account_name VARCHAR,
        account_type VARCHAR, parent_id VARCHAR, parent_name VARCHAR,
        parent_code VARCHAR, _synced_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""",
    """CREATE TABLE IF NOT EXISTS dim_sheets (
        sheet_name VARCHAR, sheet_type VARCHAR,
        _synced_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""",
    """CREATE TABLE IF NOT EXISTS dim_versions (
        version_id VARCHAR, version_name VARCHAR, version_type VARCHAR,
        start_plan VARCHAR, end_ver VARCHAR, is_locked VARCHAR, currency VARCHAR,
        _synced_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""",
    """CREATE TABLE IF NOT EXISTS dim_levels (
        level_id VARCHAR, level_name VARCHAR, short_name VARCHAR,
        parent_id VARCHAR, parent_name VARCHAR,
        _synced_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""",
    """CREATE TABLE IF NOT EXISTS dim_level_attributes (
        level_id VARCHAR, level_name VARCHAR, attribute_name VARCHAR,
        attribute_value VARCHAR, _synced_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""",
    """CREATE TABLE IF NOT EXISTS dim_dimensions (
        dimension_id VARCHAR, dimension_name VARCHAR, value_id VARCHAR,
        value_name VARCHAR, short_name VARCHAR, is_default VARCHAR,
        _synced_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""",
    """CREATE TABLE IF NOT EXISTS dim_dimension_attributes (
        dimension_id VARCHAR, dimension_name VARCHAR, value_id VARCHAR,
        value_name VARCHAR, attribute_name VARCHAR, attribute_value VARCHAR,
        _synced_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""",
    """CREATE TABLE IF NOT EXISTS dim_time (
        period_id VARCHAR, period_code VARCHAR, period_name VARCHAR,
        year VARCHAR, quarter VARCHAR, month_num VARCHAR, fiscal_year VARCHAR,
        _synced_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())""",
    """CREATE TABLE IF NOT EXISTS _sync_log (
        run_id VARCHAR DEFAULT UUID_STRING(), started_at TIMESTAMP_NTZ,
        completed_at TIMESTAMP_NTZ, phase VARCHAR, version_name VARCHAR,
        sheet_name VARCHAR, rows_written INTEGER, status VARCHAR,
        error_message VARCHAR, duration_seconds FLOAT)""",
]

VIEW_STATEMENTS = [
    "CREATE OR REPLACE VIEW v_income_statement AS SELECT * FROM fact_planning_data WHERE sheet_name = 'Income Statement'",
    "CREATE OR REPLACE VIEW v_balance_sheet AS SELECT * FROM fact_planning_data WHERE sheet_name = 'Balance Sheet'",
    "CREATE OR REPLACE VIEW v_final_external_metrics AS SELECT * FROM fact_planning_data WHERE sheet_name = 'Final External Metrics'",
    "CREATE OR REPLACE VIEW v_volume_summary AS SELECT * FROM fact_planning_data WHERE sheet_name = 'Volume Summary'",
    "CREATE OR REPLACE VIEW v_modeled_sheet AS SELECT version_name, sheet_name, raw_data, _synced_at FROM mod_generic",
    "CREATE OR REPLACE VIEW v_sync_status AS SELECT phase, version_name, MAX(completed_at) AS last_synced_at, SUM(rows_written) AS total_rows, COUNT_IF(status = 'ERROR') AS errors FROM _sync_log GROUP BY 1, 2 ORDER BY 1, 2",
]


def setup_schema(loader):
    """Create all tables if they don't exist. Migrates VARIANT columns to VARCHAR."""
    log.info("── PHASE -1: SCHEMA SETUP ──────────────────────────────────")
    cur = loader.conn.cursor()
    # One-time migration: convert VARIANT columns to VARCHAR
    for tbl, col in [("MOD_GENERIC", "RAW_DATA"), ("FACT_PLANNING_DATA", "DIMENSIONS")]:
        try:
            cur.execute(
                f"ALTER TABLE {SF_SCHEMA}.{tbl} MODIFY COLUMN {col} VARCHAR(16777216)"
            )
            log.info(f"  Migrated {tbl}.{col} → VARCHAR")
        except Exception:
            pass  # Table doesn't exist yet or already correct — fine
    for ddl in DDL_STATEMENTS:
        table_name = ddl.split("IF NOT EXISTS")[1].split("(")[0].strip()
        try:
            cur.execute(ddl)
            log.info(f"  ✅ {table_name}")
        except Exception as e:
            log.error(f"  ❌ {table_name}: {e}")
    loader.conn.commit()
    cur.close()
    log.info("Schema setup complete.\n")


def run(args):
    start_time = datetime.now(timezone.utc)
    log.info("=" * 60)
    log.info(f"Adaptive → Snowflake sync started at {start_time}")
    log.info(f"Args: phase={args.phase} version={args.version} "
             f"modeled_sheet={args.modeled_sheet}")
    log.info("=" * 60)

    loader = SnowflakeLoader()
    errors = []

    # ── Phase -1: Schema setup (idempotent) ───────────────────
    setup_schema(loader)

    # ── Phase 0: Discovery ────────────────────────────────────
    log.info("\n── PHASE 0: DISCOVERY ──────────────────────────────────────")
    versions, sheets = ac.discover()

    if args.version:
        versions = [v for v in versions if v == args.version]
        log.info(f"Filtered to version: {args.version}")

    if args.modeled_sheet:
        sheets = {k: v for k, v in sheets.items()
                  if v == "modeled" and k == args.modeled_sheet}
        log.info(f"Filtered to modeled sheet: {args.modeled_sheet}")

    standard_cube_sheets = {k: v for k, v in sheets.items() if v in ("standard", "cube")}
    modeled_sheets       = {k: v for k, v in sheets.items() if v == "modeled"}

    # ── Phase 1: Metadata ─────────────────────────────────────
    if args.phase in (None, "metadata", "all"):
        log.info("\n── PHASE 1: METADATA ───────────────────────────────────────")
        t = datetime.now(timezone.utc)
        try:
            gl, metric, custom, assumptions, acct_attrs = ac.export_accounts()
            all_accounts = gl + metric + custom + assumptions
            loader.load_dim("dim_accounts", all_accounts)
            loader.load_sync = lambda: None  # placeholder
            loader.log_sync("METADATA", None, "dim_accounts",
                            len(all_accounts), "SUCCESS", None, t)
        except Exception as e:
            log.error(f"dim_accounts: {e}")
            errors.append(f"dim_accounts: {e}")
            loader.log_sync("METADATA", None, "dim_accounts", 0, "ERROR", str(e), t)

        for fn, table in [
            (ac.export_levels,        ("dim_levels", "dim_level_attributes")),
            (ac.export_dimensions,    ("dim_dimensions", "dim_dimension_attributes")),
        ]:
            t = datetime.now(timezone.utc)
            try:
                rows1, rows2 = fn()
                loader.load_dim(table[0], rows1)
                loader.load_dim(table[1], rows2)
                loader.log_sync("METADATA", None, table[0],
                                len(rows1) + len(rows2), "SUCCESS", None, t)
            except Exception as e:
                log.error(f"{table[0]}: {e}")
                errors.append(f"{table[0]}: {e}")
                loader.log_sync("METADATA", None, table[0], 0, "ERROR", str(e), t)

        for fn, table in [
            (ac.export_versions_meta, "dim_versions"),
            (ac.export_time,          "dim_time"),
        ]:
            t = datetime.now(timezone.utc)
            try:
                rows = fn()
                loader.load_dim(table, rows)
                loader.log_sync("METADATA", None, table, len(rows), "SUCCESS", None, t)
            except Exception as e:
                log.error(f"{table}: {e}")
                errors.append(f"{table}: {e}")
                loader.log_sync("METADATA", None, table, 0, "ERROR", str(e), t)

        # Sheet list
        t = datetime.now(timezone.utc)
        try:
            sheet_rows = [{"sheet_name": k, "sheet_type": v} for k, v in sheets.items()]
            loader.load_dim("dim_sheets", sheet_rows)
            loader.log_sync("METADATA", None, "dim_sheets",
                            len(sheet_rows), "SUCCESS", None, t)
        except Exception as e:
            log.error(f"dim_sheets: {e}")
            errors.append(f"dim_sheets: {e}")

    # ── Phase 2: Standard + Cube data ────────────────────────
    if args.phase in (None, "data", "all"):
        log.info("\n── PHASE 2: STANDARD + CUBE DATA ───────────────────────────")

        # Build account→sheet lookup from dim_accounts + dim_sheets
        # (best effort — accounts may appear on multiple sheets)
        sheet_lookup = {}  # account_code -> sheet_name

        for version_name in versions:
            t = datetime.now(timezone.utc)
            log.info(f"\nVersion: {version_name}")
            try:
                raw_rows, error = ac.export_all_data(version_name)
                if error:
                    log.error(f"  exportData error: {error}")
                    errors.append(f"exportData/{version_name}: {error}")
                    loader.log_sync("STANDARD_CUBE", version_name, None,
                                    0, "ERROR", error, t)
                    continue
                n = loader.load_fact(version_name, raw_rows, sheet_lookup)
                loader.log_sync("STANDARD_CUBE", version_name, None,
                                n, "SUCCESS", None, t)
            except Exception as e:
                log.error(f"  {version_name}: {e}")
                errors.append(f"exportData/{version_name}: {e}")
                loader.log_sync("STANDARD_CUBE", version_name, None,
                                0, "ERROR", str(e), t)

    # ── Phase 3: Modeled sheets ───────────────────────────────
    if args.phase in (None, "modeled", "all"):
        log.info("\n── PHASE 3: MODELED SHEETS ─────────────────────────────────")
        log.info(f"{len(modeled_sheets)} modeled sheets × {len(versions)} versions")

        for sheet_name in modeled_sheets:
            for version_name in versions:
                t = datetime.now(timezone.utc)
                log.info(f"\n  [{sheet_name}] / [{version_name}]")
                try:
                    rows, error = ac.export_modeled_sheet(sheet_name, version_name)
                    if error:
                        log.warning(f"  ⚠️  {error}")
                        errors.append(f"modeled/{sheet_name}/{version_name}: {error}")
                        loader.log_sync("MODELED", version_name, sheet_name,
                                        0, "ERROR", error, t)
                        continue
                    n = loader.load_modeled(sheet_name, version_name, rows)
                    loader.log_sync("MODELED", version_name, sheet_name,
                                    n, "SUCCESS", None, t)
                except Exception as e:
                    log.error(f"  ❌ {e}")
                    errors.append(f"modeled/{sheet_name}/{version_name}: {e}")
                    loader.log_sync("MODELED", version_name, sheet_name,
                                    0, "ERROR", str(e), t)

    # ── Summary ───────────────────────────────────────────────
    loader.close()
    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    log.info("\n" + "=" * 60)
    log.info(f"Sync completed in {elapsed:.1f}s")
    if errors:
        log.warning(f"⚠️  {len(errors)} errors:")
        for err in errors:
            log.warning(f"  - {err}")
        sys.exit(1)
    else:
        log.info("✅ All phases completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Adaptive → Snowflake sync")
    parser.add_argument("--phase", choices=["metadata", "data", "modeled", "all"],
                        default=None, help="Run a specific phase only")
    parser.add_argument("--version", default=None,
                        help="Export a specific version only")
    parser.add_argument("--modeled-sheet", default=None,
                        help="Export a specific modeled sheet only")
    args = parser.parse_args()
    run(args)
