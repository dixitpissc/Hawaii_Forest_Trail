import pandas as pd
import os
from dotenv import load_dotenv
from storage.sqlserver.sql import fetch_table
from utils.logger_builder import build_logger

import re

load_dotenv()

def validate_vendor_table(database=None, schema=None, table: str = "Vendor"):
    """
    Validates the Vendor table for QBO migration.

    Validates:
    - Required: DisplayName (not null, ≤100 chars, unique)
    - Email format (if provided)
    - Active values must be boolean if present
    """
    if database is None:
        database = os.getenv("SQLSERVER_DATABASE")
    if schema is None:
        schema = os.getenv("SOURCE_SCHEMA", "dbo")

    logger = build_logger("vendor_validation")
    logger.info(f"🔍 Validating Vendor table [{schema}].[{table}] from database [{database}]")
    logger.info("🔗 API Doc: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/vendor#create-a-vendor")

    df = fetch_table(table=table, schema=schema)
    if df.empty:
        logger.error("❌ Vendor table is empty or not found.")
        return False

    logger.info(f"📊 Total rows: {len(df)}, Columns: {len(df.columns)}")

    # --- DisplayName checks ---
    if "DisplayName" not in df.columns:
        logger.error("❌ Required column 'DisplayName' is missing.")
        return False

    null_display = df["DisplayName"].isnull().sum()
    long_display = df["DisplayName"].astype(str).map(len).gt(100).sum()
    duplicate_display = df[df.duplicated("DisplayName", keep=False)]["DisplayName"].unique().tolist()

    if null_display > 0:
        logger.warning(f"⚠️ {null_display} Vendor DisplayName values are NULL.")
    if long_display > 0:
        logger.warning(f"⚠️ {long_display} DisplayName values exceed 100 characters.")
    if duplicate_display:
        logger.warning(f"❗ Duplicate DisplayNames: {duplicate_display}")
    else:
        logger.info("✅ DisplayName column is valid (no nulls or duplicates).")

    # --- Email address format ---
    if "PrimaryEmailAddr.Address" in df.columns:
        email_series = df["PrimaryEmailAddr.Address"].dropna().astype(str)
        invalid_emails = email_series[~email_series.str.match(r"^[^@]+@[^@]+\.[^@]+$")]
        if not invalid_emails.empty:
            logger.warning(f"⚠️ Invalid email formats found: {invalid_emails.tolist()[:5]} ...")
        else:
            logger.info("✅ All email addresses are valid.")
    else:
        logger.info("ℹ️ Column 'PrimaryEmailAddr.Address' not found — skipping email validation.")

    # --- Active flag ---
    if "Active" in df.columns:
        df["Active_str"] = df["Active"].astype(str).str.lower()
        valid = df["Active_str"].isin(["true", "false", "nan"])
        if not valid.all():
            logger.warning("⚠️ Invalid entries in Active (must be 'true', 'false', or null).")
        else:
            active_count = (df["Active_str"] == "true").sum()
            inactive_count = (df["Active_str"] == "false").sum()
            logger.info(f"🟢 Active: {active_count}, 🔴 Inactive: {inactive_count}")
    else:
        logger.info("ℹ️ Column 'Active' not found — defaults to true in QBO.")

    logger.info("🎯 Vendor validation complete.")
    return True
