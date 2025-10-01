import pandas as pd
import os
from dotenv import load_dotenv
from storage.sqlserver.sql import fetch_table
from utils.logger_builder import build_logger
import re

load_dotenv()

def validate_employee_table(database=None, schema=None, table: str = "Employee"):
    """
    Validates the Employee table for QBO migration.

    - Required: DisplayName (non-null, ≤100 chars, unique)
    - Validates email format
    - Ensures Active and BillableTime are boolean if present
    """
    if database is None:
        database = os.getenv("SQLSERVER_DATABASE")
    if schema is None:
        schema = os.getenv("SOURCE_SCHEMA", "dbo")

    logger = build_logger("employee_validation")
    logger.info(f"🔍 Validating Employee table [{schema}].[{table}] in database [{database}]")
    logger.info("🔗 API Doc: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/employee")

    df = fetch_table(table=table, schema=schema)
    if df.empty:
        logger.error("❌ Employee table is empty or not found.")
        return False

    logger.info(f"📊 Rows: {len(df)}, Columns: {len(df.columns)}")

    # --- DisplayName checks ---
    if "DisplayName" not in df.columns:
        logger.error("❌ Missing required column: DisplayName")
        return False

    null_display = df["DisplayName"].isnull().sum()
    long_display = df["DisplayName"].astype(str).map(len).gt(100).sum()
    duplicate_display = df[df.duplicated("DisplayName", keep=False)]["DisplayName"].unique().tolist()

    if null_display > 0:
        logger.warning(f"⚠️ {null_display} DisplayName values are NULL.")
    if long_display > 0:
        logger.warning(f"⚠️ {long_display} DisplayName values exceed 100 characters.")
    if duplicate_display:
        logger.warning(f"❗ Duplicate DisplayNames: {duplicate_display}")
    else:
        logger.info("✅ DisplayName is valid — no nulls or duplicates.")

    # --- Email format check ---
    if "PrimaryEmailAddr.Address" in df.columns:
        email_series = df["PrimaryEmailAddr.Address"].dropna().astype(str)
        invalid_emails = email_series[~email_series.str.match(r"^[^@]+@[^@]+\.[^@]+$")]
        if not invalid_emails.empty:
            logger.warning(f"⚠️ Invalid email formats: {invalid_emails.tolist()[:5]} ...")
        else:
            logger.info("✅ All email formats are valid.")
    else:
        logger.info("ℹ️ Column 'PrimaryEmailAddr.Address' not found — skipping email validation.")

    # --- Active flag ---
    if "Active" in df.columns:
        df["Active_str"] = df["Active"].astype(str).str.lower()
        valid = df["Active_str"].isin(["true", "false", "nan"])
        if not valid.all():
            logger.warning("⚠️ Invalid values in Active — must be true/false/null.")
        else:
            active_count = (df["Active_str"] == "true").sum()
            inactive_count = (df["Active_str"] == "false").sum()
            logger.info(f"🟢 Active: {active_count}, 🔴 Inactive: {inactive_count}")
    else:
        logger.info("ℹ️ Column 'Active' not found — will default to true in QBO.")

    # --- BillableTime flag ---
    if "BillableTime" in df.columns:
        df["BillableTime_str"] = df["BillableTime"].astype(str).str.lower()
        valid = df["BillableTime_str"].isin(["true", "false", "nan"])
        if not valid.all():
            logger.warning("⚠️ Invalid values in BillableTime — must be true/false/null.")
        else:
            logger.info("✅ BillableTime values are valid.")
    else:
        logger.info("ℹ️ Column 'BillableTime' not found — skipping check.")

    logger.info("🎯 Employee table validation complete.")
    return True
