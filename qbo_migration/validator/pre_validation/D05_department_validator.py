import pandas as pd
import os
from dotenv import load_dotenv
from storage.sqlserver.sql import fetch_table
from utils.logger_builder import build_logger

load_dotenv()

def validate_department_table(database=None, schema=None, table: str = "Department"):
    """
    Validates source Department table before migrating to QBO.

    Checks:
    - Required: Name
    - Duplicate Name
    - Name null or exceeds length (≤100 chars)
    - Optional: ParentRef.value for hierarchy
    - Optional: Active boolean validity
    """
    if database is None:
        database = os.getenv("SQLSERVER_DATABASE")
    if schema is None:
        schema = os.getenv("SOURCE_SCHEMA", "dbo")

    logger = build_logger("department_validation")
    logger.info(f"🔍 Starting validation for [{schema}].[{table}] in database [{database}]")
    logger.info("🔗 API Doc: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/department")

    df = fetch_table(table=table, schema=schema)
    if df.empty:
        logger.error("❌ Department table is empty or not found.")
        return False

    logger.info(f"📊 Rows: {len(df)}, Columns: {len(df.columns)}")

    missing = []
    if "Name" not in df.columns:
        missing.append("Name")
        logger.warning("❌ Required column missing: Name")
    else:
        logger.info("✅ Column present: Name")

    # Name validation
    if "Name" in df.columns:
        if df["Name"].isnull().any():
            logger.warning("⚠️ Some Name values are NULL — this will fail migration.")
        if df["Name"].astype(str).map(len).gt(100).any():
            logger.warning("⚠️ Some Name values exceed 100 characters — may violate API.")
        duplicates = df[df.duplicated(subset=["Name"], keep=False)]
        if not duplicates.empty:
            logger.warning(f"❗ Duplicate Department names found: {duplicates['Name'].unique().tolist()}")
        else:
            logger.info("✅ No duplicate Department names detected")

    # ParentRef.value hierarchy
    if "ParentRef.value" in df.columns:
        has_parent = df["ParentRef.value"].notna()
        total_parent = (~has_parent).sum()
        total_child = has_parent.sum()
        logger.info(f"🏷️ Parent departments: {total_parent}, Child departments: {total_child}")
    else:
        logger.warning("⚠️ Column missing: ParentRef.value — cannot assess hierarchy")

    # Active boolean validation
    if "Active" in df.columns:
        df["Active_str"] = df["Active"].astype(str).str.lower()
        valid = df["Active_str"].isin(["true", "false", "nan"])
        if not valid.all():
            logger.warning("⚠️ Invalid entries in Active (must be 'true'/'false' or null).")
        else:
            active_count = df[df["Active_str"] == "true"].shape[0]
            inactive_count = df[df["Active_str"] == "false"].shape[0]
            logger.info(f"🟢 Active: {active_count}, 🔴 Inactive: {inactive_count}")
    else:
        logger.info("ℹ️ 'Active' not present — defaults to true during migration")

    success = (len(missing) == 0)
    if success:
        logger.info("🎉 Department table validation passed — ready for QBO migration.")
        return True
    else:
        logger.warning("🚫 Department validation failed — please review alerts above.")
        return False
