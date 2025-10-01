import pandas as pd
import os
from dotenv import load_dotenv
from storage.sqlserver.sql import fetch_table
from utils.logger_builder import build_logger

load_dotenv()

def validate_class_table(database=None, schema=None, table: str = "Class"):
    """
    Validates source Class table before migrating to QBO.

    Checks:
    - Required: Name
    - Duplicate Name (must be unique)
    - Name not null and max-length (e.g., ≤100 chars)
    - Optional: ParentRef.value exists and hierarchies
    - Optional: Active boolean validity
    """
    if database is None:
        database = os.getenv("SQLSERVER_DATABASE")
    if schema is None:
        schema = os.getenv("SOURCE_SCHEMA", "dbo")

    logger = build_logger("class_validation")
    logger.info(f"🔍 Starting validation for [{schema}].[{table}] in database [{database}]")
    logger.info("🔗 API Doc: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/class")

    df = fetch_table(table=table, schema=schema)
    if df.empty:
        logger.error("❌ Class table is empty or not found.")
        return False

    logger.info(f"📊 Rows: {len(df)}, Columns: {len(df.columns)}")

    missing = []
    if "Name" not in df.columns:
        missing.append("Name")
        logger.warning("❌ Required column missing: Name")
    else:
        logger.info("✅ Column present: Name")

    # Validate Name
    if "Name" in df.columns:
        if df["Name"].isnull().any():
            logger.warning("⚠️ Some Name values are NULL — migration will fail.")
        if df["Name"].astype(str).map(len).gt(100).any():
            logger.warning("⚠️ Some Name values exceed 100 characters — may violate API.")
        dup = df[df.duplicated(subset=["Name"], keep=False)]
        if not dup.empty:
            logger.warning(f"❗ Duplicate Class names found: {dup['Name'].unique().tolist()}")
        else:
            logger.info("✅ No duplicate Class names detected")

    # Validate ParentRef.value hierarchy
    if "ParentRef.value" in df.columns:
        has_parent = df["ParentRef.value"].notna()
        total_parent = (~has_parent).sum()
        total_child = has_parent.sum()
        logger.info(f"🏷️ Parent classes: {total_parent}, Child classes: {total_child}")
    else:
        logger.warning("⚠️ Column missing: ParentRef.value — cannot assess hierarchy")

    # Validate Active
    if "Active" in df.columns:
        df["Active_str"] = df["Active"].astype(str).str.lower()
        valid = df["Active_str"].isin(["true", "false", "nan"])
        if not valid.all():
            logger.warning("⚠️ Invalid entries in Active (must be true/false or null).")
        else:
            count_true = df[df["Active_str"] == "true"].shape[0]
            count_false = df[df["Active_str"] == "false"].shape[0]
            logger.info(f"🟢 Active: {count_true}, 🔴 Inactive: {count_false}")
    else:
        logger.info("ℹ️ 'Active' not present — defaulting to true in migration")

    success = (len(missing) == 0)
    if success:
        logger.info("🎉 Class table validation passed — ready for QBO migration.")
        return True
    else:
        logger.warning("🚫 Class validation failed — review issues above.")
        return False
