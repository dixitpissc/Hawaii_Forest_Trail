import pandas as pd
import os
from dotenv import load_dotenv
from storage.sqlserver.sql import fetch_table
from utils.logger_builder import build_logger

load_dotenv()

def validate_item_category_table(database=None, schema=None, table: str = "Item"):
    """
    Validates Item Category entries from the Item table before QBO migration.

    - Ensures only rows with Type='Category' are checked
    - Checks Name: required, non-null, ≤100 chars, no duplicates
    - Type must be 'Category'
    - Active must be boolean or null
    """
    if database is None:
        database = os.getenv("SQLSERVER_DATABASE")
    if schema is None:
        schema = os.getenv("SOURCE_SCHEMA", "dbo")

    logger = build_logger("item_category_validation")
    logger.info(f"🔍 Validating Item Categories in [{schema}].[{table}] from database [{database}]")
    logger.info("🔗 API Doc: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/item#create-a-category")

    df = fetch_table(table=table, schema=schema)
    if df.empty:
        logger.error("❌ Item table is empty or not found.")
        return False

    # Filter only Category rows
    df_cat = df[df["Type"].astype(str).str.lower() == "category"]
    logger.info(f"📦 Total Item Category rows: {len(df_cat)}")

    if df_cat.empty:
        logger.warning("⚠️ No Category rows found (Type='Category'). Nothing to validate.")
        return False

    # --- Validation: Name ---
    if "Name" not in df_cat.columns:
        logger.error("❌ Missing required column: Name")
        return False

    null_name = df_cat["Name"].isnull().sum()
    long_name = df_cat["Name"].astype(str).map(len).gt(100).sum()
    duplicate_names = df_cat[df_cat.duplicated("Name", keep=False)]["Name"].unique().tolist()

    if null_name > 0:
        logger.warning(f"⚠️ {null_name} Category names are NULL.")
    if long_name > 0:
        logger.warning(f"⚠️ {long_name} Category names exceed 100 characters.")
    if duplicate_names:
        logger.warning(f"❗ Duplicate Category names: {duplicate_names}")
    else:
        logger.info("✅ No duplicate Category names.")

    # --- Validation: Type === 'Category' ---
    if not (df_cat["Type"].astype(str).str.lower() == "category").all():
        logger.warning("⚠️ Some Item rows marked as category have incorrect Type value.")

    # --- Validation: Active ---
    if "Active" in df_cat.columns:
        df_cat["Active_str"] = df_cat["Active"].astype(str).str.lower()
        valid_active = df_cat["Active_str"].isin(["true", "false", "nan"])
        if not valid_active.all():
            logger.warning("⚠️ Invalid values in Active — should be true/false or null.")
        else:
            active_count = (df_cat["Active_str"] == "true").sum()
            inactive_count = (df_cat["Active_str"] == "false").sum()
            logger.info(f"🟢 Active: {active_count}, 🔴 Inactive: {inactive_count}")
    else:
        logger.info("ℹ️ Column 'Active' not found — will default to true in QBO.")

    logger.info("🎯 Item Category validation complete.")
    return True
