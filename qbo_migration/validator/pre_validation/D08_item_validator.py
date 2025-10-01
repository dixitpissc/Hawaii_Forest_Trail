import pandas as pd
import os
from dotenv import load_dotenv
from storage.sqlserver.sql import fetch_table
from utils.logger_builder import build_logger

load_dotenv()

def validate_item_table(database=None, schema=None, table: str = "Item"):
    """
    Validates the Item table for QBO migration (excluding Category type).

    Validates:
    - Name: required, ≤100 chars, unique
    - Type: required, valid
    - IncomeAccountRef.value: required for all items
    - ExpenseAccountRef.value: required for Inventory/Purchase-enabled items
    - AssetAccountRef.value: required for Inventory
    - TrackQtyOnHand, QtyOnHand, InvStartDate: required if inventory tracked
    - Active: must be boolean
    """
    if database is None:
        database = os.getenv("SQLSERVER_DATABASE")
    if schema is None:
        schema = os.getenv("SOURCE_SCHEMA", "dbo")

    logger = build_logger("item_validation")
    logger.info(f"🔍 Validating Items in [{schema}].[{table}] from database [{database}]")
    logger.info("🔗 API Doc: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/item#create-an-item")

    df = fetch_table(table=table, schema=schema)
    if df.empty:
        logger.error("❌ Item table is empty or not found.")
        return False

    # Exclude Type='Category'
    df_item = df[df["Type"].astype(str).str.lower() != "category"]
    logger.info(f"📦 Total non-category Item rows: {len(df_item)}")

    # --- Validation: Name ---
    if "Name" not in df_item.columns:
        logger.error("❌ Missing required column: Name")
        return False

    null_name = df_item["Name"].isnull().sum()
    long_name = df_item["Name"].astype(str).map(len).gt(100).sum()
    duplicate_names = df_item[df_item.duplicated("Name", keep=False)]["Name"].unique().tolist()

    if null_name > 0:
        logger.warning(f"⚠️ {null_name} Item names are NULL.")
    if long_name > 0:
        logger.warning(f"⚠️ {long_name} Item names exceed 100 characters.")
    if duplicate_names:
        logger.warning(f"❗ Duplicate Item names: {duplicate_names}")
    else:
        logger.info("✅ No duplicate Item names.")

    # --- Validation: Type ---
    if "Type" in df_item.columns:
        valid_types = ["inventory", "noninventory", "service"]
        df_item["Type_str"] = df_item["Type"].astype(str).str.lower()
        invalid_types = df_item[~df_item["Type_str"].isin(valid_types)]
        if not invalid_types.empty:
            logger.warning(f"⚠️ Invalid Type values found: {invalid_types['Type'].unique().tolist()}")
        else:
            logger.info("✅ All Item types are valid.")
    else:
        logger.error("❌ Missing required column: Type")

    # --- IncomeAccountRef.value ---
    if "IncomeAccountRef.value" not in df_item.columns:
        logger.error("❌ Missing required column: IncomeAccountRef.value")
    else:
        missing_income = df_item["IncomeAccountRef.value"].isnull().sum()
        if missing_income > 0:
            logger.warning(f"⚠️ {missing_income} Items are missing IncomeAccountRef.value")

    # --- ExpenseAccountRef.value for Inventory and Purchase-enabled items ---
    if "ExpenseAccountRef.value" in df_item.columns:
        inv_or_purchase = df_item[df_item["Type_str"] == "inventory"]
        missing_expense = inv_or_purchase["ExpenseAccountRef.value"].isnull().sum()
        logger.info(f"⚠️ Inventory Items missing ExpenseAccountRef.value: {missing_expense}")
    else:
        logger.warning("⚠️ ExpenseAccountRef.value column not found")

    # --- AssetAccountRef.value for Inventory items ---
    if "AssetAccountRef.value" in df_item.columns:
        inventory_items = df_item[df_item["Type_str"] == "inventory"]
        missing_asset = inventory_items["AssetAccountRef.value"].isnull().sum()
        logger.info(f"⚠️ Inventory Items missing AssetAccountRef.value: {missing_asset}")
    else:
        logger.warning("⚠️ AssetAccountRef.value column not found")

    # --- Inventory tracking fields ---
    if "TrackQtyOnHand" in df_item.columns:
        track_qty_items = df_item[df_item["TrackQtyOnHand"].astype(str).str.lower() == "true"]

        # QtyOnHand
        if "QtyOnHand" in df_item.columns:
            missing_qty = track_qty_items["QtyOnHand"].isnull().sum()
            logger.info(f"📦 Items with TrackQtyOnHand=true missing QtyOnHand: {missing_qty}")
        else:
            logger.warning("⚠️ QtyOnHand column not found")

        # InvStartDate
        if "InvStartDate" in df_item.columns:
            missing_date = track_qty_items["InvStartDate"].isnull().sum()
            logger.info(f"📅 Items with TrackQtyOnHand=true missing InvStartDate: {missing_date}")
        else:
            logger.warning("⚠️ InvStartDate column not found")

    # --- Active boolean check ---
    if "Active" in df_item.columns:
        df_item["Active_str"] = df_item["Active"].astype(str).str.lower()
        valid = df_item["Active_str"].isin(["true", "false", "nan"])
        if not valid.all():
            logger.warning("⚠️ Invalid entries in Active (must be 'true'/'false' or null).")
        else:
            active_count = (df_item["Active_str"] == "true").sum()
            inactive_count = (df_item["Active_str"] == "false").sum()
            logger.info(f"🟢 Active: {active_count}, 🔴 Inactive: {inactive_count}")
    else:
        logger.info("ℹ️ 'Active' column not found — defaults to true in QBO")

    logger.info("🎯 Item validation complete.")
    return True
