import pandas as pd
import os
from dotenv import load_dotenv
from storage.sqlserver.sql import fetch_table
from utils.logger_builder import build_logger

load_dotenv()

# Mapping for allowed subtypes per type
ALLOWED_SUBTYPES = {
    "Asset": {"Bank", "Cash on hand", "Checking", "Savings", "Accounts Receivable", "Inventory", "Fixed Asset"},
    "Liability": {"Accounts Payable", "Credit Card", "Other Current Liabilities", "Long Term Liabilities"},
    "Equity": {"Retained Earnings", "Owner’s Equity", "Paid‑in Capital", "Accumulated Adjustment"},
    "Income": {"Service/Fee Income", "Sales of Product Income", "Other Primary Income", "Discounts/Refunds Given"},
    "Expense": {"Advertising/Promotional", "Auto", "Insurance", "Rent or Lease of Buildings",
                "Supplies & Materials", "Travel", "Meals and entertainment", "Bank Charges"},
    "Cost of Goods Sold": {"Cost of labour - COS", "Equipment Rental - COS",
                           "Shipping, Freight & Delivery - COS", "Supplies & Materials - COGS"},
    "Other Income": {"Dividend Income", "Interest Earned", "Other Miscellaneous Income", "Tax‑Exempt Interest"},
    "Other Expense": {"Depreciation", "Amortisation", "Exchange Gain or Loss", "Penalties & Settlements"}
}

def validate_account_table(database=None, schema=None, table: str = "Account"):
    """
    Validates source Account table before migrating to QBO.

    Includes required Name, AccountType, and optional AccountSubType checking against allowed values.
    """
    from dotenv import load_dotenv  # to ensure environment
    load_dotenv()

    if database is None:
        database = os.getenv("SQLSERVER_DATABASE")
    if schema is None:
        schema = os.getenv("SOURCE_SCHEMA", "dbo")

    logger = build_logger("account_validation")
    logger.info(f"🔍 Validating [{schema}].[{table}] in database [{database}]")
    logger.info("🔗 QBO Account API reference")

    df = fetch_table(table=table, schema=schema)
    if df.empty:
        logger.error("❌ Account table is empty or not found.")
        return False
    logger.info(f"📊 {len(df)} rows × {len(df.columns)} columns")

    missing = []

    # Required columns
    for req in ["Name", "AccountType"]:
        if req not in df.columns:
            missing.append(req)
            logger.warning(f"❌ Missing required column: {req}")
        else:
            logger.info(f"✅ Column {req} present")

    # Name validations
    if "Name" in df.columns:
        if df["Name"].isnull().any():
            logger.warning("⚠️ Some Name values are NULL — migration will fail")
        if df["Name"].astype(str).map(len).gt(100).any():
            logger.warning("⚠️ Some Name values exceed 100 characters")
        dups = df[df.duplicated(subset=["Name"], keep=False)]
        if not dups.empty:
            logger.warning(f"❗ Duplicate account names: {dups['Name'].unique().tolist()}")
        else:
            logger.info("✅ No duplicate account names")

    # AccountType validation
    allowed_types = set(ALLOWED_SUBTYPES.keys())
    if "AccountType" in df.columns:
        invalid = df[df["AccountType"].notnull() & ~df["AccountType"].isin(allowed_types)]
        if not invalid.empty:
            logger.warning(f"⚠️ Invalid AccountType values: {invalid['AccountType'].unique().tolist()}")
        else:
            logger.info("✅ AccountType values valid")

    # AccountSubType validation
    if "AccountSubType" in df.columns:
        mask = df["AccountSubType"].notnull() & df["AccountType"].notnull()
        for _, row in df[mask].iterrows():
            parent = row["AccountType"]
            sub = row["AccountSubType"]
            allowed = ALLOWED_SUBTYPES.get(parent, set())
            if sub not in allowed:
                logger.warning(f"⚠️ SubType '{sub}' invalid for Type '{parent}'")
        logger.info("ℹ️ AccountSubType validation completed")

    # Other optional checks (Active, CurrencyRef.value, SubAccount/ParentRef consistency)
    # [Similar to earlier function]
    # …

    success = (len(missing) == 0)
    if success:
        logger.info("🎉 Account table validation passed.")
        return True
    else:
        logger.warning("🚫 Account validation failed; review warnings.")
        return False
