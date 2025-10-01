import pandas as pd
import os
from dotenv import load_dotenv
from storage.sqlserver.sql import fetch_table
from utils.logger_builder import build_logger

load_dotenv()

def validate_companycurrency_table(database=None, schema=None, table: str = "CompanyCurrency"):
    """
    Validates source CompanyCurrency data before migration to QBO.

    Checks:
    - Required field: Code
    - Duplicate Code
    - ISO‑format length (3 characters)
    - Optional exchange rate numeric validity
    - Optional boolean 'Active'
    """
    if database is None:
        database = os.getenv("SQLSERVER_DATABASE")
    if schema is None:
        schema = os.getenv("SOURCE_SCHEMA", "dbo")

    logger = build_logger("companycurrency_validation")
    logger.info(f"🔍 Starting validation for [{schema}].[{table}] in database [{database}]")
    logger.info("🔗 API Doc: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/companycurrency")

    df = fetch_table(table=table, schema=schema)
    if df.empty:
        logger.error("❌ CompanyCurrency table is empty or not found.")
        return False

    logger.info(f"📊 Total rows: {len(df)}, columns: {len(df.columns)}")

    # Required field check
    missing = []
    if "Code" not in df.columns:
        missing.append("Code")
        logger.warning("❌ Column missing: Code")
    else:
        logger.info("✅ Column present: Code")

    # Code validations
    if "Code" in df.columns:
        if df["Code"].isnull().any():
            logger.warning("⚠️ Some 'Code' values are NULL — invalid for QBO.")
        # length assumption
        if df["Code"].astype(str).map(len).ne(3).any():
            invalid_len = df[df["Code"].astype(str).map(len).ne(3)]["Code"].unique().tolist()
            logger.warning(f"⚠️ 'Code' values not ISO‑3 length: {invalid_len}")
        duplicates = df[df.duplicated(subset=["Code"], keep=False)]
        if not duplicates.empty:
            logger.warning(f"❗ Duplicate Code found: {duplicates['Code'].unique().tolist()}")
        else:
            logger.info("✅ No duplicate Code")

    # ExchangeRate numeric check (if present)
    if "ExchangeRate" in df.columns:
        invalid = df[df["ExchangeRate"].notnull() & ~df["ExchangeRate"].apply(lambda x: isinstance(x, (int, float)))]
        if not invalid.empty:
            logger.warning(f"⚠️ Some 'ExchangeRate' values are non‑numeric: {invalid['ExchangeRate'].unique().tolist()}")
        else:
            logger.info("✅ 'ExchangeRate' values are numeric or NULL")

    # Active boolean validation
    if "Active" in df.columns:
        df["Active_str"] = df["Active"].astype(str).str.lower()
        valid = df["Active_str"].isin(["true", "false", "nan"])
        if not valid.all():
            logger.warning("⚠️ Invalid boolean values in 'Active'.")
        else:
            logger.info("✅ 'Active' values are valid booleans (or NULL)")

    # Final result
    passed = (len(missing) == 0)
    if passed:
        logger.info("🎉 CompanyCurrency validation passed — ready for migration.")
        return True
    else:
        logger.warning("🚫 CompanyCurrency validation failed; please review warnings.")
        return False
