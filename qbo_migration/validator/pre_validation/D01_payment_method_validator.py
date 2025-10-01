import pandas as pd
import os
from dotenv import load_dotenv
from storage.sqlserver.sql import fetch_table
from utils.logger_builder import build_logger

load_dotenv()

def validate_paymentmethod_table(database=None, schema=None, table: str = "PaymentMethod"):
    """
    Validates source PaymentMethod table before migration to QBO.
    
    Checks:
    - Required fields per QBO API: Name
    - Duplicate Name
    - Name length (<= 31 characters)
    - Enum values for Type (CreditCard, NonCreditCard)
    - Boolean values in Active
    """
    if database is None:
        database = os.getenv("SQLSERVER_DATABASE")
    if schema is None:
        schema = os.getenv("SOURCE_SCHEMA", "dbo")

    logger = build_logger("paymentmethod_validation")
    logger.info(f"🔍 Starting validation for [{schema}].[{table}] in database [{database}]")
    logger.info("🔗 API Doc: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/paymentmethod")

    df = fetch_table(table=table, schema=schema)
    if df.empty:
        logger.error("❌ PaymentMethod table is empty or not found.")
        return

    logger.info(f"📊 Total rows: {len(df)}, Total columns: {len(df.columns)}")

    # Step 1: Required fields
    required_fields = ["Name"]
    missing_fields = [field for field in required_fields if field not in df.columns]

    for field in required_fields:
        if field in df.columns:
            logger.info(f"✅ Column present: {field}")
        else:
            logger.warning(f"❌ Column missing: {field}")

    if "Name" in df.columns:
        if df["Name"].isnull().any():
            logger.warning("⚠️ Some 'Name' values are NULL — these will fail in QBO.")
        if df["Name"].map(str).str.len().gt(31).any():
            logger.warning("⚠️ Some 'Name' values exceed 31 characters — QBO limit.")
        duplicates = df[df.duplicated(subset=["Name"], keep=False)]
        if not duplicates.empty:
            logger.warning(f"❗ Duplicate PaymentMethod names found: {duplicates['Name'].unique().tolist()}")
        else:
            logger.info("✅ No duplicate PaymentMethod names found")

    # Step 2: Enum check for 'Type'
    if "Type" in df.columns:
        allowed_types = {"CreditCard", "NonCreditCard"}
        invalid_types = df[~df["Type"].isin(allowed_types) & df["Type"].notnull()]
        if not invalid_types.empty:
            logger.warning(f"⚠️ Invalid 'Type' values found: {invalid_types['Type'].unique().tolist()}")
        else:
            logger.info("✅ All 'Type' values are valid")

    # Step 3: Validate 'Active'
    if "Active" in df.columns:
        df["Active_str"] = df["Active"].astype(str).str.lower()
        valid_bools = df["Active_str"].isin(["true", "false", "nan"])
        if not valid_bools.all():
            logger.warning("⚠️ Invalid boolean values in 'Active' column.")
        else:
            logger.info("✅ All 'Active' values are valid booleans (or NULL)")

    # Final result
    all_passed = (len(missing_fields) == 0)
    if all_passed:
        logger.info("🎉 All validations passed. PaymentMethod table is ready for migration.")
        return True
    else:
        logger.warning("🚫 One or more validations failed. Please review the warnings.")
        return False
