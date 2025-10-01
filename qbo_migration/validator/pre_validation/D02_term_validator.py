import pandas as pd
import os
from dotenv import load_dotenv
from storage.sqlserver.sql import fetch_table
from utils.logger_builder import build_logger

load_dotenv()

def validate_term_table(database=None, schema=None, table: str = "Term"):
    """
    Validates source Term table before migrating to QBO.

    Checks:
    - Required field: Name
    - Duplicate Name
    - Name length sanity check (e.g. <= 100)
    - Numeric types for DueDays, DiscountDays, DiscountPercent
    - Boolean values in Active
    """
    if database is None:
        database = os.getenv("SQLSERVER_DATABASE")
    if schema is None:
        schema = os.getenv("SOURCE_SCHEMA", "dbo")

    logger = build_logger("term_validation")
    logger.info(f"🔍 Starting validation for [{schema}].[{table}] in database [{database}]")
    logger.info("🔗 API Doc: https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/term")

    df = fetch_table(table=table, schema=schema)
    if df.empty:
        logger.error("❌ Term table is empty or not found.")
        return False

    logger.info(f"📊 Rows: {len(df)}, Columns: {len(df.columns)}")

    missing_fields = []
    if "Name" not in df.columns:
        missing_fields.append("Name")
        logger.warning("❌ Column missing: Name")
    else:
        logger.info("✅ Column present: Name")

    # Name validations
    if "Name" in df.columns:
        if df["Name"].isnull().any():
            logger.warning("⚠️ Some 'Name' values are NULL — will fail QBO import.")
        if df["Name"].astype(str).map(len).gt(100).any():
            logger.warning("⚠️ Some 'Name' values exceed 100 characters (verify actual API max length).")
        duplicates = df[df.duplicated(subset=["Name"], keep=False)]
        if not duplicates.empty:
            logger.warning(f"❗ Duplicate Term names: {duplicates['Name'].unique().tolist()}")
        else:
            logger.info("✅ No duplicate Term names found")

    # DueDays, DiscountDays, DiscountPercent numeric
    for col in ["DueDays", "DiscountDays", "DiscountPercent"]:
        if col in df.columns:
            invalid = df[df[col].notnull() & ~df[col].apply(lambda x: isinstance(x, (int, float)))]
            if not invalid.empty:
                logger.warning(f"⚠️ Column '{col}' has non-numeric values.")
            else:
                logger.info(f"✅ Column '{col}' numeric or null")

    # Validate Active
    if "Active" in df.columns:
        df["Active_str"] = df["Active"].astype(str).str.lower()
        valid = df["Active_str"].isin(["true", "false", "nan"])
        if not valid.all():
            logger.warning("⚠️ Invalid boolean values in 'Active'. Use true/false or null.")
        else:
            logger.info("✅ 'Active' values valid (or null)")

    # Final
    success = (len(missing_fields) == 0)
    if success:
        logger.info("🎉 Term table validation passed")
        return True
    else:
        logger.warning("🚫 Term validation failed; review warnings")
        return False
