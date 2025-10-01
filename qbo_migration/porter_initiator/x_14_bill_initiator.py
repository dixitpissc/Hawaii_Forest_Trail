from migration.D14_bill_migrator import smart_bill_migration
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

BILL_DATE_FROM = '1900-01-01'
BILL_DATE_TO = '2080-12-31'

def initiating_bill_migration():
    """
    Checks if both Bill and Bill_Line tables in the source schema exist and contain records.
    If both conditions are satisfied, initiates the bill migration using resume_or_post_bills().
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for Bill and Bill_Line records in source database...")

        # Table existence check
        table_check_query = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' 
              AND TABLE_NAME IN ('Bill', 'Bill_Line')
        """
        existing_tables = sql.fetch_dataframe(table_check_query)["TABLE_NAME"].tolist()

        if "Bill" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Bill' does not exist. Skipping Bill migration.")
            return
        if "Bill_Line" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Bill_Line' does not exist. Skipping Bill migration.")
            return

        # Count records
        count_header = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Bill]")
        count_lines = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Bill_Line]")

        if count_header > 0 and count_lines > 0:
            logger.info(f"✅ Found {count_header} Bill and {count_lines} Bill_Line records. Starting migration...")
            smart_bill_migration(BILL_DATE_FROM,BILL_DATE_TO)
            logger.info("🎯 Bill migration completed.")
        else:
            logger.info("⏩ Skipping Bill migration due to no data in one or both tables.")

    except Exception as e:
        logger.error(f"❌ Error during Bill migration check: {str(e)}")
