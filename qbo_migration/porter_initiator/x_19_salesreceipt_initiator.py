from migration.D19_salesreceipt_migrator import smart_salesreceipt_migration
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
SALESRECEIPT_DATE_FROM='1900-01-01' 
SALESRECEIPT_DATE_TO='2080-12-31'

def initiating_salesreceipt_migration():
    """
    Checks if both SalesReceipt and SalesReceipt_Line tables in the source schema exist and contain records.
    If both conditions are satisfied, initiates the sales receipt migration using resume_or_post_salesreceipts().
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for SalesReceipt and SalesReceipt_Line records in source database...")

        # Table existence check
        table_check_query = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' 
              AND TABLE_NAME IN ('SalesReceipt', 'SalesReceipt_Line')
        """
        existing_tables = sql.fetch_dataframe(table_check_query)["TABLE_NAME"].tolist()

        if "SalesReceipt" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.SalesReceipt' does not exist. Skipping SalesReceipt migration.")
            return
        if "SalesReceipt_Line" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.SalesReceipt_Line' does not exist. Skipping SalesReceipt migration.")
            return

        # Count records
        count_header = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[SalesReceipt]")
        count_lines = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[SalesReceipt_Line]")

        if count_header > 0 and count_lines > 0:
            logger.info(f"✅ Found {count_header} SalesReceipt and {count_lines} SalesReceipt_Line records. Starting migration...")
            smart_salesreceipt_migration(SALESRECEIPT_DATE_FROM, SALESRECEIPT_DATE_TO)
            logger.info("🎯 SalesReceipt migration completed.")
        else:
            logger.info("⏩ Skipping SalesReceipt migration due to no data in one or both tables.")

    except Exception as e:
        logger.error(f"❌ Error during SalesReceipt migration check: {str(e)}")
