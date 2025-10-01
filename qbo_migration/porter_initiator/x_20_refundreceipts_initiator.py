from migration.D20_refundreceipt_migrator import smart_refundreceipt_migration
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
REFUNDRECEIPT_DATE_FROM ='1900-01-01'
REFUNDRECEIPT_DATE_TO = '2080-12-31'

def initiating_refundreceipt_migration():
    """
    Checks if both RefundReceipt and RefundReceipt_Line tables in the source schema exist and contain records.
    If both conditions are satisfied, initiates the refund receipt migration using resume_or_post_refundreceipts().
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for RefundReceipt and RefundReceipt_Line records in source database...")

        # Table existence check
        table_check_query = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' 
              AND TABLE_NAME IN ('RefundReceipt', 'RefundReceipt_Line')
        """
        existing_tables = sql.fetch_dataframe(table_check_query)["TABLE_NAME"].tolist()

        if "RefundReceipt" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.RefundReceipt' does not exist. Skipping RefundReceipt migration.")
            return
        if "RefundReceipt_Line" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.RefundReceipt_Line' does not exist. Skipping RefundReceipt migration.")
            return

        # Count records
        count_header = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[RefundReceipt]")
        count_lines = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[RefundReceipt_Line]")

        if count_header > 0 and count_lines > 0:
            logger.info(f"✅ Found {count_header} RefundReceipt and {count_lines} RefundReceipt_Line records. Starting migration...")
            smart_refundreceipt_migration(REFUNDRECEIPT_DATE_FROM,REFUNDRECEIPT_DATE_TO)
            logger.info("🎯 RefundReceipt migration completed.")
        else:
            logger.info("⏩ Skipping RefundReceipt migration due to no data in one or both tables.")

    except Exception as e:
        logger.error(f"❌ Error during RefundReceipt migration check: {str(e)}")
