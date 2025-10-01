from migration.D22_billpayment_migrator import resume_or_post_billpayments
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

BILLPAYMENT_DATE_FROM = "1900-01-01"
BILLPAYMENT_DATE_TO = "2080-12-31"

def initiating_billpayment_migration():
    """
    Checks if the BillPayment table in the source schema exists and contains records.
    If records exist, initiates the bill payment migration using resume_or_post_billpayments().
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for BillPayment records in source database...")

        # Table existence check
        table_check_query = f"""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'BillPayment'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.BillPayment' does not exist. Skipping BillPayment migration.")
            return

        # Count records
        count_header = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[BillPayment]")

        if count_header > 0:
            logger.info(f"✅ Found {count_header} BillPayment records. Starting migration...")
            resume_or_post_billpayments(BILLPAYMENT_DATE_FROM,BILLPAYMENT_DATE_TO)
            logger.info("🎯 BillPayment migration completed.")
        else:
            logger.info("⏩ Skipping BillPayment migration due to no records found.")

    except Exception as e:
        logger.error(f"❌ Error during BillPayment migration check: {str(e)}")
