from migration.D21_payment_migrator import smart_payment_migration
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
PAYMENT_DATE_FROM = "1900-01-01"
PAYMENT_DATE_TO = "2080-12-31"


def initiating_payment_migration():
    """
    Checks if the Payment table in the source schema exists and contains records.
    If records exist, initiates the payment migration using resume_or_post_payments().
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for Payment records in source database...")

        # Table existence check
        table_check_query = f"""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'Payment'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Payment' does not exist. Skipping Payment migration.")
            return

        # Count records
        count_header = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Payment]")

        if count_header > 0:
            logger.info(f"✅ Found {count_header} Payment records. Starting migration...")
            smart_payment_migration(PAYMENT_DATE_FROM,PAYMENT_DATE_TO)
            logger.info("🎯 Payment migration completed.")
        else:
            logger.info("⏩ Skipping Payment migration due to no records found.")

    except Exception as e:
        logger.error(f"❌ Error during Payment migration check: {str(e)}")
