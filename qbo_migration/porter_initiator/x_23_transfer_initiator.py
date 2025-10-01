
from migration.D23_transfer_migrator import smrt_transfer_migration
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
# Transfer
TRANSFER_DATE_FROM = "1900-01-01"
TRANSFER_DATE_TO = "2080-12-31"


def initiating_transfer_migration():
    """
    Checks if the Transfer table in the source schema exists and contains records.
    If records exist, initiates the transfer migration using smrt_transfer_migration
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for Transfer records in source database...")

        # Table existence check
        table_check_query = f"""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'Transfer'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Transfer' does not exist. Skipping Transfer migration.")
            return

        # Count records
        count_header = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Transfer]")

        if count_header > 0:
            logger.info(f"✅ Found {count_header} Transfer records. Starting migration...")
            smrt_transfer_migration(TRANSFER_DATE_FROM,TRANSFER_DATE_TO)
            logger.info("🎯 Transfer migration completed.")
        else:
            logger.info("⏩ Skipping Transfer migration due to no records found.")

    except Exception as e:
        logger.error(f"❌ Error during Transfer migration check: {str(e)}")
