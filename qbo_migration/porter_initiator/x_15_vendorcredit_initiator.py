from migration.D15_vendorcredit_migrator import smart_vendorcredit_migration
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
VENDORCREDIT_DATE_FROM='1900-01-01'
VENDORCREDIT_DATE_TO='2080-12-31'


def initiating_vendorcredit_migration():
    """
    Checks if both VendorCredit and VendorCredit_Line tables in the source schema exist and contain records.
    If both conditions are satisfied, initiates the vendor credit migration using resume_or_post_vendorcredits().
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for VendorCredit and VendorCredit_Line records in source database...")

        # Table existence check
        table_check_query = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' 
              AND TABLE_NAME IN ('VendorCredit', 'VendorCredit_Line')
        """
        existing_tables = sql.fetch_dataframe(table_check_query)["TABLE_NAME"].tolist()

        if "VendorCredit" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.VendorCredit' does not exist. Skipping VendorCredit migration.")
            return
        if "VendorCredit_Line" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.VendorCredit_Line' does not exist. Skipping VendorCredit migration.")
            return

        # Count records
        count_header = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[VendorCredit]")
        count_lines = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[VendorCredit_Line]")

        if count_header > 0 and count_lines > 0:
            logger.info(f"✅ Found {count_header} VendorCredit and {count_lines} VendorCredit_Line records. Starting migration...")
            smart_vendorcredit_migration(VENDORCREDIT_DATE_FROM,VENDORCREDIT_DATE_TO)
            logger.info("🎯 VendorCredit migration completed.")
        else:
            logger.info("⏩ Skipping VendorCredit migration due to no data in one or both tables.")

    except Exception as e:
        logger.error(f"❌ Error during VendorCredit migration check: {str(e)}")

