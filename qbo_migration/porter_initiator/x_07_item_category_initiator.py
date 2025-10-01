from migration.D07_item_category_migrator import main
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

def initiating_item_category_migration():
    """
    Checks if the Item table in the source schema exists and has any records.
    If records exist, initiates the migration using the main function.
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for Item records in source database...")

        # Step 1: Check if the table exists
        table_check_query = f"""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'Item'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Item' does not exist. Skipping migration.")
            return

        # Step 2: Check if any records exist
        count_query = f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Item]"
        count = sql.fetch_scalar(count_query)

        if count > 0:
            logger.info(f"✅ Found {count} Item records. Starting migration...")
            main()
            logger.info("🎯 Item migration completed.")
        else:
            logger.info("⏩ No Item records found. Skipping migration.")
    except Exception as e:
        logger.error(f"❌ Error during Item migration check: {str(e)}")
