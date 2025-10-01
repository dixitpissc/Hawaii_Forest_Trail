from migration.D08_item_migrator import migrate_items
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger
from validator.pre_validation.D08_item_validator import validate_item_table

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

def initiating_item_migration():
    """
    Checks if the Item table in the source schema exists and has any records.
    Runs pre-validation and logs results.
    Migration proceeds regardless of validation outcome, with clear warnings if validation fails.
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
            logger.info(f"✅ Found {count} Item records.")

            # Step 3: Run pre-validation
            logger.info("🧪 Running pre-validation for Item table...")
            is_valid = validate_item_table(schema=SOURCE_SCHEMA)

            if not is_valid:
                logger.warning("⚠️ Item validation failed. Migration will still proceed, but may encounter errors.")

            # Step 4: Proceed with migration regardless of validation result
            logger.info("🚀 Starting Item migration...")
            migrate_items()
            logger.info("🎯 Item migration completed.")
        else:
            logger.info("⏩ No Item records found. Skipping migration.")
    except Exception as e:
        logger.error(f"❌ Error during Item migration check: {str(e)}")
