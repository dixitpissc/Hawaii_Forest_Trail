from migration.D09_vendor_migrator import conditionally_migrate_vendors as migrate_vendors
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger
from validator.pre_validation.D09_vendor_validator import validate_vendor_table

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

def initiating_vendor_migration():
    """
    Checks if the Vendor table in the source schema exists and has any records.
    Runs pre-validation before initiating the migration using the migrate_vendors function.
    Migration proceeds regardless of validation result, but warnings are logged if validation fails.
    """
    try:
        logger.info("🔎 Checking for Vendor records in source database...")

        # Step 1: Check if the table exists
        table_check_query = f"""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'Vendor'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Vendor' does not exist. Skipping migration.")
            return

        # Step 2: Check if any records exist
        count_query = f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Vendor]"
        count = sql.fetch_scalar(count_query)

        if count > 0:
            logger.info(f"✅ Found {count} Vendor records.")

            # Step 3: Pre-validation
            logger.info("🧪 Running pre-validation for Vendor table...")
            is_valid = validate_vendor_table(schema=SOURCE_SCHEMA)

            if not is_valid:
                logger.warning("⚠️ Vendor validation failed. Migration will still proceed, but may encounter errors.")

            # Step 4: Proceed with migration
            logger.info("🚀 Starting Vendor migration...")
            migrate_vendors()
            logger.info("🎯 Vendor migration completed.")
        else:
            logger.info("⏩ No Vendor records found. Skipping migration.")
    except Exception as e:
        logger.error(f"❌ Error during Vendor migration check: {str(e)}")
