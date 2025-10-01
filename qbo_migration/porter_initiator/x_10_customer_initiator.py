from migration.D10_customer_migrator import migrate_customers
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger
from validator.pre_validation.D10_customer_validator import validate_customer_table

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

def initiating_customer_migration():
    """
    Checks if the Customer table in the source schema exists and has any records.
    Runs pre-validation before initiating the migration using the migrate_customers function.
    Migration proceeds even if validation fails, but a warning is logged.
    """
    try:
        logger.info("🔎 Checking for Customer records in source database...")

        # Step 1: Check if the table exists
        table_check_query = f"""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'Customer'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Customer' does not exist. Skipping migration.")
            return

        # Step 2: Check if any records exist
        count_query = f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Customer]"
        count = sql.fetch_scalar(count_query)

        if count > 0:
            logger.info(f"✅ Found {count} Customer records.")

            # Step 3: Run pre-validation
            logger.info("🧪 Running pre-validation for Customer table...")
            is_valid = validate_customer_table(schema=SOURCE_SCHEMA)

            if not is_valid:
                logger.warning("⚠️ Customer validation failed. Migration will still proceed, but may encounter errors.")

            # Step 4: Proceed with migration
            logger.info("🚀 Starting Customer migration...")
            migrate_customers()
            logger.info("🎯 Customer migration completed.")
        else:
            logger.info("⏩ No Customer records found. Skipping migration.")
    except Exception as e:
        logger.error(f"❌ Error during Customer migration check: {str(e)}")
