from migration.D05_department_migrator import migrate_departments
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger
from validator.pre_validation.D05_department_validator import validate_department_table

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

def initiating_department_migration():
    """
    Checks if the Department table in the source schema exists and has any records.
    If records exist, initiates the migration using the migrate_departments function.
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for Department records in source database...")

        # Step 1: Check if the table exists
        table_check_query = f"""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'Department'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Department' does not exist. Skipping migration.")
            return

        # Step 2: Check if any records exist
        count_query = f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Department]"
        count = sql.fetch_scalar(count_query)

        if count > 0:
            logger.info(f"✅ Found {count} Department records. Starting migration...")
            logger.info("✅ Starting Validation Process...")
            try:
                valid = validate_department_table()
                if valid:
                    migrate_departments()
                    logger.info("🎯 Department migration completed.")
                else:
                    logger.warning("🚫 Starting Department migration with possible failure.")
                    migrate_departments()
            except Exception as e:
                logger.error(f"❌ Error during Department validation or migration: {str(e)}")
        else:
            logger.info("⏩ No Department records found. Skipping migration.")

    except Exception as e:
        logger.error(f"❌ Error during Department migration check: {str(e)}")
