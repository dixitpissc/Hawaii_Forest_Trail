from migration.D04_class_migrator import migrate_classes
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger
from validator.pre_validation.D04_class_validator import validate_class_table

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

def initiating_class_migration():
    """
    Checks if the Class table in the source schema exists and has any records.
    If records exist, initiates the migration using the migrate_classes function.
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for Class records in source database...")

        # Step 1: Check if the table exists
        table_check_query = f"""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'Class'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Class' does not exist. Skipping migration.")
            return

        # Step 2: Check if any records exist
        count_query = f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Class]"
        count = sql.fetch_scalar(count_query)

        if count > 0:
            try:
                validation_passed = validate_class_table()
                if validation_passed:
                    migrate_classes()
                    logger.info("🎯 Class migration completed.")
                else:
                    logger.warning("🚫 Started migration with possible failure..")
                    migrate_classes()
            except Exception as e:
                logger.error(f"❌ Error during Class validation or migration: {str(e)}")
            logger.info(f"✅ Found {count} Class records. Starting migration...")
            
            logger.info("🎯 Class migration completed.")
        else:
            logger.info("⏩ No Class records found. Skipping migration.")
    except Exception as e:
        logger.error(f"❌ Error during Class migration check: {str(e)}")
