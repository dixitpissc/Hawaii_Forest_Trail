from migration.D02_term_migrator import migrate_terms
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger
from validator.pre_validation.D02_term_validator import validate_term_table

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

def initiating_term_migration():
    """
    Checks if the Term table in the source schema exists and has any records.
    If records exist, initiates the migration using the migrate_terms function.
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for Term records in source database...")

        # Step 1: Check if the table exists
        table_check_query = f"""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'Term'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Term' does not exist. Skipping migration.")
            return

        # Step 2: Check if any records exist
        count_query = f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Term]"
        count = sql.fetch_scalar(count_query)

        if count > 0:
            logger.info(f"✅ Found {count} Term records. Starting validation...")
            try:
                validation_passed = validate_term_table()
                if validation_passed:
                    migrate_terms()
                    logger.info("🎯 Term migration completed.")
                else:
                    logger.warning("🚫 Migration continue with possible failure..")
                    migrate_terms()
            except Exception as e:
                logger.error(f"❌ Error during Term validation or migration: {str(e)}")
        else:
            logger.info("⏩ No Term records found. Skipping migration.")
    except Exception as e:
        logger.error(f"❌ Error during Term migration check: {str(e)}")
