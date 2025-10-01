from migration.D03_currency_migrator import migrate_currencies
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger
from validator.pre_validation.D03_currency_validator import validate_companycurrency_table

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

def initiating_currency_migration():
    """
    Checks if the Currency table in the source schema exists and has any records.
    If records exist, initiates the migration using the migrate_currencies function.
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for Currency records in source database...")

        # Step 1: Check if the table exists
        table_check_query = f"""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'CompanyCurrency'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.CompanyCurrency' does not exist. Skipping migration.")
            return

        # Step 2: Check if any records exist
        count_query = f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[CompanyCurrency]"
        count = sql.fetch_scalar(count_query)

        if count > 0:
            logger.info(f"✅ Found {count} CompanyCurrency records. Starting migration...")
            logger.info(f"✅ Starting Validation Process...")
            try:
                ok = validate_companycurrency_table()
                if ok:
                    migrate_currencies()
                    logger.info("🎯 CompanyCurrency migration completed.")
                else:
                    logger.error("🎯 CompanyCurrency started with possible failure..")
                    migrate_currencies()
                    logger.info("🎯 CompanyCurrency migration completed.")
            except Exception as e:
                logger.error(f"❌ Error during CompanyCurrency validation or migration: {str(e)}")
        else:
            logger.info("⏩ No CompanyCurrency records found. Skipping migration.")

    except Exception as e:
        logger.error(f"❌ Error during Currency migration check: {str(e)}")
