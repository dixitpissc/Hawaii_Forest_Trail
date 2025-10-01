from migration.D06_account_migrator import migrate_accounts,retry_failed_accounts
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger
from validator.pre_validation.D06_account_validator import validate_account_table

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

def initiating_account_migration():
    """
    Checks if the Account table in the source schema exists and has any records.
    If records exist, initiates the migration using the migrate_accounts function.
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for Account records in source database...")

        # Step 1: Check if the table exists
        table_check_query = f"""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'Account'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Account' does not exist. Skipping migration.")
            return

        # Step 2: Check if any records exist
        count_query = f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Account]"
        count = sql.fetch_scalar(count_query)

        if count > 0:
            logger.info(f"✅ Found {count} Account records. Starting migration...")
            logger.info("✅ Running validation...")
            try:
                Validation_Passed = validate_account_table()
                if Validation_Passed:
                    migrate_accounts()
                    # retry_failed_accounts()
                    logger.info("🎯 Account migration completed.")
                else:
                    logger.warning("🚫 Starting Accounts with possible failure..")
                    migrate_accounts()
                    # retry_failed_accounts()
            except Exception as e:
                logger.error(f"❌ Error during Account validation/migration: {str(e)}")
        else:
            logger.info("⏩ No Account records found. Skipping migration.")

    except Exception as e:
        logger.error(f"❌ Error during Account migration check: {str(e)}")
