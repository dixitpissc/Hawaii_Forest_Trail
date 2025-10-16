from migration.D17_deposit_migrator import smart_deposit_migration
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

DEPOSIT_DATE_FROM='1900-01-01'
DEPOSIT_DATE_TO='2080-12-31'

def initiating_deposit_migration():
    """
    Checks if both Deposit and Deposit_Line tables in the source schema exist and contain records.
    If both conditions are satisfied, initiates the deposit migration using resume_or_post_deposits().
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for Deposit and Deposit_Line records in source database...")

        # Table existence check
        table_check_query = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' 
              AND TABLE_NAME IN ('Deposit', 'Deposit_Line')
        """
        existing_tables = sql.fetch_dataframe(table_check_query)["TABLE_NAME"].tolist()

        if "Deposit" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Deposit' does not exist. Skipping Deposit migration.")
            return
        if "Deposit_Line" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Deposit_Line' does not exist. Skipping Deposit migration.")
            return

        # Count records
        count_header = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Deposit]")
        count_lines = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Deposit_Line]")

        if count_header > 0 and count_lines > 0:
            logger.info(f"✅ Found {count_header} Deposit and {count_lines} Deposit_Line records. Starting migration...")
            smart_deposit_migration(DEPOSIT_DATE_FROM,DEPOSIT_DATE_TO)
            logger.info("🎯 Deposit migration completed.")
        else:
            logger.info("⏩ Skipping Deposit migration due to no data in one or both tables.")

    except Exception as e:
        logger.error(f"❌ Error during Deposit migration check: {str(e)}")
