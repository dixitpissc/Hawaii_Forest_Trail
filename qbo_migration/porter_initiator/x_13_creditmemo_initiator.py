from migration.D13_creditmemo_migrator import smart_creditmemo_migration
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

CREDITMEMO_DATE_FROM = "1900-01-01"
CREDITMEMO_DATE_TO = "2080-12-31"

def initiating_creditmemo_migration():
    """
    Checks if both CreditMemo and CreditMemo_Line tables in the source schema exist and contain records.
    If both conditions are satisfied, initiates the credit memo migration using resume_or_post_creditmemos().
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for CreditMemo and CreditMemo_Line records in source database...")

        # Table existence check
        table_check_query = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' 
              AND TABLE_NAME IN ('CreditMemo', 'CreditMemo_Line')
        """
        existing_tables = sql.fetch_dataframe(table_check_query)["TABLE_NAME"].tolist()

        if "CreditMemo" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.CreditMemo' does not exist. Skipping CreditMemo migration.")
            return
        if "CreditMemo_Line" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.CreditMemo_Line' does not exist. Skipping CreditMemo migration.")
            return

        # Count records
        count_header = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[CreditMemo]")
        count_lines = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[CreditMemo_Line]")

        if count_header > 0 and count_lines > 0:
            logger.info(f"✅ Found {count_header} CreditMemo and {count_lines} CreditMemo_Line records. Starting migration...")
            smart_creditmemo_migration(CREDITMEMO_DATE_FROM,CREDITMEMO_DATE_TO)
            logger.info("🎯 CreditMemo migration completed.")
        else:
            logger.info("⏩ Skipping CreditMemo migration due to no data in one or both tables.")

    except Exception as e:
        logger.error(f"❌ Error during CreditMemo migration check: {str(e)}")
