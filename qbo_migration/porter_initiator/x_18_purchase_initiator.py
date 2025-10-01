from migration.D18_purchase_migrator import smart_purchase_migration
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
PURCHASE_DATE_FROM='1900-01-01'
PURCHASE_DATE_TO='2080-12-31'

def initiating_purchase_migration():
    """
    Checks if both Purchase and Purchase_Line tables in the source schema exist and contain records.
    If both conditions are satisfied, initiates the purchase migration using resume_or_post_purchases().
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for Purchase and Purchase_Line records in source database...")

        # Table existence check
        table_check_query = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' 
              AND TABLE_NAME IN ('Purchase', 'Purchase_Line')
        """
        existing_tables = sql.fetch_dataframe(table_check_query)["TABLE_NAME"].tolist()

        if "Purchase" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Purchase' does not exist. Skipping Purchase migration.")
            return
        if "Purchase_Line" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Purchase_Line' does not exist. Skipping Purchase migration.")
            return

        # Count records
        count_header = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Purchase]")
        count_lines = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Purchase_Line]")

        if count_header > 0 and count_lines > 0:
            logger.info(f"✅ Found {count_header} Purchase and {count_lines} Purchase_Line records. Starting migration...")
            smart_purchase_migration(PURCHASE_DATE_FROM,PURCHASE_DATE_TO)
            logger.info("🎯 Purchase migration completed.")
        else:
            logger.info("⏩ Skipping Purchase migration due to no data in one or both tables.")

    except Exception as e:
        logger.error(f"❌ Error during Purchase migration check: {str(e)}")
