from migration.D12_invoice_migrator import smrt_invoice_migration
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

INVOICE_DATE_FROM="1900-01-01"
INVOICE_DATE_TO="2080-12-31"

def initiating_invoice_migration():
    """
    Checks if both Invoice and Invoice_Line tables in the source schema exist and contain records.
    If both conditions are satisfied, initiates the invoice migration using resume_or_post_invoices().
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for Invoice and Invoice_Line records in source database...")

        # Table existence checks
        table_check_query = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' 
              AND TABLE_NAME IN ('Invoice', 'Invoice_Line')
        """
        existing_tables = sql.fetch_dataframe(table_check_query)["TABLE_NAME"].tolist()

        if "Invoice" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Invoice' does not exist. Skipping Invoice migration.")
            return
        if "Invoice_Line" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Invoice_Line' does not exist. Skipping Invoice migration.")
            return

        # Record count checks
        count_invoice = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Invoice]")
        count_lines = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Invoice_Line]")

        if count_invoice > 0 and count_lines > 0:
            logger.info(f"✅ Found {count_invoice} Invoice and {count_lines} Invoice_Line records. Starting migration...")
            smrt_invoice_migration(INVOICE_DATE_FROM,INVOICE_DATE_TO)
            logger.info("🎯 Invoice migration completed.")
        else:
            logger.info("⏩ Skipping Invoice migration due to no data in one or both tables.")

    except Exception as e:
        logger.error(f"❌ Error during Invoice migration check: {str(e)}")
