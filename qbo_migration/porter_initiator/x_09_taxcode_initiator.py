from migration.D09_taxcode_migrator import resume_or_post_taxcodes
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA  = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")

def initiating_taxcode_migration():
    """
    Checks if the TaxCode table exists and has records; if yes, runs resume_or_post_taxcodes().
    """
    try:
        logger.info("🔎 Checking for TaxCode records in source database...")

        table_check_query = f"""
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'TaxCode'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.TaxCode' does not exist. Skipping migration.")
            return

        count_query = f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[TaxCode]"
        count = sql.fetch_scalar(count_query)

        if count and count > 0:
            logger.info(f"✅ Found {count} TaxCode records. Starting migration...")
            resume_or_post_taxcodes()
            logger.info("🎯 TaxCode migration completed.")
        else:
            logger.info("⏩ No TaxCode records found. Skipping migration.")

    except Exception as e:
        logger.error(f"❌ Error during TaxCode migration check: {str(e)}")
