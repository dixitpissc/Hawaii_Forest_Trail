from migration.D09_taxrate_migrator import resume_or_post_taxrates
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger
from storage.sqlserver.sql import ensure_schema_exists

SOURCE_SCHEMA  = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")

ensure_schema_exists(MAPPING_SCHEMA)

def initiating_taxrate_migration():
    """
    Checks if the TaxRate table exists and has records; if yes, runs migrate_taxrates().
    """
    try:
        logger.info("🔎 Checking for TaxRate records in source database...")

        # Step 1: table exists?
        table_check_query = f"""
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'TaxRate'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.TaxRate' does not exist. Skipping migration.")
            return

        # Step 2: count
        count_query = f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[TaxRate]"
        count = sql.fetch_scalar(count_query)

        if count and count > 0:
            logger.info(f"✅ Found {count} TaxRate records. Starting migration...")
            resume_or_post_taxrates()
            logger.info("🎯 TaxRate migration completed.")
        else:
            logger.info("⏩ No TaxRate records found. Skipping migration.")

    except Exception as e:
        logger.error(f"❌ Error during TaxRate migration check: {str(e)}")
