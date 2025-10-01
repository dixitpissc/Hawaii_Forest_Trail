from migration.D09_taxagency_migrator import resume_or_post_taxagencies
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger
from storage.sqlserver.sql import ensure_schema_exists

SOURCE_SCHEMA  = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")

# Ensure mapping schema exists
ensure_schema_exists(MAPPING_SCHEMA)

def initiating_taxagency_migration():
    """
    Checks if the TaxAgency table in the source schema exists and has any records.
    If records exist, initiates the migration using migrate_taxagencies().
    """
    try:
        logger.info("🔎 Checking for TaxAgency records in source database...")

        # Step 1: Check if source table exists
        table_check_query = f"""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'TaxAgency'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.TaxAgency' does not exist. Skipping migration.")
            return

        # Step 2: Count rows
        count_query = f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[TaxAgency]"
        count = sql.fetch_scalar(count_query)

        if count > 0:
            logger.info(f"✅ Found {count} TaxAgency records. Starting migration...")
            resume_or_post_taxagencies()
            logger.info("🎯 TaxAgency migration completed.")
        else:
            logger.info("⏩ No TaxAgency records found. Skipping migration.")

    except Exception as e:
        logger.error(f"❌ Error during TaxAgency migration check: {str(e)}")
