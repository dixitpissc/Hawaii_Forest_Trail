# porter_initiator/x_estimate_initiator.py

import os
from storage.sqlserver import sql
from utils.log_timer import global_logger as logger

# Import the orchestrator from your migrator
from migration.D11_estimate_migrator import resume_or_post_estimates  # same role as resume_or_post_transfers

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

def initiating_estimate_migration():
    """
    Checks if the Estimate tables exist and contain records.
    If records exist, initiates the estimate migration via migrate_estimates().
    Logs progress and outcomes similar to your transfer initiator.
    """
    try:
        logger.info("🔎 Checking for Estimate records in source database...")

        # Check header table existence
        tbl_exists_query = f"""
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'Estimate'
        """
        exists = sql.fetch_scalar(tbl_exists_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Estimate' does not exist. Skipping Estimate migration.")
            return

        # Count header records
        count_header = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Estimate]")

        if count_header and count_header > 0:
            logger.info(f"✅ Found {count_header} Estimate records. Starting migration...")
            resume_or_post_estimates()
            logger.info("🎯 Estimate migration completed.")
        else:
            logger.info("⏩ Skipping Estimate migration due to no records found.")

    except Exception as e:
        logger.error(f"❌ Error during Estimate migration check: {str(e)}")
