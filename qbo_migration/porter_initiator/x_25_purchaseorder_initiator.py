# porter_initiator/

import os
from storage.sqlserver import sql
from utils.log_timer import global_logger as logger

# Import the orchestrator from your migrator
from migration.D25_purchaseorder_migrator import smrt_purchaseorder_migration  # same role as resume_or_post_transfers

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

def initiating_PurchaseOrder_migration():
    """
    Checks if the PurchaseOrder tables exist and contain records.
    If records exist, initiates the PurchaseOrder migration via migrate_PurchaseOrder().
    Logs progress and outcomes similar to your transfer initiator.
    """
    try:
        logger.info("🔎 Checking for PurchaseOrder records in source database...")

        # Check header table existence
        tbl_exists_query = f"""
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'PurchaseOrder'
        """
        exists = sql.fetch_scalar(tbl_exists_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.PurchaseOrder' does not exist. Skipping PurchaseOrder migration.")
            return

        # Count header records
        count_header = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[PurchaseOrder]")

        if count_header and count_header > 0:
            logger.info(f"✅ Found {count_header} PurchaseOrder records. Starting migration...")
            smrt_purchaseorder_migration()
            logger.info("🎯 PurchaseOrder migration completed.")
        else:
            logger.info("⏩ Skipping PurchaseOrder migration due to no records found.")

    except Exception as e:
        logger.error(f"❌ Error during PurchaseOrder migration check: {str(e)}")
