# porter_initiator/x_inventory_adjustment_initiator.py

from storage.sqlserver import sql
from utils.log_timer import global_logger as logger
from migration.D26_inventoryadjustment_migrator import (
    migrate_inventoryadjustments,
    resume_or_post_inventoryadjustments,
)

def initiating_inventory_adjustment_migration(source_schema="dbo"):
    logger.info("🔎 Checking for InventoryAdjustment records in source database...")
    cnt = sql.fetch_single_value(
        f"SELECT COUNT(1) FROM [{source_schema}].[InventoryAdjustment]",
        ()  # <-- REQUIRED: empty params tuple
    )
    if not cnt or int(cnt) == 0:
        logger.info("ℹ️ No InventoryAdjustment records found. Skipping.")
        return

    logger.info(f"✅ Found {cnt} InventoryAdjustment records. Starting migration...")
    # Pick one flow (kept same as earlier)
    # migrate_inventoryadjustments()
    resume_or_post_inventoryadjustments()
