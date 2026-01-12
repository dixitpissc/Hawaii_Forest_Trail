import importlib
# from logger_builder import logger

# def run_entity_migration(entity_name: str, retry_from_last_pending: bool = False):
#     """
#     Dynamically triggers the migration function for a given QBO entity.

#     Args:
#         entity_name (str): The entity to migrate (e.g. 'Customer', 'Invoice')
#         retry_from_last_pending (bool): If True, only retry records with status NULL or Failed
#     """
#     print(f"\n🚀 Starting Dynamic Migration for Entity: {entity_name}\n" + "=" * 50)
#     try:
#         module = importlib.import_module(f"migration.{entity_name.lower()}_migrator")
#         migrate_func = getattr(module, f"migrate_{entity_name.lower()}s")  # e.g., migrate_customers
#     except (ModuleNotFoundError, AttributeError) as e:
#         print(f"❌ Could not find migration module/function for '{entity_name}': {e}")
#         return

#     try:
#         if retry_from_last_pending:
#             migrate_func(retry_only=True)
#         else:
#             migrate_func()
#     except Exception as e:
#         print(f"❌ Error during migration of {entity_name}: {e}")

from datetime import datetime, timedelta
from utils.log_timer import ProgressTimer
from utils.token_refresher import refresh_qbo_token
from storage.sqlserver import sql
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def conditionally_migrate_entity(
    entity_name: str,
    source_table: str,
    source_schema: str,
    mapping_table: str,
    mapping_schema: str,
    enrich_func,
    post_func,
    ensure_mapping_func,
    max_runtime_minutes: int = 30
):
    """
    Generic conditional migration function for any entity.

    Args:
        entity_name (str): Name for logging (e.g., 'Vendor')
        source_table (str): Name of source SQL Server table
        source_schema (str): Schema of source SQL Server table
        mapping_table (str): Name of mapping table (e.g., 'Map_Vendor')
        mapping_schema (str): Schema for mapping table
        enrich_func (callable): Function to enrich mapping table with target refs + payloads
        post_func (callable): Function to post each record to QBO
        ensure_mapping_func (callable): Function to create/refresh the mapping table
        max_runtime_minutes (int): Time in minutes before refreshing token
    """
    logger.info(f"🔍 Checking migration state for {entity_name}...")

    df_source = sql.fetch_table(source_table, source_schema)
    source_count = len(df_source)

    # Step 1: If mapping table doesn't exist — full setup
    if not sql.table_exists(mapping_table, mapping_schema):
        logger.warning(f"⚠️ {mapping_table} missing — initializing fresh migration.")
        ensure_mapping_func()
        enrich_func()
        df_mapping = sql.fetch_table(mapping_table, mapping_schema)
    else:
        df_mapping = sql.fetch_table(mapping_table, mapping_schema)
        mapping_count = len(df_mapping)

        if mapping_count == source_count:
            logger.info(f"✅ Mapping table aligned for {entity_name} — resuming pending records only.")
            df_mapping = df_mapping[df_mapping["Porter_Status"].isna()]
            if df_mapping.empty:
                logger.info(f"🎉 All {entity_name} records already migrated.")
                return
        else:
            logger.warning(f"🔁 {entity_name} mapping table out of sync — rebuilding...")
            ensure_mapping_func()
            enrich_func()
            df_mapping = sql.fetch_table(mapping_table, mapping_schema)

    # Step 2: Prepare for posting
    df_mapping["Retry_Count"] = pd.to_numeric(df_mapping["Retry_Count"], errors="coerce").fillna(0).astype(int)

    timer = ProgressTimer(len(df_mapping))
    start_time = datetime.now()

    for _, row in df_mapping.iterrows():
        if (datetime.now() - start_time) > timedelta(minutes=max_runtime_minutes):
            logger.info("⏳ Runtime limit reached — refreshing QBO token")
            refresh_qbo_token()
            start_time = datetime.now()
        post_func(row)
        timer.update()

    logger.info(f"✅ Conditional migration completed for {entity_name}.")



#Use case
# from utils.conditional_migrator import conditionally_migrate_entity

# if __name__ == "__main__":
#     conditionally_migrate_entity(
#         entity_name="Vendor",
#         source_table="Vendor",
#         source_schema=os.getenv("SOURCE_SCHEMA", "dbo"),
#         mapping_table="Map_Vendor",
#         mapping_schema=os.getenv("MAPPING_SCHEMA", "porter_entities_mapping"),
#         enrich_func=enrich_mapping_with_targets,
#         post_func=post_vendor,
#         ensure_mapping_func=ensure_mapping_table_exists
#     )
