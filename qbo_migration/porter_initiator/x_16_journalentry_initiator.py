from migration.D16_journalentry_migrator import smart_journalentry_migration    
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

JOURNALENTRY_DATE_FROM='1900-01-01'
JOURNALENTRY_DATE_TO='2080-12-31'

def initiating_journalentry_migration():
    """
    Checks if both JournalEntry and JournalEntry_Line tables in the source schema exist and contain records.
    If both conditions are satisfied, initiates the journal entry migration using resume_or_post_journalentries().
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for JournalEntry and JournalEntry_Line records in source database...")

        # Table existence check
        table_check_query = f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' 
              AND TABLE_NAME IN ('JournalEntry', 'JournalEntry_Line')
        """
        existing_tables = sql.fetch_dataframe(table_check_query)["TABLE_NAME"].tolist()

        if "JournalEntry" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.JournalEntry' does not exist. Skipping JournalEntry migration.")
            return
        if "JournalEntry_Line" not in existing_tables:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.JournalEntry_Line' does not exist. Skipping JournalEntry migration.")
            return

        # Count records
        count_header = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[JournalEntry]")
        count_lines = sql.fetch_scalar(f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[JournalEntry_Line]")

        if count_header > 0 and count_lines > 0:
            logger.info(f"✅ Found {count_header} JournalEntry and {count_lines} JournalEntry_Line records. Starting migration...")
            smart_journalentry_migration(JOURNALENTRY_DATE_FROM,JOURNALENTRY_DATE_TO)
            logger.info("🎯 JournalEntry migration completed.")
        else:
            logger.info("⏩ Skipping JournalEntry migration due to no data in one or both tables.")

    except Exception as e:
        logger.error(f"❌ Error during JournalEntry migration check: {str(e)}")
