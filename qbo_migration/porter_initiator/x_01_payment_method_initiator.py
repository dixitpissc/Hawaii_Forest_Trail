from migration.D01_payment_method_migrator import migrate_payment_methods
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger
from storage.sqlserver.sql import ensure_schema_exists
from validator.pre_validation.D01_payment_method_validator import validate_paymentmethod_table

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")

# USERID =  100250

ensure_schema_exists(mapping_schema)

def initiating_payment_method_migration():
    """
    Checks if the PaymentMethod table in the source schema exists and has any records.
    If records exist, initiates the migration using the migrate_payment_methods function.
    Logs the progress and outcome accordingly.
    """
    try:
        logger.info("🔎 Checking for PaymentMethod records in source database...")

        # Step 1: Check if the table exists
        table_check_query = f"""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'PaymentMethod'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.PaymentMethod' does not exist. Skipping migration.")
            return

        # Step 2: Check if any records exist
        count_query = f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[PaymentMethod]"
        count = sql.fetch_scalar(count_query)

        if count > 0:
            logger.info(f"✅ Found {count} PaymentMethod records. Starting migration...")
            logger.info(f"✅ Starting Validation Process...")
            try:
                validation_passed = validate_paymentmethod_table()
                if validation_passed:
                    migrate_payment_methods()
                    logger.info("🎯 PaymentMethod migration completed.")
                else:
                    logger.warning("🚫 Migration possibly failed due to failed validation.")
                    logger.warning("🚫 Migration started with possible failure..")
                    migrate_payment_methods()
            except Exception as e:
                logger.error(f"❌ Error during PaymentMethod validation or migration: {str(e)}")
        else:
            logger.info("⏩ No PaymentMethod records found. Skipping migration.")
    except Exception as e:
        logger.error(f"❌ Error during PaymentMethod migration check: {str(e)}")

