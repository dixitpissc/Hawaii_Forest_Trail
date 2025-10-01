from migration.D11_employee_migrator import migrate_employees
from validator.pre_validation.D11_employee_validator import validate_employee_table
from storage.sqlserver import sql
import os
from utils.log_timer import global_logger as logger

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")

def initiating_employee_migration():
    """
    Checks if the Employee table in the source schema exists and has any records.
    Runs pre-validation before initiating the migration using the migrate_employees function.
    Migration proceeds even if validation fails, with a warning logged.
    """
    try:
        logger.info("🔎 Checking for Employee records in source database...")

        # Step 1: Check if table exists
        table_check_query = f"""
            SELECT 1 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{SOURCE_SCHEMA}' AND TABLE_NAME = 'Employee'
        """
        exists = sql.fetch_scalar(table_check_query)

        if not exists:
            logger.warning(f"⚠️ Table '{SOURCE_SCHEMA}.Employee' does not exist. Skipping migration.")
            return

        # Step 2: Check if any records exist
        count_query = f"SELECT COUNT(1) FROM [{SOURCE_SCHEMA}].[Employee]"
        count = sql.fetch_scalar(count_query)

        if count > 0:
            logger.info(f"✅ Found {count} Employee records.")

            # Step 3: Pre-validation
            logger.info("🧪 Running pre-validation for Employee table...")
            is_valid = validate_employee_table(schema=SOURCE_SCHEMA)

            if not is_valid:
                logger.warning("⚠️ Employee validation failed. Migration will still proceed, but may encounter errors.")

            # Step 4: Proceed with migration
            logger.info("🚀 Starting Employee migration...")
            migrate_employees()
            logger.info("🎯 Employee migration completed.")
        else:
            logger.info("⏩ No Employee records found. Skipping migration.")
    except Exception as e:
        logger.error(f"❌ Error during Employee migration check: {str(e)}")
