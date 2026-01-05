import time
from datetime import datetime
from storage.sqlserver import sql
from utils.logger_builder import build_logger

# =========================
# CONFIG
# =========================
MAPPING_SCHEMA = "porter_entities_mapping"
SOURCE_SCHEMA = "dbo"
TRACKING_SCHEMA = "migration_status"
TRACKING_TABLE = "migration_live_tracker"
REFRESH_INTERVAL = 10  # seconds

logger = build_logger("migration_live_tracker")

# =========================
# LOW LEVEL SQL (SAFE)
# =========================

def run_sql_raw(sql_text: str):
    """
    Executes raw SQL (DDL/metadata/INSERT/UPDATE) using run_query which commits and does not attempt to fetch rows.
    """
    sql.run_query(sql_text)


# =========================
# SETUP
# =========================

def ensure_schema_and_table():
    logger.info("🔧 Ensuring migration_status schema and table")

    run_sql_raw(f"""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{TRACKING_SCHEMA}')
        EXEC('CREATE SCHEMA {TRACKING_SCHEMA}');
    """)

    run_sql_raw(f"""
    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '{TRACKING_SCHEMA}'
          AND t.name = '{TRACKING_TABLE}'
    )
    CREATE TABLE {TRACKING_SCHEMA}.{TRACKING_TABLE} (
        UniqueId BIGINT IDENTITY(1,1) PRIMARY KEY,

        Entity_Name VARCHAR(150),
        Source_Table VARCHAR(150),

        Total_Source_Count INT,
        Migrated_Count INT,
        Success_Count INT,
        Failed_Count INT,
        Exists_Count INT,
        Pending_Count INT DEFAULT 0,
        Total_Migration_Count INT,

        Snapshot_Timestamp DATETIME2 DEFAULT SYSDATETIME()
    );
    """)

    # Ensure older installs get the new Pending_Count column (no-op if it already exists)
    run_sql_raw(f"""
    IF NOT EXISTS (
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{TRACKING_SCHEMA}'
          AND TABLE_NAME = '{TRACKING_TABLE}'
          AND COLUMN_NAME = 'Pending_Count'
    )
    BEGIN
        ALTER TABLE {TRACKING_SCHEMA}.{TRACKING_TABLE} ADD Pending_Count INT DEFAULT 0;
    END
    """)


# =========================
# DISCOVERY
# =========================

def fetch_mapping_tables():
    """
    Returns Map_* table names from porter_entities_mapping schema as a DataFrame.
    """
    return sql.fetch_dataframe(f"""
        SELECT t.name
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '{MAPPING_SCHEMA}'
          AND t.name LIKE 'Map_%'
    """)


def source_table_exists(table_name: str) -> bool:
    result = sql.fetch_dataframe(f"""
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '{SOURCE_SCHEMA}'
          AND t.name = '{table_name}'
    """)
    return not result.empty


# =========================
# TRACKING LOGIC
# =========================

def insert_entity_snapshot(map_table: str):
    source_table = map_table.replace("Map_", "")
    entity_name = map_table.lower()
    source_fqn = f"{SOURCE_SCHEMA}.{source_table}".lower()

    if not source_table_exists(source_table):
        logger.warning(f"⚠️ Missing source table dbo.{source_table}, skipping")
        return

    # logger.info(f"📊 Tracking {entity_name}")

    # Upsert: if the entity already exists, update counts; otherwise insert a fresh row
    run_sql_raw(f"""
    IF EXISTS (SELECT 1 FROM {TRACKING_SCHEMA}.{TRACKING_TABLE} WHERE Entity_Name = '{entity_name}')
    BEGIN
        UPDATE {TRACKING_SCHEMA}.{TRACKING_TABLE}
        SET
            Total_Source_Count = src.Total_Source_Count,
            Migrated_Count = src.Migrated_Count,
            Success_Count = src.Success_Count,
            Failed_Count = src.Failed_Count,
            Exists_Count = src.Exists_Count,
            Pending_Count = src.Pending_Count,
            Total_Migration_Count = src.Total_Migration_Count,
            Snapshot_Timestamp = SYSDATETIME()
        FROM (
            SELECT
                (SELECT COUNT(*) FROM {SOURCE_SCHEMA}.{source_table}) AS Total_Source_Count,
                COUNT(CASE WHEN Target_Id IS NOT NULL THEN 1 END) AS Migrated_Count,
                COUNT(CASE WHEN Porter_Status = 'Success' AND Target_Id IS NOT NULL THEN 1 END) AS Success_Count,
                COUNT(CASE WHEN Porter_Status = 'Failed' THEN 1 END) AS Failed_Count,
                COUNT(CASE WHEN Porter_Status = 'Exists' AND Target_Id IS NOT NULL THEN 1 END) AS Exists_Count,
                COUNT(CASE WHEN Porter_Status IN ('Ready','Pending') THEN 1 END) AS Pending_Count,
                COUNT(*) AS Total_Migration_Count
            FROM {MAPPING_SCHEMA}.{map_table}
        ) src
        WHERE {TRACKING_SCHEMA}.{TRACKING_TABLE}.Entity_Name = '{entity_name}';
    END
    ELSE
    BEGIN
        INSERT INTO {TRACKING_SCHEMA}.{TRACKING_TABLE} (
            Entity_Name,
            Source_Table,
            Total_Source_Count,
            Migrated_Count,
            Success_Count,
            Failed_Count,
            Exists_Count,
            Pending_Count,
            Total_Migration_Count
        )
        SELECT
            '{entity_name}',
            '{source_fqn}',

            (SELECT COUNT(*) FROM {SOURCE_SCHEMA}.{source_table}),

            COUNT(CASE WHEN Target_Id IS NOT NULL THEN 1 END),

            COUNT(CASE WHEN Porter_Status = 'Success' AND Target_Id IS NOT NULL THEN 1 END),

            COUNT(CASE WHEN Porter_Status = 'Failed' THEN 1 END),

            COUNT(CASE WHEN Porter_Status = 'Exists' AND Target_Id IS NOT NULL THEN 1 END),

            COUNT(CASE WHEN Porter_Status IN ('Ready','Pending') THEN 1 END),

            COUNT(*)
        FROM {MAPPING_SCHEMA}.{map_table};
    END
    """)


def refresh_migration_tracker():
    logger.info("🔄 Refreshing migration tracker")

    mapping_tables = fetch_mapping_tables()

    if mapping_tables.empty:
        logger.warning("⚠️ No Map_* tables found")
        return

    for _, row in mapping_tables.iterrows():
        insert_entity_snapshot(row["name"])


# =========================
# MAIN LOOP
# =========================

def run_tracker():
    ensure_schema_and_table()
    logger.info("🚀 Migration Live Tracker started")

    while True:
        start = datetime.now()
        try:
            refresh_migration_tracker()
        except Exception as e:
            logger.exception("❌ Tracker failed")

        elapsed = (datetime.now() - start).total_seconds()
        time.sleep(max(REFRESH_INTERVAL - elapsed, 1))
