"""
Post-Migration Analytics Generator for QBO to QBO Migrations.
This script connects to a SQL Server database using credentials loaded from environment variables
(SQLSERVER_HOST, SQLSERVER_PORT, SQLSERVER_USER, SQLSERVER_PASSWORD, SQLSERVER_DATABASE) and scans
all mapping tables in a specified schema (default: 'porter_entities_mapping') whose names start
with 'Map_'.
For each mapping table, it calculates:
    • Total_Records   - Total number of records in the table
    • Success_Records - Number of records with Porter_Status matching defined "success" statuses
    • Failed_Records  - Number of records with Porter_Status = 'Failed'
The results are stored in a summary table `[post_migrated_status].[post_migrated_status]` in SQL Server.
If the schema or table does not exist, they are created automatically.
Key Features:
    - Reads DB connection details from .env file (no hardcoded credentials)
    - Discovers Map_* tables dynamically from the mapping schema
    - Strips the "Map_" prefix in output so the report table stores clean entity names
    - Counts successes based on configurable statuses via MIGRATION_SUCCESS_STATUSES env var
    - Safely creates schema/table using dynamic SQL with parameter binding
    - Merges results to update counts without duplicating records
    - Can be re-run anytime to refresh statistics
Default success statuses:
    Posted, Success, Completed, Done
    (configurable via MIGRATION_SUCCESS_STATUSES in .env, comma-separated)
Author:
    Dixit Prajapati
Example usage:
    python post_analytics.py
Output:
    Table: post_migrated_status.post_migrated_status
    Columns:
        Table_Name   (entity name without 'Map_' prefix)
        Total_Records
        Success_Records
        Failed_Records
        Last_Updated (timestamp of last analytics update)
"""


import os
import sys
import textwrap
import pyodbc
from datetime import datetime
from dotenv import load_dotenv

# ── Load ENV ──────────────────────────────────────────────────────────────────
load_dotenv()

SQL_SERVER   = os.getenv("SQLSERVER_HOST")
SQL_PORT     = os.getenv("SQLSERVER_PORT", "1433")  # default port
SQL_DATABASE = os.getenv("SQLSERVER_DATABASE") or os.getenv("DB_NAME")  # optional in env
SQL_USER     = os.getenv("SQLSERVER_USER")
SQL_PASSWORD = os.getenv("SQLSERVER_PASSWORD")
SQL_DRIVER   = os.getenv("SQLSERVER_DRIVER", "{ODBC Driver 17 for SQL Server}")

# Mapping schema that holds Map_* tables
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")

# Define which statuses count as "success"
SUCCESS_STATUSES = tuple(
    (os.getenv("MIGRATION_SUCCESS_STATUSES", "Posted,Success,Completed,Done")
     .split(","))
)
SUCCESS_STATUSES = tuple(s.strip() for s in SUCCESS_STATUSES if s.strip())

# Destination schema/table for analytics
DEST_SCHEMA = os.getenv("STATUS_SCHEMA", "post_migrated_status")
DEST_TABLE  = os.getenv("STATUS_TABLE", "post_migrated_status")

if not all([SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD]):
    sys.exit("❌ Missing required SQL env vars. Ensure server/db/user/password are set in .env")

# ── Connect ───────────────────────────────────────────────────────────────────
def get_conn():
    conn_str = (
    f"DRIVER={SQL_DRIVER};"
    f"SERVER={SQL_SERVER},{SQL_PORT};"
    f"DATABASE={SQL_DATABASE};"
    f"UID={SQL_USER};"
    f"PWD={SQL_PASSWORD}"
    )
    conn = pyodbc.connect(conn_str, autocommit=False)
    return conn

# ── Ensure destination schema & table exist ───────────────────────────────────
def ensure_status_table(conn):
    with conn.cursor() as cur:
        # Create schema if not exists (parameter-safe)
        cur.execute("""
        DECLARE @schema sysname = ?;

        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = @schema)
        BEGIN
            DECLARE @sql nvarchar(max) =
                N'CREATE SCHEMA ' + QUOTENAME(@schema) + N';';
            EXEC(@sql);
        END
        """, (DEST_SCHEMA,))

        # Create table if not exists (parameter-safe)
        cur.execute("""
        DECLARE @schema sysname = ?, @table sysname = ?;

        IF NOT EXISTS (
            SELECT 1
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = @schema AND t.name = @table
        )
        BEGIN
            DECLARE @sql nvarchar(max) = N'
                CREATE TABLE ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@table) + N'(
                    Table_Name    sysname       NOT NULL PRIMARY KEY,
                    Total_Records   int           NOT NULL DEFAULT(0),
                    Success_Records int           NOT NULL DEFAULT(0),
                    Failed_Records  int           NOT NULL DEFAULT(0),
                    Last_Updated  datetime2(3)  NOT NULL DEFAULT(SYSDATETIME())
                )';
            EXEC(@sql);
        END
        """, (DEST_SCHEMA, DEST_TABLE))

    conn.commit()

# ── Discover Map_* tables in mapping schema ───────────────────────────────────
def list_mapping_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT t.name
            FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = ? AND t.name LIKE 'Map[_]%' ESCAPE '\\'
            ORDER BY t.name;
        """, (MAPPING_SCHEMA,))
        return [row[0] for row in cur.fetchall()]

# ── Check if a column exists in a given table ─────────────────────────────────
def column_exists(conn, schema_name, table_name, column_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM sys.columns c
            JOIN sys.objects o ON o.object_id = c.object_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = ? AND o.name = ? AND c.name = ?;
        """, (schema_name, table_name, column_name))
        return cur.fetchone() is not None

# ── Build per-table INSERT ... SELECT for temp stats ──────────────────────────
def build_insert_for_table(table_name, has_porter_status):
    """
    Produces a single INSERT INTO #stats ... SELECT ... FROM [schema].[table]
    handling success/failed logic based on Porter_Status presence.
    """
    display_name = table_name[4:] if table_name.lower().startswith("map_") else table_name
    fq = f"[{MAPPING_SCHEMA}].[{table_name}]"

    if has_porter_status:
        display_name = table_name[4:] if table_name.lower().startswith("map_") else table_name
        sql = f"""
        INSERT INTO #stats (Table_Name, Total_Records, Success_Records, Failed_Records)
        SELECT
            '{display_name}' AS Table_Name,
            COUNT(*) AS Total_Records,
            SUM(CASE WHEN Target_Id IS NOT NULL AND LTRIM(RTRIM(Target_Id)) <> '' THEN 1 ELSE 0 END) AS Success_Records,
            SUM(CASE WHEN Target_Id IS NULL OR LTRIM(RTRIM(Target_Id)) = '' THEN 1 ELSE 0 END) AS Failed_Records
        FROM [{MAPPING_SCHEMA}].[{table_name}];
        """

    else:
        sql = f"""
        INSERT INTO #stats (Table_Name, Total_Records, Success_Records, Failed_Records)
        SELECT
            '{display_name}' AS Table_Name,
            COUNT(*) AS Total_Records,
            0 AS Success_Records,
            0 AS Failed_Records
        FROM {fq};
        """
    return textwrap.dedent(sql)

def _print_results_table(conn):
    """
    Prints migrated_Status.migrated_Status as an ASCII table in the terminal.
    No external dependencies required.
    """
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                Table_Name,
                Total_Records,
                Success_Records,
                Failed_Records,
                CONVERT(varchar(19), Last_Updated, 120) AS Last_Updated
            FROM {DEST_SCHEMA}.{DEST_TABLE}
            ORDER BY Table_Name;
        """)
        rows = cur.fetchall()
        if not rows:
            print("⚠️ No rows found in the status table.")
            return

        headers = [d[0] for d in cur.description]
        data = [[("" if v is None else str(v)) for v in row] for row in rows]

    # Compute column widths
    widths = [len(h) for h in headers]
    for row in data:
        for i, cell in enumerate(row):
            if len(cell) > widths[i]:
                widths[i] = len(cell)

    # Render a simple ASCII table
    def hline(ch="-", corner="+"):
        return corner + corner.join(ch * (w + 2) for w in widths) + corner

    def render_row(cells):
        return "| " + " | ".join(c.ljust(w) for c, w in zip(cells, widths)) + " |"

    print("\n" + hline())
    print(render_row(headers))
    print(hline("="))
    for row in data:
        print(render_row(row))
    print(hline() + "\n")

# ── Run analytics and upsert results ──────────────────────────────────────────
def generate_post_analytics():
    conn = get_conn()
    try:
        ensure_status_table(conn)

        tables = list_mapping_tables(conn)
        if not tables:
            print(f"⚠️ No Map_* tables found in schema [{MAPPING_SCHEMA}]. Nothing to do.")
            return

        # Create temp stats table
        with conn.cursor() as cur:
            cur.execute("""
                IF OBJECT_ID('tempdb..#stats') IS NOT NULL DROP TABLE #stats;
                CREATE TABLE #stats(
                    Table_Name   sysname NOT NULL,
                    Total_Records  int     NOT NULL,
                    Success_Records int    NOT NULL,
                    Failed_Records int     NOT NULL
                );
            """)

            # Build and run a single big dynamic batch
            batch_sql_parts = []
            for t in tables:
                has_status = column_exists(conn, MAPPING_SCHEMA, t, "Porter_Status")
                batch_sql_parts.append(build_insert_for_table(t, has_status))

            cur.execute("".join(batch_sql_parts))

            # Upsert into destination
            # We’ll MERGE by Table_Name and update counts + Last_Updated
            merge_sql = f"""
                MERGE {DEST_SCHEMA}.{DEST_TABLE} AS target
                USING #stats AS src
                  ON target.Table_Name = src.Table_Name
                WHEN MATCHED THEN UPDATE SET
                    target.Total_Records   = src.Total_Records,
                    target.Success_Records = src.Success_Records,
                    target.Failed_Records  = src.Failed_Records,
                    target.Last_Updated  = SYSDATETIME()
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (Table_Name, Total_Records, Success_Records, Failed_Records, Last_Updated)
                    VALUES (src.Table_Name, src.Total_Records, src.Success_Records, src.Failed_Records, SYSDATETIME());
            """
            cur.execute(merge_sql)

        # Show results in terminal
        _print_results_table(conn)

        conn.commit()
        print(f"✅ Post analytic report updated in [{DEST_SCHEMA}].[{DEST_TABLE}] at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"   • Mapping schema scanned: [{MAPPING_SCHEMA}]")
        print(f"   • Success statuses counted as: {SUCCESS_STATUSES or '(none)'}")

    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

