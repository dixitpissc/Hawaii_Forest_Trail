# utils/migration_progress.py

import os
from storage.sqlserver import sql  # your module
import pyodbc

def get_migration_progress(entity_name: str,
                           schema: str = "porter_entities_mapping",
                           user_id: int | str | None = None) -> dict:
    """
    Returns migration counts for porter mapping tables.

    Given an entity like "Account", this checks [schema].[Map_Account]
    and returns total rows, how many have Target_Id populated, and how many don't.

    Args:
        entity_name: Base entity name, e.g. "Account", "Vendor", "Invoice", etc.
        schema:      Fixed schema for mapping tables (default: porter_entities_mapping).
        user_id:     Optional override for USERID; if None, falls back to env USERID or 100250.

    Returns:
        {
          "schema": "porter_entities_mapping",
          "table": "Map_Account",
          "exists": True/False,
          "total_rows": int,
          "mapped_count": int,      # Target_Id present (non-null/non-empty)
          "unmapped_count": int     # Target_Id null/empty
        }
    """
    if not entity_name or not entity_name.strip():
        raise ValueError("entity_name is required (e.g., 'Account').")

    #Pass userid here

    user_id = 100250


    map_table = f"Map_{entity_name.strip()}"

    # Build safe identifier with bracket quoting
    safe_schema = f"[{schema}]"
    safe_table  = f"[{map_table}]"
    full_name   = f"{safe_schema}.{safe_table}"

    # Queries
    q_exists = f"""
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        WHERE s.name = ? AND t.name = ?
    """

    # We treat empty string Target_Id as "not mapped" too.
    q_counts = f"""
        SELECT
            COUNT(1) AS total_rows,
            SUM(CASE
                    WHEN Target_Id IS NOT NULL
                         AND LTRIM(RTRIM(CONVERT(NVARCHAR(255), Target_Id))) <> ''
                    THEN 1 ELSE 0
                END) AS mapped_count,
            SUM(CASE
                    WHEN Target_Id IS NULL
                         OR LTRIM(RTRIM(CONVERT(NVARCHAR(255), Target_Id))) = ''
                    THEN 1 ELSE 0
                END) AS unmapped_count
        FROM {full_name}
    """

    result = {
        "schema": schema,
        "table": map_table,
        "exists": False,
        "total_rows": 0,
        "mapped_count": 0,
        "unmapped_count": 0,
    }

    # Connect using your multi-tenant connector
    with sql.get_sqlserver_connection(USERID=str(user_id)) as conn:
        cur = conn.cursor()

        # 1) Check existence
        cur.execute(q_exists, (schema, map_table))
        if not cur.fetchone():
            # Table not present; return early (exists=False)
            return result

        result["exists"] = True

        # 2) Compute counts
        cur.execute(q_counts)
        row = cur.fetchone()
        if row:
            result["total_rows"]    = int(row[0] or 0)
            result["mapped_count"]  = int(row[1] or 0)
            result["unmapped_count"]= int(row[2] or 0)

    return result


# Optional convenience wrapper for multiple entities
def get_migration_progress_bulk(entities: list[str],
                                schema: str = "porter_entities_mapping",
                                user_id: int | str | None = None) -> list[dict]:
    """
    Runs get_migration_progress() for a list of entities.
    """
    out = []
    for ent in entities:
        try:
            out.append(get_migration_progress(ent, schema=schema, user_id=user_id))
        except Exception as e:
            out.append({
                "schema": schema,
                "table": f"Map_{ent}",
                "exists": False,
                "error": str(e),
                "total_rows": 0,
                "mapped_count": 0,
                "unmapped_count": 0
            })
    return out


# # Use Case
# from utils.migration_progress import get_migration_progress, get_migration_progress_bulk

# print(get_migration_progress("Account"))
# # {'schema':'porter_entities_mapping','table':'Map_Account','exists':True,
# #  'total_rows': 6123, 'mapped_count': 6010, 'unmapped_count': 113}

# print(get_migration_progress_bulk(["Account"]))
