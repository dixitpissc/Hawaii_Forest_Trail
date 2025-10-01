"""
Sequence : 07
Module: item_category_migrator.py
Author: Dixit Prajapati
Created: 2025-09-17
Description: Handles migration of item records from source system to QBO,
             including parent-child hierarchy processing, retry logic, and status tracking.
Production : Ready
Phase : 02 - Multi User
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed,get_qbo_context_migration
from utils.mapping_updater import update_mapping_status
from utils.log_timer import global_logger as logger, ProgressTimer

# === Load and Refresh ===
load_dotenv()
auto_refresh_token_if_needed()

ctx = get_qbo_context_migration()
access_token = ctx['ACCESS_TOKEN']
realm_id = ctx["REALM_ID"]

# === Config ===
environment = os.getenv("QBO_ENVIRONMENT", "sandbox").lower()
SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
SOURCE_TABLE = "Item"
MAPPING_TABLE = "Map_ItemCategory"
API_URL = "https://sandbox-quickbooks.api.intuit.com/v3/company" if environment == "sandbox" else "https://quickbooks.api.intuit.com/v3/company"
QUERY_URL = f"{API_URL}/{realm_id}/query"   # ← added

HEADERS = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

session = requests.Session()   # ← added

# --------------- NEW: QBO helpers ---------------

def _qbo_select_items(where_clause: str, fields: str = "Id, Name, Active, SyncToken, Type"):
    q = f"SELECT {fields} FROM Item WHERE {where_clause}"
    resp = session.get(QUERY_URL, headers=HEADERS, params={"query": q, "minorversion": "75"})
    if resp.status_code != 200:
        logger.warning(f"❌ QBO query failed: {resp.status_code} {resp.text[:300]}")
        return []
    return resp.json().get("QueryResponse", {}).get("Item", []) or []

def _get_item_by_id(item_id: str, fields: str = "Id, Name, Active, SyncToken, Type"):
    if not item_id:
        return None
    safe_id = str(item_id).strip()
    if not safe_id:
        return None
    try:
        r = session.get(f"{API_URL}/{realm_id}/item/{safe_id}", headers=HEADERS, params={"minorversion": "75"}, timeout=30)
        if r.status_code == 200:
            data = r.json().get("Item")
            if data:
                if fields and fields != "*":
                    wanted = [f.strip() for f in fields.split(",")]
                    return {k: data.get(k) for k in wanted}
                return data
    except Exception as e:
        logger.warning(f"⚠️ GET item/{safe_id} exception: {e}")
    # fallback: SELECT
    safe_id_sql = safe_id.replace("'", "''")
    rows = _qbo_select_items(f"Id = '{safe_id_sql}'", fields=fields or "*")
    return rows[0] if rows else None

def _reactivate_item_if_needed(item_row: dict) -> str | None:
    if not item_row:
        return None
    if item_row.get("Active") is True:
        return item_row.get("Id")
    if "SyncToken" not in item_row:
        item_row = _get_item_by_id(item_row.get("Id"))
        if not item_row:
            return None
    payload = {"Id": item_row["Id"], "SyncToken": item_row["SyncToken"], "sparse": True, "Active": True}
    upd = session.post(f"{API_URL}/{realm_id}/item", headers=HEADERS, json=payload)
    if upd.status_code == 200:
        logger.info(f"🔄 Reactivated Category Id={item_row['Id']}")
    else:
        logger.warning(f"⚠️ Reactivate failed for Category {item_row['Id']}: {upd.status_code} {upd.text[:200]}")
    return item_row["Id"]

def get_existing_qbo_category_id(row) -> str | None:
    """
    Find Item where Type='Category' and Name == Category_Name.
    If found inactive, reactivate. Returns Id or None.
    """
    cat_name = str(row.get("Category_Name") or "").strip()
    if not cat_name:
        return None
    safe = cat_name.replace("'", "''")
    cats = _qbo_select_items(f"Type = 'Category' AND Name = '{safe}'")
    if cats:
        return _reactivate_item_if_needed(cats[0])
    return None

# --------------- existing functions (unchanged) ---------------

def fetch_all_category_parents():
    query = f"""
        SELECT DISTINCT
            i.Id AS Source_Id,
            i.Name,
            i.[ParentRef.value] AS ParentRef_Value,
            p.Name AS ParentRef_Name
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE} i
        LEFT JOIN {SOURCE_SCHEMA}.{SOURCE_TABLE} p ON i.[ParentRef.value] = p.Id
        WHERE i.Id IN (
            SELECT DISTINCT [ParentRef.value] FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
            WHERE [ParentRef.value] IS NOT NULL
        )
    """
    return sql.fetch_dataframe(query)

def initialize_category_table(df):
    df = df.copy()
    df["Category_Name"] = "C-" + df["Name"].astype(str)
    df["Target_Id"] = None
    df["Porter_Status"] = None
    df["Failure_Reason"] = None
    df["Retry_Count"] = 0
    df["Payload_JSON"] = None

    sql.ensure_schema_exists(MAPPING_SCHEMA)

    # keep your original DROP+recreate behavior here
    conn = sql.get_sqlserver_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        IF OBJECT_ID('{MAPPING_SCHEMA}.{MAPPING_TABLE}', 'U') IS NOT NULL
            DROP TABLE [{MAPPING_SCHEMA}].[{MAPPING_TABLE}]
    """)
    conn.commit()
    cursor.close()

    sql.insert_dataframe(df, MAPPING_TABLE, MAPPING_SCHEMA)
    logger.info(f"✅ Initialized {MAPPING_SCHEMA}.{MAPPING_TABLE} with {len(df)} category records.")

def build_dependency_order(df):
    graph = {}
    for _, row in df.iterrows():
        graph[row["Source_Id"]] = row["ParentRef_Value"]

    visited, result = set(), []
    def visit(node_id):
        if node_id in visited or pd.isna(node_id):
            return
        parent_id = graph.get(node_id)
        if parent_id:
            visit(parent_id)
        visited.add(node_id)
        result.append(node_id)

    for node in graph:
        visit(node)

    return df.set_index("Source_Id").loc[result].reset_index()

def build_category_payload(row, id_map):
    payload = {
        "Name": row["Category_Name"],
        "Type": "Category"
    }
    parent_source_id = row["ParentRef_Value"]
    if parent_source_id in id_map:
        payload["SubItem"] = True
        payload["ParentRef"] = {"value": id_map[parent_source_id], "name": row.get("ParentRef_Name")}
    else:
        payload["SubItem"] = False
    return payload

# --------------- CHANGED: post_categories initializes table if missing ---------------

def post_categories():
    """
    Posts all categories in topological order, maintaining parent-child hierarchy.
    Also handles "exists in QBO" → capture Target_Id and mark Exists.
    """
    # If mapping table is missing/empty, build it now so orchestrator calls don't skip
    if not sql.table_exists(MAPPING_TABLE, MAPPING_SCHEMA):
        parents = fetch_all_category_parents()
        if parents.empty:
            logger.warning("⚠️ No eligible parent items found to create categories.")
            return
        initialize_category_table(parents)

    df = sql.fetch_dataframe(f"SELECT * FROM {MAPPING_SCHEMA}.{MAPPING_TABLE}")
    if df.empty:
        # Try to (re)seed if empty
        parents = fetch_all_category_parents()
        if parents.empty:
            logger.warning(f"⚠️ Mapping table [{MAPPING_SCHEMA}].[{MAPPING_TABLE}] missing or empty. Skipping category mapping.")
            return
        initialize_category_table(parents)
        df = sql.fetch_dataframe(f"SELECT * FROM {MAPPING_SCHEMA}.{MAPPING_TABLE}")
        if df.empty:
            logger.warning(f"⚠️ Could not seed mapping table. Skipping.")
            return

    df_sorted = build_dependency_order(df)
    pt = ProgressTimer(len(df_sorted))
    id_map = {}

    # preload already mapped parents (reruns)
    for _, r in df_sorted.iterrows():
        if r.get("Target_Id"):
            id_map[r["Source_Id"]] = str(r["Target_Id"])

    for _, row in df_sorted.iterrows():
        source_id = row["Source_Id"]
        try:
            # already mapped?
            if row.get("Target_Id"):
                pt.update(); continue

            # 🔎 Exists in QBO?
            existing_id = get_existing_qbo_category_id(row)
            if existing_id:
                id_map[source_id] = existing_id
                update_mapping_status(
                    MAPPING_SCHEMA, MAPPING_TABLE, source_id,
                    status="Exists", target_id=existing_id,
                    payload={"Name": row["Category_Name"], "Type": "Category", "Exists": True}
                )
                logger.info(f"🔄 Category exists: {row['Category_Name']} → {existing_id}")
                pt.update()
                continue

            # 📤 Create
            payload = build_category_payload(row, id_map)
            response = session.post(f"{API_URL}/{realm_id}/item?minorversion=75", headers=HEADERS, json=payload)
            if response.status_code == 200:
                result = response.json()["Item"]
                target_id = result["Id"]
                id_map[source_id] = target_id
                update_mapping_status(MAPPING_SCHEMA, MAPPING_TABLE, source_id,
                                      status="Success", target_id=target_id, payload=payload)
                logger.info(f"✅ Created Category: {payload['Name']} → {target_id}")
            else:
                reason = f"{response.status_code} - {response.text}"
                update_mapping_status(MAPPING_SCHEMA, MAPPING_TABLE, source_id,
                                      status="Failed", failure_reason=reason, payload=payload)
                logger.error(f"❌ Failed to post {payload['Name']}: {reason}")
        except Exception as e:
            update_mapping_status(MAPPING_SCHEMA, MAPPING_TABLE, source_id,
                                  status="Failed", failure_reason=str(e))
            logger.exception(f"❌ Exception while posting category {row.get('Category_Name')}")
        pt.update()
    logger.info("🏁 All categories processed.")

def main():
    logger.info("\n🚀 Starting QBO Category Migration\n" + "=" * 40)
    # Guard: source must have ParentRef.value
    try:
        col_check_df = sql.fetch_dataframe(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{SOURCE_TABLE}' AND COLUMN_NAME = 'ParentRef.value'
              AND TABLE_SCHEMA = '{SOURCE_SCHEMA}'
        """)
        if col_check_df.empty:
            logger.warning("⚠️ Skipping category migration — column 'ParentRef.value' not found.")
            return
    except Exception:
        logger.exception("❌ Failed to check column existence.")
        return

    # Seed if needed + post
    post_categories()
