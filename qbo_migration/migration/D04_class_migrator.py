"""
Sequence : 04
Module: class_migrator.py
Author: Dixit Prajapati
Created: 2025-09-18
Description: Handles migration of Class records from source system to QuickBooks Online (QBO),
             including parent-child hierarchy processing, retry logic, and status tracking.
Production : Ready
Phase : 02 - Multi User
"""


import os
import requests
import pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from config.mapping.class_mapping import CLASS_COLUMN_MAPPING as column_mapping
from utils.token_refresher import auto_refresh_token_if_needed,get_qbo_context_migration
from utils.mapping_updater import update_mapping_status
from utils.logger_builder import build_logger
from utils.log_timer import ProgressTimer
import json, ujson
# High-performance JSON
try:
    import orjson
    def fast_dumps(obj):
        return orjson.dumps(obj).decode()
    def fast_loads(s):
        return orjson.loads(s)
except ImportError:
    try:
        import ujson
        def fast_dumps(obj):
            return ujson.dumps(obj)
        def fast_loads(s):
            return ujson.loads(s)
    except ImportError:
        import json
        def fast_dumps(obj):
            return json.dumps(obj)
        def fast_loads(s):
            return json.loads(s)

load_dotenv()
auto_refresh_token_if_needed()

# === Logger ===
logger = build_logger("class_migration")

# === QBO Auth Config ===
ctx = get_qbo_context_migration()
access_token = ctx['ACCESS_TOKEN']
realm_id = ctx["REALM_ID"]
environment = os.getenv("QBO_ENVIRONMENT", "sandbox")

base_url = (
    "https://sandbox-quickbooks.api.intuit.com"
    if environment == "sandbox"
    else "https://quickbooks.api.intuit.com"
)

query_url = f"{base_url}/v3/company/{realm_id}/query"
post_url = f"{base_url}/v3/company/{realm_id}/class"

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# === DB Info ===
source_table = "Class"
source_schema = "dbo"
mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
mapping_table = "Map_Class"
max_retries = 3


def parse_id_from_error(err_json: dict, reason_text: str) -> str | None:
    """
    Extracts the existing QBO Id from a duplicate error response.
    Looks in Fault.Error[].Detail for 'Id=12345'; falls back to scanning reason_text.
    """
    def scan(text: str) -> str | None:
        if not text:
            return None
        # Look for 'Id=digits' pattern
        import re
        m = re.search(r"\bId=(\d+)\b", text)
        return m.group(1) if m else None

    if isinstance(err_json, dict):
        fault = err_json.get("Fault", {})
        errors = fault.get("Error", []) if isinstance(fault.get("Error"), list) else []
        for e in errors:
            detail = e.get("Detail") or ""
            found = scan(detail)
            if found:
                return found

    return scan(reason_text)

def qbo_select(query: str) -> dict | None:
    """
    Executes a QBO SELECT and returns parsed JSON or None.
    """
    resp = requests.post(query_url, headers=headers, data=query)
    if resp.status_code == 200:
        return resp.json()
    logger.debug(f"QBO query failed {resp.status_code}: {resp.text[:250]}")
    return None

def get_existing_qbo_class_id(name: str) -> str | None:
    """
    Looks for a Class by Name across Active true/false and returns Id if found.
    """
    if not name:
        return None
    safe = name.replace("'", "''")
    # Search active first (fast path)
    for active in ("true", "false"):
        query = f"SELECT Id FROM Class WHERE Name = '{safe}' AND Active = {active}"
        data = qbo_select(query)
        if not data:
            continue
        classes = data.get("QueryResponse", {}).get("Class", [])
        if classes:
            return classes[0].get("Id")
    # Fallback: without Active filter
    query = f"SELECT Id FROM Class WHERE Name = '{safe}'"
    data = qbo_select(query)
    if data:
        classes = data.get("QueryResponse", {}).get("Class", [])
        if classes:
            return classes[0].get("Id")
    return None

def try_extract_existing_id_from_error_and_backfill(row_name: str) -> str | None:
    """
    On duplicate errors, fetch existing Id by Name and return it.
    """
    existing_id = get_existing_qbo_class_id(row_name)
    if existing_id:
        logger.info(f"🧩 Resolved duplicate by lookup: {row_name} → {existing_id}")
        return existing_id
    return None


def build_hierarchy(df):
    """
    Assigns hierarchy levels to Class records based on parent-child relationships.

    Each class is assigned a 'Level' value:
        - 0 for top-level (no parent)
        - 1 for direct children
        - 2+ for deeper nested classes

    The function relies on the 'SubClass' and 'ParentRef.value' fields in the input DataFrame
    and returns a DataFrame sorted by hierarchy level (lowest to highest).

    Args:
        df (pd.DataFrame): Source DataFrame containing class records with at least 'Id',
                           'SubClass', and 'ParentRef.value' columns.

    Returns:
        pd.DataFrame: DataFrame with an added 'Level' column and sorted by it in ascending order.
    """
        
    df["Level"] = None
    id_map = df.set_index("Id").to_dict("index")

    def get_level(cid, visited=None):
        if visited is None:
            visited = set()
        if cid in visited:
            return 0
        visited.add(cid)

        row = id_map.get(cid)
        if not row or not pd.notna(row.get("SubClass")) or str(row["SubClass"]).lower() != "true":
            return 0

        parent_id = row.get("ParentRef.value")
        if not parent_id:
            return 0
        return 1 + get_level(parent_id, visited)

    df["Level"] = df["Id"].apply(get_level)
    return df.sort_values(by="Level")

def get_existing_qbo_class_id(name):
    """
    Checks if a class with the given name already exists in QBO and returns its QBO ID.

    This function sends a SQL-like query to the QBO API to find a `Class` entity
    that matches the provided name. It safely escapes single quotes and parses
    the response for a matching record.

    Args:
        name (str): The name of the class to look up.

    Returns:
        str or None: The QBO `Id` of the existing class if found, otherwise `None`.
    """
    
    name = name.replace("'", "''")
    query = f"SELECT Id FROM Class WHERE Name = '{name}'"
    response = requests.post(query_url, headers=headers, data=query)
    if response.status_code == 200:
        classes = response.json().get("QueryResponse", {}).get("Class", [])
        if classes:
            return classes[0]["Id"]
    return None

session = requests.Session()

# def migrate(df_subset, df_mapping, is_child=False, remigrate_failed=False):
#     """
#     Migrates a subset of Class records from the source DataFrame to QBO, handling parent-child
#     relationships, retry logic, and mapping updates.

#     The function checks whether each class:
#     - Has already been migrated (and optionally retries failed ones)
#     - Has a valid parent (if a subclass)
#     - Exists in QBO already (to avoid duplicates)
#     - Can be posted successfully, updating the corresponding mapping table with status and payload

#     Args:
#         df_subset (pd.DataFrame): A filtered subset of source class records to migrate.
#         df_mapping (pd.DataFrame): The current state of the mapping table (Map_Class), including statuses.
#         is_child (bool, optional): Whether this level of records contains subclasses that need parent references. Default is False.
#         remigrate_failed (bool, optional): Whether to attempt remigrating failed records. Default is False.

#     Returns:
#         None: The function performs migration as a side effect and updates the database mapping table.
#     """

#     for _, row in df_subset.iterrows():
#         source_id = row["Id"]
#         existing = df_mapping.query(f"Source_Id == '{source_id}'", engine="python")

#         if not existing.empty and pd.notnull(existing.iloc[0]["Target_Id"]):
#             if not remigrate_failed:
#                 logger.info(f"✅ Already migrated: {row['Name']}")
#                 continue
#             elif existing.iloc[0]["Porter_Status"] == "Failed":
#                 retry_count = existing.iloc[0].get("Retry_Count", 0)
#                 if retry_count >= max_retries:
#                     logger.warning(f"⛔ Skipping {row['Name']} — Retry limit reached")
#                     continue
#                 logger.info(f"🔁 Retrying: {row['Name']} (Attempt {retry_count + 1})")
#             else:
#                 continue

#         payload = {}
#         for src_col, qbo_col in column_mapping.items():
#             value = row.get(src_col)
#             if pd.notna(value):
#                 payload[qbo_col] = value

#         if is_child:
#             parent_id = row.get("ParentRef.value")
#             if pd.isna(parent_id):
#                 logger.warning(f"⏭️ Skipping {row['Name']} — missing parent")
#                 continue

#             parent_qbo_id = sql.fetch_single_value(
#                 f"SELECT Target_Id FROM [{mapping_schema}].[{mapping_table}] WHERE Source_Id = ?", (parent_id,)
#             )

#             if not parent_qbo_id:
#                 # 🔎 Try to resolve parent by name in QBO and backfill mapping
#                 parent_name = df_mapping.loc[df_mapping["Source_Id"] == parent_id, "Name"]
#                 if not parent_name.empty:
#                     pqid = get_existing_qbo_class_id(parent_name.iloc[0])
#                     if pqid:
#                         update_mapping_status(
#                             mapping_schema, mapping_table,
#                             source_id=parent_id,
#                             status="Exists",
#                             target_id=pqid
#                         )
#                         parent_qbo_id = pqid

#             if not parent_qbo_id:
#                 logger.warning(f"⏭️ Skipping {row['Name']} — parent not migrated/found")
#                 continue

#             payload["ParentRef"] = {"value": str(parent_qbo_id)}


#         try:
#             existing_id = get_existing_qbo_class_id(row["Name"])
#             if existing_id:
#                 logger.info(f"🔄 Exists in QBO: {row['Name']} → {existing_id}")
#                 update_mapping_status(
#                     mapping_schema, mapping_table,
#                     source_id=source_id,
#                     status="Exists",
#                     target_id=existing_id,
#                     payload=payload
#                 )
#                 continue

#             logger.info(f"📤 Posting: {payload.get('Name')}")
#             response = session.post(post_url, headers=headers, json=payload)


#             if response.status_code == 200:
#                 result = response.json()["Class"]
#                 target_id = result["Id"]
#                 logger.info(f"✅ Migrated: {row['Name']} → {target_id}")
#                 update_mapping_status(
#                     mapping_schema, mapping_table,
#                     source_id=source_id,
#                     status="Success",
#                     target_id=target_id,
#                     payload=payload
#                 )
#             else:
#                 # Parse error
#                 reason_text = response.text[:1000]  # keep more context
#                 target_id_from_error = None
#                 try:
#                     err_json = response.json()
#                 except Exception:
#                     err_json = {}

#                 # Detect duplicate/existing variants
#                 # QBO commonly uses code 6140 "Duplicate Name Exists Error"
#                 is_duplicate = False
#                 if isinstance(err_json, dict):
#                     fault = err_json.get("Fault", {})
#                     errors = fault.get("Error", []) if isinstance(fault.get("Error"), list) else []
#                     for err in errors:
#                         code = str(err.get("code"))
#                         msg = (err.get("Message") or "") + " " + (err.get("Detail") or "")
#                         if code == "6140" or "Duplicate" in msg or "already exists" in msg:
#                             is_duplicate = True
#                             break
#                 else:
#                     # Fallback string search
#                     if "Duplicate" in reason_text or "already exists" in reason_text:
#                         is_duplicate = True

#                 if is_duplicate:
#                     # Try to fetch the existing Id and store it
#                     target_id_from_error = try_extract_existing_id_from_error_and_backfill(row["Name"])
#                     if target_id_from_error:
#                         update_mapping_status(
#                             mapping_schema, mapping_table,
#                             source_id=source_id,
#                             status="Exists",
#                             target_id=target_id_from_error,
#                             payload=payload
#                         )
#                         logger.info(f"📌 Marked as Exists: {row['Name']} → {target_id_from_error}")
#                         return  # proceed to next row

#                 # If not duplicate or we couldn't resolve the Id, mark Failed (with retry increment)
#                 logger.error(f"❌ Failed to post {row['Name']}: {reason_text}")
#                 update_mapping_status(
#                     mapping_schema, mapping_table,
#                     source_id=source_id,
#                     status="Failed",
#                     failure_reason=reason_text,
#                     payload=payload,
#                     increment_retry=True
#                 )

#         except Exception as e:
#             logger.exception(f"❌ Error: {row['Name']} — {e}")
#             update_mapping_status(
#                 mapping_schema, mapping_table,
#                 source_id=source_id,
#                 status="Failed",
#                 failure_reason=str(e),
#                 payload=payload,
#                 increment_retry=True
#             )

def migrate(df_subset, df_mapping, is_child=False, remigrate_failed=False):
    """
    Migrates a subset of Class records from the source DataFrame to QBO, handling parent-child
    relationships, retry logic, and mapping updates.
    """
    import re

    for _, row in df_subset.iterrows():
        source_id = row["Id"]
        existing = df_mapping.query(f"Source_Id == '{source_id}'", engine="python")

        if not existing.empty and pd.notnull(existing.iloc[0]["Target_Id"]):
            if not remigrate_failed:
                logger.info(f"✅ Already migrated: {row['Name']}")
                continue
            elif existing.iloc[0]["Porter_Status"] == "Failed":
                retry_count = existing.iloc[0].get("Retry_Count", 0)
                if retry_count >= max_retries:
                    logger.warning(f"⛔ Skipping {row['Name']} — Retry limit reached")
                    continue
                logger.info(f"🔁 Retrying: {row['Name']} (Attempt {retry_count + 1})")
            else:
                continue

        # Build payload from mapping
        payload = {}
        for src_col, qbo_col in column_mapping.items():
            value = row.get(src_col)
            if pd.notna(value):
                payload[qbo_col] = value

        # Parent handling for child classes
        if is_child:
            parent_id = row.get("ParentRef.value")
            if pd.isna(parent_id):
                logger.warning(f"⏭️ Skipping {row['Name']} — missing parent")
                continue

            parent_qbo_id = sql.fetch_single_value(
                f"SELECT Target_Id FROM [{mapping_schema}].[{mapping_table}] WHERE Source_Id = ?", (parent_id,)
            )

            if not parent_qbo_id:
                # Try to resolve parent by name in QBO and backfill mapping
                parent_name = df_mapping.loc[df_mapping["Source_Id"] == parent_id, "Name"]
                if not parent_name.empty:
                    pqid = get_existing_qbo_class_id(parent_name.iloc[0])
                    if pqid:
                        update_mapping_status(
                            mapping_schema, mapping_table,
                            source_id=parent_id,
                            status="Exists",
                            target_id=pqid
                        )
                        parent_qbo_id = pqid

            if not parent_qbo_id:
                logger.warning(f"⏭️ Skipping {row['Name']} — parent not migrated/found")
                continue

            payload["ParentRef"] = {"value": str(parent_qbo_id)}

        try:
            # Pre-check: does it already exist in QBO?
            existing_id = get_existing_qbo_class_id(row["Name"])
            if existing_id:
                logger.info(f"🔄 Exists in QBO: {row['Name']} → {existing_id}")
                update_mapping_status(
                    mapping_schema, mapping_table,
                    source_id=source_id,
                    status="Exists",
                    target_id=existing_id,
                    payload=payload
                )
                continue

            logger.info(f"📤 Posting: {payload.get('Name')}")
            response = session.post(post_url, headers=headers, json=payload)

            if response.status_code == 200:
                result = response.json()["Class"]
                target_id = result["Id"]
                logger.info(f"✅ Migrated: {row['Name']} → {target_id}")
                update_mapping_status(
                    mapping_schema, mapping_table,
                    source_id=source_id,
                    status="Success",
                    target_id=target_id,
                    payload=payload
                )
            else:
                # --- Failure path: detect duplicate and backfill Target_Id ---
                reason_text = response.text[:2000]
                try:
                    err_json = response.json()
                except Exception:
                    err_json = {}

                is_duplicate = False
                if isinstance(err_json, dict):
                    fault = err_json.get("Fault", {})
                    errors = fault.get("Error", []) if isinstance(fault.get("Error"), list) else []
                    for err in errors:
                        code = str(err.get("code"))
                        msg = (err.get("Message") or "") + " " + (err.get("Detail") or "")
                        # Class duplicate is usually 6240; keep 6140 too for safety
                        if code in ("6240", "6140") or "Duplicate" in msg or "already exists" in msg:
                            is_duplicate = True
                            break
                else:
                    if "Duplicate" in reason_text or "already exists" in reason_text:
                        is_duplicate = True

                if is_duplicate:
                    # 1) Parse Id directly from error detail if present (e.g., "Id=1260637")
                    parsed_id = None
                    if isinstance(err_json, dict):
                        fault = err_json.get("Fault", {})
                        errors = fault.get("Error", []) if isinstance(fault.get("Error"), list) else []
                        for e in errors:
                            detail = e.get("Detail") or ""
                            m = re.search(r"\bId=(\d+)\b", detail)
                            if m:
                                parsed_id = m.group(1)
                                break
                    if not parsed_id:
                        m = re.search(r"\bId=(\d+)\b", reason_text or "")
                        parsed_id = m.group(1) if m else None

                    if parsed_id:
                        update_mapping_status(
                            mapping_schema, mapping_table,
                            source_id=source_id,
                            status="Exists",
                            target_id=parsed_id,
                            payload=payload
                        )
                        logger.info(f"📌 Marked as Exists (from error): {row['Name']} → {parsed_id}")
                        continue  # go to next row

                    # 2) Fallback: lookup by Name
                    target_id_from_error = try_extract_existing_id_from_error_and_backfill(row["Name"])
                    if target_id_from_error:
                        update_mapping_status(
                            mapping_schema, mapping_table,
                            source_id=source_id,
                            status="Exists",
                            target_id=target_id_from_error,
                            payload=payload
                        )
                        logger.info(f"📌 Marked as Exists (by name lookup): {row['Name']} → {target_id_from_error}")
                        continue  # next row

                # Not duplicate, or couldn't resolve an Id → mark Failed with retry increment
                logger.error(f"❌ Failed to post {row['Name']}: {reason_text}")
                update_mapping_status(
                    mapping_schema, mapping_table,
                    source_id=source_id,
                    status="Failed",
                    failure_reason=reason_text,
                    payload=payload,
                    increment_retry=True
                )

        except Exception as e:
            logger.exception(f"❌ Error: {row['Name']} — {e}")
            update_mapping_status(
                mapping_schema, mapping_table,
                source_id=source_id,
                status="Failed",
                failure_reason=str(e),
                payload=payload,
                increment_retry=True
            )


def retry_failed_classes():
    """
    Attempts to remigrate Class records that previously failed during migration.

    This function:
    - Fetches the mapping table and filters records with 'Failed' status
    - Ignores records that have reached the maximum retry limit
    - Rebuilds the hierarchy for the failed subset
    - Calls the `migrate()` function by hierarchy level to remigrate each record

    Only records with `Porter_Status = 'Failed'` and `Retry_Count < max_retries` are retried.

    Returns:
        None: The function performs retries as a side effect and updates the mapping table in place.
    """
    logger.info("\n🔁 Retrying Failed Classes\n" + "=" * 30)
    df_mapping = sql.fetch_table(mapping_table, mapping_schema)
    # Ensure Retry_Count is numeric before comparison
    df_mapping["Retry_Count"] = pd.to_numeric(df_mapping["Retry_Count"], errors="coerce").fillna(0).astype(int)

    failed_ids = df_mapping[
        (df_mapping["Porter_Status"] == "Failed") & (df_mapping["Retry_Count"] < max_retries)
    ]["Source_Id"].tolist()

    if not failed_ids:
        logger.info("✅ No classes to retry.")
        return

    df_all = sql.fetch_table(source_table, source_schema)
    df_remigrate = df_all[df_all["Id"].isin(failed_ids)]
    df_remigrate = build_hierarchy(df_remigrate)
    df_mapping = sql.fetch_table(mapping_table, mapping_schema)

    for level in sorted(df_remigrate["Level"].unique()):
        df_level = df_remigrate[df_remigrate["Level"] == level]
        migrate(df_level, df_mapping, is_child=(level > 0), remigrate_failed=True)


def migrate_classes():
    """
    Orchestrates the full migration of Class records from the source system to QuickBooks Online (QBO).

    This function:
    - Loads all class records from the source table
    - Ensures the mapping schema and mapping table (`Map_Class`) exist in SQL Server
    - Initializes the mapping table if it doesn't exist
    - Builds a parent-child hierarchy based on subclass relationships
    - Iteratively migrates each class level to QBO using the `migrate()` function
    - Tracks progress using `ProgressTimer`
    - Calls `retry_failed_classes()` to handle any previously failed records

    Returns:
        None: The function operates via side effects including API calls and database updates.
    """

    logger.info("\n🚀 Starting Class Migration\n" + "=" * 35)
    df = sql.fetch_table(source_table, source_schema)
    if df.empty:
        logger.info("⚠️ No class records found.")
        return

    sql.ensure_schema_exists(mapping_schema)

    if not sql.table_exists(mapping_table, mapping_schema):
        df_mapping = df.copy()
        df_mapping.rename(columns={"Id": "Source_Id"}, inplace=True)
        df_mapping["Target_Id"] = None
        df_mapping["Porter_Status"] = None
        df_mapping["Failure_Reason"] = None
        df_mapping["Retry_Count"] = 0
        df_mapping["Payload_JSON"] = None  # ✅ Add this line
        sql.insert_dataframe(df_mapping, mapping_table, mapping_schema)

    df_mapping = sql.fetch_table(mapping_table, mapping_schema)
    df = build_hierarchy(df)

    timer = ProgressTimer(total_records=len(df), logger=logger) 

    for level in sorted(df["Level"].unique()):
        df_level = df[df["Level"] == level]
        migrate(df_level, df_mapping, is_child=(level > 0), remigrate_failed=False)
        timer.update()

    retry_failed_classes()
    timer.stop()
    logger.info("\n🏁 Class migration completed.")

