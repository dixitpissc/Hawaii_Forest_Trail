"""
Sequence : 05
Module: department_migrator.py
Author: Dixit Prajapati
Created: 2025-09-17
Description: Handles migration of Department records from source system to QBO,
             including parent-child hierarchy processing, retry logic, and status tracking.
Production : Ready
Phase : 02 - Multi User
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from config.mapping.department_mapping import DEPARTMENT_COLUMN_MAPPING as column_mapping
from utils.token_refresher import auto_refresh_token_if_needed,get_qbo_context_migration
from utils.logger_builder import build_logger
from utils.log_timer import ProgressTimer
import json

# === Auth & Setup ===
load_dotenv()
auto_refresh_token_if_needed()
logger = build_logger("department_migration")

ctx = get_qbo_context_migration()
access_token = ctx['ACCESS_TOKEN']
realm_id = ctx["REALM_ID"]

environment = os.getenv("QBO_ENVIRONMENT", "sandbox")
base_url = "https://sandbox-quickbooks.api.intuit.com" if environment == "sandbox" else "https://quickbooks.api.intuit.com"
query_url = f"{base_url}/v3/company/{realm_id}/query"
post_url = f"{base_url}/v3/company/{realm_id}/department"

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

source_table = "Department"
source_schema = "dbo"
mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
mapping_table = f"Map_{source_table}"
max_retries = 3

def build_hierarchy(df):
    """
    Assigns hierarchical levels to Department records based on parent-child structure.

    Each department is assigned a 'Level':
        - Level 0: top-level (no parent)
        - Level 1+: nested children (based on SubDepartment + ParentRef.value)

    This enables correct ordering of migration from parent to child.

    Args:
        df (pd.DataFrame): Source department records with 'Id', 'SubDepartment', and 'ParentRef.value'.

    Returns:
        pd.DataFrame: DataFrame with an added 'Level' column, sorted by hierarchy.
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
        if not row or not pd.notna(row.get("SubDepartment")) or str(row["SubDepartment"]).lower() != "true":
            return 0

        parent_id = row.get("ParentRef.value")
        if not parent_id:
            return 0
        return 1 + get_level(parent_id, visited)

    df["Level"] = df["Id"].apply(get_level)
    return df.sort_values(by="Level")

def get_existing_qbo_department_id(name):
    """
    Queries QBO to check if a Department with the given name already exists.

    Useful for handling duplicates or confirming existence before posting.

    Args:
        name (str): The department name to query in QBO.

    Returns:
        str or None: QBO Department Id if found, else None.
    """
    name = name.replace("'", "''")
    query = f"SELECT Id FROM Department WHERE Name = '{name}'"
    response = requests.post(query_url, headers=headers, data=query)
    if response.status_code == 200:
        depts = response.json().get("QueryResponse", {}).get("Department", [])
        if depts:
            return depts[0]["Id"]
    return None

session = requests.Session()

def _coerce_value_for_qbo(value):
    """
    Convert pandas/raw values into native Python primitives QBO expects.
    - "True"/"False"/"1"/"0" -> bool
    - numeric strings -> int/float
    - pandas/numpy scalars -> native Python types
    - None/NaN -> None (skipped by caller)
    """
    # handle pandas NA
    if pd.isna(value):
        return None

    # already native
    if isinstance(value, (bool, int, float)):
        return value

    # numpy scalar -> python native
    try:
        if hasattr(value, "item"):
            v = value.item()
            if isinstance(v, (bool, int, float, str)):
                value = v
    except Exception:
        pass

    s = str(value).strip()

    # booleans
    lower = s.lower()
    if lower in ("true", "false", "t", "f", "yes", "no", "y", "n", "1", "0"):
        if lower in ("true", "t", "yes", "y", "1"):
            return True
        else:
            return False

    # integers
    try:
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            return int(s)
        # floats (includes "10.0")
        if "." in s:
            f = float(s)
            return f
    except Exception:
        pass

    # fallback: return original (string)
    return s

def store_payload_in_mapping(schema, table, source_id, porter_status=None,
                             target_id=None, failure_reason=None,
                             payload_json_pretty=None, increment_retry=False):
    """
    Directly update the mapping table with parameterized query to avoid double-encoding.
    - payload_json_pretty: a string (already json.dumps(..., indent=2, ensure_ascii=False))
    - If increment_retry True, increments Retry_Count by 1.
    """
    set_parts = []
    params = []

    if target_id is not None:
        set_parts.append("Target_Id = ?")
        params.append(target_id)
    if porter_status is not None:
        set_parts.append("Porter_Status = ?")
        params.append(porter_status)
    if failure_reason is not None:
        set_parts.append("Failure_Reason = ?")
        params.append(failure_reason)
    if payload_json_pretty is not None:
        set_parts.append("Payload_JSON = ?")
        params.append(payload_json_pretty)

    if increment_retry:
        set_parts.append("Retry_Count = ISNULL(Retry_Count, 0) + 1")

    if not set_parts:
        # nothing to update
        return

    set_clause = ", ".join(set_parts)
    sql_query = f"""
        UPDATE [{schema}].[{table}]
        SET {set_clause}
        WHERE Source_Id = ?
    """
    params.append(source_id)
    # Use parameterized update to avoid any additional quoting
    sql.run_query(sql_query, tuple(params))

def migrate(df_subset, df_mapping, is_child=False, remigrate_failed=False):
    """
    Migrates a subset of Department records to QBO, respecting hierarchy and retry logic.
    Stores pretty JSON (not double-encoded) into mapping table via store_payload_in_mapping().
    """
    for _, row in df_subset.iterrows():
        source_id = row["Id"]
        existing = df_mapping.query(f"Source_Id == '{source_id}'", engine="python")

        if not existing.empty and pd.notnull(existing.iloc[0].get("Target_Id")):
            if not remigrate_failed:
                logger.info(f"✅ Already migrated: {row['Name']}")
                continue
            elif existing.iloc[0].get("Porter_Status") == "Failed":
                retry_count = int(existing.iloc[0].get("Retry_Count", 0) or 0)
                if retry_count >= max_retries:
                    logger.warning(f"⛔ Skipping {row['Name']} — Retry limit reached")
                    continue
                logger.info(f"🔁 Retrying: {row['Name']} (Attempt {retry_count + 1})")
            else:
                continue

        # Build payload with coercion to ensure proper JSON primitives
        payload = {}
        for src_col, qbo_col in column_mapping.items():
            value = row.get(src_col)
            if pd.notna(value):
                coerced = _coerce_value_for_qbo(value)
                if coerced is None:
                    continue
                payload[qbo_col] = coerced

        # Resolve parent mapping if child records
        if is_child:
            parent_id = row.get("ParentRef.value")
            if pd.isna(parent_id) or not parent_id:
                logger.warning(f"⏭️ Skipping {row['Name']} — missing parent")
                continue
            parent_qbo_id = sql.fetch_single_value(
                f"SELECT Target_Id FROM [{mapping_schema}].[{mapping_table}] WHERE Source_Id = ?", (parent_id,)
            )
            if not parent_qbo_id:
                logger.warning(f"⏭️ Skipping {row['Name']} — parent not migrated")
                continue
            payload["ParentRef"] = {"value": str(parent_qbo_id)}

        # Ensure Active is present and a boolean
        if "Active" not in payload:
            payload["Active"] = True
        else:
            payload["Active"] = bool(payload["Active"])

        # Make pretty JSON string (we will store this exact string in DB)
        try:
            pretty_payload_json = json.dumps(payload, indent=2, ensure_ascii=False)
        except Exception:
            # fallback: coerce non-serializable values to strings
            cleaned = {}
            for k, v in payload.items():
                if isinstance(v, (int, float, bool, str, dict, list)):
                    cleaned[k] = v
                else:
                    cleaned[k] = str(v)
            pretty_payload_json = json.dumps(cleaned, indent=2, ensure_ascii=False)

        try:
            existing_id = get_existing_qbo_department_id(row["Name"])
            if existing_id:
                logger.info(f"🔄 Exists in QBO: {row['Name']} → {existing_id}")
                # Store pretty JSON directly (no additional json.dumps)
                store_payload_in_mapping(mapping_schema, mapping_table, source_id,
                                         porter_status="Exists",
                                         target_id=existing_id,
                                         failure_reason=None,
                                         payload_json_pretty=pretty_payload_json,
                                         increment_retry=False)
                continue

            logger.info(f"📤 Posting: {payload.get('Name')}")
            response = session.post(post_url, headers=headers, json=payload)

            if response.status_code in (200, 201):
                try:
                    result = response.json().get("Department", {})
                    target_id = result.get("Id")
                except Exception:
                    target_id = None
                logger.info(f"✅ Migrated: {row['Name']} → {target_id}")
                store_payload_in_mapping(mapping_schema, mapping_table, source_id,
                                         porter_status="Success",
                                         target_id=target_id,
                                         failure_reason=None,
                                         payload_json_pretty=pretty_payload_json,
                                         increment_retry=False)

            else:
                reason = response.text[:2000]
                try:
                    error_json = response.json()
                    errors = error_json.get("Fault", {}).get("Error", [])
                    duplicate_error = next(
                        (e for e in errors if "Id=" in (e.get("Detail") or "")),
                        None
                    )

                    if duplicate_error:
                        detail = duplicate_error.get("Detail", "")
                        qbo_id = detail.split("Id=")[-1].split()[0].strip().strip("';\",")
                        logger.info(f"🔁 Duplicate detected — updating existing Target_Id = {qbo_id}")
                        store_payload_in_mapping(mapping_schema, mapping_table, source_id,
                                                 porter_status="Exists",
                                                 target_id=qbo_id,
                                                 failure_reason=None,
                                                 payload_json_pretty=pretty_payload_json,
                                                 increment_retry=False)
                    else:
                        logger.error(f"❌ Failed to post {row['Name']}: {reason}")
                        store_payload_in_mapping(mapping_schema, mapping_table, source_id,
                                                 porter_status="Failed",
                                                 target_id=None,
                                                 failure_reason=reason,
                                                 payload_json_pretty=pretty_payload_json,
                                                 increment_retry=True)
                except Exception as parse_err:
                    logger.exception(f"❌ Failed to parse QBO error response for {row['Name']}: {parse_err}")
                    store_payload_in_mapping(mapping_schema, mapping_table, source_id,
                                             porter_status="Failed",
                                             target_id=None,
                                             failure_reason=str(parse_err),
                                             payload_json_pretty=pretty_payload_json,
                                             increment_retry=True)

        except Exception as e:
            logger.exception(f"❌ Error: {row['Name']} — {e}")
            store_payload_in_mapping(mapping_schema, mapping_table, source_id,
                                     porter_status="Failed",
                                     target_id=None,
                                     failure_reason=str(e),
                                     payload_json_pretty=pretty_payload_json,
                                     increment_retry=True)

def retry_failed_departments():
    """
    Retries migration for Department records that previously failed but are still within retry limit.

    - Reloads failed entries from mapping table
    - Rebuilds hierarchy for those records
    - Calls `migrate()` level-by-level with retry enabled

    Returns:
        None
    """

    logger.info("\n🔁 Retrying Failed Departments\n" + "=" * 32)
    df_mapping = sql.fetch_table(mapping_table, mapping_schema)
    df_mapping["Retry_Count"] = pd.to_numeric(df_mapping["Retry_Count"], errors="coerce").fillna(0).astype(int)
    failed_ids = df_mapping[(df_mapping["Porter_Status"] == "Failed") & (df_mapping["Retry_Count"] < max_retries)]["Source_Id"].tolist()

    if not failed_ids:
        logger.info("✅ No departments to retry.")
        return

    df_all = sql.fetch_table(source_table, source_schema)
    df_remigrate = df_all[df_all["Id"].isin(failed_ids)]
    df_remigrate = build_hierarchy(df_remigrate)
    df_mapping = sql.fetch_table(mapping_table, mapping_schema)

    for level in sorted(df_remigrate["Level"].unique()):
        df_level = df_remigrate[df_remigrate["Level"] == level]
        migrate(df_level, df_mapping, is_child=(level > 0), remigrate_failed=True)

def migrate_departments():
    """
    Orchestrates the full migration of Department records from source system to QBO.

    Process:
    - Fetches source data from SQL Server
    - Ensures the mapping schema/table exist
    - Builds hierarchy using ParentRef
    - Migrates each level (parent to child)
    - Retries failed migrations after first pass

    Returns:
        None
    """

    logger.info("\n🚀 Starting Department Migration\n" + "=" * 40)
    df = sql.fetch_table(source_table, source_schema)
    if df.empty:
        logger.info("⚠️ No department records found.")
        return

    sql.ensure_schema_exists(mapping_schema)

    if not sql.table_exists(mapping_table, mapping_schema):
        df_mapping = df.copy()
        df_mapping.rename(columns={"Id": "Source_Id"}, inplace=True)
        df_mapping["Target_Id"] = None
        df_mapping["Porter_Status"] = None
        df_mapping["Failure_Reason"] = None
        df_mapping["Retry_Count"] = 0
        df_mapping["Payload_JSON"] = None
        sql.insert_dataframe(df_mapping, mapping_table, mapping_schema)

    df_mapping = sql.fetch_table(mapping_table, mapping_schema)
    df = build_hierarchy(df)

    timer = ProgressTimer(total_records=len(df), logger=logger)

    for level in sorted(df["Level"].unique()):
        df_level = df[df["Level"] == level]
        for _, row in df_level.iterrows():
            migrate(pd.DataFrame([row]), df_mapping, is_child=(level > 0), remigrate_failed=False)
            timer.update()


    retry_failed_departments()
    timer.stop()
    logger.info("\n🏁 Department migration completed.")

