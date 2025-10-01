"""
Sequence : 09
Module: vendor_migrator.py
Author: Dixit Prajapati
Created: 2025-09-17
Description: Handles migration of Vendor records from source system to QBO,
             including parent-child hierarchy processing, retry logic, and status tracking.
Production : Ready
Development : True
Phase : 02 - Multi User
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from config.mapping.vendor_mapping import VENDOR_COLUMN_MAPPING as column_mapping
from utils.token_refresher import auto_refresh_token_if_needed,get_qbo_context_migration
from utils.log_timer import global_logger as logger, ProgressTimer
import json
from datetime import datetime, timedelta
import re

# === Load environment and refresh tokens ===
load_dotenv()
auto_refresh_token_if_needed()

# === QBO Auth ===
def get_qbo_auth():
    """
    Constructs and returns the QBO API endpoints and headers for authentication.
    Returns:
        Tuple[str, str, dict]: 
            - Vendor POST URL
            - Query endpoint URL
            - Headers including Bearer token, content type, and accept type
    """
    ctx = get_qbo_context_migration()
    environment = os.getenv("QBO_ENVIRONMENT", "sandbox")
    base_url = "https://sandbox-quickbooks.api.intuit.com" if environment == "sandbox" else "https://quickbooks.api.intuit.com"
    realm_id = ctx["REALM_ID"]
    access_token = ctx['ACCESS_TOKEN']
    post_url = f"{base_url}/v3/company/{realm_id}/vendor"
    query_url = f"{base_url}/v3/company/{realm_id}/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    return post_url, query_url, headers

# === DB info ===
source_table = "Vendor"
source_schema = os.getenv("SOURCE_SCHEMA", "dbo")
mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
mapping_table = "Map_Vendor"
account_mapping_table = "Map_Account"
term_mapping_table = "Map_Term"
currency_mapping_table = "Map_Currency"
max_retries = 3


def ensure_mapping_table_exists():
    """
    Initializes the vendor mapping table for migration.
    Behavior:
        - Fetches source Vendor table from SQL Server
        - Drops existing mapping table (if exists) and reinserts fresh records
        - Adds required metadata fields (Target_Id, Retry_Count, Porter_Status, etc.)
    """
    df_source = sql.fetch_table(source_table, source_schema)
    if not sql.table_exists(mapping_table, mapping_schema):
        logger.info(f"Mapping table [{mapping_schema}].[{mapping_table}] created")
    else:
        logger.info(f"Mapping table [{mapping_schema}].[{mapping_table}] already exists")
        sql.run_query(f"DELETE FROM [{mapping_schema}].[{mapping_table}]")

    df_mapping = df_source.copy()
    df_mapping.rename(columns={"Id": "Source_Id"}, inplace=True)
    df_mapping["Target_Id"] = None
    df_mapping["Retry_Count"] = 0
    df_mapping["Porter_Status"] = None
    df_mapping["Failure_Reason"] = None
    df_mapping["target_account"] = None
    df_mapping["target_termref"] = None
    df_mapping["target_currency"] = None
    df_mapping["Payload_JSON"] = None

    df_mapping = df_mapping.where(pd.notnull(df_mapping), None)
    sql.insert_dataframe(df_mapping, mapping_table, mapping_schema)
    logger.info(f"Inserted {len(df_mapping)} rows into {mapping_schema}.{mapping_table}")

def enrich_mapping_with_targets():
    """
    Enriches the vendor mapping table with foreign key mappings and builds JSON payloads.
    Safely maps:
        - AccountRef from Map_Account (if table and AcctNum column exist)
        - TermRef from Map_Term (if table and TermRef.name column exist)
        - CurrencyRef from Map_Currency (if table and CurrencyRef.value column exist)
    All mappings are optional; if mapping table or value not found, defaults to None or 'USD'.
    """
    df_map = sql.fetch_table(mapping_table, mapping_schema)

    # === Optional: Account Mapping ===
    if sql.table_exists(account_mapping_table, mapping_schema) and "AcctNum" in df_map.columns:
        df_account = sql.fetch_table(account_mapping_table, mapping_schema)
        df_account = df_account.rename(columns={"Source_Id": "Acct_Source", "Target_Id": "Target_Id_Account"})
        df_map = df_map.merge(
            df_account[["Acct_Source", "Target_Id_Account"]],
            left_on="AcctNum",
            right_on="Acct_Source",
            how="left"
        )
        df_map["target_account"] = df_map["Target_Id_Account"]
        logger.info("✅ Account mapping completed.")
    else:
        df_map["target_account"] = None
        logger.warning("⚠️ Skipped Account mapping — 'AcctNum' column or Map_Account table missing.")

    # === Optional: Term Mapping ===
    if sql.table_exists(term_mapping_table, mapping_schema) and "TermRef.name" in df_map.columns:
        df_term = sql.fetch_table(term_mapping_table, mapping_schema)
        df_term["term_clean"] = df_term["Name"].astype(str).str.strip().str.lower()
        df_term = df_term.rename(columns={"Target_Id": "Target_Id_Term"})
        df_map["term_clean"] = df_map["TermRef.name"].astype(str).str.strip().str.lower()
        df_map = df_map.merge(
            df_term[["term_clean", "Target_Id_Term"]],
            on="term_clean",
            how="left"
        )
        df_map["target_termref"] = df_map["Target_Id_Term"]
        logger.info("✅ Term mapping completed.")
    else:
        df_map["target_termref"] = None
        logger.warning("⚠️ Skipped Term mapping — 'TermRef.name' column or Map_Term table missing.")

    # === Optional: Currency Mapping ===
    if sql.table_exists(currency_mapping_table, mapping_schema) and "CurrencyRef.value" in df_map.columns:
        df_currency = sql.fetch_table(currency_mapping_table, mapping_schema)
        df_currency = df_currency.rename(columns={"Code": "Currency_Code"})
        df_map = df_map.merge(
            df_currency[["Currency_Code"]],
            left_on="CurrencyRef.value",
            right_on="Currency_Code",
            how="left",
            indicator=True
        )
        df_map["target_currency"] = df_map.apply(
            lambda row: row["CurrencyRef.value"] if row["_merge"] == "both" else "USD",
            axis=1
        )
        df_map.drop(columns=["Currency_Code", "_merge"], inplace=True)
        logger.info("✅ Currency mapping completed.")
    else:
        df_map["target_currency"] = "USD"
        logger.warning("⚠️ Skipped Currency mapping — 'CurrencyRef.value' column or Map_Currency table missing.")

    # === Clean-up ===
    df_map.drop(columns=["Acct_Source", "Target_Id_Account", "Target_Id_Term", "term_clean"], errors="ignore", inplace=True)

    # === Default Balance to 0.0 ===
    df_map["Balance"] = pd.to_numeric(df_map.get("Balance", 0)).fillna(0.0)

    # === JSON Payload Generation ===
    payloads = []
    for _, row in df_map.iterrows():
        payload = {}
        for src_col, qbo_col in column_mapping.items():
            if qbo_col == "Balance":
                payload["Balance"] = 0.0
                continue
            value = row.get(src_col)
            if pd.notna(value) and str(value).strip() != "":
                if "." in qbo_col:
                    parent, child = qbo_col.split(".", 1)
                    payload.setdefault(parent, {})[child] = value
                else:
                    payload[qbo_col] = value

        if pd.notna(row.get("target_termref")):
            payload["TermRef"] = {"value": str(row["target_termref"])}
        if pd.notna(row.get("target_account")):
            payload["AcctNum"] = str(row["target_account"])
        if pd.notna(row.get("target_currency")):
            payload["CurrencyRef"] = {"value": row["target_currency"]}

        payload["Active"] = True  # ✅ Always set Vendor as active

        row["Payload_JSON"] = json.dumps(payload)
        payloads.append(row)

    df_result = pd.DataFrame(payloads).where(pd.notnull, None)
    sql.run_query(f"DELETE FROM [{mapping_schema}].[{mapping_table}]")
    sql.insert_dataframe(df_result, mapping_table, mapping_schema)
    logger.info("✅ Enriched mapping and stored payload JSONs.")

def vendor_exists(display_name, query_url, headers):
    """
    Checks if a vendor already exists in QBO by querying on DisplayName.
    Args:
        display_name (str): The display name of the vendor to search for.
        query_url (str): The QBO query API endpoint.
        headers (dict): Authenticated headers for QBO request.
    Returns:
        str or None: QBO Vendor Id if found, else None.
    """
    query = f"SELECT Id FROM Vendor WHERE DisplayName = '{display_name.replace("'", "''")}'"
    response = requests.post(query_url, headers=headers, data=json.dumps({"query": query}))
    if response.status_code == 200:
        vendors = response.json().get("QueryResponse", {}).get("Vendor", [])
        if vendors:
            return vendors[0]["Id"]
    return None

session = requests.Session()

def post_vendor(row):
    """
    Attempts to post a single vendor record to QBO.
    Args:
        row (pd.Series): A single record from the mapping table representing the vendor.
    Process:
        - Checks if vendor already migrated (Success or Exists)
        - Skips if retry limit is reached
        - If duplicate vendor exists, attempts to recover QBO Id via:
            a) Pre-check with vendor_exists()
            b) Fallback using regex extraction from QBO error response
        - Posts to QBO if no match found
        - Updates Map_Vendor table with Target_Id, status, and error handling results
    """
    source_id = row["Source_Id"]
    if pd.notna(row["Target_Id"]) and row["Porter_Status"] in ["Success", "Exists"]:
        logger.info(f"Already migrated or exists: {source_id}")
        return

    if int(row.get("Retry_Count", 0)) >= max_retries:
        logger.warning(f"Skipping {source_id} — Retry limit reached")
        return

    post_url, query_url, headers = get_qbo_auth()
    display_name = row.get("DisplayName", source_id)

    # Check for duplicate
    existing_id = vendor_exists(display_name, query_url, headers)
    if existing_id:
        logger.warning(f"Vendor already exists in QBO: {display_name} → {existing_id}")
        sql.run_query(f"""
            UPDATE [{mapping_schema}].[{mapping_table}]
            SET Target_Id = ?, Porter_Status = 'Exists', Failure_Reason = NULL
            WHERE Source_Id = ?
        """, (existing_id, source_id))
        return

    payload_json = row.get("Payload_JSON")
    try:
        response = session.post(post_url, headers=headers, data=payload_json)
        if response.status_code == 200:
            qbo_id = response.json()["Vendor"]["Id"]
            logger.info(f"✅ Migrated: {display_name} → {qbo_id}")
            sql.run_query(f"""
                UPDATE [{mapping_schema}].[{mapping_table}]
                SET Target_Id = ?, Porter_Status = 'Success', Failure_Reason = NULL
                WHERE Source_Id = ?
            """, (qbo_id, source_id))

        else:
            reason = response.text[:300]
            if "Duplicate Name Exists" in reason or "duplicate name exists" in reason.lower():
                # Extract QBO ID directly from the error message
                match = re.search(r"Id=(\d+)", reason)
                if match:
                    existing_id = match.group(1)
                    logger.warning(f"🔁 Vendor already exists — recovered ID from error message: {existing_id}")
                    sql.run_query(f"""
                        UPDATE [{mapping_schema}].[{mapping_table}]
                        SET Target_Id = ?, Porter_Status = 'Exists', Failure_Reason = NULL
                        WHERE Source_Id = ?
                    """, (existing_id, source_id))
                    return

            logger.error(f"❌ Failed: {display_name} → {reason}")
            sql.run_query(f"""
                UPDATE [{mapping_schema}].[{mapping_table}]
                SET Porter_Status = 'Failed', Retry_Count = ISNULL(Retry_Count, 0) + 1, Failure_Reason = ?
                WHERE Source_Id = ?
            """, (reason, source_id))

    except Exception as e:
        logger.exception(f"❌ Exception: {display_name} → {e}")
        sql.run_query(f"""
            UPDATE [{mapping_schema}].[{mapping_table}]
            SET Porter_Status = 'Failed', Retry_Count = ISNULL(Retry_Count, 0) + 1, Failure_Reason = ?
            WHERE Source_Id = ?
        """, (str(e), source_id))

def migrate_vendors():
    """
    Orchestrates the end-to-end vendor migration process to QBO.
    Steps:
        - Initializes the mapping table
        - Enriches with required reference mappings and payloads
        - Iterates through unmigrated or failed rows
        - Posts each record using `post_vendor()`
        - Refreshes token if total runtime exceeds 30 minutes
        - Tracks progress via timer
    """
    print("\n🚀 Starting Vendor Migration\n" + "=" * 35)
    ensure_mapping_table_exists()
    enrich_mapping_with_targets()

    df_mapping = sql.fetch_table(mapping_table, mapping_schema)
    df_mapping = df_mapping[df_mapping["Porter_Status"].isna() | (df_mapping["Porter_Status"] == "Failed")]
    df_mapping["Retry_Count"] = pd.to_numeric(df_mapping["Retry_Count"], errors="coerce").fillna(0).astype(int)

    timer = ProgressTimer(len(df_mapping))
    start_time = datetime.now()

    for _, row in df_mapping.iterrows():
        # 🔁 Refresh token if more than 30 minutes have passed
        if (datetime.now() - start_time) > timedelta(minutes=30):
            logger.info("⏸️ Runtime > 30 min — refreshing token for safety")
            auto_refresh_token_if_needed()
            start_time = datetime.now()  # reset timer
        
        post_vendor(row)
        timer.update()

    print("\n🌝 Vendor migration completed.")

def conditionally_migrate_vendors():
    """
    Determines whether to resume vendor migration or start fresh based on row counts and statuses.
    
    Logic:
    - If mapping table exists and record count matches the source:
        - Process only rows where Porter_Status is NULL (pending).
    - Else:
        - Recreate mapping table, enrich data, and run full migration.
    """
    logger.info("🔍 Checking migration state for Vendor...")

    df_source = sql.fetch_table(source_table, source_schema)
    source_count = len(df_source)

    if not sql.table_exists(mapping_table, mapping_schema):
        logger.warning("⚠️ Mapping table does not exist — initializing fresh migration.")
        ensure_mapping_table_exists()
        enrich_mapping_with_targets()
        df_mapping = sql.fetch_table(mapping_table, mapping_schema)  # 🧩 Fix: load after enrich
    else:
        df_mapping = sql.fetch_table(mapping_table, mapping_schema)
        mapping_count = len(df_mapping)

        if mapping_count == source_count:
            logger.info("✅ Mapping table is aligned with source — resuming pending migrations only.")
            df_mapping = df_mapping[df_mapping["Porter_Status"].isna()]
            if df_mapping.empty:
                logger.info("🎉 All vendors already migrated. Nothing to do.")
                return
        else:
            logger.warning("🔁 Mapping table is not aligned — reinitializing from scratch.")
            ensure_mapping_table_exists()
            enrich_mapping_with_targets()
            df_mapping = sql.fetch_table(mapping_table, mapping_schema)  # 🧩 Fix: load after enrich

    df_mapping["Retry_Count"] = pd.to_numeric(df_mapping["Retry_Count"], errors="coerce").fillna(0).astype(int)

    timer = ProgressTimer(len(df_mapping))
    start_time = datetime.now()

    for _, row in df_mapping.iterrows():
        if (datetime.now() - start_time) > timedelta(minutes=30):
            logger.info("⏳ Runtime > 30 minutes — refreshing token")
            auto_refresh_token_if_needed()
            start_time = datetime.now()
        post_vendor(row)
        timer.update()

    logger.info("✅ Vendor migration completed via conditional execution.")

