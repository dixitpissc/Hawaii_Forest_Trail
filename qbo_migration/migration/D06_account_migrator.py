
"""
Sequence : 06
Module: account_migrator.py
Author: Dixit Prajapati
Created: 2025-09-17
Description: Handles migration of Accounts records from source system to QBO,
             including parent-child hierarchy processing, retry logic, and status tracking.
Production : NotReady (Require changes in failed Accounts)
Production : Ready
Phase : 02 - Multi User
"""

import os
import logging
import requests
import pandas as pd
import json
from dotenv import load_dotenv
from datetime import datetime
import sys
import time
from storage.sqlserver import sql
from config.mapping.account_mapping import ACCOUNT_COLUMN_MAPPING as column_mapping
from utils.token_refresher import auto_refresh_token_if_needed,get_qbo_context_migration
from utils.log_timer import global_logger
import re
import json


# === Initialize dual logging (console + file) ===
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG_FILE_PATH = os.path.join(LOG_DIR, log_filename)

global_logger.setLevel(logging.INFO)
global_logger.propagate = False

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Avoid duplicate handlers
if global_logger.hasHandlers():
    global_logger.handlers.clear()

# File handler — UTF-8, supports emojis
file_handler = logging.FileHandler(LOG_FILE_PATH, mode='w', encoding='utf-8')
file_handler.setFormatter(formatter)
global_logger.addHandler(file_handler)


_DUPLICATE_PATTERNS = [
    r"Duplicate\s+Name\s+Exists",
    r"Another\s+account\s+is\s+already\s+using\s+this\s+number",
    r"Another\s+account\s+is\s+already\s+using\s+number",
    r"Duplicate\s+Account\s*Number",
    r"Account\s+number\s+already\s+in\s+use",
]

def _is_duplicate_name_or_number_error(resp_text: str) -> bool:
    """Covers JSON fault code 6000 and common duplicate texts."""
    if not resp_text:
        return False
    # Try JSON first
    try:
        j = json.loads(resp_text)
        errs = (j.get("Fault", {}) or {}).get("Error", []) or []
        for e in errs:
            code = str(e.get("code", "")).strip()
            detail = (e.get("Detail") or "") + " " + (e.get("Message") or "")
            if code in {"6000", "6140"}:
                # 6000: Business validation (includes acctnum already used)
                # 6140: Duplicate Name Exists
                return True
            for pat in _DUPLICATE_PATTERNS:
                if re.search(pat, detail, flags=re.IGNORECASE):
                    return True
    except Exception:
        pass

    # Fallback: plain-text / HTML-ish responses
    text = resp_text.lower()
    if '"code":"6140"' in text or '"code":"6000"' in text:
        return True
    for pat in _DUPLICATE_PATTERNS:
        if re.search(pat, text, flags=re.IGNORECASE):
            return True
    return False

# Console handler — avoid crashing on Windows encoding issues

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
global_logger.addHandler(console_handler)

# Alias logger for convenience
logger = global_logger

auto_refresh_token_if_needed()
load_dotenv()

# === QBO Auth Config ===
ctx = get_qbo_context_migration()
access_token = ctx['ACCESS_TOKEN']
realm_id = ctx["REALM_ID"]

environment = os.getenv("QBO_ENVIRONMENT", "sandbox")

base_url = "https://sandbox-quickbooks.api.intuit.com" if environment == "sandbox" else "https://quickbooks.api.intuit.com"
query_url = f"{base_url}/v3/company/{realm_id}/query"
post_url = f"{base_url}/v3/company/{realm_id}/account"

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

source_table = "Account"
source_schema = "dbo"
mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
mapping_table = "Map_Account"
max_retries = 3

success_count = 0
failure_count = 0

def apply_duplicate_acctnum_strategy():
    """
    Disambiguates duplicate AcctNum values in Map_Account.

    Logic:
    - Skips null/empty/whitespace-only AcctNum completely (keeps Updated_AcctNum = NULL).
    - For 2 duplicates:
        - If one has 'deleted' in Name → that one gets '-del', other stays unchanged
        - Else → both get '-01', '-02'
    - For >2 duplicates:
        - If any have 'deleted' in Name:
            - Deleted ones get '-del01', '-del02'...
            - Others get '-01', '-02'...
        - Else → all get '-01', '-02', ...
    """
    logger.info("🔍 Detecting and disambiguating duplicate AcctNum entries...")

    # Ensure Updated_AcctNum column exists
    sql.run_query(f"""
        IF COL_LENGTH('{mapping_schema}.Map_Account', 'Updated_AcctNum') IS NULL
        BEGIN
            ALTER TABLE [{mapping_schema}].[Map_Account]
            ADD Updated_AcctNum NVARCHAR(100);
        END
    """)
    logger.info("📝 Ensured 'Updated_AcctNum' column exists in Map_Account.")

    # Fetch table
    df = sql.fetch_table(mapping_table, mapping_schema)

    # 🔑 Normalize AcctNum: strip whitespace and treat ""/NULL as invalid
    df["AcctNum"] = df["AcctNum"].astype(str).str.strip()
    df.loc[df["AcctNum"].isin(["", "nan", "None"]), "AcctNum"] = None

    # Filter only valid account numbers
    valid_df = df[df["AcctNum"].notna()]

    acct_counts = valid_df["AcctNum"].value_counts()
    duplicates = acct_counts[acct_counts > 1].index.tolist()

    for acctnum in duplicates:
        rows = valid_df[valid_df["AcctNum"] == acctnum].copy()
        rows.reset_index(drop=True, inplace=True)
        total = len(rows)

        # Case: 2 duplicates
        if total == 2:
            has_deleted = rows["Name"].str.lower().str.contains("deleted", na=False)
            if has_deleted.any():
                for _, row in rows.iterrows():
                    if "deleted" in str(row["Name"]).lower():
                        new_acctnum = f"{acctnum}-del"
                        sql.run_query(f"""
                            UPDATE [{mapping_schema}].[Map_Account]
                            SET Updated_AcctNum = ?
                            WHERE Source_Id = ?
                        """, (new_acctnum, row["Source_Id"]))
                        logger.warning(f"⚠️ AcctNum '{acctnum}' → '{new_acctnum}' (2x, with deleted)")
            else:
                for i, row in rows.iterrows():
                    new_acctnum = f"{acctnum}-{i+1:02d}"
                    sql.run_query(f"""
                        UPDATE [{mapping_schema}].[Map_Account]
                        SET Updated_AcctNum = ?
                        WHERE Source_Id = ?
                    """, (new_acctnum, row["Source_Id"]))
                    logger.warning(f"⚠️ AcctNum '{acctnum}' → '{new_acctnum}' (2x, no deleted)")

        # Case: >2 duplicates
        elif total > 2:
            deleted_rows = rows[rows["Name"].str.lower().str.contains("deleted", na=False)]
            normal_rows = rows[~rows["Name"].str.lower().str.contains("deleted", na=False)]

            if not deleted_rows.empty:
                for j, (_, row) in enumerate(deleted_rows.iterrows(), start=1):
                    new_acctnum = f"{acctnum}-del{j:02d}"
                    sql.run_query(f"""
                        UPDATE [{mapping_schema}].[Map_Account]
                        SET Updated_AcctNum = ?
                        WHERE Source_Id = ?
                    """, (new_acctnum, row["Source_Id"]))
                    logger.warning(f"⚠️ AcctNum '{acctnum}' → '{new_acctnum}' (deleted)")

                for j, (_, row) in enumerate(normal_rows.iterrows(), start=1):
                    new_acctnum = f"{acctnum}-{j:02d}"
                    sql.run_query(f"""
                        UPDATE [{mapping_schema}].[Map_Account]
                        SET Updated_AcctNum = ?
                        WHERE Source_Id = ?
                    """, (new_acctnum, row["Source_Id"]))
                    logger.warning(f"⚠️ AcctNum '{acctnum}' → '{new_acctnum}' (non-deleted)")
            else:
                for i, row in rows.iterrows():
                    new_acctnum = f"{acctnum}-{i+1:02d}"
                    sql.run_query(f"""
                        UPDATE [{mapping_schema}].[Map_Account]
                        SET Updated_AcctNum = ?
                        WHERE Source_Id = ?
                    """, (new_acctnum, row["Source_Id"]))
                    logger.warning(f"⚠️ AcctNum '{acctnum}' → '{new_acctnum}' (no deleted)")

    logger.info("✅ Completed duplicate AcctNum disambiguation (null/empty skipped).")


# ---------- REPLACE _qbo_select with this (keeps current behavior + supports STARTPOSITION/MAXRESULTS) ----------
def _qbo_select(query: str, fields: str = "*", startposition: int | None = None, maxresults: int | None = None):
    """
    Wrapper for QBO query endpoint. You can pass a plain WHERE clause as `query`.
    Optionally supports pagination via startposition/maxresults.
    Returns list of objects (or empty list).
    """
    q = f"SELECT {fields} FROM Account WHERE {query}"
    if startposition is not None:
        q += f" STARTPOSITION {startposition}"
    if maxresults is not None:
        q += f" MAXRESULTS {maxresults}"

    resp = session.get(
        query_url,
        headers=headers,
        params={"query": q, "minorversion": "75"}
    )
    if resp.status_code != 200:
        logger.warning(f"❌ QBO query failed")
        return []
    return resp.json().get("QueryResponse", {}).get("Account", []) or []

# ---------- Add a helper to page through all Account records (safe fallback) ----------
def _fetch_all_accounts_pagewise(batch_size: int = 1000):
    """
    Fetch all accounts pagewise (STARTPOSITION) and yield each Account dict.
    Use this only as a last-resort fallback (when you need to match by AcctNum).
    """
    start = 1
    while True:
        q = f"SELECT * FROM Account STARTPOSITION {start} MAXRESULTS {batch_size}"
        resp = session.get(query_url, headers=headers, params={"query": q, "minorversion": "75"})
        if resp.status_code != 200:
            logger.warning(f"❌ QBO paging failed: {resp.status_code} {resp.text[:200]}")
            return  # stop iteration on error
        resp_json = resp.json()
        accounts = resp_json.get("QueryResponse", {}).get("Account", []) or []
        if not accounts:
            return
        for a in accounts:
            yield a
        # If fewer than a full page returned, done
        if len(accounts) < batch_size:
            return
        start += batch_size

# ---------- REPLACE _get_existing_by_row with this safe version ----------
def _get_existing_by_row(row_dict: dict) -> str | None:
    """
    Safe lookup order:
      1) Try Name (queryable) -> returns first match (and reactivates if needed)
      2) Try FullyQualifiedName (queryable)
      3) If AcctNum provided: **do not** use WHERE AcctNum (not queryable). Instead
         page accounts and compare AcctNum client-side.
    """
    # Helper to reactivate existing row if needed
    def maybe_reactivate_and_return(acc_row):
        return _reactivate_if_needed(acc_row)

    # 1) Name
    name = str(row_dict.get("Name") or "").strip()
    if name:
        # Escape single quotes by doubling them (QBO expects SQL-style escaping)
        val = name.replace("'", "''")
        try:
            accs = _qbo_select(f"Name = '{val}'", fields="Id, Active, SyncToken, Name, AcctNum")
            if accs:
                return maybe_reactivate_and_return(accs[0])
        except Exception as e:
            logger.warning(f"⚠️ Name lookup failed for '{name}': {e}")

    # 2) FullyQualifiedName
    fqn = str(row_dict.get("FullyQualifiedName") or "").strip()
    if fqn:
        val = fqn.replace("'", "''")
        try:
            accs = _qbo_select(f"FullyQualifiedName = '{val}'", fields="Id, Active, SyncToken, Name, FullyQualifiedName, AcctNum")
            if accs:
                return maybe_reactivate_and_return(accs[0])
        except Exception as e:
            logger.warning(f"⚠️ FQN lookup failed for '{fqn}': {e}")

    # 3) Last resort: AcctNum (page and filter client-side)
    acctnum = (row_dict.get("Updated_AcctNum") or row_dict.get("AcctNum"))
    if acctnum:
        acctnum_str = str(acctnum).strip()
        if acctnum_str:
            logger.info(f"🔎 Falling back to page-and-filter by AcctNum='{acctnum_str}' (server does not support WHERE AcctNum)")
            try:
                for acc in _fetch_all_accounts_pagewise(batch_size=1000):
                    # account's AcctNum may be missing or different types; normalize to str for compare
                    if str(acc.get("AcctNum") or "").strip() == acctnum_str:
                        return maybe_reactivate_and_return(acc)
            except Exception as e:
                logger.warning(f"⚠️ AcctNum fallback failed: {e}")

    return None


def _get_account_by_id(acc_id: str, fields: str = "Id, Name, Active, SyncToken"):
    rows = _qbo_select(f"Id = '{acc_id}'", fields=fields)
    return rows[0] if rows else None

def _reactivate_if_needed(acc_row: dict) -> str | None:
    if not acc_row:
        return None
    if acc_row.get("Active") is True:
        return acc_row.get("Id")

    # ensure SyncToken is present
    if "SyncToken" not in acc_row:
        acc_row = _get_account_by_id(acc_row.get("Id"), fields="Id, Active, SyncToken, Name")
        if not acc_row:
            return None

    update_payload = {"Id": acc_row["Id"], "SyncToken": acc_row["SyncToken"], "sparse": True, "Active": True}
    upd = requests.post(f"{base_url}/v3/company/{realm_id}/account", headers=headers, json=update_payload)
    if upd.status_code == 200:
        logger.info(f"🔄 Reactivated Account Id={acc_row['Id']}")
    else:
        logger.warning(f"⚠️ Reactivate failed for {acc_row['Id']}: {upd.status_code} {upd.text[:200]}")
    return acc_row["Id"]

# def _get_existing_by_row(row_dict: dict) -> str | None:
#     # 1) Updated_AcctNum / AcctNum
#     acctnum = (row_dict.get("Updated_AcctNum") or row_dict.get("AcctNum"))
#     if acctnum:
#         val = str(acctnum).strip().replace("'", "''")
#         accs = _qbo_select(f"AcctNum = '{val}'", fields="Id, Active, SyncToken, Name, AcctNum")
#         if accs:
#             return _reactivate_if_needed(accs[0])

#     # 2) Name (no AccountType filter to avoid false negatives)
#     name = str(row_dict.get("Name") or "").strip()
#     if name:
#         # Escape single quotes for QBO query
#         val = name.replace("'", "''")
#         accs = _qbo_select(f"Name = '{val}'", fields="Id, Active, SyncToken, Name")
#         if accs:
#             return _reactivate_if_needed(accs[0])

#     # 3) FullyQualifiedName (if present in your source)
#     fqn = str(row_dict.get("FullyQualifiedName") or "").strip()
#     if fqn:
#         # Escape single quotes for QBO query
#         val = fqn.replace("'", "''")
#         accs = _qbo_select(f"FullyQualifiedName = '{val}'", fields="Id, Active, SyncToken, Name, FullyQualifiedName")
#         if accs:
#             return _reactivate_if_needed(accs[0])
#     return None

# ✅ Backward-compatible wrapper: supports old (name, account_type) AND new (row) usage
def get_existing_qbo_account_id(arg, account_type=None) -> str | None:
    if isinstance(arg, dict):
        row_dict = arg
    elif hasattr(arg, "to_dict"):  # pandas Series
        row_dict = arg.to_dict()
    else:
        # old signature path
        row_dict = {"Name": arg, "AccountType": account_type}
    return _get_existing_by_row(row_dict)

def build_account_hierarchy(df_accounts):
    """
    Calculates and assigns hierarchy levels to accounts based on ParentRef and SubAccount flag.

    Args:
        df_accounts (pd.DataFrame): Source accounts DataFrame.

    Returns:
        pd.DataFrame: Accounts sorted by hierarchical level with 'Level' column added.
    """

    df_accounts["Level"] = None
    id_to_row = df_accounts.set_index("Id").to_dict(orient="index")

    def get_level(account_id, visited=None):
        visited = visited or set()
        if account_id in visited:
            return 0
        visited.add(account_id)

        row = id_to_row.get(account_id)
        if not row or not str(row.get("SubAccount")).lower() == "true":
            return 0

        parent_id = row.get("ParentRef.value")
        return 1 + get_level(parent_id, visited) if parent_id else 0

    df_accounts["Level"] = df_accounts["Id"].apply(get_level)
    return df_accounts.sort_values(by="Level")

session = requests.Session()

def migrate(df_subset, df_mapping, remigrate_failed=False, is_child=False, timer=None):
    """
    Migrates a subset of account records to QBO, respecting parent-child relationships and retry logic.

    Args:
        df_subset (pd.DataFrame): Subset of accounts to migrate.
        df_mapping (pd.DataFrame): Mapping table with Source_Id to Target_Id info.
        remigrate_failed (bool): If True, retries only previously failed records.
        is_child (bool): If True, handles ParentRef linking for subaccounts.
        timer (ProgressTimer, optional): Progress timer for real-time feedback.

    Side Effects:
        - Posts account data to QBO.
        - Updates mapping table in SQL Server with status, errors, and QBO IDs.
        - Logs success, failure, and retries.
    """

    global success_count, failure_count

    for _, row in df_subset.iterrows():
        source_id = row["Id"]
        existing = df_mapping.query(f"Source_Id == '{source_id}'", engine='python')

        if not existing.empty and pd.notnull(existing.iloc[0]["Target_Id"]):
            if not remigrate_failed:
                logger.info(f"✅ Already migrated: {row['Name']}")
                # timer removed
                continue
            elif existing.iloc[0]['Porter_Status'] == 'Failed':
                retry_count = existing.iloc[0].get("Retry_Count", 0)
                if retry_count >= max_retries:
                    logger.warning(f"⛔ Skipping {row['Name']} — reached max retry limit")
                    # timer removed
                    continue
                logger.info(f"🔁 Retrying: {row['Name']} (Attempt {retry_count + 1})")
            else:
                # timer removed
                continue

        payload = {}
        for src_col, qbo_col in column_mapping.items():

            # ⛔ Skip AcctNum here — we will handle it separately
            if src_col == "AcctNum":
                continue

            value = row.get(src_col)
            if pd.notna(value) and str(value).strip():
                if qbo_col == "CurrencyRef.value":
                    payload["CurrencyRef"] = {"value": str(value)}
                else:
                    payload[qbo_col] = value

        # ✅ Handle duplicate-safe AcctNum
        acctnum = row.get("Updated_AcctNum") or row.get("AcctNum")
        if acctnum:
            payload["AcctNum"] = acctnum

        if is_child:
            parent_id = row.get("ParentRef.value")
            if pd.isna(parent_id):
                logger.warning(f"⏭️ Skipping child {row['Name']} — missing parent")
                # timer removed
                continue

            parent_qbo_id = sql.fetch_single_value(
                f"SELECT Target_Id FROM [{mapping_schema}].[{mapping_table}] WHERE Source_Id = ?", (parent_id,))
            if not parent_qbo_id:
                logger.warning(f"⏭️ Skipping child {row['Name']} — parent not yet migrated")
                # timer removed
                continue

            payload["ParentRef"] = {"value": str(parent_qbo_id)}
        payload["Active"] = True  # ✅ Ensure all Accounts are active


        try:
            existing_qbo_id = get_existing_qbo_account_id(row["Name"], row.get("AccountType"))

            if existing_qbo_id:
                logger.info(f"🔄 Exists in QBO: {row['Name']} → {existing_qbo_id}")
                sql.run_query(f"""
                    UPDATE [{mapping_schema}].[{mapping_table}]
                    SET Target_Id = ?, Porter_Status = 'Exists', Failure_Reason = NULL,
                        Payload_JSON = ?
                    WHERE Source_Id = ?
                """, (existing_qbo_id, json.dumps(payload, indent=2), source_id))
                if timer:
                    timer.update()
                continue

            logger.info(f"📤 Posting account: {payload.get('Name')}")
            response = session.post(post_url, headers=headers, json=payload)

            if response.status_code == 200:
                target_id = response.json()["Account"]["Id"]
                logger.info(f"✅ Migrated: {payload.get('Name')} → {target_id}")
                success_count += 1
                sql.run_query(f"""
                    UPDATE [{mapping_schema}].[{mapping_table}]
                    SET Target_Id = ?, Porter_Status = 'Success', Failure_Reason = NULL, Payload_JSON = ?
                    WHERE Source_Id = ?
                """, (target_id, json.dumps(payload, indent=2), source_id))

            else:
                reason_full = response.text  # keep full for parsing & logging

                # 🔁 If duplicate (6000/6140 or matching text), try to recover using existing logic
                if _is_duplicate_name_or_number_error(reason_full):
                    recovered_id = get_existing_qbo_account_id(row)  # ✅ reuse existing helper
                    if recovered_id:
                        logger.info(f"♻️ Duplicate detected. Recovered existing QBO Id: {row['Name']} → {recovered_id}")
                        success_count += 1
                        sql.run_query(f"""
                            UPDATE [{mapping_schema}].[{mapping_table}]
                            SET Target_Id = ?, Porter_Status = 'Exists', Failure_Reason = NULL, Payload_JSON = ?
                            WHERE Source_Id = ?
                        """, (recovered_id, json.dumps(payload, indent=2), source_id))
                        # timer removed
                        continue  # move to next record

                reason_short = (reason_full or "")[:250]
                logger.warning(f"❌ Failed to post {row['Name']}: {response.status_code} {reason_short}")
                failure_count += 1
                sql.run_query(f"""
                    UPDATE [{mapping_schema}].[{mapping_table}]
                    SET Porter_Status = 'Failed', Failure_Reason = ?, Retry_Count = ISNULL(Retry_Count, 0) + 1,
                        Payload_JSON = ?
                    WHERE Source_Id = ?
                """, (reason_short, json.dumps(payload, indent=2), source_id))

        except Exception as e:
            logger.error(f"❌ Exception during migration: {row['Name']} → {e}")
            failure_count += 1
            sql.run_query(f"""
                UPDATE [{mapping_schema}].[{mapping_table}]
                SET Porter_Status = 'Failed', Failure_Reason = ?, Retry_Count = ISNULL(Retry_Count, 0) + 1,
                    Payload_JSON = ?
                WHERE Source_Id = ?
            """, (str(e), json.dumps(payload, indent=2), source_id))
        # finally:
        #     # timer removed
        #     pass

def migrate_accounts():
    """
    Orchestrates the full Account migration process:

    - Fetches source accounts.
    - Ensures mapping schema/table exist.
    - Assigns account hierarchy levels.
    - Migrates accounts from parent to child.
    - Retries failed records if within allowed retry limit.
    - Tracks and logs progress, status, and outcomes.
    """
    
    global success_count, failure_count

    try:
        logger.info(f"\n🚀 Starting Account Migration Phase\n{'='*40}")
        logger.info(f"🌍 QBO Environment: {environment.upper()} | Realm ID: {realm_id}")

        df = sql.fetch_table(source_table, source_schema)
        if df.empty:
            logger.warning("⚠️ No source Account records found.")
            return

        total_records = len(df)
        sql.ensure_schema_exists(mapping_schema)
        df_mapping = sql.fetch_table(mapping_table, mapping_schema) if sql.table_exists(mapping_table, mapping_schema) else pd.DataFrame()

        if df_mapping.empty:
            df_mapping = df.copy()
            df_mapping.rename(columns={"Id": "Source_Id"}, inplace=True)
            df_mapping["Target_Id"] = None
            df_mapping["Porter_Status"] = None
            df_mapping["Failure_Reason"] = None
            df_mapping["Retry_Count"] = 0
            df_mapping["Payload_JSON"] = None
            sql.insert_dataframe(df_mapping, mapping_table, mapping_schema)
            # 👉 Apply AcctNum duplicate strategy here
            apply_duplicate_acctnum_strategy()
            # 🆕 Refresh mapping to get Updated_AcctNum values
            df_mapping = sql.fetch_table(mapping_table, mapping_schema)
        # 🔁 Merge Updated_AcctNum into main df
        df = df.merge(df_mapping[["Source_Id", "Updated_AcctNum"]], how="left", left_on="Id", right_on="Source_Id")

        df_mapping = sql.fetch_table(mapping_table, mapping_schema)
        df = build_account_hierarchy(df)

        for level in sorted(df["Level"].unique()):
            df_level = df[df["Level"] == level]
            logger.info(f"\n📊 Migrating Level {level} — {len(df_level)} records")
            migrate(df_level, df_mapping, remigrate_failed=False, is_child=(level > 0))

        retry_failed_accounts()

        logger.info(f"\n✅ Migration Summary: {success_count} succeeded, {failure_count} failed.")

    except Exception as e:
        logger.error(f"❌ Migration error: {e}")

def retry_failed_accounts(timer=None):
    """
    Retries migration for failed account records that are below the retry threshold.

    Args:
        timer (ProgressTimer, optional): Timer for tracking retry progress.

    Side Effects:
        - Retries only accounts marked as 'Failed'.
        - Skips records that exceeded max_retries.
        - Updates mapping table with new attempt results.
    """
        
    logger.info(f"\n🔁 Retrying Failed Accounts\n{'='*40}")
    try:
        df_mapping = sql.fetch_table(mapping_table, mapping_schema)
        df_mapping["Retry_Count"] = pd.to_numeric(df_mapping["Retry_Count"], errors="coerce").fillna(0).astype(int)

        failed_ids = df_mapping[
            (df_mapping["Porter_Status"] == "Failed") & (df_mapping["Retry_Count"] < max_retries)
        ]["Source_Id"].tolist()

        if not failed_ids:
            logger.info("✅ No eligible failed accounts to retry.")
            return

        df_all = sql.fetch_table(source_table, source_schema)
        df_remigrate = df_all[df_all["Id"].isin(failed_ids)]
        df_mapping = sql.fetch_table(mapping_table, mapping_schema)
        df_remigrate = build_account_hierarchy(df_remigrate)

        for level in sorted(df_remigrate["Level"].unique()):
            df_level = df_remigrate[df_remigrate["Level"] == level]
            migrate(df_level, df_mapping, remigrate_failed=True, is_child=(level > 0))

    except Exception as e:
        logger.error(f"❌ Error during retry: {e}")

def migrate_missing_accounts():
    """
    Detects accounts in dbo.Account that are not present in Map_Account.Source_Id,
    sets them as active (even if originally inactive), tracks those updates in
    Was_Inactive_Originally column, and initiates migration.
    """
    global success_count, failure_count

    logger.info("\n🔍 Checking for unmapped/inactive accounts...\n" + "=" * 40)

    # Step 1: Load source and mapping data
    df_source = sql.fetch_table(source_table, source_schema)
    if df_source.empty:
        logger.warning("⚠️ No source accounts found in dbo.Account.")
        return

    if sql.table_exists(mapping_table, mapping_schema):
        df_mapping = sql.fetch_table(mapping_table, mapping_schema)
    else:
        df_mapping = pd.DataFrame()

    sql.ensure_schema_exists(mapping_schema)

    # Step 2: Add tracking column if missing
    if "Was_Inactive_Originally" not in df_mapping.columns:
        try:
            sql.run_query(f"""
                IF COL_LENGTH('{mapping_schema}.{mapping_table}', 'Was_Inactive_Originally') IS NULL
                BEGIN
                    ALTER TABLE [{mapping_schema}].[{mapping_table}]
                    ADD Was_Inactive_Originally BIT DEFAULT 0;
                END
            """)
            logger.info("📝 Added 'Was_Inactive_Originally' column to Map_Account.")
        except Exception as e:
            logger.warning(f"⚠️ Could not add Was_Inactive_Originally column: {e}")

    # Step 3: Find unmapped accounts (compare dbo.Account.Id NOT IN Map_Account.Source_Id)
    mapped_ids = df_mapping["Source_Id"].unique().tolist() if not df_mapping.empty else []
    df_missing = df_source[~df_source["Id"].isin(mapped_ids)].copy()

    if df_missing.empty:
        logger.info("✅ No new accounts to migrate.")
        return

    logger.info(f"📌 Found {len(df_missing)} new account(s) to migrate.")

    # Step 4: Normalize and detect inactive status
    df_missing["Active"] = df_missing["Active"].astype(str).str.lower().str.strip()
    df_missing["Was_Inactive_Originally"] = df_missing["Active"].isin(["false", "0", "no", "n"])
    df_missing["Active"] = True  # Force Active=True for migration

    # Step 5: Build hierarchy for Level-wise migration
    df_missing = build_account_hierarchy(df_missing)

    # Step 6: Prepare mapping records for insert
    df_to_insert = df_missing.copy()
    df_to_insert.rename(columns={"Id": "Source_Id"}, inplace=True)
    df_to_insert["Target_Id"] = None
    df_to_insert["Porter_Status"] = None
    df_to_insert["Failure_Reason"] = None
    df_to_insert["Retry_Count"] = 0
    df_to_insert["Payload_JSON"] = None

    if "Level" in df_to_insert.columns:
        df_to_insert.drop(columns=["Level"], inplace=True)

    # Step 7: Insert into Map_Account
    sql.insert_dataframe(df_to_insert, mapping_table, mapping_schema)

    # 👉 Apply AcctNum duplicate strategy here
    apply_duplicate_acctnum_strategy()
    df_mapping = sql.fetch_table(mapping_table, mapping_schema)

    df_missing = df_missing.merge(df_mapping[["Source_Id", "Updated_AcctNum"]], how="left", left_on="Id", right_on="Source_Id")

    # Step 8: Reload mapping and restore original Id column
    df_mapping = sql.fetch_table(mapping_table, mapping_schema)
    df_missing["Source_Id"] = df_missing["Id"]
    df_missing = build_account_hierarchy(df_missing)

    # Step 9: Migrate level-by-level
    total_new = len(df_missing)

    for level in sorted(df_missing["Level"].unique()):
        df_level = df_missing[df_missing["Level"] == level]
        logger.info(f"\n📊 Migrating new accounts at Level {level} — {len(df_level)} records")
        migrate(df_level, df_mapping, remigrate_failed=False, is_child=(level > 0))
    logger.info(f"\n✅ Missing account migration complete — {success_count} succeeded, {failure_count} failed.")


