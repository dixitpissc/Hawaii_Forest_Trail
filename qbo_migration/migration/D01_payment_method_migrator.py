"""
Sequence : 1
Module: Payment_method_migrator.py
Author: Dixit Prajapati
Created: 2025-09-17
Description: Handles migration of payment method records from source system to QBO.
Production : Ready
Phase : 02 - Multi User
"""

import os, json, requests
import pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from utils.log_timer import global_logger as logger, ProgressTimer
from config.mapping.payment_method_mapping import PAYMENT_METHOD_COLUMN_MAPPING
# ✅ DB-backed QBO helpers (no user id needed here)
from utils.token_refresher import get_qbo_context_migration, MINOR_VERSION,auto_refresh_token_if_needed
import re

load_dotenv()
auto_refresh_token_if_needed()

# =========================
# Config
# =========================
SOURCE_TABLE   = "PaymentMethod"
SOURCE_SCHEMA  = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
MAPPING_TABLE  = "Map_PaymentMethod"
MAX_RETRIES    = 3

# =========================
# QBO auth (active user implicit)
# =========================
def get_qbo_auth():
    ctx = get_qbo_context_migration()
    base = ctx["BASE_URL"]
    realm_id = ctx["REALM_ID"]

    post_url  = f"{base}/v3/company/{realm_id}/paymentmethod?minorversion={MINOR_VERSION}"
    query_url = ctx["QUERY_URL"]

    headers_post = {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    headers_query = ctx["HEADERS"]  # has Content-Type: application/text
    return post_url, query_url, headers_post, headers_query

def ensure_mapping_table():
    """
    Prepares the payment method mapping table with payloads.
    """
    df = sql.fetch_table(SOURCE_TABLE, SOURCE_SCHEMA)

    if sql.table_exists(MAPPING_TABLE, MAPPING_SCHEMA):
        sql.run_query(f"DELETE FROM [{MAPPING_SCHEMA}].[{MAPPING_TABLE}]")

    df.rename(columns={"Id": "Source_Id"}, inplace=True)
    df["Target_Id"] = None
    df["Retry_Count"] = 0
    df["Porter_Status"] = None
    df["Failure_Reason"] = None
    df["Payload_JSON"] = None

    payloads = []
    for _, row in df.iterrows():
        payload = {}
        for src_col, qbo_col in PAYMENT_METHOD_COLUMN_MAPPING.items():
            value = row.get(src_col)
            if pd.notna(value):
                payload[qbo_col] = value

        row["Payload_JSON"] = json.dumps(payload, indent=2)
        payloads.append(row)

    df_final = pd.DataFrame(payloads).where(pd.notnull, None)
    sql.insert_dataframe(df_final, MAPPING_TABLE, MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df_final)} rows into {MAPPING_SCHEMA}.{MAPPING_TABLE}")

def query_existing_payment_method(name, query_url, headers):
    """
    Queries QBO to check if a payment method with the given name exists.
    """
    name_safe = name.replace("'", "''")
    query = f"SELECT Id FROM PaymentMethod WHERE Name = '{name_safe}'"
    resp = requests.post(query_url, headers=headers, data=json.dumps({"query": query}))
    if resp.status_code == 200:
        records = resp.json().get("QueryResponse", {}).get("PaymentMethod", [])
        if records:
            return records[0]["Id"]
    return None

session = requests.Session()

def post_payment_method(row):
    """
    Posts a single PaymentMethod record to QBO.
    Optimized: avoids redundant token refresh, uses session, structured SQL updates.
    """
    source_id = row["Source_Id"]
    name = row.get("Name")

    # Skip if already migrated
    if pd.notna(row["Target_Id"]) and row["Porter_Status"] in ("Success", "Exists"):
        logger.debug(f"⏭️ Skipped (already migrated): {source_id}")
        return

    if int(row.get("Retry_Count", 0)) >= MAX_RETRIES:
        logger.warning(f"⚠️ Skipping {source_id} — retry limit reached")
        return

    post_url, query_url, headers, headers_query = get_qbo_auth()

    # 1. Check if already exists in QBO
    existing_id = query_existing_payment_method(name, query_url, headers_query)
    if existing_id:
        logger.info(f"🔁 Exists in QBO: {name} → {existing_id}")
        sql.run_query(f"""
            UPDATE [{MAPPING_SCHEMA}].[{MAPPING_TABLE}]
            SET Target_Id = ?, Porter_Status = 'Exists', Failure_Reason = NULL
            WHERE Source_Id = ?
        """, (existing_id, source_id))
        return

    payload_json = row.get("Payload_JSON")

    try:
        resp = session.post(post_url, headers=headers, json=json.loads(payload_json))
        if resp.status_code == 200:
            qbo_id = resp.json()["PaymentMethod"]["Id"]
            logger.info(f"✅ Migrated: {name} → {qbo_id}")
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[{MAPPING_TABLE}]
                SET Target_Id = ?, Porter_Status = 'Success', Failure_Reason = NULL
                WHERE Source_Id = ?
            """, (qbo_id, source_id))

        elif resp.status_code in (401, 403):
            logger.warning("🔑 Token expired — refreshing and retrying...")
            auto_refresh_token_if_needed()
            return post_payment_method(row)  # retry once

        else:
            reason = resp.text[:300]
            if "Duplicate Name Exists" in reason:
                match = re.search(r"Id=(\d+)", reason)
                if match:
                    existing_id = match.group(1)
                    logger.warning(f"🔁 Recovered duplicate: {name} → {existing_id}")
                    sql.run_query(f"""
                        UPDATE [{MAPPING_SCHEMA}].[{MAPPING_TABLE}]
                        SET Target_Id = ?, Porter_Status = 'Exists', Failure_Reason = NULL
                        WHERE Source_Id = ?
                    """, (existing_id, source_id))
                    return

            logger.error(f"❌ Failed: {name} → {reason}")
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[{MAPPING_TABLE}]
                SET Porter_Status = 'Failed',
                    Retry_Count = ISNULL(Retry_Count, 0) + 1,
                    Failure_Reason = ?
                WHERE Source_Id = ?
            """, (reason, source_id))

    except Exception as e:
        logger.exception(f"❌ Exception posting {name}: {e}")
        sql.run_query(f"""
            UPDATE [{MAPPING_SCHEMA}].[{MAPPING_TABLE}]
            SET Porter_Status = 'Failed',
                Retry_Count = ISNULL(Retry_Count, 0) + 1,
                Failure_Reason = ?
            WHERE Source_Id = ?
        """, (str(e), source_id))

def migrate_payment_methods():
    """
    Executes PaymentMethod migration with optimized loop and progress tracking.
    """
    logger.info("\n🚀 Starting PaymentMethod Migration\n" + "=" * 40)
    ensure_mapping_table()

    df = sql.fetch_table(MAPPING_TABLE, MAPPING_SCHEMA)
    df = df[df["Porter_Status"].isna() | (df["Porter_Status"] == "Failed")]
    df["Retry_Count"] = pd.to_numeric(df["Retry_Count"], errors="coerce").fillna(0).astype(int)

    if df.empty:
        logger.info("✅ Nothing to migrate.")
        return

    pt = ProgressTimer(len(df))

    for _, row in df.iterrows():
        post_payment_method(row)
        pt.update()

    logger.info("\n🏁 PaymentMethod migration completed.\n")
