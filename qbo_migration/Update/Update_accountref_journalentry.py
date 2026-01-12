#!/usr/bin/env python3
"""
all_transaction_update_accountref.py
JournalEntry sparse-update fix: include Line.DetailType (and Amount) in sparse payload so QBO accepts it.
"""

import os
import time
import json
import requests
import pyodbc
from dotenv import load_dotenv
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import global_logger as logger

load_dotenv()
auto_refresh_token_if_needed()

# -------------------------------
# ENV / QBO
# -------------------------------
SQLSERVER_HOST = os.getenv("SQLSERVER_HOST")
SQLSERVER_PORT = os.getenv("SQLSERVER_PORT", "1433")
SQLSERVER_USER = os.getenv("SQLSERVER_USER")
SQLSERVER_PASSWORD = os.getenv("SQLSERVER_PASSWORD")
SQLSERVER_DATABASE = os.getenv("SQLSERVER_DATABASE")

QBO_ENVIRONMENT = os.getenv("QBO_ENVIRONMENT", "production").lower()
REALM_ID = os.getenv("QBO_REALM_ID")

if QBO_ENVIRONMENT == "sandbox":
    BASE = "https://sandbox-quickbooks.api.intuit.com"
elif QBO_ENVIRONMENT == "production":
    BASE = "https://quickbooks.api.intuit.com"
else:
    raise ValueError("QBO_ENVIRONMENT must be 'sandbox' or 'production'")

QUERY_URL = f"{BASE}/v3/company/{REALM_ID}/query"
QBO_BASE_URL = f"{BASE}/v3/company/{REALM_ID}"

# -------------------------------
# ACCOUNT MAPPING - update as needed
# -------------------------------
ACCOUNT_MAPPING: Dict[tuple, str] = {
    # Gas Express
    ("2838", "975"):  "2386",
    # GX Alabama Operations
    ("2838", "976") : "1150040001",
    # GX Arkansas Operations
    ("2838", "977") : "1150040000",
    # GX Georgia Operations
    ("2838", "978") : "2386",
    #GX North Carolina Operations
    ("2838", "979") : "1150040003",
    #  GX South Carolina Operations
    ("2838", "980") : "1150040002",
}

# -------------------------------
# ODBC helpers
# -------------------------------
ODBC_DRIVER = "ODBC Driver 17 for SQL Server"

def get_connection():
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};"
        f"SERVER={SQLSERVER_HOST},{SQLSERVER_PORT};"
        f"DATABASE={SQLSERVER_DATABASE};"
        f"UID={SQLSERVER_USER};PWD={SQLSERVER_PASSWORD};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=False)

def create_log_table_if_not_exists(conn):
    cur = conn.cursor()
    cur.execute("""
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'porter_entities_mapping')
    BEGIN
        EXEC('CREATE SCHEMA porter_entities_mapping');
    END
    """)
    conn.commit()

    cur.execute("""
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'porter_entities_mapping' AND TABLE_NAME = 'migration_status_alltransaction'
    )
    BEGIN
        CREATE TABLE porter_entities_mapping.migration_status_alltransaction (
            id INT IDENTITY(1,1) PRIMARY KEY,
            record_id NVARCHAR(1000),
            transaction_type NVARCHAR(1000),
            status NVARCHAR(MAX),
            record_json NVARCHAR(MAX),
            created_at DATETIME DEFAULT GETDATE()
        )
    END
    """)
    conn.commit()
    try:
        cur.execute("ALTER TABLE porter_entities_mapping.migration_status_alltransaction ALTER COLUMN status NVARCHAR(MAX)")
        cur.execute("ALTER TABLE porter_entities_mapping.migration_status_alltransaction ALTER COLUMN record_json NVARCHAR(MAX)")
        conn.commit()
    except Exception:
        pass
    cur.close()

def safe_insert_log(conn, record_id, tx_type, status, record_json=""):
    cur = conn.cursor()
    status_safe = (str(status) if status is not None else "")[:4000]
    record_json_safe = (str(record_json) if record_json is not None else "")[:1048576]
    try:
        cur.execute("""
            INSERT INTO porter_entities_mapping.migration_status_alltransaction
            (record_id, transaction_type, status, record_json)
            VALUES (?, ?, ?, ?)
        """, str(record_id), str(tx_type), status_safe, record_json_safe)
        conn.commit()
    except Exception as ex:
        logger.warning(f"Insert log failed: {ex}. Trying ALTER columns and retry.")
        try:
            cur.execute("ALTER TABLE porter_entities_mapping.migration_status_alltransaction ALTER COLUMN status NVARCHAR(MAX)")
            cur.execute("ALTER TABLE porter_entities_mapping.migration_status_alltransaction ALTER COLUMN record_json NVARCHAR(MAX)")
            conn.commit()
            cur.execute("""
                INSERT INTO porter_entities_mapping.migration_status_alltransaction
                (record_id, transaction_type, status, record_json)
                VALUES (?, ?, ?, ?)
            """, str(record_id), str(tx_type), status_safe, record_json_safe)
            conn.commit()
        except Exception as e2:
            logger.exception(f"Retry insert log failed: {e2}")
            conn.rollback()
            raise
    finally:
        cur.close()

# -------------------------------
# QBO helpers (query-based fetch)
# -------------------------------
def get_headers_for_query():
    token = os.getenv("QBO_ACCESS_TOKEN")
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/text"
    }

def fetch_record_via_query(tx_type: str, record_id: Any, retries: int = 3) -> Optional[dict]:
    q_id = str(record_id).replace("'", "''")
    query = f"SELECT * FROM {tx_type} WHERE Id = '{q_id}' STARTPOSITION 1 MAXRESULTS 1"
    headers = get_headers_for_query()
    for attempt in range(retries):
        resp = requests.post(QUERY_URL, headers=headers, data=query)
        if resp.status_code == 200:
            j = resp.json()
            qr = j.get("QueryResponse", {})
            data = qr.get(tx_type, [])
            if isinstance(data, dict):
                return data
            if isinstance(data, list) and data:
                return data[0]
            return None
        elif resp.status_code == 401:
            logger.warning("401 -> refreshing token and retrying fetch")
            auto_refresh_token_if_needed()
            headers = get_headers_for_query()
            time.sleep(0.5)
            continue
        elif resp.status_code in (429, 500, 502, 503, 504):
            wait = 2 ** attempt
            logger.warning(f"Transient QBO error {resp.status_code}. Retry in {wait}s")
            time.sleep(wait)
            continue
        else:
            logger.error(f"Failed to fetch {tx_type} {record_id}: {resp.status_code} {resp.text}")
            resp.raise_for_status()
    raise RuntimeError(f"Failed to fetch {tx_type} {record_id} after {retries} attempts")

def fetch_latest_synctoken(tx_type: str, record_id: Any) -> str:
    rec = fetch_record_via_query(tx_type, record_id)
    if not rec:
        raise RuntimeError(f"Record not found to fetch SyncToken: {tx_type} {record_id}")
    return rec.get("SyncToken", "0")

# -------------------------------
# JournalEntry sparse updater (ONLY changed lines)
# -------------------------------
def update_journal_entry(record: dict, mapping: Dict[tuple, str]) -> Optional[dict]:
    """
    Update only matching lines in a JournalEntry using sparse update.
    Keeps all original fields; only modifies AccountRef where mapping matches.
    """
    if not record:
        raise ValueError("Empty record passed to update_journal_entry")

    lines = record.get("Line", [])
    updated = False

    for line in lines:
        jed = line.get("JournalEntryLineDetail", {}) or {}
        acct = jed.get("AccountRef")
        dept = jed.get("DepartmentRef")
        if acct and isinstance(acct, dict) and "value" in acct:
            acct_val = str(acct.get("value"))
            dept_val = str(dept.get("value")) if isinstance(dept, dict) and dept.get("value") is not None else None
            key = (acct_val, dept_val)
            if key in mapping:
                new_acc = mapping[key]
                line["JournalEntryLineDetail"]["AccountRef"]["value"] = str(new_acc)
                updated = True

    if not updated:
        logger.info(f"JournalEntry {record.get('Id')} no matching lines to update; skipping.")
        return None

    # Ensure we have the latest SyncToken
    record["SyncToken"] = fetch_latest_synctoken("JournalEntry", record["Id"])
    record["sparse"] = True  # sparse update

    # Store updated JSON before sending
    updated_json_str = json.dumps(record)
    logger.info(f"Prepared sparse update JSON for JournalEntry {record.get('Id')}")

    # POST sparse update
    url = f"{QBO_BASE_URL}/journalentry?minorversion=65"
    headers = {
        "Authorization": f"Bearer {os.getenv('QBO_ACCESS_TOKEN')}",
        "Content-Type": "application/json"
    }
    resp = requests.post(url, headers=headers, json=record)

    try:
        # Try JSON first
        if resp.text:
            return resp.json()
        else:
            logger.warning(f"Empty response body for JournalEntry {record.get('Id')}")
            return {}
    except json.JSONDecodeError:
        # Possibly XML or empty
        logger.warning(f"Response not JSON: {resp.text[:500]}")
        return {"raw_response": resp.text, "status_code": resp.status_code}


# -------------------------------
# Generic updater (keeps previous behavior)
# -------------------------------
def generic_update_tx(record: dict, tx_type: str, mapping: Dict[tuple, str]) -> Optional[dict]:
    if not record:
        raise ValueError("Empty record passed to generic_update_tx")

    updated = False

    def replace_account(obj):
        nonlocal updated
        if isinstance(obj, dict):
            if "AccountRef" in obj and isinstance(obj["AccountRef"], dict) and "value" in obj["AccountRef"]:
                dept_val = obj.get("DepartmentRef", {}).get("value")
                key = (str(obj["AccountRef"]["value"]), str(dept_val) if dept_val is not None else None)
                if key in mapping:
                    obj["AccountRef"]["value"] = mapping[key]
                    updated = True
            for v in obj.values():
                replace_account(v)
        elif isinstance(obj, list):
            for item in obj:
                replace_account(item)

    replace_account(record)

    if not updated:
        return None

    if "Id" not in record:
        raise ValueError("Missing Id in record")
    if "SyncToken" not in record or record.get("SyncToken") is None:
        record["SyncToken"] = fetch_latest_synctoken(tx_type, record["Id"])
    record["sparse"] = True

    url = f"{QBO_BASE_URL}/{tx_type.lower()}?minorversion=65"
    headers = {"Authorization": f"Bearer {os.getenv('QBO_ACCESS_TOKEN')}", "Content-Type": "application/json"}
    resp = requests.post(url, headers=headers, json=record)

    try:
        # Try JSON first
        if resp.text:
            return resp.json()
        else:
            logger.warning(f"Empty response body for JournalEntry {record.get('Id')}")
            return {}
    except json.JSONDecodeError:
        # Possibly XML or empty
        logger.warning(f"Response not JSON: {resp.text[:500]}")
        return {"raw_response": resp.text, "status_code": resp.status_code}


# -------------------------------
# Single-record processing & main
# -------------------------------
def process_single(record_id: Any, tx_type: str, mapping: Dict[tuple, str]) -> str:
    """Process a single record with its own database connection for thread safety"""
    conn = None
    try:
        # Create a new connection for this thread
        conn = get_connection()
        
        rec = fetch_record_via_query(tx_type, record_id)
        if not rec:
            msg = f"{tx_type}-{record_id}: NotFound"
            safe_insert_log(conn, record_id, tx_type, msg, "")
            return msg

        result_json = None
        if tx_type.lower() == "journalentry":
            result_json = update_journal_entry(rec, mapping)
        else:
            result_json = generic_update_tx(rec, tx_type, mapping)

        if result_json is None:
            msg = f"{tx_type}-{record_id}: Skipped"
            safe_insert_log(conn, record_id, tx_type, msg, json.dumps(rec))
            return msg

        msg = f"{tx_type}-{record_id}: Updated"
        safe_insert_log(conn, record_id, tx_type, msg, json.dumps(result_json))
        return msg

    except requests.HTTPError as e:
        resp = getattr(e, "response", None)
        status_text = f"Failed: {e} - status={getattr(resp,'status_code',None)}"
        record_text = getattr(resp, "text", "")[:10000] if resp is not None else ""
        logger.error(f"{tx_type}-{record_id}: {status_text} | resp: {record_text}")
        if conn:
            safe_insert_log(conn, record_id, tx_type, status_text[:4000], record_text)
        return f"{tx_type}-{record_id}: {status_text}"

    except Exception as ex:
        err = f"Failed: {ex}"
        logger.exception(f"{tx_type}-{record_id}: {err}")
        try:
            if conn:
                safe_insert_log(conn, record_id, tx_type, err[:4000], "")
        except Exception:
            logger.exception("Failed to write error log to DB")
        return f"{tx_type}-{record_id}: {err}"
    
    finally:
        if conn:
            conn.close()

def main():
    auto_refresh_token_if_needed()
    conn = get_connection()
    create_log_table_if_not_exists(conn)
    cur = conn.cursor()
    cur.execute("SELECT id, transaction_type FROM porter_entities_mapping.transaction_update")
    rows = cur.fetchall()
    conn.close()  # Close main connection as threads will create their own
    
    if not rows:
        print("No records to process.")
        return
    
    print(f"Processing {len(rows)} records with multithreading (4 threads)...")
    
    # Process records in parallel with 4 threads
    with ThreadPoolExecutor(max_workers=6) as executor:
        # Submit all tasks
        future_to_record = {}
        for row in rows:
            record_id = row[0]
            tx_type = row[1]
            future = executor.submit(process_single, record_id, tx_type, ACCOUNT_MAPPING)
            future_to_record[future] = (record_id, tx_type)
        
        # Process completed tasks as they finish
        for future in as_completed(future_to_record):
            record_id, tx_type = future_to_record[future]
            try:
                result = future.result()
                print(result)
            except Exception as exc:
                error_msg = f"{tx_type}-{record_id}: Thread execution failed with {exc}"
                print(error_msg)
                logger.exception(error_msg)

if __name__ == "__main__":
    main()
