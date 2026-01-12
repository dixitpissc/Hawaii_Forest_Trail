#!/usr/bin/env python3
"""
all_transaction_update_accountref_extended.py
Extended to handle JournalEntry, BillPayment, Deposit, Purchase.
Adds a common header-level DepartmentRef mapper and keeps dynamic detection
from porter_entities_mapping.transaction_update.transaction_type column.
"""

import os
import time
import json
import requests
import pyodbc
from dotenv import load_dotenv
from typing import Dict, Any, Optional, List

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
    ("2838", "975"):  "2386",
    ("2838", "976") : "1150040001",
    ("2838", "977") : "1150040000",
    ("2838", "978") : "2386",
    ("2838", "979") : "1150040003",
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
# Header-level DepartmentRef: common function
# -------------------------------
def apply_header_department_mapping(record: dict, mapping: Dict[tuple, str]) -> bool:
    """If record has a header-level DepartmentRef, attempt to replace any AccountRefs
    that match mapping keys where the mapping key's dept == header dept.
    Returns True if any replacement occurred.
    """
    if not isinstance(record, dict):
        return False
    hdr_dept = None
    if "DepartmentRef" in record and isinstance(record["DepartmentRef"], dict):
        hdr_dept = record["DepartmentRef"].get("value")
    # also check some common header containers (e.g., Payment, Deposit)
    for k in ("Payment", "Deposit", "BillPayment", "Purchase"):
        if k in record and isinstance(record[k], dict) and "DepartmentRef" in record[k]:
            hdr_dept = record[k]["DepartmentRef"].get("value")
            break

    if hdr_dept is None:
        return False

    replaced = False

    def replace_account_with_header_dept(obj):
        nonlocal replaced
        if isinstance(obj, dict):
            if "AccountRef" in obj and isinstance(obj["AccountRef"], dict) and "value" in obj["AccountRef"]:
                old_acct = str(obj["AccountRef"]["value"])
                key = (old_acct, str(hdr_dept))
                if key in mapping:
                    obj["AccountRef"]["value"] = mapping[key]
                    replaced = True
            for v in obj.values():
                replace_account_with_header_dept(v)
        elif isinstance(obj, list):
            for item in obj:
                replace_account_with_header_dept(item)

    replace_account_with_header_dept(record)
    return replaced

# -------------------------------
# JournalEntry sparse updater (ONLY changed lines)
# -------------------------------
def update_journal_entry(record: dict, mapping: Dict[tuple, str]) -> Optional[dict]:
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
                line["JournalEntryLineDetail"]["AccountRef"]["value"] = str(mapping[key])
                updated = True

    if not updated:
        logger.info(f"JournalEntry {record.get('Id')} no matching lines to update; skipping.")
        return None

    record["SyncToken"] = fetch_latest_synctoken("JournalEntry", record["Id"])
    record["sparse"] = True

    url = f"{QBO_BASE_URL}/journalentry?minorversion=65"
    headers = {
        "Authorization": f"Bearer {os.getenv('QBO_ACCESS_TOKEN')}",
        "Content-Type": "application/json"
    }
    resp = requests.post(url, headers=headers, json=record)

    try:
        if resp.text:
            return resp.json()
        else:
            logger.warning(f"Empty response body for JournalEntry {record.get('Id')}")
            return {}
    except json.JSONDecodeError:
        logger.warning(f"Response not JSON: {resp.text[:500]}")
        return {"raw_response": resp.text, "status_code": resp.status_code}

# -------------------------------
# Generic updater (keeps previous behavior) with header-dept support
# -------------------------------
def generic_update_tx(record: dict, tx_type: str, mapping: Dict[tuple, str]) -> Optional[dict]:
    if not record:
        raise ValueError("Empty record passed to generic_update_tx")

    updated = False

    def replace_account(obj):
        """Recursively replace any AccountRef-like keys.
        This handles keys named exactly "AccountRef" as well as other QBO patterns
        such as "DepositToAccountRef", "BankAccountRef", "CheckPayment" containers
        where the actual account ref key ends with "AccountRef".
        """
        nonlocal updated
        if isinstance(obj, dict):
            # iterate over a static list of items to avoid runtime mutation issues
            for k, v in list(obj.items()):
                # target keys that end with 'AccountRef' (covers DepositToAccountRef, BankAccountRef, etc.)
                if k.endswith('AccountRef') and isinstance(v, dict) and 'value' in v:
                    # department may be present on the same object, or higher up the tree
                    dept_val = obj.get('DepartmentRef', {}).get('value')
                    key = (str(v['value']), str(dept_val) if dept_val is not None else None)
                    if key in mapping:
                        obj[k]['value'] = mapping[key]
                        updated = True
                # also handle exact 'AccountRef' key (kept for completeness)
                elif k == 'AccountRef' and isinstance(v, dict) and 'value' in v:
                    dept_val = obj.get('DepartmentRef', {}).get('value')
                    key = (str(v['value']), str(dept_val) if dept_val is not None else None)
                    if key in mapping:
                        obj['AccountRef']['value'] = mapping[key]
                        updated = True
                # Recurse into children
                replace_account(v)
        elif isinstance(obj, list):
            for item in obj:
                replace_account(item)


    # First, if these transaction types commonly carry department at header level, apply that mapping
    if tx_type.lower() in ("billpayment", "deposit", "purchase"):
        hdr_changed = apply_header_department_mapping(record, mapping)
        if hdr_changed:
            updated = True

    # Then do recursive replacement which checks line-level DepartmentRef as before
    replace_account(record)

    if not updated:
        return None

    if "Id" not in record:
        raise ValueError("Missing Id in record")
    if "SyncToken" not in record or record.get("SyncToken") is None:
        record["SyncToken"] = fetch_latest_synctoken(tx_type, record["Id"])
    record["sparse"] = True

    # QBO endpoint uses lowercase resource name
    url = f"{QBO_BASE_URL}/{tx_type.lower()}?minorversion=65"
    headers = {"Authorization": f"Bearer {os.getenv('QBO_ACCESS_TOKEN')}", "Content-Type": "application/json"}
    resp = requests.post(url, headers=headers, json=record)

    try:
        if resp.text:
            return resp.json()
        else:
            logger.warning(f"Empty response body for {tx_type} {record.get('Id')}")
            return {}
    except json.JSONDecodeError:
        logger.warning(f"Response not JSON: {resp.text[:500]}")
        return {"raw_response": resp.text, "status_code": resp.status_code}

# -------------------------------
# Single-record processing & main
# -------------------------------
def process_single(record_id: Any, tx_type: str, mapping: Dict[tuple, str], conn) -> str:
    try:
        rec = fetch_record_via_query(tx_type, record_id)
        if not rec:
            msg = f"{tx_type}-{record_id}: NotFound"
            safe_insert_log(conn, record_id, tx_type, msg, "")
            return msg

        result_json = None
        if tx_type.lower() == "journalentry":
            result_json = update_journal_entry(rec, mapping)
        else:
            # For billpayment, deposit, purchase we now have header-level dept handling inside generic_update_tx
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
        safe_insert_log(conn, record_id, tx_type, status_text[:4000], record_text)
        return f"{tx_type}-{record_id}: {status_text}"

    except Exception as ex:
        err = f"Failed: {ex}"
        logger.exception(f"{tx_type}-{record_id}: {err}")
        try:
            safe_insert_log(conn, record_id, tx_type, err[:4000], "")
        except Exception:
            logger.exception("Failed to write error log to DB")
        return f"{tx_type}-{record_id}: {err}"


def main():
    auto_refresh_token_if_needed()
    conn = get_connection()
    create_log_table_if_not_exists(conn)
    cur = conn.cursor()
    cur.execute("SELECT id, transaction_type FROM porter_entities_mapping.transaction_update")
    rows = cur.fetchall()
    if not rows:
        print("No records to process.")
        conn.close()
        return
    for row in rows:
        record_id = row[0]
        tx_type = row[1]
        auto_refresh_token_if_needed()
        res = process_single(record_id, tx_type, ACCOUNT_MAPPING, conn)
        print(res)
    conn.close()

if __name__ == "__main__":
    main()
