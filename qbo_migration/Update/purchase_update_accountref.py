#!/usr/bin/env python3
"""
purchase_update_accountref.py

Purpose:
Sparse update for Purchase.
Updates only AccountRef.value at header level
based on DepartmentRef.value using ACCOUNT_MAPPING.

✓ Fetches records from SQL Server
✓ Reads full Purchase from QBO
✓ Applies mapping dynamically
✓ Updates only AccountRef.value
✓ Logs results in migration_status_purchase table
✓ Handles token refresh, transient errors, and non-JSON responses
"""

import os
import time
import json
import requests
import pyodbc
import threading
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import global_logger as logger

# -------------------------------
# Load environment and refresh token
# -------------------------------
load_dotenv()
auto_refresh_token_if_needed()

# -------------------------------
# Global timer-based token refresh (same as other scripts)
# -------------------------------
class TokenRefreshTimer:
    def __init__(self, refresh_interval_minutes=50):
        self.refresh_interval = refresh_interval_minutes * 60
        self.timer = None
        self.is_running = False
        self.lock = threading.Lock()
        
    def _refresh_token(self):
        with self.lock:
            try:
                logger.info("⏰ Timer-based token refresh triggered (50 minutes elapsed)")
                auto_refresh_token_if_needed()
                logger.info("✅ Timer-based token refresh completed successfully")
            except Exception as e:
                logger.error(f"❌ Timer-based token refresh failed: {e}")
            finally:
                if self.is_running:
                    self._start_timer()
    
    def _start_timer(self):
        if self.timer:
            self.timer.cancel()
        self.timer = threading.Timer(self.refresh_interval, self._refresh_token)
        self.timer.daemon = True
        self.timer.start()
        
    def start(self):
        with self.lock:
            if not self.is_running:
                self.is_running = True
                self._start_timer()
                logger.info(f"🕐 Token refresh timer started - will refresh every {self.refresh_interval/60} minutes")
    
    def stop(self):
        with self.lock:
            self.is_running = False
            if self.timer:
                self.timer.cancel()
                self.timer = None
                logger.info("⏹️ Token refresh timer stopped")

token_timer = TokenRefreshTimer(refresh_interval_minutes=50)

# -------------------------------
# Environment + Endpoints
# -------------------------------
SQLSERVER_HOST = os.getenv("SQLSERVER_HOST")
SQLSERVER_PORT = os.getenv("SQLSERVER_PORT", "1433")
SQLSERVER_USER = os.getenv("SQLSERVER_USER")
SQLSERVER_PASSWORD = os.getenv("SQLSERVER_PASSWORD")
SQLSERVER_DATABASE = os.getenv("SQLSERVER_DATABASE")

QBO_ENVIRONMENT = os.getenv("QBO_ENVIRONMENT", "production").lower()
REALM_ID = os.getenv("QBO_REALM_ID")

BASE = (
    "https://sandbox-quickbooks.api.intuit.com"
    if QBO_ENVIRONMENT == "sandbox"
    else "https://quickbooks.api.intuit.com"
)
QUERY_URL = f"{BASE}/v3/company/{REALM_ID}/query"
QBO_BASE_URL = f"{BASE}/v3/company/{REALM_ID}"

# -------------------------------
# Mapping table (key: (existing_account_value, department_value) -> new_account_value)
# Update these pairs to match your real mapping
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

ODBC_DRIVER = "ODBC Driver 17 for SQL Server"

# -------------------------------
# SQL Connection + Logging helpers
# -------------------------------
def get_connection():
    conn_str = (
        f"DRIVER={{{ODBC_DRIVER}}};SERVER={SQLSERVER_HOST},{SQLSERVER_PORT};"
        f"DATABASE={SQLSERVER_DATABASE};UID={SQLSERVER_USER};PWD={SQLSERVER_PASSWORD};TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=False)

def safe_insert_log(conn, record_id, status, record_json=""):
    cur = conn.cursor()
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'porter_entities_mapping')
        BEGIN
            EXEC('CREATE SCHEMA porter_entities_mapping');
        END
    """)
    cur.execute("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'porter_entities_mapping'
              AND TABLE_NAME = 'migration_status_purchase'
        )
        BEGIN
            CREATE TABLE porter_entities_mapping.migration_status_purchase (
                id INT IDENTITY(1,1) PRIMARY KEY,
                record_id NVARCHAR(1000),
                status NVARCHAR(MAX),
                record_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
        END
    """)
    conn.commit()
    cur.execute("""
        INSERT INTO porter_entities_mapping.migration_status_purchase
        (record_id, status, record_json)
        VALUES (?, ?, ?)
    """, str(record_id), str(status)[:4000], str(record_json)[:1048576])
    conn.commit()
    cur.close()

# -------------------------------
# QBO Helpers
# -------------------------------
def get_headers_for_query():
    return {
        "Authorization": f"Bearer {os.getenv('QBO_ACCESS_TOKEN')}",
        "Accept": "application/json",
        "Content-Type": "application/text"
    }

def fetch_record_via_query(record_id: Any) -> Optional[dict]:
    """Fetch Purchase from QBO by Id."""
    q_id = str(record_id).replace("'", "''")
    query = f"SELECT * FROM Purchase WHERE Id = '{q_id}' STARTPOSITION 1 MAXRESULTS 1"
    headers = get_headers_for_query()

    for attempt in range(3):
        resp = requests.post(QUERY_URL, headers=headers, data=query)
        if resp.status_code == 200:
            data = resp.json().get("QueryResponse", {}).get("Purchase", [])
            if isinstance(data, list) and data:
                return data[0]
            elif isinstance(data, dict):
                return data
            else:
                logger.warning(f"No Purchase found for Id={record_id}")
                return None
        elif resp.status_code == 401:
            logger.warning("401 Unauthorized → refreshing token...")
            auto_refresh_token_if_needed()
            headers = get_headers_for_query()
        elif resp.status_code in (429, 500, 502, 503, 504):
            wait = 2 ** attempt
            logger.warning(f"Transient QBO error {resp.status_code}. Retrying in {wait}s")
            time.sleep(wait)
        else:
            logger.error(f"Fetch Purchase {record_id} failed: {resp.status_code} {resp.text}")
            return None
    return None

def fetch_latest_synctoken(record_id: Any) -> str:
    """Fetch latest SyncToken for given Purchase Id."""
    rec = fetch_record_via_query(record_id)
    if not rec:
        raise RuntimeError(f"Cannot fetch SyncToken: Purchase {record_id} not found")
    return rec.get("SyncToken", "0")

# -------------------------------
# Purchase Updater
# -------------------------------
def update_purchase_accountref(record: dict, mapping: Dict[tuple, str]) -> Optional[dict]:
    """
    Sparse update: only updates top-level AccountRef.value with retry logic for token refresh.
    Includes PaymentType in payload because QBO can require it for Purchase updates.
    """
    if not record:
        raise ValueError("Empty Purchase record")

    dept_val = str(record.get("DepartmentRef", {}).get("value", ""))
    acct_ref = record.get("AccountRef", {})  # header-level AccountRef
    if not acct_ref or "value" not in acct_ref:
        logger.info(f"No AccountRef found in Purchase {record.get('Id')}")
        return None

    acct_val = str(acct_ref["value"])
    key = (acct_val, dept_val)

    if key not in mapping:
        logger.info(f"Skipping Purchase {record.get('Id')} — no mapping for ({acct_val}, {dept_val})")
        return None

    new_acc_val = mapping[key]
    logger.info(f"Updating Purchase {record.get('Id')}: AccountRef {acct_val} → {new_acc_val}")

    # Fetch required SyncToken (and we already have full record available)
    sync_token = fetch_latest_synctoken(record["Id"])

    # Include PaymentType if present in the fetched record — QBO often requires this for Purchase
    payment_type = record.get("PaymentType")
    # Optional fallback: set a default if PaymentType missing. Change or remove as needed.
    if not payment_type:
        logger.warning(f"Purchase {record.get('Id')} has no PaymentType — using fallback 'Cash'. "
                       "Consider verifying this record before running in production.")
        payment_type = "Cash"  # <-- change to None and skip update if you prefer not to force a value

    payload = {
        "Id": record["Id"],
        "SyncToken": sync_token,
        "sparse": True,
        "AccountRef": {"value": new_acc_val}
    }

    # Only include PaymentType if we have a value
    if payment_type:
        payload["PaymentType"] = payment_type

    url = f"{QBO_BASE_URL}/purchase?minorversion=65"
    
    # Retry logic with token refresh (same as before)
    for attempt in range(3):
        headers = {
            "Authorization": f"Bearer {os.getenv('QBO_ACCESS_TOKEN')}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        
        resp = requests.post(url, headers=headers, json=payload)

        if resp.status_code == 200:
            logger.info(f"✅ Purchase {record['Id']} updated successfully.")
            try:
                result = resp.json()
            except Exception:
                result = {"raw_response": resp.text}
            return result
        elif resp.status_code == 401:
            logger.warning(f"401 Unauthorized during Purchase update → refreshing token (attempt {attempt + 1}/3)")
            auto_refresh_token_if_needed()
            time.sleep(0.5)
            continue
        elif resp.status_code in (429, 500, 502, 503, 504):
            wait = 2 ** attempt
            logger.warning(f"Transient QBO error {resp.status_code} during Purchase update. Retrying in {wait}s (attempt {attempt + 1}/3)")
            time.sleep(wait)
            continue
        else:
            logger.error(f"❌ Update failed for Purchase {record['Id']}: {resp.status_code} {resp.text[:300]}")
            try:
                result = resp.json()
            except Exception:
                result = {"raw_response": resp.text, "status_code": resp.status_code}
            return result

    logger.error(f"❌ All retries failed for Purchase {record['Id']}")
    return {"error": "All retries failed", "status_code": resp.status_code if 'resp' in locals() else "unknown"}

# -------------------------------
# Record Processing (Thread-safe)
# -------------------------------
def process_single_purchase(record_id: Any, mapping: Dict[tuple, str]) -> tuple:
    """
    Process a single Purchase record with its own database connection for thread safety.
    Returns tuple: (status_str, record_json_str) for consolidated logging.
    """
    conn = None
    try:
        conn = get_connection()
        rec = fetch_record_via_query(record_id)
        if not rec:
            msg = f"Purchase-{record_id}: NotFound"
            safe_insert_log(conn, record_id, msg)
            return (msg, "")

        result_json = update_purchase_accountref(rec, mapping)
        if result_json is None:
            msg = f"Purchase-{record_id}: Skipped"
            safe_insert_log(conn, record_id, msg, json.dumps(rec))
            return (msg, json.dumps(rec))

        msg = f"Purchase-{record_id}: Updated"
        result_json_str = json.dumps(result_json)
        safe_insert_log(conn, record_id, msg, result_json_str)
        return (msg, result_json_str)

    except requests.HTTPError as e:
        resp = getattr(e, "response", None)
        if resp and resp.status_code == 401:
            logger.warning(f"Purchase-{record_id}: 401 error detected, timer-based refresh should handle this")
        
        status_text = f"Failed: {e} - status={getattr(resp,'status_code',None)}"
        record_text = getattr(resp, "text", "")[:10000] if resp is not None else ""
        logger.error(f"Purchase-{record_id}: {status_text} | resp: {record_text}")
        
        if conn:
            safe_insert_log(conn, record_id, status_text[:4000], record_text)
        return (f"Purchase-{record_id}: {status_text}", record_text)

    except Exception as ex:
        err = f"Failed: {ex}"
        logger.exception(f"Purchase-{record_id}: {err}")
        if conn:
            safe_insert_log(conn, record_id, err[:4000], "")
        return (f"Purchase-{record_id}: {err}", "")
    finally:
        if conn:
            conn.close()

# -------------------------------
# Main
# -------------------------------
def main():
    """
    Main: process Purchase rows using multithreading and track results in
    porter_entities_mapping.migration_status_alltransaction.
    """
    try:
        auto_refresh_token_if_needed()
        token_timer.start()

        conn = get_connection()
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

        # Ensure per-record purchase log table exists
        cur.execute("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'porter_entities_mapping' AND TABLE_NAME = 'migration_status_purchase'
        )
        BEGIN
            CREATE TABLE porter_entities_mapping.migration_status_purchase (
                id INT IDENTITY(1,1) PRIMARY KEY,
                record_id NVARCHAR(1000),
                status NVARCHAR(MAX),
                record_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
        END
        """)
        conn.commit()

        # Read Purchase rows to process
        cur.execute("""
            SELECT id, transaction_type
            FROM porter_entities_mapping.transaction_update
            WHERE transaction_type = 'Purchase'
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            print("No Purchase records to process.")
            return

        print(f"Processing {len(rows)} Purchase records with multithreading (8 workers)...")
        print(f"🕐 Token refresh timer is active - will refresh every 50 minutes")

        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_record = {}
            for row in rows:
                record_id = row[0]
                tx_type = row[1] if len(row) > 1 else 'Purchase'
                future = executor.submit(process_single_purchase, record_id, ACCOUNT_MAPPING)
                future_to_record[future] = (record_id, tx_type)
            
            for future in as_completed(future_to_record):
                record_id, tx_type = future_to_record[future]
                
                try:
                    status_str, record_json_str = future.result()
                    
                    try:
                        conn3 = get_connection()
                        cur3 = conn3.cursor()
                        status_safe = (str(status_str) if status_str is not None else "")[:4000]
                        record_json_safe = (str(record_json_str) if record_json_str is not None else "")[:1048576]

                        cur3.execute("""
                            INSERT INTO porter_entities_mapping.migration_status_alltransaction
                            (record_id, transaction_type, status, record_json)
                            VALUES (?, ?, ?, ?)
                        """, str(record_id), str(tx_type), status_safe, record_json_safe)
                        conn3.commit()
                        cur3.close()
                        conn3.close()
                    except Exception as log_ex:
                        logger.exception(f"Failed to write consolidated tracking for {tx_type}-{record_id}: {log_ex}")
                        try:
                            if conn3:
                                conn3.rollback()
                                conn3.close()
                        except Exception:
                            pass

                    print(status_str)
                    
                except Exception as exc:
                    error_msg = f"{tx_type}-{record_id}: Thread execution failed with {exc}"
                    print(error_msg)
                    logger.exception(error_msg)
                    
                    try:
                        conn_err = get_connection()
                        cur_err = conn_err.cursor()
                        cur_err.execute("""
                            INSERT INTO porter_entities_mapping.migration_status_alltransaction
                            (record_id, transaction_type, status, record_json)
                            VALUES (?, ?, ?, ?)
                        """, str(record_id), str(tx_type), error_msg[:4000], "")
                        conn_err.commit()
                        cur_err.close()
                        conn_err.close()
                    except Exception:
                        logger.exception(f"Failed to log thread failure for {record_id}")

        print("All Purchase records processing completed.")
        
    except KeyboardInterrupt:
        print("\n🛑 Processing interrupted by user")
        logger.info("Processing interrupted by user (Ctrl+C)")
    except Exception as e:
        print(f"\n❌ Unexpected error during processing: {e}")
        logger.exception(f"Unexpected error during processing: {e}")
    finally:
        token_timer.stop()
        print("🏁 Process completed and timer stopped")

if __name__ == "__main__":
    main()
