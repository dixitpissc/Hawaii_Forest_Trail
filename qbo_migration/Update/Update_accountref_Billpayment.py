#!/usr/bin/env python3
"""
billpayment_update_accountref.py

Purpose:
Sparse update for BillPayment.
Updates only CheckPayment.BankAccountRef.value at header level
based on DepartmentRef.value using ACCOUNT_MAPPING.

✓ Fetches records from SQL Server
✓ Reads full BillPayment from QBO
✓ Applies mapping dynamically
✓ Updates only BankAccountRef.value
✓ Logs results in migration_status_billpayment table
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
# Global timer-based token refresh
# -------------------------------
class TokenRefreshTimer:
    def __init__(self, refresh_interval_minutes=50):
        self.refresh_interval = refresh_interval_minutes * 60  # Convert to seconds
        self.timer = None
        self.is_running = False
        self.lock = threading.Lock()
        
    def _refresh_token(self):
        """Internal method to refresh token and reset timer"""
        with self.lock:
            try:
                logger.info("⏰ Timer-based token refresh triggered (50 minutes elapsed)")
                auto_refresh_token_if_needed()
                logger.info("✅ Timer-based token refresh completed successfully")
            except Exception as e:
                logger.error(f"❌ Timer-based token refresh failed: {e}")
            finally:
                # Reset the timer for next refresh
                if self.is_running:
                    self._start_timer()
    
    def _start_timer(self):
        """Start the timer for next token refresh"""
        if self.timer:
            self.timer.cancel()
        self.timer = threading.Timer(self.refresh_interval, self._refresh_token)
        self.timer.daemon = True  # Dies when main thread dies
        self.timer.start()
        
    def start(self):
        """Start the token refresh timer"""
        with self.lock:
            if not self.is_running:
                self.is_running = True
                self._start_timer()
                logger.info(f"🕐 Token refresh timer started - will refresh every {self.refresh_interval/60} minutes")
    
    def stop(self):
        """Stop the token refresh timer"""
        with self.lock:
            self.is_running = False
            if self.timer:
                self.timer.cancel()
                self.timer = None
                logger.info("⏹️ Token refresh timer stopped")

# Global timer instance
token_timer = TokenRefreshTimer(refresh_interval_minutes=50)

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
# Mapping table
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
# SQL Connection + Logging
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
              AND TABLE_NAME = 'migration_status_billpayment'
        )
        BEGIN
            CREATE TABLE porter_entities_mapping.migration_status_billpayment (
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
        INSERT INTO porter_entities_mapping.migration_status_billpayment
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
    """Fetch BillPayment from QBO by Id."""
    q_id = str(record_id).replace("'", "''")
    query = f"SELECT * FROM BillPayment WHERE Id = '{q_id}' STARTPOSITION 1 MAXRESULTS 1"
    headers = get_headers_for_query()

    for attempt in range(3):
        resp = requests.post(QUERY_URL, headers=headers, data=query)
        if resp.status_code == 200:
            data = resp.json().get("QueryResponse", {}).get("BillPayment", [])
            if isinstance(data, list) and data:
                return data[0]
            elif isinstance(data, dict):
                return data
            else:
                logger.warning(f"No BillPayment found for Id={record_id}")
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
            logger.error(f"Fetch BillPayment {record_id} failed: {resp.status_code} {resp.text}")
            return None
    return None


def fetch_latest_synctoken(record_id: Any) -> str:
    """Fetch latest SyncToken for given BillPayment Id."""
    rec = fetch_record_via_query(record_id)
    if not rec:
        raise RuntimeError(f"Cannot fetch SyncToken: BillPayment {record_id} not found")
    return rec.get("SyncToken", "0")

# -------------------------------
# BillPayment Updater
# -------------------------------
def update_billpayment_bankaccount(record: dict, mapping: Dict[tuple, str]) -> Optional[dict]:
    """Sparse update: only updates CheckPayment.BankAccountRef.value with retry logic for token refresh"""
    if not record:
        raise ValueError("Empty BillPayment record")

    dept_val = str(record.get("DepartmentRef", {}).get("value", ""))
    check_pay = record.get("CheckPayment", {})
    bank_ref = check_pay.get("BankAccountRef", {})

    if not bank_ref or "value" not in bank_ref:
        logger.info(f"No CheckPayment.BankAccountRef found in BillPayment {record.get('Id')}")
        return None

    acct_val = str(bank_ref["value"])
    key = (acct_val, dept_val)

    if key not in mapping:
        logger.info(f"Skipping BillPayment {record.get('Id')} — no mapping for ({acct_val}, {dept_val})")
        return None

    new_acc_val = mapping[key]
    logger.info(f"Updating BillPayment {record.get('Id')}: BankAccountRef {acct_val} → {new_acc_val}")

    payload = {
        "Id": record["Id"],
        "SyncToken": fetch_latest_synctoken(record["Id"]),
        "sparse": True,
        "CheckPayment": {
            "BankAccountRef": {"value": new_acc_val}
        }
    }

    url = f"{QBO_BASE_URL}/billpayment?minorversion=65"
    
    # Retry logic with token refresh
    for attempt in range(3):
        headers = {
            "Authorization": f"Bearer {os.getenv('QBO_ACCESS_TOKEN')}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        
        resp = requests.post(url, headers=headers, json=payload)

        if resp.status_code == 200:
            logger.info(f"✅ BillPayment {record['Id']} updated successfully.")
            try:
                result = resp.json()
            except Exception:
                result = {"raw_response": resp.text}
            return result
        elif resp.status_code == 401:
            logger.warning(f"401 Unauthorized during BillPayment update → refreshing token (attempt {attempt + 1}/3)")
            auto_refresh_token_if_needed()
            time.sleep(0.5)  # Brief pause after token refresh
            continue
        elif resp.status_code in (429, 500, 502, 503, 504):
            wait = 2 ** attempt
            logger.warning(f"Transient QBO error {resp.status_code} during BillPayment update. Retrying in {wait}s (attempt {attempt + 1}/3)")
            time.sleep(wait)
            continue
        else:
            logger.error(f"❌ Update failed for BillPayment {record['Id']}: {resp.status_code} {resp.text[:300]}")
            try:
                result = resp.json()
            except Exception:
                result = {"raw_response": resp.text, "status_code": resp.status_code}
            return result

    # If we get here, all retries failed
    logger.error(f"❌ All retries failed for BillPayment {record['Id']}")
    return {"error": "All retries failed", "status_code": resp.status_code if 'resp' in locals() else "unknown"}

# -------------------------------
# Record Processing (Thread-safe version)
# -------------------------------
def process_single_billpayment(record_id: Any, mapping: Dict[tuple, str]) -> tuple:
    """
    Process a single BillPayment record with its own database connection for thread safety.
    Returns tuple: (status_str, record_json_str) for consolidated logging.
    """
    conn = None
    try:
        conn = get_connection()
        rec = fetch_record_via_query(record_id)
        if not rec:
            msg = f"BillPayment-{record_id}: NotFound"
            safe_insert_log(conn, record_id, msg)
            return (msg, "")

        result_json = update_billpayment_bankaccount(rec, mapping)
        if result_json is None:
            msg = f"BillPayment-{record_id}: Skipped"
            safe_insert_log(conn, record_id, msg, json.dumps(rec))
            return (msg, json.dumps(rec))

        msg = f"BillPayment-{record_id}: Updated"
        result_json_str = json.dumps(result_json)
        safe_insert_log(conn, record_id, msg, result_json_str)
        return (msg, result_json_str)

    except requests.HTTPError as e:
        # Handle HTTP errors specifically, potentially with token refresh
        resp = getattr(e, "response", None)
        if resp and resp.status_code == 401:
            logger.warning(f"BillPayment-{record_id}: 401 error detected, timer-based refresh should handle this")
        
        status_text = f"Failed: {e} - status={getattr(resp,'status_code',None)}"
        record_text = getattr(resp, "text", "")[:10000] if resp is not None else ""
        logger.error(f"BillPayment-{record_id}: {status_text} | resp: {record_text}")
        
        if conn:
            safe_insert_log(conn, record_id, status_text[:4000], record_text)
        return (f"BillPayment-{record_id}: {status_text}", record_text)

    except Exception as ex:
        err = f"Failed: {ex}"
        logger.exception(f"BillPayment-{record_id}: {err}")
        if conn:
            safe_insert_log(conn, record_id, err[:4000], "")
        return (f"BillPayment-{record_id}: {err}", "")
    finally:
        if conn:
            conn.close()

def main():
    """
    Main: process BillPayment rows using multithreading (6 workers) and track results in
    porter_entities_mapping.migration_status_alltransaction.
    """
    try:
        # Initial token refresh and start timer
        auto_refresh_token_if_needed()
        token_timer.start()

        # Ensure consolidated tracking table exists
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

        # Ensure the per-record billpayment log table exists
        cur.execute("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'porter_entities_mapping' AND TABLE_NAME = 'migration_status_billpayment'
        )
        BEGIN
            CREATE TABLE porter_entities_mapping.migration_status_billpayment (
                id INT IDENTITY(1,1) PRIMARY KEY,
                record_id NVARCHAR(1000),
                status NVARCHAR(MAX),
                record_json NVARCHAR(MAX),
                created_at DATETIME DEFAULT GETDATE()
            )
        END
        """)
        conn.commit()

        # Read BillPayment rows to process
        cur.execute("""
            SELECT id, transaction_type
            FROM porter_entities_mapping.transaction_update
            WHERE transaction_type = 'BillPayment'
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        if not rows:
            print("No BillPayment records to process.")
            return

        print(f"Processing {len(rows)} BillPayment records with multithreading (6 workers)...")
        print(f"🕐 Token refresh timer is active - will refresh every 50 minutes")

        # Process records in parallel with 6 threads
        with ThreadPoolExecutor(max_workers=8) as executor:
            # Submit all tasks
            future_to_record = {}
            for row in rows:
                record_id = row[0]
                tx_type = row[1] if len(row) > 1 else 'BillPayment'
                future = executor.submit(process_single_billpayment, record_id, ACCOUNT_MAPPING)
                future_to_record[future] = (record_id, tx_type)
            
            # Process completed tasks as they finish
            for future in as_completed(future_to_record):
                record_id, tx_type = future_to_record[future]
                
                try:
                    # Get result from the worker thread
                    status_str, record_json_str = future.result()
                    
                    # Insert consolidated tracking row
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
                    # Handle thread execution failure
                    error_msg = f"{tx_type}-{record_id}: Thread execution failed with {exc}"
                    print(error_msg)
                    logger.exception(error_msg)
                    
                    # Still try to log the failure
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

        print("All BillPayment records processing completed.")
        
    except KeyboardInterrupt:
        print("\n🛑 Processing interrupted by user")
        logger.info("Processing interrupted by user (Ctrl+C)")
    except Exception as e:
        print(f"\n❌ Unexpected error during processing: {e}")
        logger.exception(f"Unexpected error during processing: {e}")
    finally:
        # Always stop the timer when exiting
        token_timer.stop()
        print("🏁 Process completed and timer stopped")


if __name__ == "__main__":
    main()
