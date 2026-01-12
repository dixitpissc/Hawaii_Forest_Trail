import os
import logging
import requests
import pandas as pd
import json
import time
import threading
from collections import deque
from dotenv import load_dotenv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from storage.sqlserver import sql
# from storage.sqlserver.sql import executemany  # <-- used for batched UPDATEs
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import ProgressTimer
import pyodbc

def executemany(query: str, params_list: list):
    """
    Execute a SQL UPDATE/INSERT/DELETE statement multiple times efficiently.

    Parameters:
        query (str)       : SQL query with parameter placeholders (?) 
        params_list (list): List of tuples. Each tuple contains parameters for one execution.

    Example:
        executemany(
            "UPDATE MyTable SET col1=?, col2=? WHERE id=?",
            [(v1, v2, id1), (v3, v4, id2)]
        )
    """
    if not params_list:
        return

    conn = sql.get_connection()  # Your existing helper
    cursor = conn.cursor()

    try:
        cursor.fast_executemany = True  # ⚡ makes executemany ~10x faster
        cursor.executemany(query, params_list)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

# ===========================================================
# LOGGING SETUP
# ===========================================================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = f"customer_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG_FILE_PATH = os.path.join(LOG_DIR, log_filename)

logger = logging.getLogger("customer_update")
logger.setLevel(logging.INFO)
logger.propagate = False

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

if logger.hasHandlers():
    logger.handlers.clear()

file_handler = logging.FileHandler(LOG_FILE_PATH, mode="w", encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# ===========================================================
# ENV + QBO SETUP
# ===========================================================
load_dotenv()
auto_refresh_token_if_needed()

access_token = os.getenv("QBO_ACCESS_TOKEN")
realm_id = os.getenv("QBO_REALM_ID")
environment = os.getenv("QBO_ENVIRONMENT", "sandbox")

base_url = (
    "https://sandbox-quickbooks.api.intuit.com"
    if environment == "sandbox"
    else "https://quickbooks.api.intuit.com"
)

query_url = f"{base_url}/v3/company/{realm_id}/query"
customer_post_url = f"{base_url}/v3/company/{realm_id}/customer?minorversion=75"

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Source / mapping tables
source_schema = "dbo"
source_table = "Customers"
mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
mapping_table = "Map_Customer"

max_retries = 3
MAX_WORKERS = 8            # parallelism
MAX_CALLS_PER_SECOND = 8   # throttle limit
BATCH_INSERT_SIZE = 1000   # for initial mapping insert
FETCH_BATCH_SIZE = 500     # <-- how many mapping rows to read per update batch

# ===========================================================
# RATE LIMITER – ≤ 8 HTTP calls / second across all threads
# ===========================================================
_rate_lock = threading.Lock()
_call_times = deque()

def rate_limited():
    """
    Simple sliding-window rate limiter.
    Guarantees we don't exceed MAX_CALLS_PER_SECOND in any 1-second window.
    """
    global _call_times
    while True:
        with _rate_lock:
            now = time.time()
            # Drop timestamps older than 1 second
            while _call_times and now - _call_times[0] > 1.0:
                _call_times.popleft()

            if len(_call_times) < MAX_CALLS_PER_SECOND:
                _call_times.append(now)
                return

            sleep_for = 1.0 - (now - _call_times[0])

        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            time.sleep(0.001)

# ===========================================================
# HELPER: INSERT DATAFRAME IN BATCHES (ONLY FOR PREPARE)
# ===========================================================
def insert_dataframe_in_batches(df: pd.DataFrame, table: str, schema: str, batch_size: int = BATCH_INSERT_SIZE):
    """
    Insert a DataFrame into SQL Server in batches to avoid one huge commit.
    Used only during prepare_mapping to populate Map_Customer the first time.
    """
    total = len(df)
    if total == 0:
        return

    batch_num = 1
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch_df = df.iloc[start:end].copy()
        logger.info(
            f"💾 Inserting batch {batch_num}: rows {start+1}-{end} of {total} "
            f"into [{schema}].[{table}]"
        )
        sql.insert_dataframe(batch_df, table, schema)
        batch_num += 1

# ===========================================================
# QBO FETCH HELPERS
# ===========================================================
def fetch_all_qbo_customers(page_size: int = 1000):
    """
    Fetch ALL QBO Customers using pagination (STARTPOSITION / MAXRESULTS).
    Returns a list of customer dicts.
    """
    all_customers = []
    start_position = 1

    logger.info("📥 Loading all QBO Customers into memory using pagination...")

    while True:
        query = f"SELECT * FROM Customer STARTPOSITION {start_position} MAXRESULTS {page_size}"
        params = {"query": query, "minorversion": "75"}

        try:
            rate_limited()
            resp = requests.get(query_url, headers=headers, params=params, timeout=60)
            if resp.status_code != 200:
                logger.warning(
                    f"⚠️ Failed to fetch page starting at {start_position}. "
                    f"HTTP {resp.status_code}: {resp.text[:500]}"
                )
                break

            resp_json = resp.json()
            customers = resp_json.get("QueryResponse", {}).get("Customer", [])
            count = len(customers)
            if count == 0:
                break

            all_customers.extend(customers)
            logger.info(f"  ➕ Loaded {count} customers (start={start_position})")

            if count < page_size:
                break

            start_position += page_size

        except Exception as e:
            logger.exception(f"Exception while fetching QBO customers page: {e}")
            break

    logger.info(f"✅ Total QBO customers loaded into memory: {len(all_customers)}")
    return all_customers


def build_customer_cache():
    """
    Build in-memory caches for QBO Customers:
    - by CompanyName
    - by DisplayName
    """
    all_customers = fetch_all_qbo_customers()
    by_company = {}
    by_display = {}

    for cust in all_customers:
        comp = cust.get("CompanyName")
        disp = cust.get("DisplayName")

        if comp:
            by_company.setdefault(comp, cust)
        if disp:
            by_display.setdefault(disp, cust)

    logger.info(
        f"📦 Customer cache built: "
        f"{len(all_customers)} total, "
        f"{len(by_company)} by CompanyName, "
        f"{len(by_display)} by DisplayName"
    )
    return by_company, by_display


def fetch_qbo_customer(company_name, display_name):
    """
    Fallback live fetch from QBO for stale-object retry (not used in prepare_mapping).
    """
    if pd.notna(company_name) and company_name:
        safe_name = company_name.replace("'", "''")
        query = f"SELECT * FROM Customer WHERE CompanyName='{safe_name}'"
    else:
        safe_name = (display_name or "").replace("'", "''")
        query = f"SELECT * FROM Customer WHERE DisplayName='{safe_name}'"

    params = {"query": query, "minorversion": "75"}

    try:
        rate_limited()
        resp = requests.get(query_url, headers=headers, params=params, timeout=30)
        if resp.status_code == 200:
            cust_list = resp.json().get("QueryResponse", {}).get("Customer", [])
            if cust_list:
                return cust_list[0]
    except Exception as e:
        logger.exception(f"Error fetching QBO customer (live): {e}")

    return None

# ===========================================================
# PAYLOAD BUILDER
# ===========================================================
def create_address_payload(row, qbo_customer):
    """
    Build sparse payload to add BillAddr/ShipAddr only if missing in QBO.
    Everything else remains unchanged.
    row can be a pandas Series or dict.
    """
    bill_missing = ("BillAddr" not in qbo_customer) or (not qbo_customer.get("BillAddr"))
    ship_missing = ("ShipAddr" not in qbo_customer) or (not qbo_customer.get("ShipAddr"))

    if not bill_missing and not ship_missing:
        # Nothing to update
        return None

    payload = {
        "Id": qbo_customer["Id"],
        "SyncToken": qbo_customer["SyncToken"],
        "sparse": True,
    }

    # --- BillAddr ---
    if bill_missing:
        billaddr = {}
        for col in [
            "BillAddr_Line1",
            "BillAddr_Line2",
            "BillAddr_Line3",
            "BillAddr_Line4",
            "BillAddr_Line5",
            "BillAddr_City",
            "BillAddr_Country",
            "BillAddr_CountrySubDivisionCode",
            "BillAddr_PostalCode",
        ]:
            if col in row and pd.notna(row[col]):
                key = col.replace("BillAddr_", "")
                billaddr[key] = row[col]

        if billaddr:
            payload["BillAddr"] = billaddr

    # --- ShipAddr ---
    if ship_missing:
        shipaddr = {}
        for col in [
            "ShipAddr_Line1",
            "ShipAddr_Line2",
            "ShipAddr_Line3",
            "ShipAddr_Line4",
            "ShipAddr_Line5",
            "ShipAddr_City",
            "ShipAddr_Country",
            "ShipAddr_CountrySubDivisionCode",
            "ShipAddr_PostalCode",
        ]:
            if col in row and pd.notna(row[col]):
                key = col.replace("ShipAddr_", "")
                shipaddr[key] = row[col]

        if shipaddr:
            payload["ShipAddr"] = shipaddr

    if "BillAddr" not in payload and "ShipAddr" not in payload:
        return None

    return payload

# ===========================================================
# STEP 1 – PREPARE MAPPING (MULTI-THREADED, USING CACHE)
# ===========================================================
def _prepare_single_record(index, row_dict, customers_by_company, customers_by_display):
    """
    Worker for prepare_mapping (runs with ThreadPoolExecutor).
    row_dict is the source Customers row as a dict.
    Returns a mapping dict with Target_Id, SyncToken, Payload_JSON, etc.
    """
    company_name = row_dict.get("CompanyName")
    display_name = row_dict.get("DisplayName")

    result = {
        "index": index,
        "Target_Id": None,
        "SyncToken": None,
        "Payload_JSON": None,
        "Porter_Status": None,
        "Failure_Reason": None,
        "Retry_Count": 0,
    }

    qbo_cust = None
    if company_name and company_name in customers_by_company:
        qbo_cust = customers_by_company[company_name]
    elif display_name and display_name in customers_by_display:
        qbo_cust = customers_by_display[display_name]

    if not qbo_cust:
        result["Porter_Status"] = "NotFound"
        return result

    result["Target_Id"] = qbo_cust.get("Id")
    result["SyncToken"] = qbo_cust.get("SyncToken")

    payload = create_address_payload(row_dict, qbo_cust)
    if payload:
        result["Payload_JSON"] = json.dumps(payload, indent=2)
        result["Porter_Status"] = "Ready"
    else:
        result["Porter_Status"] = "Skip"

    return result


def prepare_mapping():
    logger.info("🚀 Preparing Map_Customer with in-memory QBO cache + multi-threading...")

    df = sql.fetch_table(source_table, source_schema)
    if df.empty:
        logger.warning("⚠️ No records found in source Customers table.")
        return

    sql.ensure_schema_exists(mapping_schema)

    # Maintain source id
    df["Source_Id"] = df["Id"]

    # Initialize mapping columns
    df["Target_Id"] = None
    df["SyncToken"] = None
    df["Payload_JSON"] = None
    df["Porter_Status"] = None
    df["Failure_Reason"] = None
    df["Retry_Count"] = 0

    # Build in-memory cache from QBO
    customers_by_company, customers_by_display = build_customer_cache()

    total = len(df)
    logger.info(f"🧮 Customers to prepare: {total}")
    timer = ProgressTimer(total, logger=logger)

    jobs = []
    for idx, row in df.iterrows():
        jobs.append((idx, row.to_dict()))

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_job = {
            executor.submit(
                _prepare_single_record,
                idx,
                row_dict,
                customers_by_company,
                customers_by_display,
            ): idx
            for idx, row_dict in jobs
        }

        for future in as_completed(future_to_job):
            res = future.result()
            results.append(res)
            timer.update()

    timer.stop()

    # Apply results back to df
    for res in results:
        idx = res["index"]
        df.at[idx, "Target_Id"] = res["Target_Id"]
        df.at[idx, "SyncToken"] = res["SyncToken"]
        df.at[idx, "Payload_JSON"] = res["Payload_JSON"]
        df.at[idx, "Porter_Status"] = res["Porter_Status"]
        df.at[idx, "Failure_Reason"] = res["Failure_Reason"]
        df.at[idx, "Retry_Count"] = res["Retry_Count"]

    # One-time initial insert of full mapping (batched)
    insert_dataframe_in_batches(df, mapping_table, mapping_schema, batch_size=BATCH_INSERT_SIZE)
    logger.info("✅ Map_Customer prepared successfully (multi-thread + cache + batched insert).")

# ===========================================================
# STEP 2 – PARALLEL UPDATE (STREAMING 500 ROWS AT A TIME)
# ===========================================================
def update_single_customer(record):
    """
    Worker function executed in a thread for updates.
    record is a dict with:
        index (row index in df),
        Source_Id,
        CompanyName, DisplayName,
        Payload_JSON, Target_Id, SyncToken, Retry_Count,
        Mode, Batch_No
    Returns dict with updated fields (Porter_Status, Failure_Reason, SyncToken, Retry_Count).
    """
    idx = record["index"]
    source_id = record.get("Source_Id")
    payload_json = record.get("Payload_JSON")
    company_name = record.get("CompanyName")
    display_name = record.get("DisplayName")
    mode = record.get("Mode")
    batch_no = record.get("Batch_No")

    # 🔧 Normalize retry_count to int
    raw_retry = record.get("Retry_Count")
    try:
        if raw_retry is None or (isinstance(raw_retry, float) and pd.isna(raw_retry)):
            retry_count = 0
        else:
            retry_count = int(raw_retry)
    except Exception:
        logger.warning(
            f"[{mode}][Batch {batch_no}] Source_Id={source_id}: "
            f"Unexpected Retry_Count={raw_retry!r}, resetting to 0."
        )
        retry_count = 0

    if not payload_json:
        # Nothing to send to QBO – treat as skip/pass-through
        logger.info(f"[{mode}][Batch {batch_no}] Source_Id={source_id}: No payload, skipping.")
        return {
            "index": idx,
            "Porter_Status": record.get("Porter_Status", "Skip"),
            "Failure_Reason": record.get("Failure_Reason"),
            "SyncToken": record.get("SyncToken"),
            "Retry_Count": retry_count,
        }

    try:
        payload = json.loads(payload_json)
    except Exception as e:
        msg = f"Invalid JSON payload: {e}"
        logger.error(f"[{mode}][Batch {batch_no}] Source_Id={source_id}: {msg}")
        return {
            "index": idx,
            "Porter_Status": "Failed",
            "Failure_Reason": msg,
            "SyncToken": record.get("SyncToken"),
            "Retry_Count": retry_count,
        }

    last_text = None
    current_sync = payload.get("SyncToken")

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                f"[{mode}][Batch {batch_no}] Source_Id={source_id}: "
                f"Posting to QBO (attempt {attempt}/{max_retries})..."
            )
            rate_limited()
            resp = requests.post(customer_post_url, headers=headers, json=payload, timeout=30)
            last_text = resp.text[:300]

            if resp.status_code in (200, 201):
                logger.info(
                    f"[{mode}][Batch {batch_no}] Source_Id={source_id}: "
                    f"✅ Updated successfully (attempt {attempt})."
                )
                return {
                    "index": idx,
                    "Porter_Status": "Updated",
                    "Failure_Reason": None,
                    "SyncToken": current_sync,
                    "Retry_Count": retry_count + (attempt - 1),
                }

            # Handle stale object error (5010)
            stale = False
            try:
                rj = resp.json()
                errors = rj.get("Fault", {}).get("Error", [])
                if isinstance(errors, dict):
                    errors = [errors]
                for err in errors:
                    code = str(err.get("code", "")).strip()
                    msg = str(err.get("Message", "") or err.get("message", ""))
                    if code == "5010" or "Stale Object Error" in msg:
                        stale = True
                        break
            except Exception:
                pass

            if stale:
                logger.warning(
                    f"[{mode}][Batch {batch_no}] Source_Id={source_id}: "
                    f"Stale Object Error, refreshing SyncToken..."
                )
                qbo_cust = fetch_qbo_customer(company_name, display_name)
                if qbo_cust and qbo_cust.get("SyncToken"):
                    current_sync = qbo_cust["SyncToken"]
                    payload["SyncToken"] = current_sync
                    continue  # retry with new token
                else:
                    msg = "Stale Object Error and unable to refresh SyncToken"
                    logger.error(
                        f"[{mode}][Batch {batch_no}] Source_Id={source_id}: {msg}"
                    )
                    return {
                        "index": idx,
                        "Porter_Status": "Failed",
                        "Failure_Reason": msg,
                        "SyncToken": current_sync,
                        "Retry_Count": retry_count + attempt,
                    }

            # Any other HTTP failure
            msg = f"HTTP {resp.status_code}: {last_text}"
            logger.error(
                f"[{mode}][Batch {batch_no}] Source_Id={source_id}: {msg}"
            )
            return {
                "index": idx,
                "Porter_Status": "Failed",
                "Failure_Reason": msg,
                "SyncToken": current_sync,
                "Retry_Count": retry_count + attempt,
            }

        except requests.exceptions.RequestException as e:
            last_text = str(e)
            logger.error(
                f"[{mode}][Batch {batch_no}] Source_Id={source_id}: "
                f"Request exception on attempt {attempt}: {last_text}"
            )
            if attempt == max_retries:
                return {
                    "index": idx,
                    "Porter_Status": "Failed",
                    "Failure_Reason": last_text,
                    "SyncToken": current_sync,
                    "Retry_Count": retry_count + attempt,
                }
            else:
                continue
        except Exception as e:
            last_text = str(e)
            logger.exception(
                f"[{mode}][Batch {batch_no}] Source_Id={source_id}: "
                f"Unexpected error: {last_text}"
            )
            return {
                "index": idx,
                "Porter_Status": "Failed",
                "Failure_Reason": last_text,
                "SyncToken": current_sync,
                "Retry_Count": retry_count + attempt,
            }

    # Should rarely get here
    logger.error(
        f"[{mode}][Batch {batch_no}] Source_Id={source_id}: "
        f"Exhausted retries, last_error={last_text}"
    )
    return {
        "index": idx,
        "Porter_Status": "Failed",
        "Failure_Reason": last_text or "Unknown error",
        "SyncToken": current_sync,
        "Retry_Count": retry_count + max_retries,
    }

def update_mapping_rows_in_db(update_rows):
    """
    Update Map_Customer rows in SQL Server for the given list of tuples.

    Each tuple in update_rows must be:
        (Porter_Status, Failure_Reason, SyncToken, Retry_Count, Source_Id)

    Uses the same connection that storage.sqlserver.sql uses internally.
    """
    if not update_rows:
        return

    update_sql = f"""
    UPDATE [{mapping_schema}].[{mapping_table}]
    SET Porter_Status = ?, 
        Failure_Reason = ?, 
        SyncToken = ?, 
        Retry_Count = ?
    WHERE Source_Id = ?
    """

    # Get a raw DB-API connection from your sql helper.
    # This assumes sql.get_connection() exists in storage.sqlserver.sql
    conn = sql.get_connection()
    cursor = conn.cursor()
    try:
        # If this is pyodbc, fast_executemany will speed it up a lot
        if hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True

        cursor.executemany(update_sql, update_rows)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


FETCH_BATCH_SIZE = 500  # logical batch size for processing

def update_qbo_customers_parallel(only_failed: bool = False):
    """
    Run multi-threaded updates for Customers from Map_Customer.

    only_failed=False → process Porter_Status in ('Ready', NULL)
    only_failed=True  → process Porter_Status = 'Failed'

    Internally:
      - Loads full mapping DF once
      - Processes indices in logical batches of 500
      - Writes the full DF back at the end in 1000-row insert batches
    """
    mode = "FAILED RETRY" if only_failed else "MAIN"
    logger.info(
        f"🚀 Starting parallel customer update [{mode}] with {MAX_WORKERS} workers "
        f"({FETCH_BATCH_SIZE}-row logical batches)..."
    )

    df = sql.fetch_table(mapping_table, mapping_schema)
    if df.empty:
        logger.warning("⚠️ Map_Customer is empty.")
        return

    # Determine which rows to process
    if only_failed:
        mask = df["Porter_Status"].eq("Failed")
    else:
        mask = df["Porter_Status"].isna() | df["Porter_Status"].eq("Ready")

    indices = df.index[mask].tolist()
    total_to_process = len(indices)

    if total_to_process == 0:
        logger.info(f"ℹ️ No records to process for mode [{mode}].")
        return

    logger.info(f"📌 [{mode}] Total records to process: {total_to_process}")
    global_timer = ProgressTimer(total_to_process, logger=logger)

    batch_no = 1

    for start in range(0, total_to_process, FETCH_BATCH_SIZE):
        end = min(start + FETCH_BATCH_SIZE, total_to_process)
        batch_indices = indices[start:end]
        batch_count = len(batch_indices)

        logger.info(
            f"📦 [{mode}] Logical batch #{batch_no}: processing rows {start+1}-{end} "
            f"({batch_count} records)..."
        )

        # Build jobs for this batch
        jobs = []
        for idx in batch_indices:
            row = df.loc[idx]
            jobs.append(
                {
                    "index": idx,  # actual df index
                    "Source_Id": row.get("Source_Id"),
                    "CompanyName": row.get("CompanyName"),
                    "DisplayName": row.get("DisplayName"),
                    "Payload_JSON": row.get("Payload_JSON"),
                    "Target_Id": row.get("Target_Id"),
                    "SyncToken": row.get("SyncToken"),
                    "Retry_Count": row.get("Retry_Count"),
                    "Porter_Status": row.get("Porter_Status"),
                    "Failure_Reason": row.get("Failure_Reason"),
                    "Mode": mode,
                    "Batch_No": batch_no,
                }
            )

        updated_count = 0
        failed_count = 0
        skipped_count = 0

        # Run this logical batch in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_job = {executor.submit(update_single_customer, job): job for job in jobs}
            for future in as_completed(future_to_job):
                res = future.result()
                idx = res["index"]

                porter_status = res.get("Porter_Status")
                failure_reason = res.get("Failure_Reason")
                sync_token = res.get("SyncToken")
                retry_count = res.get("Retry_Count")

                # Apply into main df
                df.at[idx, "Porter_Status"] = porter_status
                df.at[idx, "Failure_Reason"] = failure_reason
                if sync_token is not None:
                    df.at[idx, "SyncToken"] = sync_token
                df.at[idx, "Retry_Count"] = retry_count

                # Counters
                if porter_status == "Updated":
                    updated_count += 1
                elif porter_status == "Failed":
                    failed_count += 1
                elif porter_status in ("Skip", "NotFound"):
                    skipped_count += 1

                global_timer.update()

        logger.info(
            f"✅ [{mode}] Finished logical batch #{batch_no}: "
            f"Updated={updated_count}, Failed={failed_count}, Skipped={skipped_count}"
        )
        batch_no += 1

    global_timer.stop()

    # Finally: write back full DF in insert batches
    logger.info(
        f"💾 [{mode}] Writing updated Map_Customer back to SQL in 1000-row batches..."
    )
    insert_dataframe_in_batches(df, mapping_table, mapping_schema, batch_size=1000)
    logger.info(f"✅ Parallel customer update [{mode}] completed.")

# ===========================================================
# MAIN
# ===========================================================
if __name__ == "__main__":
    # 1) Build mapping with Target_Id + SyncToken + Payload_JSON (one-time, full write)
    # prepare_mapping()

    # 2) Main parallel update (Ready records) in streaming 500-row batches
    # update_qbo_customers_parallel(only_failed=False)

    # 3) Retry pass for Failed records (again streaming 500-row batches)
    update_qbo_customers_parallel(only_failed=True)
