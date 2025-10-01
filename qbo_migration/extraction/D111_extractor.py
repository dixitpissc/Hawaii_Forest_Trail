"""
Sequence : 0001
Module: extraction.py
Author: Dixit Prajapati
Created: 2025-07-26
Description: Handle extraction process with pause continue and exists functionality with logging
            and status handling in sql server
Production : Ready
Phase : 01
"""

import os
import time
import pandas as pd
import requests
import pyodbc
from typing import Dict, List, Any
from utils.log_timer import global_logger as logger
from utils.token_refresher import get_qbo_context, USER_ID_DEFAULT
from dotenv import load_dotenv
from utils.company_dynamic_DB_name import get_user_connection   # ✅ new

USERID = 100250

# exit()
def get_qbo_runtime(user_id: int = USERID) -> Dict[str, Any]:
    """Fetch QBO context dynamically when extraction starts."""
    ctx = get_qbo_context(user_id=user_id, context="extraction")
    return {
        "ENVIRONMENT": ctx["ENVIRONMENT"],
        "REALM_ID": ctx["REALM_ID"],
        "ACCESS_TOKEN": ctx["ACCESS_TOKEN"],
        "BASE_URL": ctx["BASE_URL"],
        "QUERY_URL": ctx["QUERY_URL"],
        "HEADERS": ctx["HEADERS"],
    }

# # === Load Environment and Refresh Token if Needed ===
load_dotenv()

odbc_driver = "ODBC Driver 17 for SQL Server"


def get_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {os.getenv('SOURCE_QBO_ACCESS_TOKEN')}",
        "Accept": "application/json",
        "Content-Type": "application/text"
    }


def check_extraction_control(entity: str, control_status: str):
    """
    Controls extraction flow based on provided control_status value.
    Args:
        entity (str): Entity/table name being processed
        control_status (str): One of ['continue', 'pause', 'exit']
    """
    while True:
        status = control_status.lower().strip()
        if status == "pause":
            logger.info(f"⏸️ Extraction paused for {entity}. Waiting to resume...")
            time.sleep(5)
        elif status == "exit":
            logger.warning(f"🛑 Extraction terminated by user for {entity}.")
            raise SystemExit("Extraction exited due to user command.")
        else:
            return  # continue

def log_extraction_status(seq_no: int, table: str, row_count: int, col_count: int, status: str, error_msg: str = None):
    """
    Inserts extraction status into SQL Server for tracking.
    """
    conn = get_user_connection(USERID)
    cursor = conn.cursor()

    # Ensure schema and table exist
    cursor.execute("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'extraction_status'
        ) BEGIN EXEC('CREATE SCHEMA extraction_status'); END;

        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'extraction_status' AND TABLE_NAME = 'Extraction_Status'
        ) BEGIN
            CREATE TABLE extraction_status.Extraction_Status (
                SequenceNo INT,
                TableName NVARCHAR(255),
                TotalRecordCount INT,
                TotalColumnCount INT,
                ExtractionStatus NVARCHAR(50),
                ErrorMessage NVARCHAR(MAX),
                LoggedAt DATETIME DEFAULT GETDATE()
            )
        END
    """)

    # Insert record
    cursor.execute("""
        INSERT INTO extraction_status.Extraction_Status 
        (SequenceNo, TableName, TotalRecordCount, TotalColumnCount, ExtractionStatus, ErrorMessage)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (seq_no, table, row_count, col_count, status, error_msg))

    conn.commit()
    cursor.close()
    conn.close()

# === Updated fetch_all_records to include control check ===

def fetch_all_records(entity: str, sort_col: str = "Id", batch_size: int = 1000,
                      control_status: str = "continue", user_id: int = None) -> List[Dict[str, Any]]:
    """
    Fetch all records for a given entity, using the provided user_id for context.
    """
    all_records = []
    start = 1

    # Use the provided user_id if given, else fallback to USERID
    effective_user_id = user_id if user_id is not None else USERID
    qbo = get_qbo_runtime(user_id=effective_user_id)
    headers = qbo["HEADERS"]
    url = qbo["QUERY_URL"]

    # Entities where inactive records should be included
    INCLUDE_INACTIVE_ENTITIES = {
        "Account", "Term", "PaymentMethod", "Currency", "CompanyCurrency", "Department",
        "Class", "Customer", "CustomerType", "Vendor", "Employee", "Item"
    }

    while True:
        check_extraction_control(entity, control_status)

        # ✅ Build the query conditionally to include inactive records
        if entity in INCLUDE_INACTIVE_ENTITIES:
            query = f"SELECT * FROM {entity} WHERE Active IN (true, false) ORDERBY {sort_col} ASC STARTPOSITION {start} MAXRESULTS {batch_size}"
        else:
            query = f"SELECT * FROM {entity} ORDERBY {sort_col} ASC STARTPOSITION {start} MAXRESULTS {batch_size}"

        for attempt in range(3):
            response = requests.post(url, headers=headers, data=query)

            if response.status_code == 200:
                break
            elif response.status_code == 401 and attempt == 0:
                logger.warning("🔒 Access token expired – refreshing...")
                # ✅ get a refreshed runtime context
                qbo = get_qbo_runtime(user_id=USERID)
                headers = qbo["HEADERS"]
                url = qbo["QUERY_URL"]
            elif response.status_code in [500, 502, 503, 504, 429]:
                wait = 2 ** attempt
                logger.warning(f"⏳ Retry {attempt + 1}/3 in {wait}s for {entity} at start={start} (status={response.status_code})")
                time.sleep(wait)
            else:
                logger.error(f"❌ Non-retryable error while fetching {entity} at start={start}: {response.status_code} {response.text}")
                raise RuntimeError(f"❌ Failed to fetch {entity} (start={start}): {response.status_code} {response.text}")
        else:
            logger.critical(f"❌ Exhausted retries for {entity} (start={start}): {response.status_code} {response.text}")
            raise RuntimeError(f"❌ Failed after 3 retries for {entity} (start={start}): {response.status_code} {response.text}")

        result = response.json().get("QueryResponse", {})
        data = result.get(entity, [])
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            data = []

        if not data:
            logger.info(f"✅ Completed fetching {entity}. Total records fetched: {len(all_records)}")
            break

        all_records.extend(data)

        if len(data) < batch_size:
            logger.info(f"✅ Fetched last page for {entity}. Total records: {len(all_records)}")
            break

        logger.info(f"📆 Fetched batch {start}–{start + batch_size - 1} for {entity} ({len(data)} records)")
        start += batch_size
        time.sleep(0.25)

    return all_records

def quote_col(col_name: str) -> str:
    """Safely quote SQL Server column names, allowing dots."""
    return f'"{col_name}"'



def write_to_sqlserver(
    df: pd.DataFrame,
    table_name: str,
    append: bool = False,
    batch_size: int = 5000,
    dedup_col: str = "Id",
    user_id: int = None
) -> None:
    if df.empty:
        logger.warning(f"⚠️ Skipping empty DataFrame for {table_name}")
        return

    # Use user_id for per-user DB connection if provided
    effective_user_id = user_id if user_id is not None else USERID
    conn = get_user_connection(effective_user_id)   # ✅ per-user DB
    cursor = conn.cursor()
    cursor.fast_executemany = True

    try:
        # === Step 1: Create table if it doesn't exist ===
        quoted_columns = ', '.join([f'{quote_col(col)} NVARCHAR(MAX)' for col in df.columns])
        cursor.execute(f"""
            SET QUOTED_IDENTIFIER ON;
            IF OBJECT_ID(N'{table_name}', N'U') IS NULL
            BEGIN
                CREATE TABLE [{table_name}] ({quoted_columns})
            END
        """)

        # === Step 2: Add missing columns ===
        cursor.execute(f"""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'
        """)
        existing_cols = {row[0] for row in cursor.fetchall()}
        new_cols = [col for col in df.columns if col not in existing_cols]

        for col in new_cols:
            logger.info(f"➕ Adding new column '{col}' to table {table_name}")
            cursor.execute(f"""ALTER TABLE [{table_name}] ADD {quote_col(col)} NVARCHAR(MAX)""")

        # === Step 3: Clear data (if not appending) ===
        if not append:
            logger.info(f"🧹 Deleting old data from {table_name}")
            cursor.execute(f'DELETE FROM [{table_name}]')
            conn.commit()

        # === Step 4: Prepare insert SQL ===
        placeholders = ', '.join(['?'] * len(df.columns))
        quoted_col_names = ', '.join([quote_col(col) for col in df.columns])
        insert_sql = f'INSERT INTO [{table_name}] ({quoted_col_names}) VALUES ({placeholders})'

        # === Step 5: Optional deduplication ===
        if dedup_col and dedup_col in df.columns:
            cursor.execute(f"SELECT DISTINCT {quote_col(dedup_col)} FROM [{table_name}]")
            existing_ids = {str(row[0]) for row in cursor.fetchall()}
            original_len = len(df)
            df = df[~df[dedup_col].astype(str).isin(existing_ids)]
            logger.info(f"🧹 Removed {original_len - len(df)} duplicates based on `{dedup_col}`")

        # === Step 6: Insert in batches ===
        total = len(df)
        for start in range(0, total, batch_size):
            end = min(start + batch_size, total)
            batch = df.iloc[start:end]

            try:
                cursor.executemany(insert_sql, batch.values.tolist())
                conn.commit()
                logger.info(f"✅ Inserted rows {start}–{end} into {table_name}")
            except Exception as e:
                conn.rollback()
                logger.error(f"❌ Failed to insert batch {start}–{end} into {table_name}: {e}")
                raise

    finally:
        cursor.close()
        conn.close()

def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """Recursively flatten nested dictionaries."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if all(isinstance(i, dict) for i in v):
                for i, sub in enumerate(v):
                    items.extend(flatten_dict(sub, f"{new_key}[{i}]", sep=sep).items())
            else:
                items.append((new_key, str(v)))
        else:
            items.append((new_key, v))
    return dict(items)


def normalize_and_store(records: List[Dict[str, Any]], entity: str, user_id: int = USERID) -> None:
    """Normalize and store QBO records to SQL Server, including LinkedTxn."""
    try:
        if not records:
            logger.warning(f"⚠️ No data found for {entity}")
            return

        if user_id is None:
            raise ValueError("user_id must be provided to normalize_and_store")

        main_records = []
        line_items = []
        linked_txns = []

        for r in records:
            # Main transaction record
            flat_main = flatten_dict({k: v for k, v in r.items() if k not in ["Line", "LinkedTxn"]})
            main_records.append(flat_main)

            # Line items
            for line in r.get("Line", []):
                flat_line = flatten_dict(line)
                flat_line["Parent_Id"] = r.get("Id")
                flat_line["TxnDate"] = r.get("TxnDate")
                flat_line["DocNumber"] = r.get("DocNumber")
                line_items.append(flat_line)

                for linked in line.get("LinkedTxn", []):
                    flat_link = flatten_dict(linked)
                    flat_link["SourceEntity"] = entity
                    flat_link["SourceId"] = r.get("Id")
                    flat_link["SourceDocNumber"] = r.get("DocNumber")
                    flat_link["SourceTxnType"] = r.get("TxnType")
                    flat_link["TxnDate"] = r.get("TxnDate")
                    flat_link["Parent_Entity"] = entity
                    linked_txns.append(flat_link)

        # === Store main records ===
        df_main = pd.DataFrame(main_records)
        main_count = 0
        if not df_main.empty:
            if "Id" in df_main.columns:
                df_main.drop_duplicates(subset="Id", inplace=True)
            df_main = df_main.astype(str).where(pd.notna(df_main), None)
            main_count = len(df_main)
            write_to_sqlserver(df_main, entity, user_id=USERID)

        # === Store line items ===
        df_line = pd.DataFrame(line_items)
        line_count = 0
        if not df_line.empty:
            df_line["Parent_Entity"] = entity
            df_line = df_line.astype(str).where(pd.notna(df_line), None)
            line_count = len(df_line)
            write_to_sqlserver(df_line, f"{entity}_Line", user_id=user_id)

        # === Store LinkedTxn entries ===
        df_linked = pd.DataFrame(linked_txns)
        linked_count = 0
        if not df_linked.empty:
            df_linked.drop_duplicates(inplace=True)
            df_linked = df_linked.astype(str).where(pd.notna(df_linked), None)
            linked_count = len(df_linked)
            write_to_sqlserver(df_linked, "LinkedTxn", user_id=user_id)

        if not main_count and not line_count and not linked_count:
            logger.warning(f"⚠️ No valid content to store for {entity}")
        else:
            logger.info(f"✅ Stored → {entity}: {main_count} rows | {entity}_Line: {line_count} rows | LinkedTxn: {linked_count} rows")
            print(f"✅ Stored → {entity}: {main_count} rows | {entity}_Line: {line_count} rows | LinkedTxn: {linked_count} rows")

    except Exception as e:
        logger.error(f"❌ Failed to normalize/store {entity}: {e}")

