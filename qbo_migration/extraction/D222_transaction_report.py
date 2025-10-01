import os, requests, time
import pandas as pd
from typing import Dict
from dotenv import load_dotenv
import pyodbc
from datetime import datetime, timedelta
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import global_logger as logger

# === Load environment and auto-refresh token ===
load_dotenv()
auto_refresh_token_if_needed()

# === SQL Server credentials ===
server = os.getenv("SQLSERVER_HOST")
port = os.getenv("SQLSERVER_PORT", "1433")
user = os.getenv("SQLSERVER_USER")
password = os.getenv("SQLSERVER_PASSWORD")
database = os.getenv("SQLSERVER_DATABASE")
odbc_driver = "ODBC Driver 17 for SQL Server"

def get_connection():
    return pyodbc.connect(
        f"DRIVER={{{odbc_driver}}};"
        f"SERVER={server},{port};DATABASE={database};"
        f"UID={user};PWD={password}",
        autocommit=False
    )

def ensure_database_exists():
    master_conn_str = (
        f"DRIVER={{{odbc_driver}}};SERVER={server},{port};UID={user};PWD={password};DATABASE=master"
    )
    with pyodbc.connect(master_conn_str, autocommit=True) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'{database}')
            BEGIN CREATE DATABASE [{database}] END
        """)
        print(f"✅ Verified database exists: {database}")

def write_to_sqlserver(df: pd.DataFrame, table_name: str = "TransactionReport"):
    if df.empty:
        logger.warning(f"⚠️ Skipping empty DataFrame for {table_name}")
        return

    ensure_database_exists()
    conn = get_connection()
    cursor = conn.cursor()
    cursor.fast_executemany = True

    def quote(col): return f'"{col}"'

    # Create table if not exists
    quoted_columns = ', '.join([f'{quote(col)} NVARCHAR(MAX)' for col in df.columns])
    cursor.execute(f"""
        SET QUOTED_IDENTIFIER ON;
        IF OBJECT_ID(N'{table_name}', N'U') IS NULL
        BEGIN CREATE TABLE [{table_name}] ({quoted_columns}) END
    """)

    # Add new columns if missing
    cursor.execute(f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'
    """)
    existing_cols = {row[0] for row in cursor.fetchall()}
    new_cols = [col for col in df.columns if col not in existing_cols]
    for col in new_cols:
        logger.info(f"➕ Adding column '{col}' to {table_name}")
        cursor.execute(f"""ALTER TABLE [{table_name}] ADD {quote(col)} NVARCHAR(MAX)""")

    # Insert data
    placeholders = ', '.join(['?'] * len(df.columns))
    col_names = ', '.join([quote(col) for col in df.columns])
    insert_sql = f'INSERT INTO [{table_name}] ({col_names}) VALUES ({placeholders})'

    try:
        cursor.executemany(insert_sql, df.values.tolist())
        conn.commit()
        logger.info(f"📥 Inserted {len(df)} rows into {table_name}")
    except Exception as e:
        logger.error(f"❌ Insert failed: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


# === QBO API Config ===
QBO_ENVIRONMENT = os.getenv("QBO_ENVIRONMENT", "production").lower()
realm_id = os.getenv("QBO_REALM_ID")

base_url = "https://quickbooks.api.intuit.com" if QBO_ENVIRONMENT == "production" else "https://sandbox-quickbooks.api.intuit.com"
report_url = f"{base_url}/v3/company/{realm_id}/reports/TransactionListWithSplits"

def get_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {os.getenv('QBO_ACCESS_TOKEN')}",
        "Accept": "application/json"
    }

def fetch_transaction_batch(start_date: str, end_date: str) -> pd.DataFrame:
    logger.info(f"📊 Fetching TransactionListWithSplits for {start_date} to {end_date}")
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "summarize_column_by": "Total",
        "minorversion": "65"
    }

    for attempt in range(3):
        response = requests.get(report_url, headers=get_headers(), params=params)
        if response.status_code == 200:
            break
        elif response.status_code == 401:
            logger.warning("🔒 Token expired – refreshing...")
            auto_refresh_token_if_needed()
        elif response.status_code in [429, 500, 503]:
            wait = 2 ** attempt
            logger.warning(f"⏳ Retry {attempt + 1}/3 in {wait}s — {response.status_code}")
            time.sleep(wait)
        else:
            raise RuntimeError(f"❌ Failed: {response.status_code} - {response.text}")
    else:
        raise RuntimeError(f"❌ Retries exhausted. Final error: {response.text}")

    report = response.json()
    columns = [col.get("ColTitle", f"Col{i}") for i, col in enumerate(report.get("Columns", {}).get("Column", []))]
    rows = report.get("Rows", {}).get("Row", [])
    if not rows:
        logger.warning("⚠️ No rows returned.")
        return pd.DataFrame()

    records = []
    for row in rows:
        if "ColData" in row:
            values = [col.get("value", None) for col in row["ColData"]]
            if len(values) < len(columns):
                values.extend([None] * (len(columns) - len(values)))
            records.append(values)

    df = pd.DataFrame(records, columns=columns)
    return df


def generate_monthly_ranges(start: str, end: str):
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    current = start_date
    ranges = []

    while current < end_date:
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        ranges.append((current.strftime('%Y-%m-%d'), (next_month - timedelta(days=1)).strftime('%Y-%m-%d')))
        current = next_month

    return ranges


def run_transaction_extraction():
    date_ranges = generate_monthly_ranges("2023-01-01", "2025-12-31")
    for start, end in date_ranges:
        try:
            df = fetch_transaction_batch(start, end)
            if not df.empty:
                write_to_sqlserver(df)
        except Exception as e:
            logger.error(f"❌ Batch {start} to {end} failed: {e}")


if __name__ == "__main__":
    run_transaction_extraction()
