# storage/sqlserver/sql.py
import re
import os
import time
import pyodbc
import pandas as pd
from dotenv import load_dotenv
import warnings
from functools import lru_cache

warnings.filterwarnings("ignore", category=UserWarning, message="pandas only supports SQLAlchemy.*")
load_dotenv()

# --------------------------------------------------------------------------------------
# Constants / defaults (unchanged names)
# --------------------------------------------------------------------------------------
USERID = 100250

# Transient error codes we should retry (deadlock/timeouts/transport)
_TRANSIENT_SQLSTATE_PREFIXES = {"08", "40"}  # connection failures / tx rollback
_TRANSIENT_ERROR_CODES = {1205, 1222}        # deadlock, lock request timeout

_DEFAULT_LOGIN_TIMEOUT = int(os.getenv("SQLSERVER_LOGIN_TIMEOUT", "15"))
_DEFAULT_QUERY_TIMEOUT = int(os.getenv("SQLSERVER_QUERY_TIMEOUT", "60"))
_EXECUTEMANY_CHUNK = int(os.getenv("SQLSERVER_EXECUTEMANY_CHUNK", "1000"))
_MAX_RETRIES = int(os.getenv("SQLSERVER_MAX_RETRIES", "3"))
_RETRY_BACKOFF_BASE = float(os.getenv("SQLSERVER_RETRY_BACKOFF_BASE", "0.5"))

# --------------------------------------------------------------------------------------
# Connection helpers (names preserved for public functions)
# --------------------------------------------------------------------------------------
def _odbc_driver():
    # Allow override; default to widely available 17
    return os.getenv("SQLSERVER_ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

def _build_conn_str(*, host, port, database, user, password):
    # Keep same formatting but add recommended attrs via connection args when opening
    return (
        f"DRIVER={{{_odbc_driver()}}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};PWD={password};"
        "TrustServerCertificate=yes;"
        "Mars_Connection=yes;"
    )

def _connect(conn_str: str) -> pyodbc.Connection:
    """
    Centralized connector with login timeout; caller controls commits.
    """
    return pyodbc.connect(
        conn_str,
        autocommit=False,
        timeout=_DEFAULT_LOGIN_TIMEOUT
    )

def _retryable(fn, *args, **kwargs):
    """
    Minimal retry wrapper for transient ODBC issues.
    """
    attempts = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except pyodbc.Error as e:
            attempts += 1
            # Inspect SQLSTATE and native error code
            sqlstate = None
            code = None
            try:
                if e.args and len(e.args) > 0 and isinstance(e.args[0], tuple):
                    # Some drivers pack tuples: (SQLSTATE, msg)
                    sqlstate = e.args[0][0]
                elif e.args and isinstance(e.args[0], str) and len(e.args[0]) >= 5:
                    sqlstate = e.args[0][:5]
            except Exception:
                pass
            try:
                if hasattr(e, "args") and len(e.args) >= 2 and isinstance(e.args[1], int):
                    code = e.args[1]
            except Exception:
                pass

            if (
                attempts <= _MAX_RETRIES and
                ((sqlstate and sqlstate[:2] in _TRANSIENT_SQLSTATE_PREFIXES) or (code in _TRANSIENT_ERROR_CODES))
            ):
                time.sleep(_RETRY_BACKOFF_BASE * (2 ** (attempts - 1)))
                continue
            raise

# --------------------------------------------------------------------------------------
# ControlTower database resolver (names preserved)
# --------------------------------------------------------------------------------------
def _get_controltower_connection():
    """
    Connects to the ControlTower catalog DB using CT_* env vars.
    Env (examples):
      CT_SQLSERVER_HOST=localhost
      CT_SQLSERVER_PORT=1433
      CT_SQLSERVER_USER=sa
      CT_SQLSERVER_PASSWORD=
      CT_SQLSERVER_DATABASE=ControlTower
    """
    ct_conn_str = _build_conn_str(
        host=os.getenv("CT_SQLSERVER_HOST", "localhost"),
        port=os.getenv("CT_SQLSERVER_PORT", "1433"),
        database=os.getenv("CT_SQLSERVER_DATABASE", "ControlTower"),
        user=os.getenv("CT_SQLSERVER_USER", "sa"),
        password=os.getenv("CT_SQLSERVER_PASSWORD", ""),
    )
    return _connect(ct_conn_str)

@lru_cache(maxsize=256)
def _resolve_user_database(user_id: str) -> str:
    """
    Reads the per-user database from ControlTower.[Schema].[Table].
    Schema/table default to: ct.UserDatabase
    Table schema:
      UserId, CompanyName, DatabaseName, CreatedAt
    """
    ct_schema = os.getenv("CT_SQLSERVER_Schema", "ct")
    ct_table  = os.getenv("CT_SQLSERVER_Table", "UserDatabase")
    query = f"SELECT TOP (1) DatabaseName FROM [{ct_schema}].[{ct_table}] WHERE UserId = ?"

    with _get_controltower_connection() as conn:
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        cur = conn.cursor()
        _retryable(cur.execute, query, (user_id,))
        row = cur.fetchone()

    if not row or not row[0]:
        raise RuntimeError(f"No DatabaseName found in {ct_schema}.{ct_table} for UserId='{user_id}'")

    return str(row[0])

def get_sqlserver_connection(USERID=USERID):
    """
    1) Look up USERID's database from ControlTower (CT_* envs).
    2) Connect to that database using main SQLSERVER_* envs for host/user/pass.
       - SQLSERVER_HOST
       - SQLSERVER_PORT
       - SQLSERVER_USER
       - SQLSERVER_PASSWORD
       - SQLSERVER_DATABASE is ignored (we use the looked-up DB)
    """
    target_db = _resolve_user_database(USERID)

    host = os.getenv("SQLSERVER_HOST", "127.0.0.1")
    port = os.getenv("SQLSERVER_PORT", "1433")
    user = os.getenv("SQLSERVER_USER", "sa")
    pwd  = os.getenv("SQLSERVER_PASSWORD", "")

    conn_str = _build_conn_str(
        host=host,
        port=port,
        database=target_db,
        user=user,
        password=pwd
    )
    return _connect(conn_str)

# --------------------------------------------------------------------------------------
# Schema / DDL helpers (names preserved)
# --------------------------------------------------------------------------------------
def ensure_schema_exists(schema_name: str):
    with get_sqlserver_connection(USERID) as conn:
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        with conn.cursor() as cursor:
            # Check if schema exists
            cursor.execute("SELECT 1 FROM sys.schemas WHERE name = ?", schema_name)
            exists = cursor.fetchone()
            if not exists:
                # Use dynamic SQL safely
                sql = f"EXEC('CREATE SCHEMA {schema_name}')"
                cursor.execute(sql)
        conn.commit()
    print(f"✅ Schema ensured: {schema_name}")


# --------------------------------------------------------------------------------------
# Simple fetch / scalar (names preserved)
# --------------------------------------------------------------------------------------
def fetch_single_value(query: str, params: tuple) -> str:
    with get_sqlserver_connection(USERID) as conn:
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        with conn.cursor() as cursor:
            _retryable(cursor.execute, query, params)
            result = cursor.fetchone()
    return result[0] if result else None

# --------------------------------------------------------------------------------------
# DataFrame insert (names preserved)
# --------------------------------------------------------------------------------------
def _ensure_table_for_df(cur: pyodbc.Cursor, schema: str, table: str, columns_iterable):
    full_table = f"[{schema}].[{table}]"
    col_defs = ', '.join([f"[{c}] NVARCHAR(MAX)" for c in columns_iterable])
    _retryable(cur.execute, f"CREATE TABLE {full_table} ({col_defs})")

def _add_missing_columns(cur: pyodbc.Cursor, schema: str, table: str, target_cols_set):
    # Use INFORMATION_SCHEMA for robust detection
    existing = set()
    _retryable(
        cur.execute,
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """,
        (schema, table)
    )
    for row in cur.fetchall():
        existing.add(row[0])

    full_table = f"[{schema}].[{table}]"
    for col in target_cols_set - existing:
        _retryable(cur.execute, f"ALTER TABLE {full_table} ADD [{col}] NVARCHAR(MAX)")

def insert_dataframe(df: pd.DataFrame, table: str, schema: str = "dbo", if_exists="append", conn=None):
    if df.empty:
        print(f"⚠️ No rows to insert into {schema}.{table}")
        return
    def _do_insert(conn):
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        with conn.cursor() as cursor:
            full_table = f"[{schema}].[{table}]"
            columns = [str(c) for c in df.columns]
            cols_list = ', '.join(f"[{c}]" for c in columns)
            placeholders = ', '.join('?' for _ in columns)
            if if_exists == "replace":
                _retryable(cursor.execute, f"IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL DROP TABLE {full_table}")
                _ensure_table_for_df(cursor, schema, table, columns)
            else:
                if not table_exists(table, schema, conn=conn):
                    _ensure_table_for_df(cursor, schema, table, columns)
                else:
                    _add_missing_columns(cursor, schema, table, set(columns))
            cursor.fast_executemany = True
            data = [tuple(None if pd.isna(v) else v for v in row) for row in df.itertuples(index=False, name=None)]
            if data:
                for i in range(0, len(data), _EXECUTEMANY_CHUNK):
                    batch = data[i:i+_EXECUTEMANY_CHUNK]
                    _retryable(cursor.executemany, f"INSERT INTO {full_table} ({cols_list}) VALUES ({placeholders})", batch)
        conn.commit()
    if conn:
        _do_insert(conn)
    else:
        with get_sqlserver_connection(USERID) as conn:
            _do_insert(conn)

# --------------------------------------------------------------------------------------
# Generic query exec (names preserved)
# --------------------------------------------------------------------------------------
def run_query(query: str, params: tuple = (), conn=None):
    def _do_run(conn):
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        with conn.cursor() as cursor:
            _retryable(cursor.execute, query, params)
        conn.commit()
    if conn:
        _do_run(conn)
    else:
        with get_sqlserver_connection(USERID) as conn:
            _do_run(conn)

# --------------------------------------------------------------------------------------
# Upsert single row dict (names preserved)
# --------------------------------------------------------------------------------------
def upsert_row(df_row: dict, schema: str, table: str, conn=None):
    def _do_upsert(conn):
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        with conn.cursor() as cursor:
            columns = list(df_row.keys())
            full_table = f"[{schema}].[{table}]"
            update_clause = ', '.join([f"[{col}] = ?" for col in columns if col != "Source_Id"])
            insert_columns = ', '.join([f"[{col}]" for col in columns])
            placeholders = ', '.join(['?' for _ in columns])
            sql_stmt = f"""
                IF EXISTS (SELECT 1 FROM {full_table} WHERE [Source_Id] = ?)
                    UPDATE {full_table}
                    SET {update_clause}
                    WHERE [Source_Id] = ?
                ELSE
                    INSERT INTO {full_table} ({insert_columns})
                    VALUES ({placeholders})
            """
            params = [df_row["Source_Id"]] \
                     + [df_row[col] for col in columns if col != "Source_Id"] \
                     + [df_row["Source_Id"]] \
                     + [df_row[col] for col in columns]
            _retryable(cursor.execute, sql_stmt, tuple(params))
        conn.commit()
    if conn:
        _do_upsert(conn)
    else:
        with get_sqlserver_connection(USERID) as conn:
            _do_upsert(conn)

# --------------------------------------------------------------------------------------
# Status update (names preserved)
# --------------------------------------------------------------------------------------
def update_status(source_id: str, status: str, message: str = None, qbo_id: str = None,
                  schema: str = "porter_entities_mapping", table: str = "Map_Account", conn=None):
    def _do_update(conn):
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        with conn.cursor() as cursor:
            full_table = f"[{schema}].[{table}]"
            update_stmt = f"""
                UPDATE {full_table}
                SET porter_status = ?, porter_message = ?, porter_qbo_id = ?
                WHERE Source_Id = ?
            """
            _retryable(cursor.execute, update_stmt, (status, message, qbo_id, source_id))
        conn.commit()
    if conn:
        _do_update(conn)
    else:
        with get_sqlserver_connection(USERID) as conn:
            _do_update(conn)

# --------------------------------------------------------------------------------------
# Fetch helpers (names preserved)
# --------------------------------------------------------------------------------------
def fetch_dataframe(query: str, conn=None, params=None) -> pd.DataFrame:
    def _do_fetch(conn):
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        if params is not None:
            df = pd.read_sql(query, conn, params=params)
        else:
            df = pd.read_sql(query, conn)
        return df
    if conn:
        return _do_fetch(conn)
    else:
        with get_sqlserver_connection(USERID) as conn:
            return _do_fetch(conn)

def fetch_table(table: str, schema: str = "dbo", conn=None) -> pd.DataFrame:
    def _do_fetch(conn):
        if not table_exists(table, schema, conn=conn):
            print(f"⚠️ Table [{schema}].[{table}] does not exist. Returning empty DataFrame.")
            return pd.DataFrame()
        query = f"SELECT * FROM [{schema}].[{table}]"
        return fetch_dataframe(query, conn=conn)
    if conn:
        return _do_fetch(conn)
    else:
        with get_sqlserver_connection(USERID) as conn:
            return _do_fetch(conn)

def table_exists(table: str, schema: str = "dbo", conn=None) -> bool:
    def _do_exists(conn):
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        with conn.cursor() as cursor:
            _retryable(
                cursor.execute,
                """
                SELECT 1
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """,
                (schema, table)
            )
            exists = cursor.fetchone() is not None
        return exists
    if conn:
        return _do_exists(conn)
    else:
        with get_sqlserver_connection(USERID) as conn:
            return _do_exists(conn)

def fetch_value_dict(query: str, key_col: str, value_col: str) -> dict:
    df = fetch_dataframe(query)
    if df.empty or key_col not in df.columns or value_col not in df.columns:
        return {}
    return dict(zip(df[key_col], df[value_col]))

def fetch_scalar(query: str, params: tuple = ()) -> any:
    """
    Execute a scalar query and return a single value (first column of the first row).
    """
    with get_sqlserver_connection(USERID) as conn:
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        with conn.cursor() as cursor:
            _retryable(cursor.execute, query, params)
            result = cursor.fetchone()
            return result[0] if result else None

def fetch_streaming(query: str, params: tuple = ()):
    """
    Generator to stream rows from a SQL Server query using pyodbc.
    Safely opens a cursor and yields each row as a dictionary.
    """
    conn = get_sqlserver_connection(USERID)
    conn.timeout = _DEFAULT_QUERY_TIMEOUT
    cursor = conn.cursor()
    try:
        _retryable(cursor.execute, query, params)
        columns = [col[0] for col in cursor.description]
        while True:
            row = cursor.fetchone()
            if not row:
                break
            yield dict(zip(columns, row))
    finally:
        cursor.close()
        conn.close()

def fetch_table_with_params(query: str, params: tuple) -> pd.DataFrame:
    """
    Execute a parameterized SQL query and return the result as a DataFrame.
    """
    with get_sqlserver_connection(USERID) as conn:
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        df = pd.read_sql(query, conn, params=params)
    return df

# --------------------------------------------------------------------------------------
# Column helpers (names preserved)
# --------------------------------------------------------------------------------------
def sanitize_column_name(col: str) -> str:
    """
    Replaces characters that are invalid for SQL Server column names.
    """
    return re.sub(r'[.\[\]]', '_', col)

def insert_invoice_map_dataframe(df: pd.DataFrame, table: str, schema: str = "dbo"):
    """
    Inserts DataFrame into specified SQL Server table. Automatically creates the table if it does not exist.
    """
    if df.empty:
        print(f"⚠️ Skipped insertion: DataFrame for table '{schema}.{table}' is empty.")
        return

    if df.columns.empty:
        raise ValueError(f"❌ Cannot insert: DataFrame has no columns for '{schema}.{table}'.")

    with get_sqlserver_connection(USERID) as conn:
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        with conn.cursor() as cur:
            full_table = f"[{schema}].[{table}]"

            # Sanitize column names (behavior preserved)
            original_columns = df.columns.tolist()
            sanitized_columns = [sanitize_column_name(col) for col in original_columns]
            rename_mapping = dict(zip(original_columns, sanitized_columns))
            df_renamed = df.rename(columns=rename_mapping)

            # Create table if not exists; else add missing columns (fewer round trips)
            if not table_exists(table, schema):
                _ensure_table_for_df(cur, schema, table, df_renamed.columns)
            else:
                _add_missing_columns(cur, schema, table, set(df_renamed.columns))

            insert_cols = ', '.join([f"[{col}]" for col in df_renamed.columns])
            placeholders = ', '.join(['?' for _ in df_renamed.columns])

            # Fast executemany in chunks
            cur.fast_executemany = True
            data = [
                tuple(None if pd.isna(row[col]) else row[col] for col in df_renamed.columns)
                for _, row in df_renamed.iterrows()
            ]
            if data:
                for i in range(0, len(data), _EXECUTEMANY_CHUNK):
                    batch = data[i:i+_EXECUTEMANY_CHUNK]
                    _retryable(cur.executemany, f"INSERT INTO {full_table} ({insert_cols}) VALUES ({placeholders})", batch)

        conn.commit()

def fetch_column(query: str, params: tuple = ()) -> list:
    """
    Execute a query that returns a single column and return it as a Python list.
    """
    with get_sqlserver_connection(USERID) as conn:
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        df = pd.read_sql(query, conn, params=params)
        return df.iloc[:, 0].tolist() if not df.empty else []

def update_multiple_columns(table: str, source_id: str, column_value_map: dict, schema: str = "dbo"):
    """
    Updates multiple columns in a specified table where Source_Id matches.

    Args:
        table (str): Target table name.
        source_id (str): The Source_Id to match.
        column_value_map (dict): Dictionary of column-value pairs to update.
        schema (str): SQL schema (default: dbo).
    """
    if not column_value_map:
        return

    with get_sqlserver_connection(USERID) as conn:
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        with conn.cursor() as cur:
            full_table = f"[{schema}].[{table}]"
            set_clause = ", ".join([f"[{col}] = ?" for col in column_value_map])
            params = list(column_value_map.values()) + [source_id]

            query = f"""
                UPDATE {full_table}
                SET {set_clause}
                WHERE [Source_Id] = ?
            """
            _retryable(cur.execute, query, params)
        conn.commit()

def ensure_column_exists(table: str, column: str, col_type: str = "NVARCHAR(MAX)", schema: str = "dbo"):
    """
    Ensures a column exists in the specified table; if not, it adds it.
    """
    with get_sqlserver_connection(USERID) as conn:
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        with conn.cursor() as cursor:
            _retryable(
                cursor.execute,
                """
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
                """,
                (schema, table, column)
            )
            if not cursor.fetchone():
                full_table = f"[{schema}].[{table}]"
                print(f"➕ Adding column [{column}] to table [{schema}].[{table}]")
                _retryable(cursor.execute, f"ALTER TABLE {full_table} ADD [{column}] {col_type}")
        conn.commit()

def fetch_all(query: str, params: tuple = ()) -> list:
    """
    Executes a query and returns all rows as a list of tuples.
    Useful for building mapping dictionaries.
    """
    with get_sqlserver_connection(USERID) as conn:
        conn.timeout = _DEFAULT_QUERY_TIMEOUT
        with conn.cursor() as cursor:
            _retryable(cursor.execute, query, params)
            rows = cursor.fetchall()
    return rows

# --------------------------------------------------------------------------------------
# Cache utility (name preserved)
# --------------------------------------------------------------------------------------
def clear_cache():
    """
    Clears any in-memory cache used by the SQL module (if applicable).
    This is a no-op unless caching is implemented.
    """
    try:
        if hasattr(clear_cache, "_memo"):
            clear_cache._memo.clear()
        # Also clear the user DB resolver cache
        _resolve_user_database.cache_clear()
        print("🧹 SQL cache cleared.")
    except Exception as e:
        print(f"⚠️ Could not clear SQL cache: {e}")


def executemany(query: str, param_list):
    if not param_list:
        return
    conn = get_sqlserver_connection()
    try:
        cur = conn.cursor()
        try:
            cur.fast_executemany = True
        except Exception:
            pass
        cur.executemany(query, param_list)
        conn.commit()
    finally:
        conn.close()


# # storage/sqlserver/sql.py
# import re
# import os
# import pyodbc
# import pandas as pd
# from dotenv import load_dotenv
# import warnings
# warnings.filterwarnings("ignore", category=UserWarning, message="pandas only supports SQLAlchemy.*")
# from functools import lru_cache

# load_dotenv()

# USERID = 100250

# def _odbc_driver():
#     # Allow override; default to widely available 17
#     return os.getenv("SQLSERVER_ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

# def _build_conn_str(*, host, port, database, user, password):
#     return (
#         f"DRIVER={{{_odbc_driver()}}};"
#         f"SERVER={host},{port};"
#         f"DATABASE={database};"
#         f"UID={user};PWD={password};"
#         "TrustServerCertificate=yes;"
#     )

# def _get_controltower_connection():
#     """
#     Connects to the ControlTower catalog DB using CT_* env vars.
#     Env (examples):
#       CT_SQLSERVER_HOST=localhost
#       CT_SQLSERVER_PORT=1433
#       CT_SQLSERVER_USER=sa
#       CT_SQLSERVER_PASSWORD=
#       CT_SQLSERVER_DATABASE=ControlTower
#     """
#     ct_conn_str = _build_conn_str(
#         host=os.getenv("CT_SQLSERVER_HOST", "localhost"),
#         port=os.getenv("CT_SQLSERVER_PORT", "1433"),
#         database=os.getenv("CT_SQLSERVER_DATABASE", "ControlTower"),
#         user=os.getenv("CT_SQLSERVER_USER", "sa"),
#         password=os.getenv("CT_SQLSERVER_PASSWORD", ""),
#     )
#     return pyodbc.connect(ct_conn_str)

# @lru_cache(maxsize=256)
# def _resolve_user_database(user_id: str) -> str:
#     """
#     Reads the per-user database from ControlTower.[Schema].[Table].
#     Schema/table default to: ct.UserDatabase
#     Table schema:
#       UserId, CompanyName, DatabaseName, CreatedAt
#     """
#     ct_schema = os.getenv("CT_SQLSERVER_Schema", "ct")
#     ct_table  = os.getenv("CT_SQLSERVER_Table", "UserDatabase")
#     query = f"SELECT TOP (1) DatabaseName FROM [{ct_schema}].[{ct_table}] WHERE UserId = ?"

#     with _get_controltower_connection() as conn:
#         cur = conn.cursor()
#         cur.execute(query, (user_id,))
#         row = cur.fetchone()

#     if not row or not row[0]:
#         raise RuntimeError(f"No DatabaseName found in {ct_schema}.{ct_table} for UserId='{user_id}'")

#     return str(row[0])

# def get_sqlserver_connection(USERID=USERID):
#     """
#     1) Look up USERID's database from ControlTower (CT_* envs).
#     2) Connect to that database using main SQLSERVER_* envs for host/user/pass.
#        - SQLSERVER_HOST
#        - SQLSERVER_PORT
#        - SQLSERVER_USER
#        - SQLSERVER_PASSWORD
#        - SQLSERVER_DATABASE is ignored (we use the looked-up DB)
#     """
#     target_db = _resolve_user_database(USERID)

#     host = os.getenv("SQLSERVER_HOST", "127.0.0.1")
#     port = os.getenv("SQLSERVER_PORT", "1433")
#     user = os.getenv("SQLSERVER_USER", "sa")
#     pwd  = os.getenv("SQLSERVER_PASSWORD", "")

#     conn_str = _build_conn_str(
#         host=host,
#         port=port,
#         database=target_db,
#         user=user,
#         password=pwd
#     )
#     return pyodbc.connect(conn_str)

# def ensure_schema_exists(schema_name: str):
#     conn = get_sqlserver_connection(USERID)
#     cursor = conn.cursor()
#     cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = ?) EXEC('CREATE SCHEMA [{schema_name}]')", schema_name)
#     conn.commit()
#     cursor.close()
#     conn.close()
#     print(f"✅ Schema ensured: {schema_name}")

# def fetch_single_value(query: str, params: tuple) -> str:
#     conn = get_sqlserver_connection(USERID)
#     cursor = conn.cursor()
#     cursor.execute(query, params)
#     result = cursor.fetchone()
#     conn.close()
#     return result[0] if result else None

# def insert_dataframe(df: pd.DataFrame, table: str, schema: str = "dbo", if_exists="append"):
#     if df.empty:
#         print(f"⚠️ No rows to insert into {schema}.{table}")
#         return

#     conn = get_sqlserver_connection(USERID)
#     cursor = conn.cursor()

#     full_table = f"[{schema}].[{table}]"
#     columns = ', '.join([f"[{col}]" for col in df.columns])
#     placeholders = ', '.join(['?' for _ in df.columns])

#     if if_exists == "replace":
#         cursor.execute(f"IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL DROP TABLE {full_table}")
#         col_defs = ', '.join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
#         cursor.execute(f"CREATE TABLE {full_table} ({col_defs})")

#     elif not table_exists(table, schema):
#         # First-time creation for 'append' mode
#         col_defs = ', '.join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
#         cursor.execute(f"CREATE TABLE {full_table} ({col_defs})")

#     for _, row in df.iterrows():
#         cursor.execute(f"INSERT INTO {full_table} ({columns}) VALUES ({placeholders})", tuple(row))

#     conn.commit()
#     cursor.close()
#     conn.close()
#     print(f"✅ Inserted {len(df)} rows into {schema}.{table}")

# def run_query(query: str, params: tuple = ()):
#     conn = get_sqlserver_connection(USERID)
#     cursor = conn.cursor()
#     cursor.execute(query, params)
#     conn.commit()
#     cursor.close()
#     conn.close()

# def upsert_row(df_row: dict, schema: str, table: str):
#     conn = get_sqlserver_connection(USERID)
#     cursor = conn.cursor()

#     columns = list(df_row.keys())
#     full_table = f"[{schema}].[{table}]"

#     # Generate UPDATE SET clause
#     update_clause = ', '.join([f"[{col}] = ?" for col in columns if col != "Source_Id"])
#     insert_columns = ', '.join([f"[{col}]" for col in columns])
#     placeholders = ', '.join(['?' for _ in columns])

#     sql = f"""
#         IF EXISTS (SELECT 1 FROM {full_table} WHERE [Source_Id] = ?)
#             UPDATE {full_table}
#             SET {update_clause}
#             WHERE [Source_Id] = ?
#         ELSE
#             INSERT INTO {full_table} ({insert_columns})
#             VALUES ({placeholders})
#     """

#     params = [df_row["Source_Id"]] + [df_row[col] for col in columns if col != "Source_Id"] + [df_row["Source_Id"]] + [df_row[col] for col in columns]

#     cursor.execute(sql, tuple(params))
#     conn.commit()
#     cursor.close()
#     conn.close()

# def update_status(source_id: str, status: str, message: str = None, qbo_id: str = None,
#                   schema: str = "porter_entities_mapping", table: str = "Map_Account"):

#     conn = get_sqlserver_connection(USERID)
#     cursor = conn.cursor()
#     full_table = f"[{schema}].[{table}]"

#     update_stmt = f"""
#         UPDATE {full_table}
#         SET porter_status = ?, porter_message = ?, porter_qbo_id = ?
#         WHERE Source_Id = ?
#     """
#     cursor.execute(update_stmt, (status, message, qbo_id, source_id))
#     conn.commit()
#     cursor.close()
#     conn.close()

# # === Fetch using raw SQL ===
# def fetch_dataframe(query: str) -> pd.DataFrame:
#     conn = get_sqlserver_connection(USERID)
#     try:
#         df = pd.read_sql(query, conn)
#     finally:
#         conn.close()
#     return df

# # ✅ NEW FUNCTION: fetch table using schema + table
# def fetch_table(table: str, schema: str = "dbo") -> pd.DataFrame:
#     if not table_exists(table, schema):
#         print(f"⚠️ Table [{schema}].[{table}] does not exist. Returning empty DataFrame.")
#         return pd.DataFrame()
#     query = f"SELECT * FROM [{schema}].[{table}]"
#     return fetch_dataframe(query)

# def table_exists(table: str, schema: str = "dbo") -> bool:
#     query = """
#         SELECT 1
#         FROM INFORMATION_SCHEMA.TABLES
#         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
#     """
#     conn = get_sqlserver_connection(USERID)
#     cursor = conn.cursor()
#     cursor.execute(query, (schema, table))
#     exists = cursor.fetchone() is not None
#     cursor.close()
#     conn.close()
#     return exists

# def fetch_value_dict(query: str, key_col: str, value_col: str) -> dict:
#     df = fetch_dataframe(query)
#     if df.empty or key_col not in df.columns or value_col not in df.columns:
#         return {}
#     return dict(zip(df[key_col], df[value_col]))

# def fetch_scalar(query: str, params: tuple = ()) -> any:
#     """
#     Execute a scalar query and return a single value (first column of the first row).
#     """
#     conn = get_sqlserver_connection(USERID)
#     cursor = conn.cursor()
#     try:
#         cursor.execute(query, params)
#         result = cursor.fetchone()
#         return result[0] if result else None
#     finally:
#         cursor.close()
#         conn.close()

# def fetch_streaming(query: str, params: tuple = ()):
#     """
#     Generator to stream rows from a SQL Server query using pyodbc.
#     Safely opens a cursor and yields each row as a dictionary.
#     """
#     conn = get_sqlserver_connection(USERID)
#     cursor = conn.cursor()
#     try:
#         cursor.execute(query, params)
#         columns = [col[0] for col in cursor.description]
#         while True:
#             row = cursor.fetchone()
#             if not row:
#                 break
#             yield dict(zip(columns, row))
#     finally:
#         cursor.close()
#         conn.close()

# def fetch_table_with_params(query: str, params: tuple) -> pd.DataFrame:
#     """
#     Execute a parameterized SQL query and return the result as a DataFrame.
#     """
#     conn = get_sqlserver_connection(USERID)
#     try:
#         df = pd.read_sql(query, conn, params=params)
#     finally:
#         conn.close()
#     return df

# def sanitize_column_name(col: str) -> str:
#     """
#     Replaces characters that are invalid for SQL Server column names.
#     """
#     return re.sub(r'[.\[\]]', '_', col)

# def insert_invoice_map_dataframe(df: pd.DataFrame, table: str, schema: str = "dbo"):
#     """
#     Inserts DataFrame into specified SQL Server table. Automatically creates the table if it does not exist.
#     """
#     if df.empty:
#         print(f"⚠️ Skipped insertion: DataFrame for table '{schema}.{table}' is empty.")
#         return

#     if df.columns.empty:
#         raise ValueError(f"❌ Cannot insert: DataFrame has no columns for '{schema}.{table}'.")

#     conn = get_sqlserver_connection(USERID)
#     cur = conn.cursor()

#     full_table = f"[{schema}].[{table}]"

#     # Sanitize column names
#     original_columns = df.columns.tolist()
#     sanitized_columns = [sanitize_column_name(col) for col in original_columns]
#     rename_mapping = dict(zip(original_columns, sanitized_columns))

#     df_renamed = df.rename(columns=rename_mapping)

#     # Create table if not exists
#     col_defs = ', '.join([f"[{col}] NVARCHAR(MAX)" for col in df_renamed.columns])

#     if not table_exists(table, schema):
#         create_sql = f"CREATE TABLE {full_table} ({col_defs})"
#         # print(f"🛠️ Creating table with SQL: {create_sql}")
#         cur.execute(create_sql)
#     else:
#         # Add missing columns
#         existing_cols = set(col.column_name for col in cur.columns(table=table, schema=schema))
#         for col in df_renamed.columns:
#             if col not in existing_cols:
#                 alter_sql = f"ALTER TABLE {full_table} ADD [{col}] NVARCHAR(MAX)"
#                 print(f"➕ Adding missing column with SQL: {alter_sql}")
#                 cur.execute(alter_sql)

#     insert_cols = ', '.join([f"[{col}]" for col in df_renamed.columns])
#     placeholders = ', '.join(['?' for _ in df_renamed.columns])

#     for _, row in df_renamed.iterrows():
#         cur.execute(
#             f"INSERT INTO {full_table} ({insert_cols}) VALUES ({placeholders})",
#             tuple(row[col] for col in df_renamed.columns)
#         )

#     conn.commit()
#     cur.close()
#     conn.close()

# def fetch_column(query: str, params: tuple = ()) -> list:
#     """
#     Execute a query that returns a single column and return it as a Python list.
#     """
#     conn = get_sqlserver_connection(USERID)
#     try:
#         df = pd.read_sql(query, conn, params=params)
#         return df.iloc[:, 0].tolist() if not df.empty else []
#     finally:
#         conn.close()

# def update_multiple_columns(table: str, source_id: str, column_value_map: dict, schema: str = "dbo"):
#     """
#     Updates multiple columns in a specified table where Source_Id matches.

#     Args:
#         table (str): Target table name.
#         source_id (str): The Source_Id to match.
#         column_value_map (dict): Dictionary of column-value pairs to update.
#         schema (str): SQL schema (default: dbo).
#     """
#     if not column_value_map:
#         return

#     conn = get_sqlserver_connection(USERID)
#     cur = conn.cursor()

#     full_table = f"[{schema}].[{table}]"
#     set_clause = ", ".join([f"[{col}] = ?" for col in column_value_map])
#     params = list(column_value_map.values()) + [source_id]

#     query = f"""
#         UPDATE {full_table}
#         SET {set_clause}
#         WHERE [Source_Id] = ?
#     """
#     cur.execute(query, params)
#     conn.commit()
#     cur.close()
#     conn.close()

# def ensure_column_exists(table: str, column: str, col_type: str = "NVARCHAR(MAX)", schema: str = "dbo"):
#     """
#     Ensures a column exists in the specified table; if not, it adds it.

#     Args:
#         table (str): Table name
#         column (str): Column name
#         col_type (str): SQL type (default NVARCHAR(MAX))
#         schema (str): Schema name (default dbo)
#     """
#     conn = get_sqlserver_connection(USERID)
#     cursor = conn.cursor()
#     check_query = """
#         SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
#         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
#     """
#     cursor.execute(check_query, (schema, table, column))
#     if not cursor.fetchone():
#         full_table = f"[{schema}].[{table}]"
#         alter_sql = f"ALTER TABLE {full_table} ADD [{column}] {col_type}"
#         print(f"➕ Adding column [{column}] to table [{schema}].[{table}]")
#         cursor.execute(alter_sql)
#         conn.commit()
#     cursor.close()
#     conn.close()

# def table_exists(table: str, schema: str = "dbo") -> bool:
#     """
#     Checks if a table exists in the given schema.

#     Args:
#         table (str): Table name
#         schema (str): Schema name (default is 'dbo')

#     Returns:
#         bool: True if table exists, False otherwise
#     """
#     conn = get_sqlserver_connection(USERID)
#     cursor = conn.cursor()
#     query = """
#         SELECT 1
#         FROM INFORMATION_SCHEMA.TABLES
#         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
#     """
#     cursor.execute(query, (schema, table))
#     exists = cursor.fetchone() is not None
#     cursor.close()
#     conn.close()
#     return exists

# def fetch_all(query: str, params: tuple = ()) -> list:
#     """
#     Executes a query and returns all rows as a list of tuples.
#     Useful for building mapping dictionaries.
#     """
#     conn = get_sqlserver_connection(USERID)
#     cursor = conn.cursor()
#     try:
#         cursor.execute(query, params)
#         return cursor.fetchall()
#     finally:
#         cursor.close()
#         conn.close()

# # In storage/sqlserver/sql.py

# def clear_cache():
#     """
#     Clears any in-memory cache used by the SQL module (if applicable).
#     This is a no-op unless caching is implemented.
#     """
#     try:
#         if hasattr(clear_cache, "_memo"):
#             clear_cache._memo.clear()
#         print("🧹 SQL cache cleared.")
#     except Exception as e:
#         print(f"⚠️ Could not clear SQL cache: {e}")
