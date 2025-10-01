# copy_sqlserver_database.py
# pip install pyodbc

import pyodbc
import sys

# ========= Hardcoded credentials (edit these) =========
# Source
SRC_SQLSERVER_HOST = "20.51.187.236"
SRC_SQLSERVER_PORT = "1433"          # e.g. 1433 or empty if using default/DSN rules
SRC_SQLSERVER_USER = "sa"
SRC_SQLSERVER_PASSWORD = "Password123!"
SRC_SQLSERVER_DATABASE = "GX_Arkansas_Operations"

# Target
TGT_SQLSERVER_HOST = "127.0.0.1"
TGT_SQLSERVER_PORT = "1433"
TGT_SQLSERVER_USER = "sa"
TGT_SQLSERVER_PASSWORD = "Issc@123"
TGT_SQLSERVER_DATABASE = "F_GX_Arkansas_Operations"
# ======================================================

# Behavior flags
APPEND_IF_TARGET_TABLE_EXISTS = True   # if False, existing tables are skipped entirely
BATCH_SIZE = 2000

# ================== CONNECTION ==================
def get_conn(host, port, user, pwd, db):
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={host},{port};DATABASE={db};UID={user};PWD={pwd};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=False)

# ================== METADATA HELPERS ==================
def list_source_tables(src_conn):
    sql = """
    SELECT s.name AS schema_name, t.name AS table_name
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE t.is_ms_shipped = 0
    ORDER BY s.name, t.name;
    """
    with src_conn.cursor() as cur:
        return [(r[0], r[1]) for r in cur.execute(sql).fetchall()]

def ensure_schema(tgt_conn, schema):
    with tgt_conn.cursor() as cur:
        safe_schema = schema.replace(']', ']]')  # escape
        # wrap CREATE SCHEMA in EXEC so SQL Server accepts it in IF
        sql = f"""
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{safe_schema}')
        EXEC('CREATE SCHEMA [{safe_schema}]');
        """
        cur.execute(sql)
    tgt_conn.commit()




def table_exists(tgt_conn, schema, table):
    with tgt_conn.cursor() as cur:
        row = cur.execute("""
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """, (schema, table)).fetchone()
    return bool(row)

def get_columns(src_conn, schema, table):
    """
    Returns a list of dicts describing columns (excluding computed):
    name, type_name, max_length, precision, scale, is_nullable, is_identity
    """
    q = """
    SELECT
        c.name,
        t.name AS type_name,
        c.max_length,
        c.precision,
        c.scale,
        c.is_nullable,
        c.is_identity,
        COLUMNPROPERTY(c.object_id, c.name, 'IsComputed') AS is_computed
    FROM sys.columns c
    JOIN sys.types t ON t.user_type_id = c.user_type_id
    WHERE c.object_id = OBJECT_ID(?)
    ORDER BY c.column_id;
    """
    obj = f"[{schema}].[{table}]"
    out = []
    with src_conn.cursor() as cur:
        for (name, type_name, max_len, prec, scale, is_null, is_ident, is_comp) in cur.execute(q, obj):
            if is_comp == 1:
                continue  # skip computed columns
            out.append({
                "name": name,
                "type_name": type_name.lower(),
                "max_length": max_len,
                "precision": prec,
                "scale": scale,
                "is_nullable": bool(is_null),
                "is_identity": bool(is_ident),
            })
    return out

def build_type_decl(col):
    t = col["type_name"]
    # Length-bearing types
    if t in ("varchar", "nvarchar", "varbinary", "char", "nchar", "binary"):
        ml = col["max_length"]
        if t in ("nvarchar", "nchar"):
            # nv types store length in bytes; divide by 2 to get characters
            if ml == -1:
                length = "MAX"
            else:
                length = str(ml // 2)
        elif t in ("varchar", "char", "varbinary", "binary"):
            length = "MAX" if ml == -1 else str(ml)
        return f"{t}({length})"
    # Decimal/NUMERIC
    if t in ("decimal", "numeric"):
        p = col["precision"] or 18
        s = col["scale"] or 0
        return f"{t}({p},{s})"
    # Time/datetime2/datetimeoffset with scale
    if t in ("time", "datetime2", "datetimeoffset"):
        s = col["scale"]
        if s is not None:
            return f"{t}({s})"
        return t
    # Else as-is (bit, int, bigint, float, real, money, smallmoney, date, smalldatetime, datetime, uniqueidentifier, image, text, ntext, xml, geography, geometry, hierarchyid etc.)
    return t

def build_create_table_sql(schema, table, columns):
    col_defs = []
    for col in columns:
        name = f"[{col['name'].replace(']', ']]')}]"
        type_decl = build_type_decl(col)
        nullness = "NULL" if col["is_nullable"] else "NOT NULL"
        identity = " IDENTITY(1,1)" if col["is_identity"] else ""
        col_defs.append(f"{name} {type_decl}{identity} {nullness}")
    cols_sql = ",\n    ".join(col_defs) if col_defs else ""
    return f"CREATE TABLE [{schema}].[{table}] (\n    {cols_sql}\n);"

def create_table_if_needed(src_conn, tgt_conn, schema, table):
    if table_exists(tgt_conn, schema, table):
        return False  # already exists
    ensure_schema(tgt_conn, schema)
    cols = get_columns(src_conn, schema, table)
    if not cols:
        # empty/only-computed — create a single-rowguidcol? Skip instead.
        return False
    ddl = build_create_table_sql(schema, table, cols)
    with tgt_conn.cursor() as cur:
        cur.execute(ddl)
    tgt_conn.commit()
    return True

# ================== DATA COPY ==================
def get_insert_plan(src_conn, tgt_conn, schema, table):
    cols = get_columns(src_conn, schema, table)
    if not cols:
        return None, None

    col_names = [c["name"] for c in cols]

    # Wrap column names in brackets, escaping any closing bracket
    target_cols_sql = ", ".join(f"[{c.replace(']', ']]')}]" for c in col_names)
    placeholders = ", ".join("?" for _ in col_names)

    insert_sql = f"INSERT INTO [{schema}].[{table}] ({target_cols_sql}) VALUES ({placeholders})"

    # Properly join column names for the SELECT part
    select_cols_sql = ", ".join(f"[{c.replace(']', ']]')}]" for c in col_names)
    select_sql = f"SELECT {select_cols_sql} FROM [{schema}].[{table}]"

    return insert_sql, select_sql


def copy_table_data(src_conn, tgt_conn, schema, table):
    insert_sql, select_sql = get_insert_plan(src_conn, tgt_conn, schema, table)
    if not insert_sql:
        print(f"   · No non-computed columns to copy for {schema}.{table}; skipping data.")
        return

    with src_conn.cursor() as s_cur, tgt_conn.cursor() as t_cur:
        t_cur.fast_executemany = True
        s_cur.execute(select_sql)
        rows_copied = 0
        while True:
            rows = s_cur.fetchmany(BATCH_SIZE)
            if not rows:
                break
            t_cur.executemany(insert_sql, rows)
            rows_copied += len(rows)
        tgt_conn.commit()
    print(f"   · Copied {rows_copied} rows.")

# ================== MAIN ==================
def main():
    print("Connecting…")
    src = get_conn(SRC_SQLSERVER_HOST, SRC_SQLSERVER_PORT, SRC_SQLSERVER_USER, SRC_SQLSERVER_PASSWORD, SRC_SQLSERVER_DATABASE)
    tgt = get_conn(TGT_SQLSERVER_HOST, TGT_SQLSERVER_PORT, TGT_SQLSERVER_USER, TGT_SQLSERVER_PASSWORD, TGT_SQLSERVER_DATABASE)
    try:
        tables = list_source_tables(src)
        if not tables:
            print("No tables found on source.")
            return
        print(f"Found {len(tables)} tables to process.\n")

        for schema, table in tables:
            print(f"▶ {schema}.{table}")
            created = create_table_if_needed(src, tgt, schema, table)
            if created:
                print("   · Created on target.")
                copy_table_data(src, tgt, schema, table)
            else:
                if not table_exists(tgt, schema, table):
                    print("   · Skipped (no columns/empty definition).")
                    continue
                if APPEND_IF_TARGET_TABLE_EXISTS:
                    print("   · Exists on target — appending rows.")
                    copy_table_data(src, tgt, schema, table)
                else:
                    print("   · Exists on target — skipped.")
        print("\n✅ Done.")
    finally:
        src.close()
        tgt.close()

if __name__ == "__main__":
    main()
