import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()


def get_company_database(user_id: int) -> str:
    """
    Fetch company name from ControlTower [ct.Users] and build a safe DB name with user_id.
    """
    admin_conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('CT_SQLSERVER_HOST')},{os.getenv('CT_SQLSERVER_PORT')};"
        f"UID={os.getenv('CT_SQLSERVER_USER')};"
        f"PWD={os.getenv('CT_SQLSERVER_PASSWORD')};"
        f"DATABASE={os.getenv('CT_SQLSERVER_DATABASE')};"
    )

    with pyodbc.connect(admin_conn_str, autocommit=True) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT companyName FROM ct.Users WHERE Id = ?", user_id)
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"No company found for user_id={user_id}")
        company = row[0]

    # ✅ sanitize and generate DB name (company + user_id)
    safe_name = "".join(c if c.isalnum() else "_" for c in company)
    return f"QBO_{safe_name}_{user_id}"


def ensure_user_database(user_id: int) -> str:
    """
    Create DB for user if missing and insert into ct.UserDatabase (append only).
    """
    dbname = get_company_database(user_id)

    # --- Step 1: ensure user database exists ---
    master_conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('SQLSERVER_HOST')},{os.getenv('SQLSERVER_PORT')};"
        f"UID={os.getenv('SQLSERVER_USER')};"
        f"PWD={os.getenv('SQLSERVER_PASSWORD')};"
        f"DATABASE=master;"
    )
    with pyodbc.connect(master_conn_str, autocommit=True) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
            IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'{dbname}')
            BEGIN
                CREATE DATABASE [{dbname}];
            END
        """)

    # --- Step 2: ensure ct.UserDatabase table exists and insert mapping (no updates) ---
    admin_conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('CT_SQLSERVER_HOST')},{os.getenv('CT_SQLSERVER_PORT')};"
        f"UID={os.getenv('CT_SQLSERVER_USER')};"
        f"PWD={os.getenv('CT_SQLSERVER_PASSWORD')};"
        f"DATABASE={os.getenv('CT_SQLSERVER_DATABASE')};"
    )
    with pyodbc.connect(admin_conn_str, autocommit=True) as conn:
        cursor = conn.cursor()

        # Ensure schema exists
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'ct')
            BEGIN EXEC('CREATE SCHEMA ct'); END;
        """)

        # Ensure ct.UserDatabase table exists
        cursor.execute("""
            IF NOT EXISTS (
                SELECT * FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'ct' AND TABLE_NAME = 'UserDatabase'
            )
            BEGIN
                CREATE TABLE ct.UserDatabase (
                    UserId BIGINT NOT NULL,
                    CompanyName NVARCHAR(255) NOT NULL,
                    DatabaseName NVARCHAR(255) NOT NULL,
                    CreatedAt DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
                    CONSTRAINT PK_UserDatabase PRIMARY KEY (UserId, DatabaseName)
                );
            END
        """)

        # Insert only if not exists (append only, no updates)
        cursor.execute("""
            DECLARE @companyName NVARCHAR(255);
            SELECT @companyName = companyName FROM ct.Users WHERE Id = ?;

            IF NOT EXISTS (SELECT 1 FROM ct.UserDatabase WHERE UserId = ? AND DatabaseName = ?)
                INSERT INTO ct.UserDatabase (UserId, CompanyName, DatabaseName)
                VALUES (?, @companyName, ?);
        """, (user_id, user_id, dbname, user_id, dbname))

    return dbname


def get_user_connection(user_id: int):
    """
    Return pyodbc connection for the user’s DB (auto-create if missing).
    """
    dbname = ensure_user_database(user_id)
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('SQLSERVER_HOST')},{os.getenv('SQLSERVER_PORT')};"
        f"DATABASE={dbname};"
        f"UID={os.getenv('SQLSERVER_USER')};PWD={os.getenv('SQLSERVER_PASSWORD')}",
        autocommit=False
    )
