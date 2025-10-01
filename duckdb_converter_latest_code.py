import os
import duckdb
import pandas as pd
import pyodbc
import warnings

# Suppress the specific warning
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

# Define your databases here
databases = [
"Hawaii_Forest_Trail_01102025"
]


def mssql_to_duckdb(server, database, username, password, output_file, log_file):
    """Connects to SQL Server, extracts data, and converts it to a DuckDB file."""
    conn_str = (
        f'DRIVER={{SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password}'
    )
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    tables = cursor.fetchall()
    
    if os.path.exists(output_file):
        os.remove(output_file)
    
    con = duckdb.connect(output_file)
    
    with open(log_file, "w") as log:
        for schema, table in tables:
            log.write(f"Processing table: {schema}.{table}\n")
            print(f"Processing table: {schema}.{table}")
            df = pd.read_sql(f"SELECT * FROM [{schema}].[{table}]", conn)
            con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')  # Preserve schema casing
            con.execute(f'CREATE TABLE "{schema}"."{table}" AS SELECT * FROM df')
    
    con.close()
    conn.close()
    print(f"DuckDB file created: {output_file}")
    return output_file

if __name__ == "__main__":
    # Configuration
    SERVER_NAME = "127.0.0.1"
    PORT = "1433"
    USERNAME = "sa"
    PASSWORD = "Issc@123"  

    # Server on VM
    # SERVER_NAME = "20.51.187.236"
    # PORT = "1433"
    # USERNAME = "sa"
    # PASSWORD = "Password123!"  


    for database in databases:
        DATABASE_NAME = str(database)
        OUTPUT_FILE = f"{database}.duckdb"
        LOG_FILE = f"{database}.txt"
        mssql_to_duckdb(SERVER_NAME, DATABASE_NAME, USERNAME, PASSWORD, OUTPUT_FILE, LOG_FILE)
