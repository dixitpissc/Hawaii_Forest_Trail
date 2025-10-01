import os
import pandas as pd
import pyodbc
from dotenv import load_dotenv

def generate_dbo_pivot_report():
    load_dotenv()
    SERVER = os.getenv("SQLSERVER_HOST")
    DATABASE = os.getenv("SQLSERVER_DATABASE")
    USERNAME = os.getenv("SQLSERVER_USER")
    PASSWORD = os.getenv("SQLSERVER_PASSWORD")
    PORT = os.getenv("SQLSERVER_PORT", "1433")

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER},{PORT};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
    )
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE'
    """)
    tables = [row.TABLE_NAME for row in cursor.fetchall()]

    report = []
    for tbl in tables:
        try:
            df = pd.read_sql(f"SELECT * FROM dbo.[{tbl}]", conn)
            report.append({
                "Table Name": tbl,
                "Column Count": len(df.columns),
                "Row Count": len(df),
                "Empty Values": int(df.isnull().sum().sum()),
                "Unique Records": len(df.drop_duplicates())
            })
        except Exception as e:
            report.append({
                "Table Name": tbl,
                "Column Count": "Error",
                "Row Count": "Error",
                "Empty Values": "Error",
                "Unique Records": f"Error: {str(e)}"
            })

    report_df = pd.DataFrame(report)

    numeric_cols = ["Column Count", "Row Count", "Empty Values", "Unique Records"]
    for col in numeric_cols:
        report_df[col] = pd.to_numeric(report_df[col], errors='coerce')

    total_row = {
        "Table Name": "TOTAL",
        "Column Count": report_df["Column Count"].sum(),
        "Row Count": report_df["Row Count"].sum(),
        "Empty Values": report_df["Empty Values"].sum(),
        "Unique Records": report_df["Unique Records"].sum()
    }
    report_df = pd.concat([report_df, pd.DataFrame([total_row])], ignore_index=True)

    print(report_df.to_markdown(index=False))




def summary():
    df_report = generate_dbo_pivot_report()
    print(df_report)
    # optionally: report_df.to_csv("dbo_summary_report.csv", index=False)
