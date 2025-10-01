import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv

def generate_migration_status_report():
    """
    Generates a migration validation report comparing record counts and amounts
    between the source DBO schema and the porter_entities_mapping schema.
    Only records in Map_* tables with non-null Target_Id are counted.
    The final report is saved to [Migration_status].[Validation_Table].
    """

    # Load env variables
    load_dotenv()
    SERVER = os.getenv("SQLSERVER_HOST")
    DATABASE = os.getenv("SQLSERVER_DATABASE")
    USER = os.getenv("SQLSERVER_USER")
    PASSWORD = os.getenv("SQLSERVER_PASSWORD")
    PORT = os.getenv("SQLSERVER_PORT", "1433")
    SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
    MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
    TARGET_SCHEMA = "Migration_status"
    TARGET_TABLE = "Validation_Table"

    # DB connection
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER},{PORT};"
        f"DATABASE={DATABASE};"
        f"UID={USER};"
        f"PWD={PASSWORD}"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    ENTITIES = [
        ("PaymentMethod", "Master"), ("Class", "Master"), ("Currency", "Master"),
        ("Term", "Master"), ("Department", "Master"), ("Account", "Master"),
        ("Customer", "Master"), ("Vendor", "Master"), ("Employee", "Master"),
        ("Item", "Master"), ("ItemCategory", "Master"),
        ("Invoice", "Transaction"), ("Payment", "Transaction"), ("CreditMemo", "Transaction"),
        ("SalesReceipt", "Transaction"), ("RefundReceipt", "Transaction"), ("Estimate", "Transaction"),
        ("Bill", "Transaction"), ("BillPayment", "Transaction"), ("VendorCredit", "Transaction"),
        ("PurchaseOrder", "Transaction"), ("Purchase", "Transaction"), ("Deposit", "Transaction"),
        ("JournalEntry", "Transaction"), ("Transfer", "Transaction"), ("InventoryAdjustment", "Transaction"),
    ]

    TRANSACTION_ENTITIES_WITH_AMOUNT = {
        "Invoice", "Payment", "CreditMemo", "SalesReceipt", "RefundReceipt",
        "Estimate", "Bill", "BillPayment", "VendorCredit", "PurchaseOrder",
        "Purchase", "Deposit", "JournalEntry", "Transfer", "InventoryAdjustment"
    }

    def get_count_and_amount(schema, table, entity_name, is_mapping=False):
        """
        Returns (count, raw_amount_float, formatted_amount_string).
        If table doesn't exist, returns 0 and "NA".
        """
        amount_column = "TotalAmt" if entity_name in TRANSACTION_ENTITIES_WITH_AMOUNT else None
        count = 0
        raw_amount = None
        formatted_amount = "NA"

        # Count
        try:
            query = f"SELECT COUNT(*) FROM [{schema}].[{table}]"
            if is_mapping:
                query += " WHERE Target_Id IS NOT NULL"
            cursor.execute(query)
            count = cursor.fetchone()[0]
        except Exception as e:
            print(f"⚠️ Failed to count rows in {schema}.{table}: {e}")
            return 0, None, "NA"

        # Amount
        if amount_column:
            try:
                query = f"SELECT SUM(TRY_CAST([{amount_column}] AS FLOAT)) FROM [{schema}].[{table}]"
                if is_mapping:
                    query += " WHERE Target_Id IS NOT NULL"
                cursor.execute(query)
                result = cursor.fetchone()
                raw_amount = float(result[0]) if result and result[0] is not None else None
                formatted_amount = f"{raw_amount:,.4f}" if raw_amount is not None else "NA"
            except Exception as e:
                print(f"⚠️ Failed to sum {amount_column} in {schema}.{table}: {e}")
                raw_amount = None
                formatted_amount = "NA"

        return count, raw_amount, formatted_amount

    report_rows = []

    for idx, (entity, entity_type) in enumerate(ENTITIES, start=1):
        source_table = entity
        porter_table = f"Map_{entity}"

        source_count, source_amount_raw, source_amount_fmt = get_count_and_amount(SOURCE_SCHEMA, source_table, entity)
        porter_count, porter_amount_raw, porter_amount_fmt = get_count_and_amount(MAPPING_SCHEMA, porter_table, entity, is_mapping=True)

        if source_amount_raw is not None and porter_amount_raw is not None:
            amount_diff = f"{source_amount_raw - porter_amount_raw:,.4f}"
        else:
            amount_diff = "NA"

        report_rows.append({
            "#": idx,
            "Entity": entity.replace("Method", " Method"),
            "Entity type": entity_type,
            "Records (DBO schema)": source_count,
            "Records (Porter schema)": porter_count,
            "Records (Difference)": source_count - porter_count,
            "Amount (DBO schema)": source_amount_fmt,
            "Amount (Porter schema)": porter_amount_fmt,
            "Amount (Difference)": amount_diff
        })

    df_report = pd.DataFrame(report_rows)

    # Create target schema if not exists
    cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{TARGET_SCHEMA}') EXEC('CREATE SCHEMA {TARGET_SCHEMA}')")
    conn.commit()

    # Recreate target table
    cursor.execute(f"""
    IF OBJECT_ID('{TARGET_SCHEMA}.{TARGET_TABLE}', 'U') IS NOT NULL
        DROP TABLE {TARGET_SCHEMA}.{TARGET_TABLE};

    CREATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE} (
        [#] INT,
        [Entity] NVARCHAR(100),
        [Entity type] NVARCHAR(50),
        [Records (DBO schema)] INT,
        [Records (Porter schema)] INT,
        [Records (Difference)] INT,
        [Amount (DBO schema)] NVARCHAR(50),
        [Amount (Porter schema)] NVARCHAR(50),
        [Amount (Difference)] NVARCHAR(50)
    )
    """)
    conn.commit()

    # Insert results
    for _, row in df_report.iterrows():
        cursor.execute(f"""
            INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, tuple(row))
    conn.commit()

    print(f"✅ Migration validation report stored in [{TARGET_SCHEMA}].[{TARGET_TABLE}]")

    cursor.close()
    conn.close()
