import pandas as pd
import sqlalchemy
import pyodbc

# ✅ Replace with actual values
SOURCE_CONFIG = {
    "server": "20.51.187.236",
    "port": 1433,  # ✅ Important: add correct port
    "database": "Gas_Express_LLC",
    "username": "sa",
    "password": "Password123!"
}

DEST_CONFIG = {
    "server": "127.0.0.1",
    "port": 1433,  # ✅ Important: add correct port
    "database": "Gas_Express_LLC",
    "username": "sa",
    "password": "Issc@123"
}

def create_engine_with_port(config):
    conn_str = (
        f"mssql+pyodbc://{config['username']}:{config['password']}@"
        f"{config['server']},{config['port']}/{config['database']}?"
        f"driver=ODBC+Driver+17+for+SQL+Server"
    )
    print("🔍 Connecting using:", conn_str)  # Debug line
    return sqlalchemy.create_engine(conn_str)


def get_table_list(engine):
    query = """
    SELECT TABLE_SCHEMA, TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE='BASE TABLE'
    """
    return pd.read_sql(query, engine)

def copy_all_tables():
    print("🔄 Connecting to databases...")
    source_engine = create_engine_with_port(SOURCE_CONFIG)
    dest_engine = create_engine_with_port(DEST_CONFIG)

    tables = get_table_list(source_engine)

    print(f"📋 Found {len(tables)} tables to copy.")

    for _, row in tables.iterrows():
        schema = row['TABLE_SCHEMA']
        table = row['TABLE_NAME']
        full_table_name = f"{schema}.{table}"

        print(f"🚛 Copying: {full_table_name}")
        try:
            df = pd.read_sql(f"SELECT * FROM [{schema}].[{table}]", source_engine)
            if not df.empty:
                df.to_sql(table, dest_engine, schema=schema, if_exists='append', index=False, method='multi')
                print(f"✅ Copied {len(df)} rows to {full_table_name}")
            else:
                print(f"⚠️ Skipped {full_table_name} (no rows)")
        except Exception as e:
            print(f"❌ Failed to copy {full_table_name}: {str(e)}")

    print("✅ Database copy completed.")

if __name__ == "__main__":
    copy_all_tables()
