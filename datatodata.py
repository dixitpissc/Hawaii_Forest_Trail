import pandas as pd
import sqlalchemy
import pyodbc
import time
from sqlalchemy.pool import NullPool
from sqlalchemy import text

# ✅ Replace with actual values
SOURCE_CONFIG = {
    "server": "127.0.0.1",
    "port": 1433,  # ✅ Important: port as integer
    "database": "Hawaii_Forest_Trail_01102025",
    "username": "sa",
    "password": "Issc@123"
}

DEST_CONFIG = {
    "server": "127.0.0.1",
    "port": 1433,  # ✅ Important: port as integer
    "database": "Hawaii_Forest_Trail_Retail_IES1",
    "username": "sa",
    "password": "Issc@123"
}

# Performance optimization constants
BATCH_SIZE = 10000  # Process data in batches for large tables
FAST_EXECUTEMANY = True  # Enable fast executemany for better insert performance

def create_engine_with_port(config, include_database=True):
    """Create SQLAlchemy engine with optional database specification."""
    from urllib.parse import quote_plus
    
    # URL encode the password to handle special characters
    password_encoded = quote_plus(config['password'])
    database_name = config['database'] if include_database else 'master'
    
    # Create proper connection string for SQL Server with port
    conn_str = (
        f"mssql+pyodbc://{config['username']}:{password_encoded}@"
        f"{config['server']}:{config['port']}/{database_name}?"
        f"driver=ODBC+Driver+17+for+SQL+Server"
        f"&TrustServerCertificate=yes"
    )
    
    print("🔍 Connecting using:", conn_str.replace(password_encoded, '***'))  # Hide password in logs
    
    # Optimize engine settings for performance
    return sqlalchemy.create_engine(
        conn_str,
        poolclass=NullPool,  # Disable connection pooling for better memory management
        echo=False,  # Disable SQL logging for performance
        pool_pre_ping=True,  # Enable connection health checks
        connect_args={
            "timeout": 30,
            "autocommit": True,
            "fast_executemany": FAST_EXECUTEMANY,
            "check_same_thread": False
        }
    )

def test_raw_connection(config):
    """Test raw pyodbc connection for troubleshooting."""
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={config['server']},{config['port']};"
            f"DATABASE={config['database']};"
            f"UID={config['username']};"
            f"PWD={config['password']};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=30;"
        )
        
        print(f"🔍 Testing raw connection to {config['server']}:{config['port']}...")
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        print("✅ Raw pyodbc connection successful")
        return True
    except Exception as e:
        print(f"❌ Raw pyodbc connection failed: {str(e)}")
        return False

def get_table_list(engine):
    """Get list of tables sorted by size (smallest first) for better resource management."""
    query = """
    SELECT 
        s.name AS TABLE_SCHEMA,
        t.name AS TABLE_NAME,
        p.rows AS ROW_COUNT,
        CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS TABLE_SIZE_MB
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.object_id = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name NOT LIKE 'dt%'
        AND t.is_ms_shipped = 0
        AND i.object_id > 255
    GROUP BY s.name, t.name, p.rows
    ORDER BY ROW_COUNT ASC  -- Process smaller tables first
    """
    return pd.read_sql(query, engine)

def get_table_row_count(engine, schema, table):
    """Get accurate row count for a table."""
    try:
        count_query = f"SELECT COUNT(*) as row_count FROM [{schema}].[{table}]"
        result = pd.read_sql(count_query, engine)
        return result.iloc[0]['row_count']
    except Exception:
        return 0

def clean_dataframe_for_insert(df):
    """Clean DataFrame for better SQL Server compatibility."""
    df_cleaned = df.copy()
    
    for col in df_cleaned.columns:
        if df_cleaned[col].dtype == 'object':
            # Handle string columns that might be too long
            try:
                # Replace None with empty string for string columns
                df_cleaned[col] = df_cleaned[col].fillna('')
                
                # Truncate very long strings (SQL Server varchar limit considerations)
                max_length = 4000  # Conservative limit for varchar columns
                if df_cleaned[col].dtype == 'object':
                    df_cleaned[col] = df_cleaned[col].astype(str).apply(
                        lambda x: x[:max_length] if len(str(x)) > max_length else x
                    )
            except Exception:
                pass
        
        elif df_cleaned[col].dtype in ['float64', 'float32']:
            # Handle numeric columns with NaN values
            df_cleaned[col] = df_cleaned[col].fillna(0)
            
        elif df_cleaned[col].dtype in ['int64', 'int32']:
            # Handle integer columns with NaN values
            df_cleaned[col] = df_cleaned[col].fillna(0)
    
    return df_cleaned

def safe_bulk_insert(df, table_name, engine, schema, max_retries=3):
    """Perform bulk insert with fallback strategies for problematic data."""
    
    # Clean the dataframe first
    df_clean = clean_dataframe_for_insert(df)
    
    # Strategy 1: Try bulk insert with multi method
    for attempt in range(max_retries):
        try:
            df_clean.to_sql(
                table_name, 
                engine, 
                schema=schema, 
                if_exists='append', 
                index=False, 
                method='multi',
                chunksize=500  # Smaller chunks for better reliability
            )
            return True, f"Success on attempt {attempt + 1} with bulk insert"
            
        except Exception as e:
            if "COUNT field incorrect" in str(e) or "String data, length mismatch" in str(e):
                print(f"   ⚠️ Bulk insert attempt {attempt + 1} failed: {str(e)[:100]}...")
                if attempt < max_retries - 1:
                    # Reduce chunk size and try again
                    continue
            else:
                break
    
    # Strategy 2: Fall back to smaller chunks
    try:
        print("   🔄 Falling back to smaller chunks...")
        df_clean.to_sql(
            table_name, 
            engine, 
            schema=schema, 
            if_exists='append', 
            index=False, 
            method=None,  # Use default method
            chunksize=100
        )
        return True, "Success with small chunks fallback"
        
    except Exception as e:
        print(f"   ⚠️ Small chunks failed: {str(e)[:100]}...")
    
    # Strategy 3: Row-by-row insertion (last resort)
    try:
        print("   🐌 Falling back to row-by-row insertion...")
        successful_rows = 0
        
        for index, row in df_clean.iterrows():
            try:
                row_df = pd.DataFrame([row])
                row_df.to_sql(
                    table_name, 
                    engine, 
                    schema=schema, 
                    if_exists='append', 
                    index=False, 
                    method=None
                )
                successful_rows += 1
                
                # Progress indicator for large datasets
                if successful_rows % 50 == 0:
                    print(f"     📊 Inserted {successful_rows}/{len(df_clean)} rows...")
                    
            except Exception as row_error:
                print(f"     ❌ Failed to insert row {index}: {str(row_error)[:100]}...")
                continue
        
        if successful_rows > 0:
            return True, f"Partial success: {successful_rows}/{len(df_clean)} rows inserted"
        else:
            return False, "All row insertions failed"
            
    except Exception as e:
        return False, f"Row-by-row insertion failed: {str(e)}"

def copy_table_in_batches(source_engine, dest_engine, schema, table, total_rows):
    """Copy a table in batches for better memory management and performance."""
    if total_rows == 0:
        print(f"⚠️ Skipped {schema}.{table} (no rows)")
        return True
    
    print(f"� Copying: {schema}.{table} ({total_rows:,} rows)")
    start_time = time.time()
    
    try:
        # For small tables, copy all at once
        if total_rows <= BATCH_SIZE:
            query = f"SELECT * FROM [{schema}].[{table}]"
            df = pd.read_sql(query, source_engine)
            
            if not df.empty:
                # Optimize data types for better performance
                df = optimize_dataframe_dtypes(df)
                
                # Use safe bulk insert with fallback strategies
                success, message = safe_bulk_insert(df, table, dest_engine, schema)
                
                if success:
                    elapsed = time.time() - start_time
                    print(f"✅ Copied {len(df):,} rows to {schema}.{table} in {elapsed:.2f}s ({message})")
                    return True
                else:
                    print(f"❌ Failed to copy {schema}.{table}: {message}")
                    return False
            else:
                print(f"⚠️ No data found in {schema}.{table}")
                return True
        
        # For large tables, use batched copying
        else:
            print(f"� Processing in batches of {BATCH_SIZE:,} rows...")
            total_copied = 0
            
            for offset in range(0, total_rows, BATCH_SIZE):
                batch_start = time.time()
                
                # Use OFFSET/FETCH for SQL Server pagination
                query = f"""
                SELECT * FROM [{schema}].[{table}] 
                ORDER BY (SELECT NULL) 
                OFFSET {offset} ROWS 
                FETCH NEXT {BATCH_SIZE} ROWS ONLY
                """
                
                df_batch = pd.read_sql(query, source_engine)
                
                if not df_batch.empty:
                    # Optimize data types for better performance
                    df_batch = optimize_dataframe_dtypes(df_batch)
                    
                    # Use safe bulk insert with fallback strategies
                    success, message = safe_bulk_insert(df_batch, table, dest_engine, schema)
                    
                    if not success:
                        print(f"❌ Failed to insert batch at offset {offset}: {message}")
                        return False
                    
                    total_copied += len(df_batch)
                    batch_elapsed = time.time() - batch_start
                    progress = (total_copied / total_rows) * 100
                    
                    print(f"   📊 Batch {offset//BATCH_SIZE + 1}: {len(df_batch):,} rows in {batch_elapsed:.2f}s "
                          f"(Progress: {progress:.1f}%)")
            
            elapsed = time.time() - start_time
            print(f"✅ Copied {total_copied:,} rows to {schema}.{table} in {elapsed:.2f}s")
            return True
            
    except Exception as e:
        print(f"❌ Failed to copy {schema}.{table}: {str(e)}")
        return False

def optimize_dataframe_dtypes(df):
    """Optimize DataFrame data types for better memory usage and performance."""
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                # Try to convert to numeric if possible (without deprecated errors parameter)
                df[col] = pd.to_numeric(df[col], downcast='integer')
            except (ValueError, TypeError):
                # If conversion fails, keep original dtype
                pass
        elif df[col].dtype in ['int64', 'int32']:
            # Downcast integers to save memory
            try:
                df[col] = pd.to_numeric(df[col], downcast='integer')
            except (ValueError, TypeError):
                pass
        elif df[col].dtype in ['float64', 'float32']:
            # Downcast floats to save memory
            try:
                df[col] = pd.to_numeric(df[col], downcast='float')
            except (ValueError, TypeError):
                pass
    
    return df

def database_exists(engine, database_name):
    """Check if a database exists on the server."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) as db_count FROM sys.databases WHERE name = :db_name"
            ), {"db_name": database_name})
            return result.fetchone()[0] > 0
    except Exception as e:
        print(f"⚠️ Error checking database existence: {str(e)}")
        return False

def create_database(engine, database_name):
    """Create a new database on the server."""
    try:
        with engine.connect() as conn:
            # Use text() for raw SQL execution
            conn.execute(text(f"CREATE DATABASE [{database_name}]"))
            print(f"✅ Database '{database_name}' created successfully")
            return True
    except Exception as e:
        print(f"❌ Failed to create database '{database_name}': {str(e)}")
        return False

def get_source_schemas(source_engine):
    """Get list of unique schemas from source database tables."""
    try:
        query = """
        SELECT DISTINCT s.name AS schema_name
        FROM sys.tables t
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.is_ms_shipped = 0
        AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
        ORDER BY s.name
        """
        result = pd.read_sql(query, source_engine)
        return result['schema_name'].tolist()
    except Exception as e:
        print(f"❌ Failed to get source schemas: {str(e)}")
        return []

def schema_exists(engine, schema_name):
    """Check if a schema exists in the destination database."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT COUNT(*) as schema_count FROM sys.schemas WHERE name = :schema_name"
            ), {"schema_name": schema_name})
            return result.fetchone()[0] > 0
    except Exception as e:
        print(f"⚠️ Error checking schema existence for '{schema_name}': {str(e)}")
        return False

def create_schema(engine, schema_name):
    """Create a new schema in the destination database."""
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA [{schema_name}]"))
            print(f"✅ Schema '{schema_name}' created successfully")
            return True
    except Exception as e:
        print(f"❌ Failed to create schema '{schema_name}': {str(e)}")
        return False

def ensure_schemas_exist(source_engine, dest_engine):
    """Ensure all source schemas exist in destination database."""
    print("🔍 Checking and creating schemas...")
    
    # Get all unique schemas from source
    source_schemas = get_source_schemas(source_engine)
    
    if not source_schemas:
        print("⚠️ No schemas found in source database")
        return True
    
    print(f"📋 Found {len(source_schemas)} schemas in source: {', '.join(source_schemas)}")
    
    schemas_created = 0
    schemas_existed = 0
    
    for schema_name in source_schemas:
        if schema_exists(dest_engine, schema_name):
            print(f"✅ Schema '{schema_name}' already exists")
            schemas_existed += 1
        else:
            print(f"🏗️ Creating schema '{schema_name}'...")
            if create_schema(dest_engine, schema_name):
                schemas_created += 1
            else:
                print(f"❌ Failed to create schema '{schema_name}'. Migration may fail for tables in this schema.")
                return False
    
    print(f"📊 Schema summary: {schemas_existed} existed, {schemas_created} created")
    return True

def copy_all_tables():
    print("🔄 Connecting to databases...")
    start_total_time = time.time()
    
    # First, test raw source connection for troubleshooting
    print("🔍 Testing source database connection...")
    if not test_raw_connection(SOURCE_CONFIG):
        print("❌ Raw source connection failed. Please check:")
        print("  1. SQL Server is running and accessible")
        print("  2. Server address and port are correct")
        print("  3. Username and password are correct")
        print("  4. Database exists")
        print("  5. Network connectivity")
        return
    
    # Create SQLAlchemy engine for source
    source_engine = create_engine_with_port(SOURCE_CONFIG)
    try:
        with source_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Source database connection established successfully")
    except Exception as e:
        print(f"❌ SQLAlchemy source connection failed: {str(e)}")
        print("Raw connection works but SQLAlchemy failed. This might be a driver or URL format issue.")
        return

    # Check if destination database exists, create if it doesn't
    print("🔍 Checking destination database...")
    dest_server_engine = create_engine_with_port(DEST_CONFIG, include_database=False)
    
    try:
        database_name = DEST_CONFIG['database']
        
        if not database_exists(dest_server_engine, database_name):
            print(f"⚠️ Destination database '{database_name}' does not exist. Creating...")
            if not create_database(dest_server_engine, database_name):
                print("❌ Failed to create destination database. Aborting migration.")
                return
        else:
            print(f"✅ Destination database '{database_name}' already exists")
        
        dest_server_engine.dispose()  # Close server connection
        
        # Now connect to the specific database
        dest_engine = create_engine_with_port(DEST_CONFIG)
        
        # Test destination database connection
        with dest_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Destination database connection established successfully")
        
    except Exception as e:
        print(f"❌ Destination setup failed: {str(e)}")
        return

    # Step 1: Ensure all schemas exist in destination
    print("\n" + "="*50)
    print("STEP 1: SCHEMA VERIFICATION AND CREATION")
    print("="*50)
    
    if not ensure_schemas_exist(source_engine, dest_engine):
        print("❌ Schema setup failed. Aborting migration.")
        return
    
    print("✅ All required schemas are ready in destination database")
    
    # Step 2: Get tables and prepare for migration
    print("\n" + "="*50)
    print("STEP 2: TABLE MIGRATION PREPARATION")
    print("="*50)
    
    tables = get_table_list(source_engine)
    print(f"📋 Found {len(tables)} tables to copy.")
    
    # Display table summary
    total_rows = tables['ROW_COUNT'].sum() if 'ROW_COUNT' in tables.columns else 0
    total_size_mb = tables['TABLE_SIZE_MB'].sum() if 'TABLE_SIZE_MB' in tables.columns else 0
    print(f"📊 Total estimated rows: {total_rows:,}")
    print(f"💾 Total estimated size: {total_size_mb:.2f} MB")
    
    # Step 3: Start table migration
    print("\n" + "="*50)
    print("STEP 3: TABLE DATA MIGRATION")
    print("="*50)

    successful_copies = 0
    failed_copies = 0

    for _, row in tables.iterrows():
        schema = row['TABLE_SCHEMA']
        table = row['TABLE_NAME']
        row_count = row.get('ROW_COUNT', 0)
        
        # Get actual row count if estimate is not available
        if row_count is None or row_count == 0:
            row_count = get_table_row_count(source_engine, schema, table)
        
        success = copy_table_in_batches(source_engine, dest_engine, schema, table, row_count)
        
        if success:
            successful_copies += 1
        else:
            failed_copies += 1
        
        print()  # Add spacing between tables

    # Close connections explicitly
    source_engine.dispose()
    dest_engine.dispose()
    
    total_elapsed = time.time() - start_total_time
    print(f"✅ Database copy completed in {total_elapsed:.2f}s")
    print(f"📈 Summary: {successful_copies} successful, {failed_copies} failed")

if __name__ == "__main__":
    copy_all_tables()
