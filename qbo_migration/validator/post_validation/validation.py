

import os
import pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql

load_dotenv()

SOURCE_DB = os.getenv("SOURCE_DATABASE")
TRANSPORTED_DB = "qbo_transported"
VALIDATION_SCHEMA = "QBO_validation"
MAPPING_SCHEMA = "porter_entities_mapping"
FIXED_SCHEMA = "dbo"

def table_exists(database: str, table: str, schema: str = "dbo") -> bool:
    query = f"""
        SELECT 1 FROM [{database}].INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    """
    try:
        return sql.fetch_scalar(query, params=(schema, table)) is not None
    except:
        return False

def sanitize_floats(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if pd.api.types.is_float_dtype(df[col]):
            df[col] = (
                df[col]
                .apply(lambda x: None if pd.isna(x) or x in [float("inf"), float("-inf")] else round(x, 6))
            )
    return df

def compare_source_and_target(table: str):
    # Step 1: Load source records
    if not table_exists(SOURCE_DB, table, FIXED_SCHEMA):
        print(f"⚠️ Source table {table} not found in {SOURCE_DB}.dbo")
        return
    df_source = sql.fetch_dataframe(f"SELECT * FROM [{SOURCE_DB}].[dbo].[{table}]")
    if df_source.empty:
        print(f"⚠️ Source table {table} is empty.")
        return

    # Step 2: Load mapping
    map_table = f"Map_{table}"
    if not table_exists(SOURCE_DB, map_table, MAPPING_SCHEMA):
        print(f"⚠️ Mapping table {map_table} not found.")
        return
    df_map = sql.fetch_dataframe(f"SELECT Source_Id, Target_Id FROM [{SOURCE_DB}].[{MAPPING_SCHEMA}].[{map_table}]")

    # Step 3: Merge mapping into source
    df_merged = df_source.merge(df_map, how="left", left_on="Id", right_on="Source_Id")

    if "Target_Id" not in df_merged.columns or df_merged["Target_Id"].isna().all():
        print(f"⚠️ No Target_Id found for table {table}. Skipping.")
        return

    # Step 4: Fetch transported (target) data
    target_ids = df_merged["Target_Id"].dropna().astype(str).unique().tolist()
    if not table_exists(TRANSPORTED_DB, table, FIXED_SCHEMA):
        print(f"⚠️ Target table {table} not found in {TRANSPORTED_DB}.dbo")
        return

    id_list = ",".join(f"'{x}'" for x in target_ids)
    df_target = sql.fetch_dataframe(f"SELECT * FROM [{TRANSPORTED_DB}].[dbo].[{table}] WHERE Id IN ({id_list})")
    if df_target.empty:
        print(f"⚠️ No target records found in {TRANSPORTED_DB}.dbo.{table}")
        return

    df_target = df_target.rename(columns=lambda x: f"{x}_Target")
    df_combined = df_merged.merge(df_target, how="left", left_on="Target_Id", right_on="Id_Target")

    # Step 5: Compare columns
    source_cols = [col for col in df_source.columns if col != "Id"]
    target_cols = [f"{col}_Target" for col in source_cols]
    compare_cols = [col for col in source_cols if f"{col}_Target" in df_combined.columns]

    results = []
    for _, row in df_combined.iterrows():
        result = {"Source_Id": row.get("Id"), "Target_Id": row.get("Target_Id")}
        for col in compare_cols:
            val_s = row.get(col)
            val_t = row.get(f"{col}_Target")
            result[f"{col}_Source"] = val_s
            result[f"{col}_Target"] = val_t
            if pd.isna(val_s) and pd.isna(val_t):
                result[f"{col}_Status"] = "✅ Match (null)"
            elif str(val_s).strip() == str(val_t).strip():
                result[f"{col}_Status"] = "✅ Match"
            else:
                result[f"{col}_Status"] = "❌ Mismatch"
        results.append(result)

    # df_result = pd.DataFrame(results)
    # df_result = sanitize_floats(df_result)
    # sql.ensure_schema_exists(VALIDATION_SCHEMA)
    # sql.insert_dataframe(df_result, table=f"{table}_Validation", schema=VALIDATION_SCHEMA, if_exists="replace")
    # print(f"✅ Compared {table}. Results stored in {SOURCE_DB}.{VALIDATION_SCHEMA}.{table}_Validation")

    df_result = pd.DataFrame(results)

    if table in ["Item", "Invoice"]:
        df_result = sanitize_floats(df_result)

        # Clean up NaN, inf, -inf to None (null)
        df_result = df_result.where(
            ~df_result.applymap(lambda x: isinstance(x, float) and (pd.isna(x) or x in [float("inf"), float("-inf")])),
            other=None
        )

        # Also drop NaNs from object columns explicitly (SQL Server doesn’t handle NaN strings well)
        df_result = df_result.where(pd.notna(df_result), None)

        sql.ensure_schema_exists(VALIDATION_SCHEMA)
        sql.insert_dataframe(
            df_result,
            table=f"{table}_Validation",
            schema=VALIDATION_SCHEMA,
            if_exists="replace"
        )
    else:
        sql.ensure_schema_exists(VALIDATION_SCHEMA)
        sql.insert_dataframe(
            df_result,
            table=f"{table}_Validation",
            schema=VALIDATION_SCHEMA,
            if_exists="replace"
        )

    sql.ensure_schema_exists(VALIDATION_SCHEMA)

    if table in ["Item", "Invoice"]:
        # Row-by-row insertion with error tracing
        for idx, row in df_result.iterrows():
            try:
                single_row_df = pd.DataFrame([row])
                # Apply strict float sanitization
                for col in single_row_df.columns:
                    val = row[col]
                    if isinstance(val, (int, float, str)):
                        try:
                            val_clean = float(val)
                            val_clean = None if val_clean in [float("inf"), float("-inf")] else round(val_clean, 6)
                            single_row_df[col] = [val_clean]
                        except:
                            continue
                sql.insert_dataframe(
                    single_row_df,
                    table=f"{table}_Validation",
                    schema=VALIDATION_SCHEMA,
                    if_exists="append"
                )
            except Exception as row_err:
                print(f"❌ Row {idx} failed for table {table}: {row_err}")
                print(f"🧨 Problematic Row:\n{row.to_dict()}")
                break  # Optional: remove if you want to try inserting the rest
    else:
        sql.insert_dataframe(df_result, table=f"{table}_Validation", schema=VALIDATION_SCHEMA, if_exists="replace")

    print(f"✅ Compared {table}. Results stored in {SOURCE_DB}.{VALIDATION_SCHEMA}.{table}_Validation")


def main():
    table_list = [
        "Account", 
        "Term", "PaymentMethod", "Currency", "CompanyCurrency",
        "Department", "Class", "TaxAgency", "TaxRate", "TaxCode",
        "Customer", "CustomerType", "Vendor", "Employee", "Item", "CustomFieldDefinition",
        "JournalCode", "Invoice", "Payment", "CreditMemo", "SalesReceipt", "RefundReceipt",
        "Estimate", "Bill", "BillPayment", "VendorCredit", "PurchaseOrder", "Purchase",
        "Deposit", "JournalEntry", "Transfer", "InventoryAdjustment", "ReimburseCharge",
        "Preferences", "Attachable", "Budget"
    ]
    for table in table_list:
        try:
            print(f"\n🚀 Processing {table}...")
            compare_source_and_target(table)
        except Exception as e:
            print(f"❌ Failed processing {table}: {e}")

if __name__ == "__main__":
    main()
