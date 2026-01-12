# utils/retry_handler.py

import pandas as pd
from storage.sqlserver import sql

def initialize_mapping_table(df_source, mapping_table, mapping_schema, id_column="Id"):
    """
    Creates the initial mapping table if it doesn't exist.
    Adds columns: Source_Id, Target_Id, Porter_Status, Failure_Reason, Retry_Count
    """
    if not sql.table_exists(mapping_table, mapping_schema):
        df_mapping = df_source.copy()
        df_mapping.rename(columns={id_column: "Source_Id"}, inplace=True)
        df_mapping["Target_Id"] = None
        df_mapping["Porter_Status"] = None
        df_mapping["Failure_Reason"] = None
        df_mapping["Retry_Count"] = 0
        sql.insert_dataframe(df_mapping, mapping_table, mapping_schema)
        print(f"✅ Inserted {len(df_mapping)} rows into {mapping_schema}.{mapping_table}")


def get_retryable_subset(source_table, source_schema, mapping_table, mapping_schema, max_retries=3):
    """
    Fetch failed records from mapping table that haven't exceeded max retry count
    """
    df_mapping = sql.fetch_table(mapping_table, mapping_schema)
    df_mapping["Retry_Count"] = pd.to_numeric(df_mapping["Retry_Count"], errors="coerce").fillna(0).astype(int)

    failed_ids = df_mapping[
        (df_mapping["Porter_Status"] == "Failed") & (df_mapping["Retry_Count"] < max_retries)
    ]["Source_Id"].tolist()

    if not failed_ids:
        print("✅ No records to retry.")
        return pd.DataFrame(), df_mapping

    df_all = sql.fetch_table(source_table, source_schema)
    df_remigrate = df_all[df_all["Id"].isin(failed_ids)]
    return df_remigrate, df_mapping
