from utils.log_timer import global_logger as logger
from dotenv import load_dotenv
import os
from storage.sqlserver import sql
load_dotenv()

MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")

def apply_duplicate_docnumber_strategy(table_name: str, schema: str = MAPPING_SCHEMA):
    """
    Generic function to resolve duplicate DocNumbers in any mapping table.
    
    - First occurrence keeps original DocNumber.
    - Subsequent ones get -01, -02, etc. (if length <=18), or prefixed like 01xxxx (if length >18).
    - Results are stored in Duplicate_Docnumber column.

    Args:
        table_name (str): Name of the mapping table (e.g., Map_Invoice, Map_Bill).
        schema (str): Schema name (default: MAPPING_SCHEMA).
    """
    logger.info(f"🔍 Detecting duplicate DocNumbers in {schema}.{table_name} and applying uniqueness logic...")

    # Ensure the column exists
    sql.run_query(f"""
        IF COL_LENGTH('{schema}.{table_name}', 'Duplicate_Docnumber') IS NULL
        BEGIN
            ALTER TABLE [{schema}].[{table_name}]
            ADD Duplicate_Docnumber NVARCHAR(30);
        END
    """)
    logger.info(f"📝 Ensured 'Duplicate_Docnumber' column exists in {schema}.{table_name}.")

    df = sql.fetch_table(table_name, schema)
    if df.empty or "DocNumber" not in df.columns or "Source_Id" not in df.columns:
        logger.warning(f"⚠️ Table {schema}.{table_name} missing DocNumber or Source_Id columns.")
        return

    doc_counts = df["DocNumber"].value_counts()
    duplicates = doc_counts[doc_counts > 1].index.tolist()

    for docnum in duplicates:
        rows = df[df["DocNumber"] == docnum]
        rows = rows.reset_index(drop=True)

        for i, (_, row) in enumerate(rows.iterrows()):
            sid = row["Source_Id"]

            if i == 0:
                new_docnum = docnum
            else:
                if len(str(docnum)) <= 18:
                    new_docnum = f"{docnum}-{i:02d}"
                else:
                    new_docnum = f"{i:02d}{str(docnum)[2:]}"  # drop first 2 chars

            sql.run_query(f"""
                UPDATE [{schema}].[{table_name}]
                SET Duplicate_Docnumber = ?
                WHERE Source_Id = ?
            """, (new_docnum, sid))
            logger.warning(f"⚠️ Duplicate DocNumber '{docnum}' updated to '{new_docnum}' in {table_name}")

    # Handle non-duplicates
    uniques = df[~df["DocNumber"].isin(duplicates)]
    for _, row in uniques.iterrows():
        sid = row["Source_Id"]
        docnum = row["DocNumber"]
        sql.run_query(f"""
            UPDATE [{schema}].[{table_name}]
            SET Duplicate_Docnumber = ?
            WHERE Source_Id = ?
        """, (docnum, sid))

    logger.info(f"✅ Applied Duplicate_Docnumber strategy in {table_name} to {len(duplicates)} duplicated DocNumbers.")


# Use case
# apply_duplicate_docnumber_strategy("Map_Invoice")
# apply_duplicate_docnumber_strategy("Map_Bill")
# apply_duplicate_docnumber_strategy("Map_Purchase")


def apply_duplicate_docnumber_strategy_dynamic(
    target_table: str,
    schema: str,
    docnumber_column: str = "DocNumber",
    source_id_column: str = "Source_Id",
    duplicate_column: str = "Duplicate_Docnumber",
    check_against_tables: list = None,
):
    """
    Dynamically resolves duplicate DocNumbers within a mapping table and optionally across others.

    Args:
        target_table (str): Table in which to apply deduplication.
        schema (str): Schema of the mapping tables.
        docnumber_column (str): Column name holding original DocNumbers.
        source_id_column (str): Unique row identifier.
        duplicate_column (str): Column to store new resolved DocNumbers.
        check_against_tables (list): Optional list of other tables to avoid DocNumber conflicts with.
    """
    logger.info(f"🔍 Applying dynamic duplicate strategy to {target_table}...")

    # Ensure the duplicate column exists
    sql.run_query(f"""
        IF COL_LENGTH('{schema}.{target_table}', '{duplicate_column}') IS NULL
        BEGIN
            ALTER TABLE [{schema}].[{target_table}]
            ADD {duplicate_column} NVARCHAR(30);
        END
    """)

    # Build conflict set from other tables
    used_docnumbers = set()
    check_against_tables = check_against_tables or []

    for table in check_against_tables:
        try:
            df_check = sql.fetch_table(table, schema)
            used_docnumbers |= set(df_check[docnumber_column].dropna().astype(str))
            if duplicate_column in df_check.columns:
                used_docnumbers |= set(df_check[duplicate_column].dropna().astype(str))
            logger.info(f"✅ Checked DocNumbers from {table}")
        except Exception:
            logger.warning(f"⚠️ Skipped missing table: {table}")

    # Load target table
    df = sql.fetch_table(target_table, schema)
    df = df[df[docnumber_column].notna() & (df[docnumber_column].astype(str).str.strip().str.lower() != "null") & (df[docnumber_column].astype(str).str.strip() != "")]
    df[docnumber_column] = df[docnumber_column].astype(str)

    doc_counts = df[docnumber_column].value_counts()
    duplicates = doc_counts[doc_counts > 1].index.tolist()
    updated_docnumbers = set()

    # Process duplicates
    for docnum in duplicates:
        rows = df[df[docnumber_column] == docnum]
        for i, (_, row) in enumerate(rows.iterrows()):
            sid = row[source_id_column]

            if i == 0 and docnum not in used_docnumbers:
                proposed = docnum
            else:
                proposed = (
                    f"{docnum}-{i+1:02d}"
                    if len(docnum) <= 18
                    else f"{i+1:02d}{docnum[2:]}"[:21]
                )
                while proposed in used_docnumbers or proposed in updated_docnumbers:
                    i += 1
                    proposed = (
                        f"{docnum}-{i+1:02d}"
                        if len(docnum) <= 18
                        else f"{i+1:02d}{docnum[2:]}"[:21]
                    )

            sql.run_query(f"""
                UPDATE [{schema}].[{target_table}]
                SET {duplicate_column} = ?
                WHERE {source_id_column} = ?
            """, (proposed, sid))
            if proposed != docnum:
                logger.warning(f"⚠️ Duplicate '{docnum}' → '{proposed}'")
            updated_docnumbers.add(proposed)

    # Process uniques
    uniques = df[~df[docnumber_column].isin(duplicates)]
    for _, row in uniques.iterrows():
        sid = row[source_id_column]
        docnum = row[docnumber_column]
        final_docnum = docnum
        counter = 1
        while final_docnum in used_docnumbers or final_docnum in updated_docnumbers:
            final_docnum = (
                f"{docnum}-{counter:02d}"
                if len(docnum) <= 18
                else f"{counter:02d}{docnum[2:]}"[:21]
            )
            counter += 1

        sql.run_query(f"""
            UPDATE [{schema}].[{target_table}]
            SET {duplicate_column} = ?
            WHERE {source_id_column} = ?
        """, (final_docnum, sid))

        if final_docnum != docnum:
            logger.warning(f"⚠️ Unique '{docnum}' → '{final_docnum}' due to conflict")
        updated_docnumbers.add(final_docnum)

    logger.info(f"✅ Finished deduplication in {target_table} — {len(duplicates)} duplicates handled.")

## How to Use
# apply_duplicate_docnumber_strategy_dynamic(
#     target_table="Map_CreditMemo",
#     schema=MAPPING_SCHEMA,
#     check_against_tables=["Map_Invoice", "Map_Bill", "Map_VendorCredit"]
# )
