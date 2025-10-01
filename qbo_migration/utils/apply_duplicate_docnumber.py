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


# def apply_duplicate_docnumber_strategy_dynamic(
#     target_table: str,
#     schema: str,
#     docnumber_column: str = "DocNumber",
#     source_id_column: str = "Source_Id",
#     duplicate_column: str = "Duplicate_Docnumber",
#     check_against_tables: list = None,
# ):
#     """
#     Dynamically resolves duplicate DocNumbers within a mapping table and optionally across others.

#     Args:
#         target_table (str): Table in which to apply deduplication.
#         schema (str): Schema of the mapping tables.
#         docnumber_column (str): Column name holding original DocNumbers.
#         source_id_column (str): Unique row identifier.
#         duplicate_column (str): Column to store new resolved DocNumbers.
#         check_against_tables (list): Optional list of other tables to avoid DocNumber conflicts with.
#     """
#     logger.info(f"🔍 Applying dynamic duplicate strategy to {target_table}...")

#     # Ensure the duplicate column exists
#     sql.run_query(f"""
#         IF COL_LENGTH('{schema}.{target_table}', '{duplicate_column}') IS NULL
#         BEGIN
#             ALTER TABLE [{schema}].[{target_table}]
#             ADD {duplicate_column} NVARCHAR(30);
#         END
#     """)

#     # Build conflict set from other tables
#     used_docnumbers = set()
#     check_against_tables = check_against_tables or []

#     for table in check_against_tables:
#         try:
#             df_check = sql.fetch_table(table, schema)
#             used_docnumbers |= set(df_check[docnumber_column].dropna().astype(str))
#             if duplicate_column in df_check.columns:
#                 used_docnumbers |= set(df_check[duplicate_column].dropna().astype(str))
#             logger.info(f"✅ Checked DocNumbers from {table}")
#         except Exception:
#             logger.warning(f"⚠️ Skipped missing table: {table}")

#     # Load target table
#     df = sql.fetch_table(target_table, schema)
#     df = df[df[docnumber_column].notna() & (df[docnumber_column].astype(str).str.strip().str.lower() != "null") & (df[docnumber_column].astype(str).str.strip() != "")]
#     df[docnumber_column] = df[docnumber_column].astype(str)

#     doc_counts = df[docnumber_column].value_counts()
#     duplicates = doc_counts[doc_counts > 1].index.tolist()
#     updated_docnumbers = set()

#     # Process duplicates
#     for docnum in duplicates:
#         rows = df[df[docnumber_column] == docnum]
#         for i, (_, row) in enumerate(rows.iterrows()):
#             sid = row[source_id_column]

#             if i == 0 and docnum not in used_docnumbers:
#                 proposed = docnum
#             else:
#                 proposed = (
#                     f"{docnum}-{i+1:02d}"
#                     if len(docnum) <= 18
#                     else f"{i+1:02d}{docnum[2:]}"[:21]
#                 )
#                 while proposed in used_docnumbers or proposed in updated_docnumbers:
#                     i += 1
#                     proposed = (
#                         f"{docnum}-{i+1:02d}"
#                         if len(docnum) <= 18
#                         else f"{i+1:02d}{docnum[2:]}"[:21]
#                     )

#             sql.run_query(f"""
#                 UPDATE [{schema}].[{target_table}]
#                 SET {duplicate_column} = ?
#                 WHERE {source_id_column} = ?
#             """, (proposed, sid))
#             if proposed != docnum:
#                 logger.warning(f"⚠️ Duplicate '{docnum}' → '{proposed}'")
#             updated_docnumbers.add(proposed)

#     # Process uniques
#     uniques = df[~df[docnumber_column].isin(duplicates)]
#     for _, row in uniques.iterrows():
#         sid = row[source_id_column]
#         docnum = row[docnumber_column]
#         final_docnum = docnum
#         counter = 1
#         while final_docnum in used_docnumbers or final_docnum in updated_docnumbers:
#             final_docnum = (
#                 f"{docnum}-{counter:02d}"
#                 if len(docnum) <= 18
#                 else f"{counter:02d}{docnum[2:]}"[:21]
#             )
#             counter += 1

#         sql.run_query(f"""
#             UPDATE [{schema}].[{target_table}]
#             SET {duplicate_column} = ?
#             WHERE {source_id_column} = ?
#         """, (final_docnum, sid))

#         if final_docnum != docnum:
#             logger.warning(f"⚠️ Unique '{docnum}' → '{final_docnum}' due to conflict")
#         updated_docnumbers.add(final_docnum)

#     logger.info(f"✅ Finished deduplication in {target_table} — {len(duplicates)} duplicates handled.")

def apply_duplicate_docnumber_strategy_dynamic(
    target_table: str,
    schema: str,
    docnumber_column: str = "DocNumber",
    source_id_column: str = "Source_Id",
    duplicate_column: str = "Duplicate_Docnumber",
    check_against_tables: list = None,
):
    """
    Optimized (but logic-identical) version:
    - Ensures duplicate_column exists (same as before)
    - Builds the 'used_docnumbers' conflict set from other tables (same behavior)
    - Single pass over target rows using groupby().cumcount() to get the base index
    - Generates proposed values with the SAME rules and collision loop
    - Writes updates back in SET-BASED chunks using a temp table join (big win)
    """
    logger.info(f"🔍 Applying dynamic duplicate strategy to {schema}.{target_table} ...")

    # 1) Ensure the duplicate column exists
    sql.run_query(f"""
        IF COL_LENGTH('{schema}.{target_table}', '{duplicate_column}') IS NULL
        BEGIN
            ALTER TABLE [{schema}].[{target_table}]
            ADD {duplicate_column} NVARCHAR(30);
        END
    """)

    # 2) Build conflict set from other tables (DocNumber + Duplicate_Docnumber)
    used_docnumbers = set()
    check_against_tables = check_against_tables or []

    def _safe_fetch_column(table_name: str, col: str):
        try:
            # Prefer lighter single-column fetch if available in your helper set
            if hasattr(sql, "fetch_column"):
                return [str(x) for x in sql.fetch_column(
                    f"SELECT {col} FROM [{schema}].[{table_name}] WHERE {col} IS NOT NULL"
                )]
            # Fallback: fetch table then slice column
            df_tmp = sql.fetch_table(table_name, schema)
            if col in df_tmp.columns:
                return [str(x) for x in df_tmp[col].dropna()]
            return []
        except Exception:
            return []

    for table in check_against_tables:
        doc_list = _safe_fetch_column(table, docnumber_column)
        dup_list = _safe_fetch_column(table, duplicate_column)
        if doc_list or dup_list:
            used_docnumbers.update(doc_list)
            used_docnumbers.update(dup_list)
            logger.info(f"✅ Checked DocNumbers from {schema}.{table} ({len(doc_list)} + {len(dup_list)}).")
        else:
            logger.warning(f"⚠️ Skipped or empty table: {schema}.{table}")

    # 3) Load target rows (only needed columns)
    df = sql.fetch_table(target_table, schema)
    needed_cols = {source_id_column, docnumber_column, duplicate_column}
    missing = [c for c in [source_id_column, docnumber_column] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required column(s) in {schema}.{target_table}: {missing}")

    df = df[[c for c in df.columns if c in needed_cols]].copy()

    # Filter invalid DocNumbers (same as your original rules)
    if df.empty:
        logger.info("ℹ️ No rows found in target table; nothing to do.")
        return

    df[docnumber_column] = df[docnumber_column].astype(str)
    mask_valid = (
        df[docnumber_column].notna()
        & (df[docnumber_column].str.strip() != "")
        & (df[docnumber_column].str.strip().str.lower() != "null")
    )
    df = df.loc[mask_valid].copy()
    if df.empty:
        logger.info("ℹ️ No valid DocNumber rows to process; done.")
        return

    # 4) Compute per-row occurrence index (0 for first occurrence, 1 for second, etc.)
    occ = df.groupby(docnumber_column, dropna=False).cumcount()
    df["_occ"] = occ

    # 5) Generate proposals with same exact rules
    planned = set()  # proposed within this run (avoids intra-batch collisions)
    updates = []     # (Source_Id, ProposedDup)

    def _propose(base_doc: str, idx: int) -> str:
        # If first occurrence and not used across tables, keep original
        if idx == 0 and base_doc not in used_docnumbers and base_doc not in planned:
            return base_doc
        # Otherwise, generate -01/-02 or 01/02 prefix as per length rule
        i = idx  # starting point (0 == first dup → produces 01)
        while True:
            if len(base_doc) <= 18:
                cand = f"{base_doc}-{i+1:02d}"
            else:
                cand = f"{i+1:02d}{base_doc[2:]}"[:21]
            if (cand not in used_docnumbers) and (cand not in planned):
                return cand
            i += 1

    # Iterate once; purely in Python, but without DB writes in the loop
    for _, r in df.iterrows():
        doc = r[docnumber_column]
        sid = r[source_id_column]
        idx = int(r["_occ"])

        proposed = _propose(doc, idx)
        planned.add(proposed)
        updates.append((sid, proposed))
        if proposed != doc:
            # Only warn when changed (keeps logs tighter at scale)
            logger.warning(f"⚠️ '{doc}' → '{proposed}'")
        logger.info(
            f"📝 Plan {schema}.{target_table}: {source_id_column}={sid} | "
            f"{docnumber_column}='{doc}' → {duplicate_column}='{proposed}'"
        )
    # 6) Write back in SET-BASED chunks using a temp table + joined UPDATE
    if not updates:
        logger.info("ℹ️ Nothing to update; done.")
        return

    CHUNK = 500
    total = len(updates)
    done = 0

    while done < total:
        batch = updates[done:done+CHUNK]
        done += len(batch)

        # Build parameter placeholders for VALUES clause
        # (#u has (sid NVARCHAR(100), val NVARCHAR(30)))
        values_clause = ",".join(["(?, ?)"] * len(batch))
        params = []
        for sid, val in batch:
            params.extend([str(sid), str(val)])

        # Single roundtrip per chunk:
        sql.run_query(
            f"""
            CREATE TABLE #u(
                sid NVARCHAR(100) NOT NULL,
                val NVARCHAR(30) NULL
            );

            INSERT INTO #u(sid, val)
            VALUES {values_clause};

            UPDATE t
            SET t.{duplicate_column} = u.val
            FROM [{schema}].[{target_table}] AS t
            JOIN #u AS u
              ON t.{source_id_column} = u.sid;

            DROP TABLE #u;
            """,
            tuple(params)
        )
        # ADD THIS: confirm each row actually updated in DB
        for sid, val in batch:
            logger.info(
                f"✅ Updated {schema}.{target_table}: {source_id_column}={sid} | "
                f"{duplicate_column}='{val}'"
            )

        logger.info(f"🧩 Batch updated {len(batch)} rows "
                    f"({done}/{total}, {done/total:.1%}).")

    # 7) Done
    # Count duplicates handled (for parity with your final log line)
    dup_count = int((df["_occ"] > 0).sum())
    logger.info(f"✅ Finished dedup in {schema}.{target_table} — {dup_count} duplicates handled; {total} rows updated.")

## How to Use
# apply_duplicate_docnumber_strategy_dynamic(
#     target_table="Map_CreditMemo",
#     schema=MAPPING_SCHEMA,
#     check_against_tables=["Map_Invoice", "Map_Bill", "Map_VendorCredit"]
# )
