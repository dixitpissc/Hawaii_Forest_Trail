"""
Sequence : 15
Module: vendorcredit_migrator.py
Author: Dixit Prajapati
Created: 2025-09-15
Description: Handles migration of VendorCredit records from source system to QBO.
Production : Ready
Development : Require when necessary
Phase : 02 - Multi User + Tax
"""

from utils.token_refresher import get_qbo_context_migration
from dotenv import load_dotenv
from storage.sqlserver import sql
from config.mapping.vendorcredit_mappping import VENDORCREDIT_HEADER_MAPPING, VENDORCREDIT_LINE_MAPPING
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import global_logger as logger
from utils.mapping_updater import update_mapping_status
from storage.sqlserver import sql
from utils.apply_duplicate_docnumber import apply_duplicate_docnumber_strategy_dynamic
import os, json, requests, pandas as pd
from utils.payload_cleaner import deep_clean

try:
    import orjson
    def dumps_fast(obj):
        return orjson.dumps(obj, option=orjson.OPT_INDENT_2).decode("utf-8")
except Exception:
    def dumps_fast(obj):
        return json.dumps(obj, indent=2, ensure_ascii=False)
    
load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")

# Optional flag to apply duplicate DocNumber strategy
ENABLE_VENDORCREDIT_DOCNUMBER_DEDUP = True

def column_exists(table: str, schema: str, column: str) -> bool:
    q = """
    SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?
    """
    conn = sql.get_sqlserver_connection()
    try:
        cur = conn.cursor()
        cur.execute(q, (schema, table, column))
        return cur.fetchone() is not None
    finally:
        conn.close()

def executemany(query: str, param_list):
    if not param_list:
        return
    conn = sql.get_sqlserver_connection()
    try:
        cur = conn.cursor()
        try:
            cur.fast_executemany = True
        except Exception:
            pass
        cur.executemany(query, param_list)
        conn.commit()
    finally:
        conn.close()

def get_qbo_auth():
    ctx = get_qbo_context_migration()
    base = ctx["BASE_URL"]
    realm = ctx["REALM_ID"]
    return f"{base}/v3/company/{realm}/vendorcredit", {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

# ---------------------------- TAX DETAIL HELPERS ----------------------------

def _coerce_float(val):
    """Safely convert to float, return None for invalid/missing values."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None

def _coerce_bool(val):
    """Safely convert to bool, return None for invalid/missing values."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ('true', '1', 'yes', 'on')
    try:
        return bool(int(val))
    except (ValueError, TypeError):
        return bool(val)

def _value_str(val):
    """Convert to string if not None/NaN, otherwise return None."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s else None

def add_txn_tax_detail_from_row(payload: dict, row) -> None:
    """
    Builds payload['TxnTaxDetail'] using header + up to 4 TaxLine components from Map_VendorCredit columns.
    Columns used (examples):
      - Mapped_TxnTaxCodeRef (preferred) or TxnTaxDetail_TxnTaxCodeRef_value
      - TxnTaxDetail_TotalTax
      - TxnTaxDetail_TaxLine_{i}__Amount
      - TxnTaxDetail_TaxLine_{i}__DetailType
      - TxnTaxDetail_TaxLine_{i}__TaxLineDetail_TaxRateRef_value
      - TxnTaxDetail_TaxLine_{i}__TaxLineDetail_PercentBased
      - TxnTaxDetail_TaxLine_{i}__TaxLineDetail_TaxPercent
      - TxnTaxDetail_TaxLine_{i}__TaxLineDetail_NetAmountTaxable
    """
    tax_detail = {}
    # Header-level tax code (prefer mapped)
    code = row.get("Mapped_TxnTaxCodeRef") or row.get("TxnTaxDetail_TxnTaxCodeRef_value")
    code_str = _value_str(code)
    if code_str:
        tax_detail["TxnTaxCodeRef"] = {"value": code_str}

    # Optional header total tax
    total_tax = _coerce_float(row.get("TxnTaxDetail_TotalTax"))
    if total_tax is not None:
        tax_detail["TotalTax"] = total_tax

    # Tax lines (0..3 based on your schema)
    tax_lines = []
    for i in range(4):
        amt = _coerce_float(row.get(f"TxnTaxDetail_TaxLine_{i}__Amount"))
        dtyp = _value_str(row.get(f"TxnTaxDetail_TaxLine_{i}__DetailType"))
        # If neither amount nor detail type exists, skip this slot
        if amt is None and not dtyp:
            continue

        line = {}
        # QBO expects "DetailType": "TaxLineDetail" for tax lines; use source if provided, else default correctly
        line["DetailType"] = dtyp if dtyp else "TaxLineDetail"
        if amt is not None:
            line["Amount"] = amt

        # Build inner TaxLineDetail
        inner = {}
        net_taxable = _coerce_float(row.get(f"TxnTaxDetail_TaxLine_{i}__TaxLineDetail_NetAmountTaxable"))
        if net_taxable is not None:
            inner["NetAmountTaxable"] = net_taxable

        tax_percent = _coerce_float(row.get(f"TxnTaxDetail_TaxLine_{i}__TaxLineDetail_TaxPercent"))
        if tax_percent is not None:
            inner["TaxPercent"] = tax_percent

        rate_ref = row.get("Mapped_TxnTaxRateRef")
        if rate_ref:
            inner["TaxRateRef"] = {"value": rate_ref}

        percent_based = _coerce_bool(row.get(f"TxnTaxDetail_TaxLine_{i}__TaxLineDetail_PercentBased"))
        if percent_based is not None:
            inner["PercentBased"] = percent_based

        if inner:
            line["TaxLineDetail"] = inner

        # Only append if we have a meaningful line (Amount or a non-empty inner)
        if ("Amount" in line) or ("TaxLineDetail" in line):
            tax_lines.append(line)

    if tax_lines:
        tax_detail["TaxLine"] = tax_lines

    # Attach only if we built anything (header code, total, or at least one line)
    if tax_detail:
        payload["TxnTaxDetail"] = tax_detail

def ensure_mapping_table(VENDORCREDIT_DATE_FROM='1900-01-01',VENDORCREDIT_DATE_TO='2080-12-31'):
    """
    Ensures that the vendor credit records from the source table are prepared and inserted
    into the mapping table [Map_VendorCredit] with all necessary mapped values.
    - Loads source data from [dbo].[VendorCredit].
    - Applies mappings for vendor, account, class, currency, term, etc.
    - Adds any missing columns to the mapping table dynamically.
    - Resolves line-level mappings like ClassRef if needed.
    - Inserts the cleaned and enriched data into [porter_entities_mapping].[Map_VendorCredit].
    """
    # Robust mapping logic (vectorized, diagnostics, preloaded)
    df = sql.fetch_table(table="VendorCredit", schema=SOURCE_SCHEMA)
    if df.empty:
        logger.warning("❌ No vendor credit records found.")
        return


    # ✅ Apply TxnDate filter if provided
    if "TxnDate" in df.columns:
        if VENDORCREDIT_DATE_FROM and VENDORCREDIT_DATE_TO:
            df = df[(df["TxnDate"] >= VENDORCREDIT_DATE_FROM) & (df["TxnDate"] <= VENDORCREDIT_DATE_TO)]

    df["Source_Id"] = df["Id"]
    df["Target_Id"] = None
    df["Porter_Status"] = "Ready"
    df["Retry_Count"] = 0
    df["Failure_Reason"] = None
    df["Payload_JSON"] = None

    # Ensure all header columns exist
    for col in VENDORCREDIT_HEADER_MAPPING.values():
        if col not in df.columns:
            df[col] = None

    # Preload all mapping tables into dicts for vectorized mapping
    def preload_map(table):
        if sql.table_exists(table, MAPPING_SCHEMA):
            mdf = sql.fetch_dataframe(f"SELECT CAST(Source_Id AS NVARCHAR(255)) AS Source_Id, CAST(Target_Id AS NVARCHAR(255)) AS Target_Id FROM [{MAPPING_SCHEMA}].[{table}] WITH (NOLOCK)")
            if not mdf.empty:
                return dict(zip(mdf["Source_Id"], mdf["Target_Id"]))
        return {}

    maps = {name: preload_map(f"Map_{name}") for name in ["Vendor", "Account", "Department", "Currency", "Term", "PaymentMethod", "Item", "Class"]}

    # NEW: TaxCode mapping (optional but preferred)
    if sql.table_exists("Map_TaxCode", MAPPING_SCHEMA):
        taxcode_map = sql.fetch_table_with_params(
            f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_TaxCode]", tuple()
        )
        taxcode_dict = dict(zip(taxcode_map["Source_Id"], taxcode_map["Target_Id"]))
    else:
        taxcode_dict = {}

    # NEW: Taxrate mapping (optional but preferred)
    if sql.table_exists("Map_TaxRate", MAPPING_SCHEMA):
        taxrate_map = sql.fetch_table_with_params(
            f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_TaxRate]", tuple()
        )
        taxrate_dict = dict(zip(taxrate_map["Source_Id"], taxrate_map["Target_Id"]))
    else:
        taxrate_dict = {}

    # Vectorized header mapping
    header_map = {
        "Mapped_Vendor": ("VendorRef.value", "Vendor"),
        "Mapped_Account": ("APAccountRef.value", "Account"),
        "Mapped_Department": ("DepartmentRef.value", "Department"),
        "Mapped_Currency": ("CurrencyRef.value", "Currency"),
        "Mapped_Term": ("SalesTermRef.value", "Term"),
        "Mapped_PaymentMethod": ("PaymentMethodRef.value", "PaymentMethod")
    }
    for target_col, (src_col, map_key) in header_map.items():
        if src_col in df.columns:
            if target_col == "Mapped_Currency":
                # Use mapped value if available, else fallback to source code
                mapped = df[src_col].astype(str).map(maps[map_key])
                df[target_col] = mapped.where(mapped.notnull(), df[src_col])
            else:
                df[target_col] = df[src_col].astype(str).map(maps[map_key])
        else:
            df[target_col] = None

    # NEW: Map header-level TxnTaxCodeRef (from source header column)
    # Source column name provided: [TxnTaxDetail.TxnTaxCodeRef.value]
    source_tax_col = "TxnTaxDetail.TxnTaxCodeRef.value"
    if source_tax_col in df.columns:
        def _map_taxcode(src):
            if pd.isna(src):
                return None
            tgt = taxcode_dict.get(src)
            if not tgt:
                # If mapping is missing, leave None (QBO will default/allow per-tenant settings)
                logger.debug(f"🔎 Missing TaxCode mapping for Source_Id={src}")
            return tgt
        df["Mapped_TxnTaxCodeRef"] = df[source_tax_col].map(_map_taxcode)
    else:
        df["Mapped_TxnTaxCodeRef"] = None
    
    #######################

    source_tax_col = "TxnTaxDetail.TaxLine[0].TaxLineDetail.TaxRateRef.value"
    if source_tax_col in df.columns:
        def _map_taxrate(src):
            if pd.isna(src):
                return None
            tgt = taxrate_dict.get(src)
            if not tgt:
                # If mapping is missing, leave None (QBO will default/allow per-tenant settings)
                logger.debug(f"🔎 Missing TaxRate mapping for Source_Id={src}")
            return tgt
        df["Mapped_TxnTaxRateRef"] = df[source_tax_col].map(_map_taxrate)
    else:
        df["Mapped_TxnTaxRateRef"] = None

    # Line-level mapped fields (populate for each VendorCredit)
    line_refs = {
        "Mapped_Line_AccountRefs": "Mapped_AccountRef",
        "Mapped_Line_ClassRefs": "Mapped_ClassRef",
        "Mapped_Line_DepartmentRefs": "Mapped_DepartmentRef",
        "Mapped_Line_ItemRefs": "Mapped_ItemRef"
    }
    # Prepare line mapping tables
    line_map_tables = {
        "Mapped_AccountRef": ("Map_Account", "AccountBasedExpenseLineDetail.AccountRef.value"),
        "Mapped_ItemRef": ("Map_Item", "ItemBasedExpenseLineDetail.ItemRef.value"),
        "Mapped_ClassRef": ("Map_Class", "AccountBasedExpenseLineDetail.ClassRef.value"),
        "Mapped_DepartmentRef": ("Map_Department", "AccountBasedExpenseLineDetail.DepartmentRef.value")
    }
    # Fetch all lines for all VendorCredits
    all_lines = sql.fetch_dataframe(f"SELECT * FROM [{SOURCE_SCHEMA}].[VendorCredit_Line] WITH (NOLOCK)")
    # Pre-map line references
    for ref_col, (map_table, src_col) in line_map_tables.items():
        if src_col in all_lines.columns and sql.table_exists(map_table, MAPPING_SCHEMA):
            ref_map = maps[map_table.replace("Map_", "")]
            all_lines[ref_col] = all_lines[src_col].astype(str).map(ref_map)
        else:
            all_lines[ref_col] = None
    # Group and aggregate line refs for each VendorCredit
    for ref_field, line_col in line_refs.items():
        agg = all_lines.groupby("Parent_Id")[line_col].apply(lambda x: ",".join(x.dropna().astype(str).unique()) if not x.empty else None)
        df[ref_field] = df["Source_Id"].map(agg)

    # Diagnostics: log missing mappings
    for col in ["Mapped_Vendor", "Mapped_Account"]:
        missing = df[col].isnull().sum()
        if missing:
            logger.warning(f"⚠️ {missing} records missing {col} mapping.")

    # Table creation/insert
    if sql.table_exists("Map_VendorCredit", MAPPING_SCHEMA):
        logger.warning("⚠️ Skipping DELETE to preserve Duplicate_DocNumber — use truncate only when initializing.")
    else:
        logger.info("ℹ️ Creating Map_VendorCredit table for the first time.")
    sql.insert_dataframe(df, "Map_VendorCredit", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df)} rows into {MAPPING_SCHEMA}.Map_VendorCredit")

def get_lines(parent_id):
    """
    Retrieves and enriches the line-level data for a given VendorCredit record.
    Args:
        parent_id (str or int): The source Id of the VendorCredit (header) record.
    Returns:
        pd.DataFrame: Line-level records with mapped AccountRef, ClassRef, and ItemRef values.
    """
    lines = sql.fetch_table_with_params(
        f"SELECT * FROM [{SOURCE_SCHEMA}].[VendorCredit_Line] WHERE Parent_Id = ?", (parent_id,)
    )

    if lines.empty:
        return lines

    # Map AccountRef, ItemRef, ClassRef
    mapping_cols = {
        "Mapped_AccountRef": ("Map_Account", "AccountBasedExpenseLineDetail.AccountRef.value"),
        "Mapped_ItemRef": ("Map_Item", "ItemBasedExpenseLineDetail.ItemRef.value"),
        "Mapped_ClassRef": ("Map_Class", "AccountBasedExpenseLineDetail.ClassRef.value")
    }

    for target_col, (map_table, source_col) in mapping_cols.items():
        if source_col not in lines.columns:
            lines[target_col] = None
            continue

        if not sql.table_exists(map_table, MAPPING_SCHEMA):
            logger.warning(f"⚠️ Skipping {target_col} — table {MAPPING_SCHEMA}.{map_table} not found.")
            lines[target_col] = None
            continue

        lines[target_col] = lines[source_col].map(
            lambda x: sql.fetch_single_value(
                f"SELECT Target_Id FROM [{MAPPING_SCHEMA}].[{map_table}] WHERE Source_Id = ?", (x,)
            ) if pd.notna(x) else None
        )

    return lines

# ---------------------------- DOCNUMBER DEDUPLICATION ----------------------------
def apply_duplicate_docnumber_strategy_for_vendorcredit():
    """
    Deduplicates DocNumbers in Map_VendorCredit:
    - Uses dynamic utility to check against Invoice, CreditMemo, Bill.
    """
    apply_duplicate_docnumber_strategy_dynamic(
        target_table="Map_VendorCredit",
        schema=MAPPING_SCHEMA,
        check_against_tables=[ "Map_Estimate","Map_Bill","Map_Purchase"] #,"Map_VendorCredit","Map_Invoice", "Map_CreditMemo",]
    )

def build_payload(row, lines):
    """
    Constructs the VendorCredit JSON payload to be submitted to QBO.
    Args:
        row (pd.Series): A single row from the Map_VendorCredit table (header).
        lines (pd.DataFrame): Associated line items for the given VendorCredit.
    Returns:
        dict: JSON payload formatted according to QBO VendorCredit API schema.
    """
    # Highly optimized: vectorized line processing, fast serialization
    doc_number = row["Duplicate_DocNumber"] if "Duplicate_DocNumber" in row and pd.notna(row["Duplicate_DocNumber"]) else row.get(VENDORCREDIT_HEADER_MAPPING["DocNumber"])
    payload = {
        "TxnDate": row.get(VENDORCREDIT_HEADER_MAPPING["TxnDate"]),
        "DocNumber": doc_number,
        "PrivateNote": row.get(VENDORCREDIT_HEADER_MAPPING["PrivateNote"]),
        "CurrencyRef": {"value": str(row["Mapped_Currency"])} if "Mapped_Currency" in row and pd.notna(row["Mapped_Currency"]) else None,
        "VendorRef": {"value": str(row["Mapped_Vendor"])} if "Mapped_Vendor" in row and pd.notna(row["Mapped_Vendor"]) else None,
        "APAccountRef": {"value": str(row["Mapped_Account"])} if "Mapped_Account" in row and pd.notna(row["Mapped_Account"]) else None,
        "DepartmentRef": {"value": str(row["Mapped_Department"])} if "Mapped_Department" in row and pd.notna(row["Mapped_Department"]) else None
    }
    payload = {k: v for k, v in payload.items() if v is not None}

    # Vectorized line processing
    if not lines.empty:
        # Precompute all line fields as arrays for speed
        detail_types = lines["DetailType"].values
        amounts = lines["Amount"].values
        descriptions = lines["Description"].values if "Description" in lines.columns else [None]*len(lines)
        linenums = lines["LineNum"].values if "LineNum" in lines.columns else [None]*len(lines)
        mapped_accountrefs = lines["Mapped_AccountRef"].values if "Mapped_AccountRef" in lines.columns else [None]*len(lines)
        mapped_classrefs = lines["Mapped_ClassRef"].values if "Mapped_ClassRef" in lines.columns else [None]*len(lines)
        mapped_departmentrefs = lines["Mapped_DepartmentRef"].values if "Mapped_DepartmentRef" in lines.columns else [None]*len(lines)
        mapped_itemrefs = lines["Mapped_ItemRef"].values if "Mapped_ItemRef" in lines.columns else [None]*len(lines)
        taxcoderefs = lines["AccountBasedExpenseLineDetail.TaxCodeRef.value"].values if "AccountBasedExpenseLineDetail.TaxCodeRef.value" in lines.columns else ["NON"]*len(lines)
        billablestatuses = lines["AccountBasedExpenseLineDetail.BillableStatus"].values if "AccountBasedExpenseLineDetail.BillableStatus" in lines.columns else [None]*len(lines)
        qtys = lines["ItemBasedExpenseLineDetail.Qty"].values if "ItemBasedExpenseLineDetail.Qty" in lines.columns else [0]*len(lines)
        unitprices = lines["ItemBasedExpenseLineDetail.UnitPrice"].values if "ItemBasedExpenseLineDetail.UnitPrice" in lines.columns else [0]*len(lines)

        payload_lines = []
        for i in range(len(lines)):
            detail_type = detail_types[i]
            amount = amounts[i]
            if not detail_type or pd.isna(amount):
                continue
            line_obj = {
                "Amount": float(amount),
                "DetailType": detail_type,
                "Description": descriptions[i],
                "LineNum": int(linenums[i]) if pd.notna(linenums[i]) else None
            }
            detail = {}
            if detail_type == "AccountBasedExpenseLineDetail":
                detail = {
                    "AccountRef": {"value": str(mapped_accountrefs[i])} if pd.notna(mapped_accountrefs[i]) else None,
                    "ClassRef": {"value": str(mapped_classrefs[i])} if pd.notna(mapped_classrefs[i]) else None,
                    "DepartmentRef": {"value": str(mapped_departmentrefs[i])} if pd.notna(mapped_departmentrefs[i]) else None,
                    "TaxCodeRef": {"value": taxcoderefs[i] or "NON"},
                    "BillableStatus": billablestatuses[i]
                }
            elif detail_type == "ItemBasedExpenseLineDetail":
                detail = {
                    "ItemRef": {"value": str(mapped_itemrefs[i])} if pd.notna(mapped_itemrefs[i]) else None,
                    "ClassRef": {"value": str(mapped_classrefs[i])} if pd.notna(mapped_classrefs[i]) else None,
                    "DepartmentRef": {"value": str(mapped_departmentrefs[i])} if pd.notna(mapped_departmentrefs[i]) else None,
                    "TaxCodeRef": {"value": taxcoderefs[i] or "NON"},
                    "Qty": float(qtys[i] or 0),
                    "UnitPrice": float(unitprices[i] or 0)
                }
            detail = {k: v for k, v in detail.items() if v is not None}
            if detail:
                line_obj[detail_type] = detail
                payload_lines.append(line_obj)
        if payload_lines:
            payload["Line"] = payload_lines

    # NEW: Add tax detail if available
    add_txn_tax_detail_from_row(payload, row)

    return deep_clean(payload)

def generate_payloads(batch_size=500):
    """
    Generates QBO-compatible JSON payloads for vendor credit records in 'Ready' status.
    - Loads records from Map_VendorCredit where Payload_JSON is null or empty.
    - Builds the payload using header and line-level data.
    - Updates the Payload_JSON column in the mapping table.
    - Marks failed records with appropriate failure reasons.
    Args:
        batch_size (int): Number of records to process in each batch. Default is 500.
    """
    logger.info("🛠️ Generating payloads for vendor credits (batched)...")
    while True:
        df = sql.fetch_table_with_params(
            f"""SELECT TOP {batch_size} * FROM [{MAPPING_SCHEMA}].[Map_VendorCredit]
                WHERE Porter_Status = 'Ready' AND (Payload_JSON IS NULL OR Payload_JSON = '')""", ()
        )
        if df.empty:
            break

        update_params_list = []
        for _, row in df.iterrows():
            sid = row["Source_Id"]
            duplicate_doc = sql.fetch_single_value(
                f"SELECT Duplicate_DocNumber FROM [{MAPPING_SCHEMA}].[Map_VendorCredit] WHERE Source_Id = ?", (sid,)
            )
            if duplicate_doc:
                row["Duplicate_DocNumber"] = duplicate_doc

            missing_fields = [field for field in ["Mapped_Vendor", "Mapped_Account", "Mapped_Currency"] if not row.get(field)]
            if missing_fields:
                update_params_list.append([
                    None,
                    f"Missing required mapping(s): {', '.join(missing_fields)}",
                    None, None, None, None,
                    sid
                ])
                continue

            lines = get_lines(sid)
            if lines.empty:
                update_params_list.append([
                    None,
                    "No lines found",
                    None, None, None, None,
                    sid
                ])
                continue

            payload = build_payload(row, lines)
            if not payload.get("Line"):
                update_params_list.append([
                    None,
                    "No valid line details",
                    None, None, None, None,
                    sid
                ])
                continue

            mapped_lines_data = {
                "Mapped_Line_AccountRefs": ",".join(
                    lines["Mapped_AccountRef"].dropna().astype(str).unique()
                ) if "Mapped_AccountRef" in lines.columns else None,
                "Mapped_Line_ClassRefs": ",".join(
                    lines["Mapped_ClassRef"].dropna().astype(str).unique()
                ) if "Mapped_ClassRef" in lines.columns else None,
                "Mapped_Line_DepartmentRefs": ",".join(
                    lines["Mapped_DepartmentRef"].dropna().astype(str).unique()
                ) if "Mapped_DepartmentRef" in lines.columns else None,
                "Mapped_Line_ItemRefs": ",".join(
                    lines["Mapped_ItemRef"].dropna().astype(str).unique()
                ) if "Mapped_ItemRef" in lines.columns else None
            }

            update_params_list.append([
                dumps_fast(payload),
                None,
                mapped_lines_data["Mapped_Line_AccountRefs"],
                mapped_lines_data["Mapped_Line_ClassRefs"],
                mapped_lines_data["Mapped_Line_DepartmentRefs"],
                mapped_lines_data["Mapped_Line_ItemRefs"],
                sid
            ])

        # Batched update for speed
        update_sql = f"""
            UPDATE [{MAPPING_SCHEMA}].[Map_VendorCredit]
            SET Payload_JSON = ?,
                Failure_Reason = ?,
                Mapped_Line_AccountRefs = ?,
                Mapped_Line_ClassRefs = ?,
                Mapped_Line_DepartmentRefs = ?,
                Mapped_Line_ItemRefs = ?
            WHERE Source_Id = ?
        """
        if update_params_list:
            conn = sql.get_sqlserver_connection()
            try:
                cur = conn.cursor()
                try:
                    cur.fast_executemany = True
                except Exception:
                    pass
                cur.executemany(update_sql, update_params_list)
                conn.commit()
            finally:
                conn.close()
        logger.info(f"✅ Processed {len(df)} records in this batch (batched update).")

session = requests.Session()

# ---------- shared helpers (idempotent) ----------
# Fast JSON loader (uses orjson if available)
if "_fast_loads" not in globals():
    try:
        import orjson as _orjson
        def _fast_loads(s):
            if isinstance(s, dict):
                return s
            if isinstance(s, (bytes, bytearray)):
                return _orjson.loads(s)
            if isinstance(s, str):
                return _orjson.loads(s.encode("utf-8"))
            return s
    except Exception:
        import json as _json
        def _fast_loads(s):
            if isinstance(s, dict):
                return s
            if isinstance(s, (bytes, bytearray)):
                return _json.loads(s.decode("utf-8"))
            if isinstance(s, str):
                return _json.loads(s)
            return s

# Turn an entity URL into the /batch endpoint URL for that entity
if "_derive_batch_url" not in globals():
    def _derive_batch_url(entity_url: str, entity_name: str) -> str:
        """
        Example:
          entity_url: https://.../v3/company/<realm>/VendorCredit?minorversion=65
          entity_name: "VendorCredit"
          -> https://.../v3/company/<realm>/batch?minorversion=65
        """
        qpos = entity_url.find("?")
        query = entity_url[qpos:] if qpos != -1 else ""
        path = entity_url[:qpos] if qpos != -1 else entity_url

        seg = f"/{entity_name}".lower()
        lower = path.lower()
        if lower.endswith(seg):
            path = path[: -len(seg)]
        return f"{path}/batch{query}"

# Ensure a shared HTTP session exists (safe no-op if already defined)
try:
    session
except NameError:
    import requests
    session = requests.Session()

def _post_batch_vendorcredits(eligible_batch, url, headers, timeout=40, post_batch_limit=20, max_manual_retries=1):
    """
    High-throughput single-request batch posting for VendorCredits (no multithreading).
    - Pre-decodes Payload_JSON once per row (uses _fast_loads if available).
    - Uses QBO /batch endpoint to create multiple records per HTTP request.
    - Refreshes token once on 401/403; marks granular per-item results.
    Returns:
        successes: list[(qid, sid)]
        failures:  list[(reason, sid)]
    """
    successes, failures = [], []

    if eligible_batch is None or eligible_batch.empty:
        return successes, failures

    # Build worklist (filter + fast JSON loads)
    work = []
    for _, r in eligible_batch.iterrows():
        sid = r["Source_Id"]
        if r.get("Porter_Status") == "Success":
            continue
        if int(r.get("Retry_Count") or 0) >= 5:
            continue
        pj = r.get("Payload_JSON")
        if not pj:
            failures.append(("Missing Payload_JSON", sid))
            continue
        try:
            payload = _fast_loads(pj)  # falls back to json.loads if helper not present
        except Exception as e:
            failures.append((f"Bad JSON: {e}", sid))
            continue
        work.append((sid, payload))

    if not work:
        return successes, failures

    # Derive batch endpoint from entity URL
    batch_url = _derive_batch_url(url, "VendorCredit")

    # Process in chunks
    idx = 0
    while idx < len(work):
        auto_refresh_token_if_needed()
        chunk = work[idx:idx + post_batch_limit]
        idx += post_batch_limit
        if not chunk:
            continue

        def _do_post(_url, _headers):
            body = {
                "BatchItemRequest": [
                    {"bId": str(sid), "operation": "create", "VendorCredit": payload}
                    for (sid, payload) in chunk
                ]
            }
            return session.post(batch_url, headers=_headers, json=body, timeout=timeout)

        # Attempt with optional single token refresh on 401/403
        attempted_refresh = False
        for attempt in range(max_manual_retries + 1):
            try:
                resp = _do_post(batch_url, headers)
                sc = resp.status_code
                if sc == 200:
                    rj = resp.json()
                    items = rj.get("BatchItemResponse", []) or []
                    seen = set()

                    for item in items:
                        bid = item.get("bId")
                        seen.add(bid)
                        vc = item.get("VendorCredit")
                        if vc and "Id" in vc:
                            qid = vc["Id"]
                            successes.append((qid, bid))
                            logger.info(f"✅ VendorCredit {bid} → QBO {qid}")
                            continue

                        fault = item.get("Fault") or {}
                        errs = fault.get("Error") or []
                        if errs:
                            msg = errs[0].get("Message") or ""
                            det = errs[0].get("Detail") or ""
                            reason = (msg + " | " + det).strip()[:1000]
                        else:
                            reason = "Unknown batch failure"
                        failures.append((reason, bid))
                        logger.error(f"❌ VendorCredit {bid} failed: {reason}")

                    # Any missing responses → mark failed
                    for sid, _ in chunk:
                        if str(sid) not in seen:
                            failures.append(("No response for bId", sid))
                            logger.error(f"❌ No response for VendorCredit {sid}")
                    break

                elif sc in (401, 403) and not attempted_refresh:
                    logger.warning(f"🔐 {sc} on VendorCredit batch ({len(chunk)} items); refreshing token and retrying once...")
                    auto_refresh_token_if_needed()
                    url, headers = get_qbo_auth()
                    batch_url = _derive_batch_url(url, "VendorCredit")
                    attempted_refresh = True
                    continue

                else:
                    reason = (resp.text or f"HTTP {sc}")[:1000]
                    for sid, _ in chunk:
                        failures.append((reason, sid))
                    logger.error(f"❌ VendorCredit batch failed ({len(chunk)} items): {reason}")
                    break

            except Exception as e:
                reason = f"Batch exception: {e}"
                for sid, _ in chunk:
                    failures.append((reason, sid))
                logger.exception(f"❌ Exception during VendorCredit batch POST ({len(chunk)} items)")
                break

    return successes, failures

def _apply_batch_updates_vendorcredit(successes, failures):
    """
    Apply set-based status updates to Map_VendorCredit for performance.
    """
    if successes:
        executemany(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_VendorCredit] "
            f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
            f"WHERE Source_Id=?",
            [(qid, sid) for qid, sid in successes]
        )
    if failures:
        executemany(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_VendorCredit] "
            f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
            f"WHERE Source_Id=?",
            [(reason, sid) for reason, sid in failures]
        )

def post_vendorcredit(row):
    """
    Single-record fallback (kept for compatibility). Prefer using the batch poster.
    """
    sid = row.get("Source_Id")
    if row.get("Porter_Status") == "Success":
        return
    if int(row.get("Retry_Count") or 0) >= 5:
        return
    pj = row.get("Payload_JSON")
    if not pj:
        update_mapping_status(MAPPING_SCHEMA, "Map_VendorCredit", sid, "Failed",
                              failure_reason="Missing Payload_JSON", increment_retry=True)
        logger.warning(f"⚠️ VendorCredit {sid} skipped — missing Payload_JSON")
        return

    url, headers = get_qbo_auth()
    try:
        payload = _fast_loads(pj)
    except Exception as e:
        update_mapping_status(MAPPING_SCHEMA, "Map_VendorCredit", sid, "Failed",
                              failure_reason=f"Bad JSON: {e}", increment_retry=True)
        return

    try:
        resp =  session.post(url, headers=headers, json=payload, timeout=20)
        if resp.status_code == 200:
            qid = (resp.json().get("VendorCredit") or {}).get("Id")
            if qid:
                update_mapping_status(MAPPING_SCHEMA, "Map_VendorCredit", sid, "Success", target_id=qid)
                logger.info(f"✅ VendorCredit {sid} → QBO {qid}")
                return
            reason = "No Id in response"
        elif resp.status_code in (401, 403):
            logger.warning(f"🔐 {resp.status_code} on VendorCredit {sid}; refreshing token and retrying once...")
            auto_refresh_token_if_needed()
            url, headers = get_qbo_auth()
            resp2 = session.post(url, headers=headers, json=payload, timeout=20)
            if resp2.status_code == 200:
                qid = (resp2.json().get("VendorCredit") or {}).get("Id")
                if qid:
                    update_mapping_status(MAPPING_SCHEMA, "Map_VendorCredit", sid, "Success", target_id=qid)
                    logger.info(f"✅ VendorCredit {sid} → QBO {qid}")
                    return
                reason = "No Id in response (after refresh)"
            else:
                reason = (resp2.text or f"HTTP {resp2.status_code}")[:500]
        else:
            reason = (resp.text or f"HTTP {resp.status_code}")[:500]

        update_mapping_status(MAPPING_SCHEMA, "Map_VendorCredit", sid, "Failed",
                              failure_reason=reason, increment_retry=True)
        logger.error(f"❌ Failed VendorCredit {sid}: {reason}")

    except Exception as e:
        update_mapping_status(MAPPING_SCHEMA, "Map_VendorCredit", sid, "Failed",
                              failure_reason=str(e), increment_retry=True)
        logger.exception(f"❌ Exception posting VendorCredit {sid}")

def migrate_vendorcredits(VENDORCREDIT_DATE_FROM,VENDORCREDIT_DATE_TO, retry_only: bool = False):
    """
    Posts VendorCredits using pre-generated Payload_JSON via the QBO Batch API.
    - Single-threaded, high-throughput (multiple items per HTTP request).
    - Optional retry_only to post only Failed rows.
    """
    print("\n🚀 Starting VendorCredit Migration Phase\n" + "=" * 40)

    ensure_mapping_table(VENDORCREDIT_DATE_FROM,VENDORCREDIT_DATE_TO)

    # Optional DocNumber deduplication step (as per your pipeline)
    if 'ENABLE_VENDORCREDIT_DOCNUMBER_DEDUP' in globals() and ENABLE_VENDORCREDIT_DOCNUMBER_DEDUP:
        apply_duplicate_docnumber_strategy_for_vendorcredit()

    generate_payloads(batch_size=1000)

    rows = sql.fetch_table("Map_VendorCredit", MAPPING_SCHEMA)
    if retry_only:
        eligible = rows[rows["Porter_Status"].isin(["Failed"])].reset_index(drop=True)
    else:
        eligible = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)

    if eligible.empty:
        logger.info("⚠️ No eligible VendorCredits to post.")
        return

    url, headers = get_qbo_auth()

    select_batch_size = 300   # how many rows we slice from DB per outer loop
    post_batch_limit  = 10    # how many VC items per QBO batch call (<=30)

    total = len(eligible)
    logger.info(f"📤 Posting {total} VendorCredit(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        slice_df = eligible.iloc[i:i + select_batch_size]
        successes, failures = _post_batch_vendorcredits(
            slice_df, url, headers, timeout=40, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates_vendorcredit(successes, failures)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    # Optional one-pass fallback retry for failed+no Target_Id via single-record POST
    failed_df = sql.fetch_table("Map_VendorCredit", MAPPING_SCHEMA)
    failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
    if not failed_records.empty:
        logger.info(f"🔁 Reprocessing {len(failed_records)} failed VendorCredits (single-record fallback)...")
        for _, row in failed_records.iterrows():
            post_vendorcredit(row)

    print("\n🏁 VendorCredit migration completed.")

def resume_or_post_vendorcredits(VENDORCREDIT_DATE_FROM,VENDORCREDIT_DATE_TO, retry_only: bool = False):
    """
    Resumes/continues VendorCredit migration:
    - Rebuild mapping if Map_VendorCredit missing/incomplete.
    - Generate only missing Payload_JSON.
    - Post eligible rows via Batch API (single-threaded).
    """
    logger.info("🔁 Initiating resume_or_post_vendorcredits process...")
    print("\n🔁 Resuming VendorCredit Migration (conditional mode)\n" + "=" * 50)

    # 1) Table existence
    if not sql.table_exists("Map_VendorCredit", MAPPING_SCHEMA):
        logger.warning("❌ Map_VendorCredit table does not exist. Running full migration.")
        migrate_vendorcredits(VENDORCREDIT_DATE_FROM,VENDORCREDIT_DATE_TO, retry_only=retry_only)
        return

    mapped_df = sql.fetch_table("Map_VendorCredit", MAPPING_SCHEMA)
    source_df = sql.fetch_table("VendorCredit", SOURCE_SCHEMA)

    # 2) DocNumber dedup check (if in your pipeline)
    if "Duplicate_DocNumber" not in mapped_df.columns:
        logger.info("🔍 'Duplicate_DocNumber' column missing. Running deduplication...")
        apply_duplicate_docnumber_strategy_for_vendorcredit()
        mapped_df = sql.fetch_table("Map_VendorCredit", MAPPING_SCHEMA)
    elif mapped_df["Duplicate_DocNumber"].isnull().any() or (mapped_df["Duplicate_DocNumber"] == "").any():
        logger.info("🔍 Some Duplicate_DocNumber values missing. Running deduplication...")
        apply_duplicate_docnumber_strategy_for_vendorcredit()
        mapped_df = sql.fetch_table("Map_VendorCredit", MAPPING_SCHEMA)
    else:
        logger.info("✅ All Duplicate_DocNumber values present. Skipping deduplication.")

    # 3) Ensure payloads exist (generate only missing)
    payload_missing = mapped_df["Payload_JSON"].isnull() | (mapped_df["Payload_JSON"] == "")
    missing_count = int(payload_missing.sum())
    if missing_count > 0:
        logger.info(f"🧰 {missing_count} VendorCredits missing Payload_JSON. Generating for those...")
        generate_payloads(batch_size=1000)
        mapped_df = sql.fetch_table("Map_VendorCredit", MAPPING_SCHEMA)
    else:
        logger.info("✅ All VendorCredits have Payload_JSON.")

    # 4) Choose eligible set
    if retry_only:
        eligible = mapped_df[mapped_df["Porter_Status"].isin(["Failed"])].reset_index(drop=True)
    else:
        eligible = mapped_df[mapped_df["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)

    if eligible.empty:
        logger.info("⚠️ No eligible VendorCredits to post.")
        return

    url, headers = get_qbo_auth()

    select_batch_size = 300
    post_batch_limit  = 10

    total = len(eligible)
    logger.info(f"📤 Posting {total} VendorCredit(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        slice_df = eligible.iloc[i:i + select_batch_size]
        successes, failures = _post_batch_vendorcredits(
            slice_df, url, headers, timeout=40, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates_vendorcredit(successes, failures)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    # Optional one-pass fallback retry via single-record POST
    failed_df = sql.fetch_table("Map_VendorCredit", MAPPING_SCHEMA)
    failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
    if not failed_records.empty:
        logger.info(f"🔁 Reprocessing {len(failed_records)} failed VendorCredits (single-record fallback)...")
        for _, row in failed_records.iterrows():
            post_vendorcredit(row)

    print("\n🏁 VendorCredit posting completed.")

# --- SMART VENDORCREDIT MIGRATION ---
def smart_vendorcredit_migration(VENDORCREDIT_DATE_FROM,VENDORCREDIT_DATE_TO):
    """
    Smart migration entrypoint for VendorCredit:
    - If Map_VendorCredit table exists and row count matches VendorCredit table, resume/post VendorCredits.
    - If table missing or row count mismatch, perform full migration.
    """
    logger.info("� Running smart_vendorcredit_migration...")
    full_process = False
    if sql.table_exists("Map_VendorCredit", MAPPING_SCHEMA):
        mapped_df = sql.fetch_table("Map_VendorCredit", MAPPING_SCHEMA)
        source_df = sql.fetch_table("VendorCredit", SOURCE_SCHEMA)
        if len(mapped_df) == len(source_df):
            logger.info("✅ Table exists and row count matches. Resuming/posting VendorCredits.")
            resume_or_post_vendorcredits(VENDORCREDIT_DATE_FROM,VENDORCREDIT_DATE_TO)
            # After resume_or_post, reprocess failed records one more time
            failed_df = sql.fetch_table("Map_VendorCredit", MAPPING_SCHEMA)
            failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
            if not failed_records.empty:
                logger.info(f"🔁 Reprocessing {len(failed_records)} failed VendorCredits with null Target_Id after main migration...")
                for _, row in failed_records.iterrows():
                    post_vendorcredit(row)
            return
        else:
            logger.warning(f"❌ Row count mismatch: Map_VendorCredit={len(mapped_df)}, VendorCredit={len(source_df)}. Running full migration.")
            full_process = True
    else:
        logger.warning("❌ Map_VendorCredit table does not exist. Running full migration.")
        full_process = True
    if full_process:
        migrate_vendorcredits(VENDORCREDIT_DATE_FROM,VENDORCREDIT_DATE_TO)
        # After full migration, reprocess failed records with null Target_Id one more time
        failed_df = sql.fetch_table("Map_VendorCredit", MAPPING_SCHEMA)
        failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
        if not failed_records.empty:
            logger.info(f"🔁 Reprocessing {len(failed_records)} failed VendorCredits with null Target_Id after main migration...")
            for _, row in failed_records.iterrows():
                post_vendorcredit(row)


