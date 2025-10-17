
"""
Sequence : 20
Module: RefundReceipt_migrator.py
Author: Dixit Prajapati
Created: 2025-09-16
Description: Handles migration of Refundreceipt records from QBO to QBO.
Production : Working
Development : Require when necessary
Phase : 02 - Multi User + Tax
"""

import os, json, pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import global_logger as logger
from utils.cleaner import remove_null_fields
import requests
from utils.apply_duplicate_docnumber import apply_duplicate_docnumber_strategy_dynamic
from utils.token_refresher import get_qbo_context_migration
from storage.sqlserver.sql import executemany
from utils.payload_cleaner import deep_clean

load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
DATE_FROM = os.getenv("REFUNDRECEIPT_DATE_FROM", "1900-01-01")
DATE_TO = os.getenv("REFUNDRECEIPT_DATE_TO", "2040-01-01")

ENABLE_GLOBAL_REFUNDRECEIPT_DOCNUMBER_DEDUP=True

def apply_duplicate_docnumber_strategy():
    apply_duplicate_docnumber_strategy_dynamic(
    target_table="Map_Refundreceipt",
    schema=MAPPING_SCHEMA,
    check_against_tables=["Map_Bill","Map_Invoice","Map_VendorCredit","Map_JournalEntry","Map_Deposit","Map_Purchase","Map_Salesreceipt"])

def safe_float(val):
    """
    Safely converts the input value to a float.
    Args:
        val (Any): A numeric or string value to convert.
    Returns:
        float: Converted float value, or 0.0 if conversion fails.
    """
    try: return float(val)
    except: return 0.0

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
    Builds payload['TxnTaxDetail'] using header + up to 4 TaxLine components from Map_RefundReceipt columns.
    Columns used (examples):
      - Mapped_TxnTaxCodeRef (preferred) or TxnTaxDetail_TxnTaxCodeRef_value
      - TxnTaxDetail_TotalTax (or TxnTaxDetail.TotalTax)
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

    # Optional header total tax (try both column name formats)
    total_tax = _coerce_float(row.get("TxnTaxDetail_TotalTax")) or _coerce_float(row.get("TxnTaxDetail.TotalTax"))
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

def get_qbo_auth():
    """
    Constructs the QBO RefundReceipt API endpoint URL and headers.
    Returns:
        tuple: (URL string, headers dictionary) with OAuth2 access token and content type.
    """
    env = os.getenv("QBO_ENVIRONMENT", "sandbox")
    base = "https://sandbox-quickbooks.api.intuit.com" if env == "sandbox" else "https://quickbooks.api.intuit.com"
    ctx = get_qbo_context_migration()
    realm = ctx["REALM_ID"]
    return f"{base}/v3/company/{realm}/refundreceipt", {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

# def ensure_mapping_table(REFUNDRECEIPT_DATE_FROM,REFUNDRECEIPT_DATE_TO):
#     """
#     Ensures the Map_RefundReceipt table is initialized and enriched with master mappings.
#     - Loads source RefundReceipt data
#     - Resolves mapped Customer, Account, and PaymentMethod
#     - Creates or updates mapping table schema
#     - Inserts enriched data into Map_RefundReceipt
#     """
#     logger.info("🔧 Preparing Map_RefundReceipt from RefundReceipt source (optimized)")

#     DATE_FROM = REFUNDRECEIPT_DATE_FROM
#     DATE_TO = REFUNDRECEIPT_DATE_TO

#     # 1. Date filter
#     query = f"""
#         SELECT * FROM [{SOURCE_SCHEMA}].[RefundReceipt]
#         WHERE 1=1
#         {"AND TxnDate >= ?" if DATE_FROM else ""}
#         {"AND TxnDate <= ?" if DATE_TO else ""}
#     """
#     params = tuple(p for p in [DATE_FROM, DATE_TO] if p)
#     df = sql.fetch_table_with_params(query, params)
#     if df.empty:
#         logger.warning("⚠️ No records found in RefundReceipt source")
#         return

#     # 2. Core migration fields
#     df["Source_Id"] = df["Id"]
#     required_cols = [
#         "Target_Id", "Porter_Status", "Retry_Count", "Failure_Reason", "Payload_JSON",
#         "Mapped_Customer", "Mapped_Account", "Mapped_PaymentMethod",
#         "Mapped_Line_ItemRef", "Mapped_Line_ClassRef"
#     ]
#     for col in required_cols:
#         if col not in df.columns:
#             df[col] = None
#     df["Target_Id"] = None
#     df["Porter_Status"] = "Ready"
#     df["Retry_Count"] = 0
#     df["Failure_Reason"] = None
#     df["Payload_JSON"] = None

#     # 3. Bulk load mapping tables into dicts
#     def load_map(table):
#         t = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[{table}]", tuple())
#         return dict(zip(t["Source_Id"], t["Target_Id"])) if not t.empty else {}

#     customer_dict = load_map("Map_Customer")
#     account_dict = load_map("Map_Account")
#     payment_dict = load_map("Map_PaymentMethod")
#     item_dict = load_map("Map_Item")
#     class_dict = load_map("Map_Class")

#     # 4. Bulk fetch RefundReceipt_Line and group
#     lines = sql.fetch_table_with_params(f"SELECT * FROM [{SOURCE_SCHEMA}].[RefundReceipt_Line]", tuple())
#     lines_grouped = lines.groupby("Parent_Id") if not lines.empty else {}

#     # 5. Vectorized header-level mappings
#     if "CustomerRef.value" in df.columns:
#         df["Mapped_Customer"] = df["CustomerRef.value"].map(lambda x: customer_dict.get(x) if pd.notna(x) else None)
#     if "DepositToAccountRef.value" in df.columns:
#         df["Mapped_Account"] = df["DepositToAccountRef.value"].map(lambda x: account_dict.get(x) if pd.notna(x) else None)
#     if "PaymentMethodRef.value" in df.columns:
#         df["Mapped_PaymentMethod"] = df["PaymentMethodRef.value"].map(lambda x: payment_dict.get(x) if pd.notna(x) else None)

#     # 6. Vectorized line-level mappings (Item, Class)
#     def get_refs(parent_id, col, mapping_dict):
#         if col not in lines.columns:
#             return None
#         if parent_id not in lines_grouped.groups:
#             return None
#         vals = lines_grouped.get_group(parent_id)[col].dropna().unique()
#         targets = [str(mapping_dict.get(v)) for v in vals if v in mapping_dict]
#         return ";".join(sorted(set(targets))) if targets else None

#     if "SalesItemLineDetail.ItemRef.value" in lines.columns:
#         df["Mapped_Line_ItemRef"] = df["Id"].map(lambda x: get_refs(x, "SalesItemLineDetail.ItemRef.value", item_dict))
#     if "SalesItemLineDetail.ClassRef.value" in lines.columns:
#         df["Mapped_Line_ClassRef"] = df["Id"].map(lambda x: get_refs(x, "SalesItemLineDetail.ClassRef.value", class_dict))

#     # 7. Create or update mapping table
#     if not sql.table_exists("Map_RefundReceipt", MAPPING_SCHEMA):
#         create_cols = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
#         sql.run_query(f"CREATE TABLE [{MAPPING_SCHEMA}].[Map_RefundReceipt] ({create_cols})")
#     else:
#         existing = sql.fetch_table("Map_RefundReceipt", MAPPING_SCHEMA)
#         for col in df.columns:
#             if col not in existing.columns:
#                 sql.run_query(f"""
#                     IF COL_LENGTH('{MAPPING_SCHEMA}.Map_RefundReceipt', '{col}') IS NULL
#                     BEGIN ALTER TABLE [{MAPPING_SCHEMA}].[Map_RefundReceipt] ADD [{col}] NVARCHAR(MAX); END
#                 """)
#         sql.run_query(f"DELETE FROM [{MAPPING_SCHEMA}].[Map_RefundReceipt]")

#     sql.insert_dataframe(df, "Map_RefundReceipt", MAPPING_SCHEMA)
#     logger.info(f"✅ Inserted {len(df)} mapped rows into Map_RefundReceipt (optimized)")

def ensure_mapping_table(REFUNDRECEIPT_DATE_FROM='1900-01-01', REFUNDRECEIPT_DATE_TO='2080-12-31'):
    """
    Build/refresh porter_entities_mapping.Map_RefundReceipt with header-level mappings.
    We intentionally persist only a safe whitelist of columns (no ']' in names),
    after computing mapped values from any raw columns that include bracketed indices.
    """
    def _qid(name: str) -> str:
        return f"[{name}]"

    logger.info("🔧 Preparing Map_RefundReceipt from RefundReceipt source (optimized)")

    DATE_FROM = REFUNDRECEIPT_DATE_FROM
    DATE_TO   = REFUNDRECEIPT_DATE_TO

    # 1) Read source headers
    query = f"""
        SELECT * FROM {_qid(SOURCE_SCHEMA)}.{_qid('RefundReceipt')}
        WHERE 1=1
        {"AND TxnDate >= ?" if DATE_FROM else ""}
        {"AND TxnDate <= ?" if DATE_TO else ""}
    """
    params = tuple(p for p in [DATE_FROM, DATE_TO] if p)
    df = sql.fetch_table_with_params(query, params)
    if df.empty:
        logger.warning("⚠️ No records found in RefundReceipt source")
        return

    # 2) Initialize core migration fields
    df["Source_Id"] = df["Id"]
    required_cols = [
        "Target_Id","Porter_Status","Retry_Count","Failure_Reason","Payload_JSON",
        "Mapped_Customer","Mapped_Account","Mapped_PaymentMethod",
        "Mapped_Line_ItemRef","Mapped_Line_ClassRef",
        "Mapped_Department",
        "Mapped_TxnTaxCodeRef","Mapped_TxnTaxRateRef",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None
    df["Target_Id"]      = None
    df["Porter_Status"]  = "Ready"
    df["Retry_Count"]    = 0
    df["Failure_Reason"] = None
    df["Payload_JSON"]   = None

    # 3) Load mapping dicts
    def load_map(table):
        t = sql.fetch_table_with_params(
            f"SELECT Source_Id, Target_Id FROM {_qid(MAPPING_SCHEMA)}.{_qid(table)}",
            tuple()
        )
        return dict(zip(t["Source_Id"], t["Target_Id"])) if not t.empty else {}

    customer_dict = load_map("Map_Customer")
    account_dict  = load_map("Map_Account")
    payment_dict  = load_map("Map_PaymentMethod")
    item_dict     = load_map("Map_Item")
    class_dict    = load_map("Map_Class")
    department_dict = load_map("Map_Department") if sql.table_exists("Map_Department", MAPPING_SCHEMA) else {}
    taxcode_dict  = load_map("Map_TaxCode") if sql.table_exists("Map_TaxCode", MAPPING_SCHEMA) else {}
    taxrate_dict  = load_map("Map_TaxRate") if sql.table_exists("Map_TaxRate", MAPPING_SCHEMA) else {}

    # 4) Fetch all lines once and group
    lines = sql.fetch_table_with_params(
        f"SELECT * FROM {_qid(SOURCE_SCHEMA)}.{_qid('RefundReceipt_Line')}",
        tuple()
    )
    lines_grouped = lines.groupby("Parent_Id") if not lines.empty else {}

    # 5) Header-level mappings (Customer/Account/PaymentMethod/Department)
    if "CustomerRef.value" in df.columns:
        df["Mapped_Customer"] = df["CustomerRef.value"].map(lambda x: customer_dict.get(x) if pd.notna(x) else None)
    if "DepositToAccountRef.value" in df.columns:
        df["Mapped_Account"] = df["DepositToAccountRef.value"].map(lambda x: account_dict.get(x) if pd.notna(x) else None)
    if "PaymentMethodRef.value" in df.columns:
        df["Mapped_PaymentMethod"] = df["PaymentMethodRef.value"].map(lambda x: payment_dict.get(x) if pd.notna(x) else None)
    if "DepartmentRef.value" in df.columns:
        df["Mapped_Department"] = df["DepartmentRef.value"].map(lambda x: department_dict.get(x) if pd.notna(x) else None)

    # 6) Compute mapped tax fields from raw header columns
    #    TaxCode: TxnTaxDetail.TxnTaxCodeRef.value
    tc_src = "TxnTaxDetail.TxnTaxCodeRef.value"
    if tc_src in df.columns:
        df["Mapped_TxnTaxCodeRef"] = df[tc_src].map(lambda v: None if pd.isna(v) else taxcode_dict.get(v))

    #    TaxRate: prefer TaxLine[0]..., else fallback to TaxLine[1]...
    tr0 = "TxnTaxDetail.TaxLine[0].TaxLineDetail.TaxRateRef.value"
    tr1 = "TxnTaxDetail.TaxLine[1].TaxLineDetail.TaxRateRef.value"
    # Build a temporary column with the chosen source rate ref
    if tr0 in df.columns or tr1 in df.columns:
        def pick_rate(row):
            v0 = row.get(tr0) if tr0 in row.index else None
            v1 = row.get(tr1) if tr1 in row.index else None
            if pd.notna(v0): return v0
            if pd.notna(v1): return v1
            return None
        df["_Raw_TaxRateRef"] = df.apply(pick_rate, axis=1)
        df["Mapped_TxnTaxRateRef"] = df["_Raw_TaxRateRef"].map(lambda v: None if pd.isna(v) else taxrate_dict.get(v))
        df.drop(columns=["_Raw_TaxRateRef"], errors="ignore", inplace=True)

    # 7) Aggregate line refs to header
    def get_refs(parent_id, col, mapping_dict):
        if col not in lines.columns: return None
        if parent_id not in lines_grouped.groups: return None
        vals = lines_grouped.get_group(parent_id)[col].dropna().unique()
        targets = [str(mapping_dict.get(v)) for v in vals if v in mapping_dict]
        return ";".join(sorted(set(targets))) if targets else None

    if "SalesItemLineDetail.ItemRef.value" in lines.columns:
        df["Mapped_Line_ItemRef"] = df["Id"].map(lambda x: get_refs(x, "SalesItemLineDetail.ItemRef.value", item_dict))
    if "SalesItemLineDetail.ClassRef.value" in lines.columns:
        df["Mapped_Line_ClassRef"] = df["Id"].map(lambda x: get_refs(x, "SalesItemLineDetail.ClassRef.value", class_dict))

    # 8) Whitelist of columns to persist (no ']' in names)
    #    (Aligned to your RefundReceipt SELECT list)
    whitelist = [
        "Source_Id", "Id", "DocNumber", "TxnDate",
        "CurrencyRef.value",
        "Mapped_Customer","Mapped_Account","Mapped_PaymentMethod","Mapped_Department",
        "PrintStatus", "PaymentRefNum",
        "CustomerMemo.value", "TotalAmt", "Balance",
        "TxnTaxDetail.TotalTax",                # keep dot-form total tax
        # Bill address
        "BillAddr.Line1","BillAddr.Line2","BillAddr.Line3","BillAddr.Line4",
        "BillAddr.City","BillAddr.Country","BillAddr.CountrySubDivisionCode","BillAddr.PostalCode",
        # ShipFrom address (as per RefundReceipt columns you shared)
        "ShipFromAddr.Line1","ShipFromAddr.Line2","ShipFromAddr.Line3",
        # Emails (present in your SELECT)
        "BillEmail.Address","BillEmailCc.Address","BillEmailBcc.Address",
        # Aggregated line-level refs
        "Mapped_Line_ItemRef","Mapped_Line_ClassRef",
        # Mapped tax
        "Mapped_TxnTaxCodeRef","Mapped_TxnTaxRateRef",
        # Control columns
        "Target_Id","Porter_Status","Retry_Count","Failure_Reason","Payload_JSON",
        # Optional fields present in your SELECT (include if needed downstream)
        "ApplyTaxAfterDiscount", "PrivateNote", "DepartmentRef.value", "DepartmentRef.name",
        "DepositToAccountRef.value", "DepositToAccountRef.name",
    ]
    keep_cols = [c for c in whitelist if c in df.columns]
    df_out = df[keep_cols].copy()

    # 9) DDL (safe — no ']' in persisted column names)
    if not sql.table_exists("Map_RefundReceipt", MAPPING_SCHEMA):
        col_defs = ", ".join([f"{_qid(col)} NVARCHAR(MAX)" for col in df_out.columns])
        sql.run_query(f"CREATE TABLE {_qid(MAPPING_SCHEMA)}.{_qid('Map_RefundReceipt')} ({col_defs})")
    else:
        existing = sql.fetch_table("Map_RefundReceipt", MAPPING_SCHEMA)
        for col in df_out.columns:
            if col not in existing.columns:
                sql.run_query(
                    f"""
                    IF COL_LENGTH('{MAPPING_SCHEMA}.Map_RefundReceipt', '{col}') IS NULL
                    BEGIN
                        ALTER TABLE {_qid(MAPPING_SCHEMA)}.{_qid('Map_RefundReceipt')} ADD {_qid(col)} NVARCHAR(MAX);
                    END
                    """
                )
        sql.run_query(f"DELETE FROM {_qid(MAPPING_SCHEMA)}.{_qid('Map_RefundReceipt')}")

    # 10) Insert
    sql.insert_dataframe(df_out, "Map_RefundReceipt", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df_out)} mapped rows into Map_RefundReceipt (optimized)")


def update_mapping_status(
    mapping_table: str,
    source_id: str,
    status: str,
    failure_reason: str = None,
    mapping_schema: str = MAPPING_SCHEMA,
    target_id: str = None,
    increment_retry: bool = True
):
    """
    Updates the status and metadata for a record in the mapping table.
    Args:
        mapping_table (str): Name of the mapping table (e.g., Map_RefundReceipt).
        source_id (str): Source system identifier.
        status (str): New Porter_Status (e.g., Success, Failed).
        failure_reason (str, optional): Error message or explanation.
        mapping_schema (str): SQL schema for the mapping table.
        target_id (str, optional): ID returned by QBO (if any).
        increment_retry (bool): Whether to increment Retry_Count on failure.
    """
    retry_sql = "ISNULL(Retry_Count, 0) + 1" if increment_retry and status == "Failed" else "ISNULL(Retry_Count, 0)"
    sql.run_query(f"""
        UPDATE [{mapping_schema}].[{mapping_table}]
        SET Porter_Status = ?,
            Failure_Reason = ?,
            Target_Id = ?,
            Retry_Count = {retry_sql}
        WHERE Source_Id = ?
    """, (status, failure_reason, target_id, source_id))

# Reusable address builder
ADDRESS_FIELDS = ("Line1","Line2","Line3","Line4","Line5","City","Country","CountrySubDivisionCode","PostalCode")

def _add_addr_block(payload: dict, row_get, prefix: str, key: str | None = None, fields: tuple[str, ...] = ADDRESS_FIELDS) -> None:
    """
    Add an address block to payload if any fields are present.
      prefix: e.g. "BillAddr", "ShipAddr", "ShipFromAddr"
      key: payload key; defaults to prefix (override if you want a different name)
    """
    k = key or prefix
    block = {f: row_get(f"{prefix}.{f}") for f in fields if row_get(f"{prefix}.{f}")}
    if block:
        payload[k] = block


def build_refundreceipt_payload(row, lines):
    """
    Builds the JSON payload for a RefundReceipt, including customer, header, and line details.
    Args:
        row (pandas.Series): A single RefundReceipt record from Map_RefundReceipt.
        lines (pandas.DataFrame): Line items linked by Parent_Id from RefundReceipt_Line.
    Returns:
        dict or None: Structured JSON payload dictionary if valid, otherwise None.
    """
    # Preload mapping dicts for line-level lookups (avoid per-row SQL)
    if not hasattr(build_refundreceipt_payload, "_item_dict"):
        build_refundreceipt_payload._item_dict = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Item]", tuple()).set_index("Source_Id")["Target_Id"].to_dict()
    if not hasattr(build_refundreceipt_payload, "_class_dict"):
        build_refundreceipt_payload._class_dict = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Class]", tuple()).set_index("Source_Id")["Target_Id"].to_dict()
    item_dict = build_refundreceipt_payload._item_dict
    class_dict = build_refundreceipt_payload._class_dict

    row_get = row.get if hasattr(row, "get") else lambda k: row[k] if k in row else None
    payload = {
        "domain": "QBO",
        "DocNumber": row_get("Duplicate_Docnumber") or row_get("DocNumber"),
        "TxnDate": str(row_get("TxnDate") or "")[:10],
        "CustomerRef": {"value": str(row_get("Mapped_Customer"))} if row_get("Mapped_Customer") else None,
        "CurrencyRef": {"value": row_get("CurrencyRef.value") or "USD"},
        "PaymentMethodRef": {"value": str(row_get("Mapped_PaymentMethod"))} if row_get("Mapped_PaymentMethod") else None,
        "DepositToAccountRef": {"value": str(row_get("Mapped_Account"))} if row_get("Mapped_Account") else None,
        "DepartmentRef": {"value": str(row_get("Mapped_Department"))} if row_get("Mapped_Department") else None,
        "TxnTaxDetail": {"TotalTax": 0},
        "Line": []
    }
    # Optional fields
    if row_get("CustomerMemo.value"):
        payload["CustomerMemo"] = {"value": row_get("CustomerMemo.value")}
    if row_get("PrivateNote"):
        payload["PrivateNote"] = row_get("PrivateNote")
    if row_get("PrintStatus"):
        payload["PrintStatus"] = row_get("PrintStatus")
    if row_get("BillEmail.Address"):
        payload["BillEmail"] = {"Address": row_get("BillEmail.Address")}
    # BillAddr
    # billaddr_fields = ["Line1", "Line2", "Line3", "Line4", "Line5", "City", "Country", "CountrySubDivisionCode", "PostalCode"]
    # billaddr = {k: row_get(f"BillAddr.{k}") for k in billaddr_fields if row_get(f"BillAddr.{k}")}
    # if billaddr:
    #     payload["BillAddr"] = billaddr
    _add_addr_block(payload, row_get, "BillAddr")
    _add_addr_block(payload, row_get, "ShipFromAddr")  # present in your RefundReceipt SELECT
    # If your dataset includes ShipAddr too, this is safe:
    _add_addr_block(payload, row_get, "ShipAddr")

    has_valid_line = False
    for _, ln in lines.iterrows():
        ln_get = ln.get if hasattr(ln, "get") else lambda k: ln[k] if k in ln else None
        detail_type = ln_get("DetailType")
        amount = safe_float(ln_get("Amount"))
        if amount == 0:
            logger.warning(f"⚠️ RefundReceipt line with Amount 0 — Source_Id {row_get('Source_Id')}, LineNum {ln_get('LineNum')}")
        if not detail_type:
            continue
        line_entry = {
            "DetailType": detail_type,
            "Amount": amount
        }
        if not pd.isna(ln_get("LineNum")):
            try:
                line_entry["LineNum"] = int(safe_float(ln_get("LineNum")))
            except:
                pass
        if not pd.isna(ln_get("Description")):
            line_entry["Description"] = ln_get("Description")
        # Map ItemRef and ClassRef using preloaded dicts
        if detail_type == "SalesItemLineDetail":
            item = ln_get("Mapped_Item")
            if not item:
                item_src = ln_get("SalesItemLineDetail.ItemRef.value")
                item = item_dict.get(item_src)
            sid = {
                "ItemRef": {"value": str(item)} if item else None,
                "Qty": safe_float(ln_get("SalesItemLineDetail.Qty")),
                "UnitPrice": safe_float(ln_get("SalesItemLineDetail.UnitPrice")),
                "ServiceDate": ln_get("SalesItemLineDetail.ServiceDate"),
                "TaxCodeRef": {"value": ln_get("SalesItemLineDetail.TaxCodeRef.value") or "NON"}
            }
            class_val = ln_get("Mapped_Class")
            if not class_val:
                class_src = ln_get("SalesItemLineDetail.ClassRef.value")
                class_val = class_dict.get(class_src) if class_src else None
            if class_val:
                sid["ClassRef"] = {"value": str(class_val)}
            # Remove nulls
            sid = {k: v for k, v in sid.items() if v is not None}
            line_entry["SalesItemLineDetail"] = sid
            has_valid_line = True
        elif detail_type == "SubTotalLineDetail":
            line_entry["SubTotalLineDetail"] = {}
        payload["Line"].append(line_entry)
    if not has_valid_line or not payload["Line"]:
        return None
    
    # NEW: Add tax detail if available
    add_txn_tax_detail_from_row(payload, row)
    
    return deep_clean(payload)

def generate_refundreceipt_payloads(batch_size = 500):
    """
    Generates and stores QBO-compliant JSON payloads for RefundReceipts.
    - Loads failed or unprocessed records from Map_RefundReceipt
    - Fetches corresponding line items
    - Constructs payloads using `build_refundreceipt_payload`
    - Saves result in Payload_JSON column and marks status as 'Ready' or 'Failed'
    """
    logger.info("🛠️ Generating RefundReceipt Payloads (optimized batch)...")
    while True:
        df = sql.fetch_table_with_params(f"""
            SELECT TOP {batch_size} * FROM [{MAPPING_SCHEMA}].[Map_RefundReceipt]
            WHERE Porter_Status = 'Ready'
              AND (Payload_JSON IS NULL OR Payload_JSON = '' OR Payload_JSON = 'null')
        """, ())
        if df.empty:
            logger.info("✅ All payloads generated.")
            break
        sids = df["Source_Id"].tolist()
        all_lines = sql.fetch_table_with_params(f"SELECT * FROM [{SOURCE_SCHEMA}].[RefundReceipt_Line] WHERE Parent_Id IN ({','.join(['?']*len(sids))}) ORDER BY Parent_Id, LineNum", tuple(sids)) if sids else pd.DataFrame()
        lines_grouped = all_lines.groupby("Parent_Id") if not all_lines.empty else {}
        for idx, row in df.iterrows():
            sid = row["Source_Id"]
            lines = lines_grouped.get_group(sid) if sid in lines_grouped.groups else pd.DataFrame()
            if lines.empty:
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_RefundReceipt]
                    SET Porter_Status='Failed', Failure_Reason='No lines found'
                    WHERE Source_Id = ?
                """, (sid,))
                continue
            payload = build_refundreceipt_payload(row, lines)
            if not payload:
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_RefundReceipt]
                    SET Porter_Status='Failed', Failure_Reason='No valid SalesItemLineDetail lines'
                    WHERE Source_Id = ?
                """, (sid,))
                continue
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_RefundReceipt]
                SET Payload_JSON = ?
                WHERE Source_Id = ?
            """, (json.dumps(payload, indent=2), sid))

session = requests.Session()

# ============================ Shared Helpers (add once) ============================
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
                return _json.loads(s.decode("utf-8")) if isinstance(s, (bytes, bytearray)) else _json.loads(s)
            return s

# Convert any entity endpoint → /batch endpoint (robust to wrong trailing entity)
if "_derive_batch_url" not in globals():
    def _derive_batch_url(entity_url: str, entity_name: str) -> str:
        """
        entity_url: https://.../v3/company/<realm>/<Entity>?minorversion=XX   (any entity)
        entity_name: "RefundReceipt" (ignored if path doesn't end with it)
        -> https://.../v3/company/<realm>/batch?minorversion=XX
        """
        qpos = entity_url.find("?")
        query = entity_url[qpos:] if qpos != -1 else ""
        path = entity_url[:qpos] if qpos != -1 else entity_url

        # try stripping the expected trailing entity segment
        seg = f"/{entity_name}".lower()
        lower = path.lower()
        if lower.endswith(seg):
            path = path[: -len(seg)]
        else:
            # if it ends with some other entity, just drop the last segment
            parts = path.rsplit("/", 1)
            if len(parts) == 2 and parts[1]:
                path = parts[0]

        # normalize to /v3/company/<realm>
        marker = "/v3/company/"
        i = path.lower().find(marker)
        if i != -1:
            j = path.find("/", i + len(marker))
            if j != -1:
                path = path[:j]

        return f"{path}/batch{query}"

# Ensure a shared HTTP session exists
try:
    session
except NameError:
    import requests
    session = requests.Session()

# ============================ Batch Poster & Updates ==============================
def _post_batch_refundreceipts(eligible_batch, url, headers, timeout=40, post_batch_limit=20, max_manual_retries=1):
    """
    Single-threaded batch posting for RefundReceipts via QBO /batch.
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
            payload = _fast_loads(pj)
        except Exception as e:
            failures.append((f"Bad JSON: {e}", sid))
            continue
        work.append((sid, payload))

    if not work:
        return successes, failures

    batch_url = _derive_batch_url(url, "RefundReceipt")

    # Process in chunks
    idx = 0
    while idx < len(work):
        auto_refresh_token_if_needed()
        chunk = work[idx:idx + post_batch_limit]
        idx += post_batch_limit
        if not chunk:
            continue

        def _do_post(_headers):
            body = {
                "BatchItemRequest": [
                    {"bId": str(sid), "operation": "create", "RefundReceipt": payload}
                    for (sid, payload) in chunk
                ]
            }
            return session.post(batch_url, headers=_headers, json=body, timeout=timeout)

        attempted_refresh = False
        for attempt in range(max_manual_retries + 1):
            try:
                resp = _do_post(headers)
                sc = resp.status_code

                if sc == 200:
                    rj = resp.json()
                    items = rj.get("BatchItemResponse", []) or []
                    seen = set()

                    for item in items:
                        bid = item.get("bId")
                        seen.add(bid)
                        ent = item.get("RefundReceipt")
                        if ent and "Id" in ent:
                            qid = ent["Id"]
                            successes.append((qid, bid))
                            logger.info(f"✅ RefundReceipt {bid} → QBO {qid}")
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
                        logger.error(f"❌ RefundReceipt {bid} failed: {reason}")

                    # Any missing responses → mark failed
                    for sid, _ in chunk:
                        if str(sid) not in seen:
                            failures.append(("No response for bId", sid))
                            logger.error(f"❌ No response for RefundReceipt {sid}")
                    break

                elif sc in (401, 403) and not attempted_refresh:
                    logger.warning(f"🔐 {sc} on RefundReceipt batch ({len(chunk)} items); refreshing token and retrying once...")
                    try:
                        auto_refresh_token_if_needed()
                    except Exception:
                        pass
                    url, headers = get_qbo_auth()
                    batch_url = _derive_batch_url(url, "RefundReceipt")
                    attempted_refresh = True
                    continue

                else:
                    reason = (resp.text or f"HTTP {sc}")[:1000]
                    for sid, _ in chunk:
                        failures.append((reason, sid))
                    logger.error(f"❌ RefundReceipt batch failed ({len(chunk)} items): {reason}")
                    break

            except Exception as e:
                reason = f"Batch exception: {e}"
                for sid, _ in chunk:
                    failures.append((reason, sid))
                logger.exception(f"❌ Exception during RefundReceipt batch POST ({len(chunk)} items)")
                break

    return successes, failures

def _apply_batch_updates_refundreceipt(successes, failures):
    """
    Set-based updates for Map_RefundReceipt.
    """
    try:
        if successes:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_RefundReceipt] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                [(qid, sid) for qid, sid in successes]
            )
        if failures:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_RefundReceipt] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                [(reason, sid) for reason, sid in failures]
            )
    except NameError:
        # Fallback to per-row updates if executemany helper is not available
        for qid, sid in successes or []:
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_RefundReceipt] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                (qid, sid)
            )
        for reason, sid in failures or []:
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_RefundReceipt] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                (reason, sid)
            )

# ============================ Updated Posting APIs ===============================
def post_refundreceipts(row):
    """
    Single-record fallback; prefer batch path in migrate/resume.
    """
    sid = row["Source_Id"]
    if row.get("Porter_Status") == "Success":
        return
    if not row.get("Payload_JSON"):
        return
    if int(row.get("Retry_Count") or 0) >= 5:
        return

    url, headers = get_qbo_auth()
    try:
        payload = _fast_loads(row["Payload_JSON"])
    except Exception as e:
        sql.run_query(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_RefundReceipt] "
            f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count, 0) + 1, Failure_Reason = ? "
            f"WHERE Source_Id = ?",
            (f"Bad JSON: {e}", sid)
        )
        return

    # single POST to /refundreceipt endpoint (compatibility fallback)
    try:
        endpoint = url.replace("salesreceipt", "refundreceipt")
        resp = session.post(endpoint, headers=headers, json=payload, timeout=20)
        if resp.status_code == 200:
            qid = (resp.json().get("RefundReceipt") or {}).get("Id")
            if qid:
                sql.run_query(
                    f"UPDATE [{MAPPING_SCHEMA}].[Map_RefundReceipt] "
                    f"SET Target_Id = ?, Porter_Status = 'Success', Failure_Reason = NULL "
                    f"WHERE Source_Id = ?",
                    (qid, sid)
                )
                return
            reason = "No Id in response"
        elif resp.status_code in (401, 403):
            try:
                auto_refresh_token_if_needed()
            except Exception:
                pass
            url, headers = get_qbo_auth()
            endpoint = url.replace("salesreceipt", "refundreceipt")
            resp2 = session.post(endpoint, headers=headers, json=payload, timeout=20)
            if resp2.status_code == 200:
                qid = (resp2.json().get("RefundReceipt") or {}).get("Id")
                if qid:
                    sql.run_query(
                        f"UPDATE [{MAPPING_SCHEMA}].[Map_RefundReceipt] "
                        f"SET Target_Id = ?, Porter_Status = 'Success', Failure_Reason = NULL "
                        f"WHERE Source_Id = ?",
                        (qid, sid)
                    )
                    return
                reason = "No Id in response (after refresh)"
            else:
                reason = (resp2.text or f"HTTP {resp2.status_code}")[:500]
        else:
            reason = (resp.text or f"HTTP {resp.status_code}")[:500]

        sql.run_query(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_RefundReceipt] "
            f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count, 0) + 1, Failure_Reason = ? "
            f"WHERE Source_Id = ?",
            (reason, sid)
        )
    except Exception as e:
        sql.run_query(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_RefundReceipt] "
            f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count, 0) + 1, Failure_Reason = ? "
            f"WHERE Source_Id = ?",
            (str(e), sid)
        )

def migrate_refundreceipts(REFUNDRECEIPT_DATE_FROM,REFUNDRECEIPT_DATE_TO):
    """
    Full end-to-end migration for RefundReceipts using QBO /batch.
    """
    print("\n🚀 Starting RefundReceipt Migration\n" + "=" * 40)
    ensure_mapping_table(REFUNDRECEIPT_DATE_FROM,REFUNDRECEIPT_DATE_TO)
    if 'ENABLE_GLOBAL_REFUNDRECEIPT_DOCNUMBER_DEDUP' in globals() and ENABLE_GLOBAL_REFUNDRECEIPT_DOCNUMBER_DEDUP:
        apply_duplicate_docnumber_strategy()
    generate_refundreceipt_payloads()

    rows = sql.fetch_table("Map_RefundReceipt", MAPPING_SCHEMA)
    eligible = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("⚠️ No RefundReceipts to process.")
        return

    url, headers = get_qbo_auth()
    select_batch_size = 300   # DB slice size
    post_batch_limit  = 5    # items per QBO batch call (<=30). Set to 3 for exactly-3-at-a-time.
    timeout           = 40

    total = len(eligible)
    logger.info(f"📤 Posting {total} RefundReceipt(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        batch = eligible.iloc[i:i + select_batch_size]
        successes, failures = _post_batch_refundreceipts(
            batch, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates_refundreceipt(successes, failures)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    # Optional one-pass fallback retry for failures without Target_Id
    failed_df = sql.fetch_table("Map_RefundReceipt", MAPPING_SCHEMA)
    failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
    if not failed_records.empty:
        logger.info(f"🔁 Reprocessing {len(failed_records)} failed RefundReceipts (single-record fallback)...")
        for _, row in failed_records.iterrows():
            post_refundreceipts(row)

    print("\n🏁 RefundReceipt migration completed.")

def run_refundreceipt_migration(REFUNDRECEIPT_DATE_FROM,REFUNDRECEIPT_DATE_TO):
    """
    Main entry point for running full RefundReceipt migration.
    """
    print("\n=== 🚀 Starting RefundReceipt Migration ===")
    migrate_refundreceipts(REFUNDRECEIPT_DATE_FROM,REFUNDRECEIPT_DATE_TO)
    print("✅ RefundReceipt migration completed.\n")

def resume_or_post_refundreceipts(REFUNDRECEIPT_DATE_FROM,REFUNDRECEIPT_DATE_TO):
    """
    Resume posting pending/failed RefundReceipts using /batch.
    """
    print("\n🔁 Resuming RefundReceipt Migration\n" + "=" * 45)

    # 1. Ensure mapping table exists
    if not sql.table_exists("Map_RefundReceipt", MAPPING_SCHEMA):
        ensure_mapping_table(REFUNDRECEIPT_DATE_FROM,REFUNDRECEIPT_DATE_TO)
        logger.info("✅ Created mapping table Map_RefundReceipt.")

    mapped = sql.fetch_table("Map_RefundReceipt", MAPPING_SCHEMA)
    source = sql.fetch_table("RefundReceipt", SOURCE_SCHEMA)

    # 2. If mapping table is out of sync, rebuild it
    if len(mapped) != len(source):
        ensure_mapping_table(REFUNDRECEIPT_DATE_FROM,REFUNDRECEIPT_DATE_TO)
        mapped = sql.fetch_table("Map_RefundReceipt", MAPPING_SCHEMA)
        logger.info("✅ Mapping table rebuilt to match source.")

    # 3. If any Duplicate_Docnumber is missing, apply deduplication
    if 'Duplicate_Docnumber' in mapped.columns and mapped['Duplicate_Docnumber'].isnull().any():
        if 'ENABLE_GLOBAL_REFUNDRECEIPT_DOCNUMBER_DEDUP' in globals() and ENABLE_GLOBAL_REFUNDRECEIPT_DOCNUMBER_DEDUP:
            apply_duplicate_docnumber_strategy()
            mapped = sql.fetch_table("Map_RefundReceipt", MAPPING_SCHEMA)
            logger.info("✅ Applied duplicate docnumber strategy.")

    # 4. If any payloads are missing, generate them
    missing_payload = mapped[(mapped["Payload_JSON"].isnull()) | (mapped["Payload_JSON"] == "") | (mapped["Payload_JSON"] == "null")]
    if not missing_payload.empty:
        generate_refundreceipt_payloads()
        mapped = sql.fetch_table("Map_RefundReceipt", MAPPING_SCHEMA)
        logger.info(f"✅ Generated payloads for {len(missing_payload)} records.")

    # 5. Post only eligible records
    eligible = mapped[(mapped["Porter_Status"].isin(["Ready", "Failed"])) &
                      mapped["Payload_JSON"].notna() &
                      (mapped["Payload_JSON"] != "") &
                      (mapped["Payload_JSON"] != "null")].reset_index(drop=True)
    if eligible.empty:
        logger.info("⚠️ No RefundReceipts to post.")
        return

    url, headers = get_qbo_auth()
    select_batch_size = 300
    post_batch_limit  = 5
    timeout           = 40

    total = len(eligible)
    logger.info(f"📤 Posting {total} RefundReceipt(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        batch = eligible.iloc[i:i + select_batch_size]
        successes, failures = _post_batch_refundreceipts(
            batch, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates_refundreceipt(successes, failures)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    print("\n🏁 RefundReceipt posting completed.")

# ============================ SMART RefundReceipt Migration ============================
def smart_refundreceipt_migration(REFUNDRECEIPT_DATE_FROM, REFUNDRECEIPT_DATE_TO):
    """
    Smart migration entrypoint for RefundReceipts:
    - If Map_RefundReceipt table exists and row count matches RefundReceipt table, resume/post.
    - If table missing or row count mismatch, perform full migration.
    - After either path, reprocess failed records with null Target_Id one more time.
    """
    logger.info("🔎 Running smart_refundreceipt_migration...")
    full_process = False

    if sql.table_exists("Map_RefundReceipt", MAPPING_SCHEMA):
        mapped_df = sql.fetch_table("Map_RefundReceipt", MAPPING_SCHEMA)
        source_df = sql.fetch_table("RefundReceipt", SOURCE_SCHEMA)

        if len(mapped_df) == len(source_df):
            logger.info("✅ Table exists and row count matches. Resuming/posting RefundReceipts.")
            resume_or_post_refundreceipts(REFUNDRECEIPT_DATE_FROM, REFUNDRECEIPT_DATE_TO)

            # Retry failed ones after resume
            failed_df = sql.fetch_table("Map_RefundReceipt", MAPPING_SCHEMA)
            failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
            if not failed_records.empty:
                logger.info(f"🔁 Reprocessing {len(failed_records)} failed RefundReceipts with null Target_Id after main migration...")
                for _, row in failed_records.iterrows():
                    post_refundreceipts(row)
            return
        else:
            logger.warning(f"❌ Row count mismatch: Map_RefundReceipt={len(mapped_df)}, RefundReceipt={len(source_df)}. Running full migration.")
            full_process = True
    else:
        logger.warning("❌ Map_RefundReceipt table does not exist. Running full migration.")
        full_process = True

    # ----- Full migration path -----
    if full_process:
        ensure_mapping_table(REFUNDRECEIPT_DATE_FROM, REFUNDRECEIPT_DATE_TO)

        if 'ENABLE_GLOBAL_REFUNDRECEIPT_DOCNUMBER_DEDUP' in globals() and ENABLE_GLOBAL_REFUNDRECEIPT_DOCNUMBER_DEDUP:
            apply_duplicate_docnumber_strategy()
            try:
                sql.clear_cache()
            except Exception:
                pass
            import time; time.sleep(1)

        generate_refundreceipt_payloads()

        post_query = f"SELECT * FROM {MAPPING_SCHEMA}.Map_RefundReceipt WHERE Porter_Status = 'Ready'"
        post_rows = sql.fetch_table_with_params(post_query, tuple())
        if post_rows.empty:
            logger.warning("⚠️ No RefundReceipts with built JSON to post.")
            return

        url, headers = get_qbo_auth()
        select_batch_size = 300
        post_batch_limit  = 5
        timeout           = 40

        total = len(post_rows)
        logger.info(f"📤 Posting {total} RefundReceipt record(s) via QBO Batch API (limit {post_batch_limit}/call)...")

        for i in range(0, total, select_batch_size):
            slice_df = post_rows.iloc[i:i + select_batch_size]
            successes, failures = _post_batch_refundreceipts(
                slice_df, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
            )
            _apply_batch_updates_refundreceipt(successes, failures)

            done = min(i + select_batch_size, total)
            logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

        # Retry failed ones after full migration
        failed_df = sql.fetch_table("Map_RefundReceipt", MAPPING_SCHEMA)
        failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
        if not failed_records.empty:
            logger.info(f"🔁 Reprocessing {len(failed_records)} failed RefundReceipts with null Target_Id after main migration...")
            for _, row in failed_records.iterrows():
                post_refundreceipts(row)

        logger.info("🏁 SMART RefundReceipt migration complete.")
