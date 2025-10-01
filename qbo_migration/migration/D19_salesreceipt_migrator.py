"""
Sequence : 19
Module: SalesReceipt_migrator.py
Author: Dixit Prajapati
Created: 2025-07-29
Description: Handles migration of SalesReceipt records from QBO to QBO.
Production : Working
Development : Require when necessary
Phase : 02 Multiuser + Tax
"""

import os, json, requests, pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import global_logger as logger
# from config.mapping.salesreceipt_mapping import SALESRECEIPT_HEADER_MAPPING as HEADER_MAP, SALESRECEIPT_LINE_MAPPING as LINE_MAP
from utils.apply_duplicate_docnumber import apply_duplicate_docnumber_strategy_dynamic
from utils.token_refresher import get_qbo_context_migration
from storage.sqlserver.sql import executemany

auto_refresh_token_if_needed()
load_dotenv()

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
DATE_FROM = os.getenv("SALESRECEIPT_DATE_FROM")
DATE_TO = os.getenv("SALESRECEIPT_DATE_TO")

ENABLE_GLOBAL_SALESRECEIPT_DOCNUMBER_DEDUP=True

def apply_duplicate_docnumber_strategy():
        apply_duplicate_docnumber_strategy_dynamic(
        target_table="Map_SalesReceipt",
        schema=MAPPING_SCHEMA,
        check_against_tables=["Map_Bill","Map_Invoice","Map_VendorCredit","Map_JournalEntry","Map_Deposit","Map_Purchase"])

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
    Builds payload['TxnTaxDetail'] using header + up to 4 TaxLine components from Map_SalesReceipt columns.
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
    
    # Optional header total tax
    total_tax = _coerce_float(row.get("TxnTaxDetail.TotalTax"))
    if total_tax is not None:
        tax_detail["TotalTax"] = total_tax

    # Header-level tax code (prefer mapped)
    code = row.get("Mapped_TxnTaxCodeRef") or row.get("TxnTaxDetail_TxnTaxCodeRef_value")
    code_str = _value_str(code)
    if code_str:
        tax_detail["TxnTaxCodeRef"] = {"value": code_str}


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

# ---------------------------- ADDRESS HELPERS ----------------------------

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

def get_qbo_auth():
    env = os.getenv("QBO_ENVIRONMENT", "sandbox")
    base = "https://sandbox-quickbooks.api.intuit.com" if env == "sandbox" else "https://quickbooks.api.intuit.com"
    ctx = get_qbo_context_migration()
    realm = ctx["REALM_ID"]
    return f"{base}/v3/company/{realm}/salesreceipt", {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

def remove_null_fields(obj):
    if isinstance(obj, dict):
        return {k: remove_null_fields(v) for k, v in obj.items() if v is not None}
    elif isinstance(obj, list):
        return [remove_null_fields(i) for i in obj if i is not None]
    else:
        return obj

def ensure_mapping_table(SALESRECEIPT_DATE_FROM='1900-01-01', SALESRECEIPT_DATE_TO='2080-12-31'):
    """
    Build/refresh porter_entities_mapping.Map_SalesReceipt with header-level mappings.
    We intentionally DROP any raw source columns that contain ']' (e.g. TaxLine[0]...),
    after computing mapped values from them, to avoid SQL identifier escaping issues
    in downstream DDL/INSERT.
    """
    def _qid(name: str) -> str:
        return f"[{name}]"

    logger.info("🔧 Preparing Map_SalesReceipt from SalesReceipt source (optimized)")
    DATE_FROM = SALESRECEIPT_DATE_FROM
    DATE_TO   = SALESRECEIPT_DATE_TO

    # 1) Read source headers
    query = f"""
        SELECT * FROM {_qid(SOURCE_SCHEMA)}.{_qid('SalesReceipt')}
        WHERE 1=1
        {"AND TxnDate >= ?" if DATE_FROM else ""}
        {"AND TxnDate <= ?" if DATE_TO else ""}
    """
    params = tuple(p for p in [DATE_FROM, DATE_TO] if p)
    df = sql.fetch_table_with_params(query, params)
    if df.empty:
        logger.warning("⚠️ No records found in SalesReceipt source")
        return

    # 2) Initialize core migration fields
    df["Source_Id"] = df["Id"]
    required_cols = [
        "Target_Id","Porter_Status","Retry_Count","Failure_Reason","Payload_JSON",
        "Mapped_Customer","Mapped_Account","Mapped_PaymentMethod","Mapped_Project",
        "Mapped_Line_ItemRef","Mapped_Line_ClassRef","Mapped_Department",
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
            f"SELECT Source_Id, Target_Id FROM {_qid(MAPPING_SCHEMA)}.{_qid(table)}", tuple()
        )
        return dict(zip(t["Source_Id"], t["Target_Id"])) if not t.empty else {}

    customer_dict = load_map("Map_Customer")
    account_dict  = load_map("Map_Account")
    payment_dict  = load_map("Map_PaymentMethod")
    project_dict  = load_map("Map_Project") if "ProjectRef.value" in df.columns else {}
    item_dict     = load_map("Map_Item")
    class_dict    = load_map("Map_Class")
    department_dict = load_map("Map_Department") if sql.table_exists("Map_Department", MAPPING_SCHEMA) else {}
    taxcode_dict  = load_map("Map_TaxCode") if sql.table_exists("Map_TaxCode", MAPPING_SCHEMA) else {}
    taxrate_dict  = load_map("Map_TaxRate") if sql.table_exists("Map_TaxRate", MAPPING_SCHEMA) else {}

    # 4) Lines grouping
    lines = sql.fetch_table_with_params(
        f"SELECT * FROM {_qid(SOURCE_SCHEMA)}.{_qid('SalesReceipt_Line')}", tuple()
    )
    lines_grouped = lines.groupby("Parent_Id") if not lines.empty else {}

    # 5) Header mappings
    if "CustomerRef.value" in df.columns:
        df["Mapped_Customer"] = df["CustomerRef.value"].map(lambda x: customer_dict.get(x) if pd.notna(x) else None)
    if "DepositToAccountRef.value" in df.columns:
        df["Mapped_Account"] = df["DepositToAccountRef.value"].map(lambda x: account_dict.get(x) if pd.notna(x) else None)
    if "PaymentMethodRef.value" in df.columns:
        df["Mapped_PaymentMethod"] = df["PaymentMethodRef.value"].map(lambda x: payment_dict.get(x) if pd.notna(x) else None)
    if "ProjectRef.value" in df.columns:
        df["Mapped_Project"] = df["ProjectRef.value"].map(lambda x: project_dict.get(x) if pd.notna(x) else None)
    if "DepartmentRef.value" in df.columns:
        df["Mapped_Department"] = df["DepartmentRef.value"].map(lambda x: department_dict.get(x) if pd.notna(x) else None)

    # 6) Compute mapped tax fields from raw columns (then we can drop the raw cols)
    tc_src = "TxnTaxDetail.TxnTaxCodeRef.value"
    if tc_src in df.columns:
        df["Mapped_TxnTaxCodeRef"] = df[tc_src].map(lambda v: None if pd.isna(v) else taxcode_dict.get(v))

    tr_src = "TxnTaxDetail.TaxLine[0].TaxLineDetail.TaxRateRef.value"
    if tr_src in df.columns:
        df["Mapped_TxnTaxRateRef"] = df[tr_src].map(lambda v: None if pd.isna(v) else taxrate_dict.get(v))

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

    # 8) Build a **whitelist** of columns to persist — excludes any names with ']'
    #    (These are exactly what your build_payload() reads + control columns.)
    whitelist = [
        "Source_Id", "Id", "DocNumber", "TxnDate",
        "CurrencyRef.value",
        "Mapped_Customer", "Mapped_Account", "Mapped_PaymentMethod", "Mapped_Project",
        "Mapped_Department",
        "PrintStatus", "EmailStatus", "PaymentRefNum",
        "CustomerMemo.value", "TotalAmt", "Balance", "TxnTaxDetail.TotalTax",
        "BillAddr.Line1", "BillAddr.Line2", "BillAddr.Line3", "BillAddr.Line4", "BillAddr.Line5",
        "BillAddr.City", "BillAddr.Country", "BillAddr.CountrySubDivisionCode", "BillAddr.PostalCode",
        "ShipAddr.Line1", "ShipAddr.Line2", "ShipAddr.Line3", "ShipAddr.Line4", "ShipAddr.Line5",
        "ShipAddr.City", "ShipAddr.Country", "ShipAddr.CountrySubDivisionCode", "ShipAddr.PostalCode",
        "ShipFromAddr.Line1", "ShipFromAddr.Line2", "ShipFromAddr.Line3", "ShipFromAddr.Line4", "ShipFromAddr.Line5",
        "ShipFromAddr.City", "ShipFromAddr.Country", "ShipFromAddr.CountrySubDivisionCode", "ShipFromAddr.PostalCode",
        "Mapped_Line_ItemRef", "Mapped_Line_ClassRef",
        "Mapped_TxnTaxCodeRef", "Mapped_TxnTaxRateRef",
        "Target_Id", "Porter_Status", "Retry_Count", "Failure_Reason", "Payload_JSON",
    ]

    # Keep only columns that actually exist in df
    keep_cols = [c for c in whitelist if c in df.columns]
    df_out = df[keep_cols].copy()

    # 9) DDL (simple bracket quoting is fine now — no ']' in names)
    if not sql.table_exists("Map_SalesReceipt", MAPPING_SCHEMA):
        col_defs = ", ".join([f"{_qid(col)} NVARCHAR(MAX)" for col in df_out.columns])
        sql.run_query(f"CREATE TABLE {_qid(MAPPING_SCHEMA)}.{_qid('Map_SalesReceipt')} ({col_defs})")
    else:
        existing = sql.fetch_table("Map_SalesReceipt", MAPPING_SCHEMA)
        for col in df_out.columns:
            if col not in existing.columns:
                sql.run_query(
                    f"""
                    IF COL_LENGTH('{MAPPING_SCHEMA}.Map_SalesReceipt', '{col}') IS NULL
                    BEGIN
                        ALTER TABLE {_qid(MAPPING_SCHEMA)}.{_qid('Map_SalesReceipt')} ADD {_qid(col)} NVARCHAR(MAX);
                    END
                    """
                )
        sql.run_query(f"DELETE FROM {_qid(MAPPING_SCHEMA)}.{_qid('Map_SalesReceipt')}")

    # 10) Insert
    sql.insert_dataframe(df_out, "Map_SalesReceipt", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df_out)} mapped rows into Map_SalesReceipt (optimized)")


def get_lines(sid):
    """
    Fetches all line items related to a specific SalesReceipt by Source_Id.
    Args:
        sid (str or int): The Source_Id / Parent_Id of the SalesReceipt.
    Returns:
        pandas.DataFrame: Line item rows from the SalesReceipt_Line table ordered by LineNum.
    """
    return sql.fetch_table_with_params(f"""
        SELECT * FROM [{SOURCE_SCHEMA}].[SalesReceipt_Line]
        WHERE Parent_Id = ?
        ORDER BY LineNum ASC
    """, (sid,))

def build_payload(row, lines):
    """
    Constructs a QBO-compliant SalesReceipt JSON payload for the given header and lines.
    Handles dynamic support for SalesItemLineDetail and AccountBasedExpenseLineDetail based
    on the DetailType in each line. Also removes null and unsupported properties from the output.
    Args:
        row (pandas.Series): A single record from Map_SalesReceipt.
        lines (pandas.DataFrame): The corresponding line items from Map_SalesReceipt_Line.
    Returns:
        dict or None: A complete payload dictionary ready to POST to QBO, or None if invalid.
    """
    # Preload mapping dicts for line-level lookups (avoid per-row SQL)
    if not hasattr(build_payload, "_item_dict"):
        build_payload._item_dict = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Item]", tuple()).set_index("Source_Id")["Target_Id"].to_dict()
    if not hasattr(build_payload, "_class_dict"):
        build_payload._class_dict = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Class]", tuple()).set_index("Source_Id")["Target_Id"].to_dict()
    item_dict = build_payload._item_dict
    class_dict = build_payload._class_dict

    # Use index-based lookups for performance
    row_get = row.get if hasattr(row, "get") else lambda k: row[k] if k in row else None
    payload = {
        "domain": "QBO",
        "DocNumber": row_get("Duplicate_Docnumber") or row_get("DocNumber"),
        "TxnDate": row_get("TxnDate"),
        "CustomerRef": {"value": str(row_get("Mapped_Customer"))} if row_get("Mapped_Customer") else None,
        "CurrencyRef": {"value": row_get("CurrencyRef.value") or "USD"},
        "PrintStatus": row_get("PrintStatus"),
        "EmailStatus": row_get("EmailStatus"),
        "PaymentRefNum": row_get("PaymentRefNum"),
        "DepositToAccountRef": {"value": str(row_get("Mapped_Account"))} if row_get("Mapped_Account") else None,
        "PaymentMethodRef": {"value": str(row_get("Mapped_PaymentMethod"))} if row_get("Mapped_PaymentMethod") else None,
        "CustomerMemo": {"value": row_get("CustomerMemo.value")} if row_get("CustomerMemo.value") else None,
        "Balance": safe_float(row_get("Balance")),
        "Line": []
    }
    if row_get("Mapped_Project"):
        payload["ProjectRef"] = {"value": str(row_get("Mapped_Project"))}
    if row_get("Mapped_Department"):
        payload["DepartmentRef"] = {"value": str(row_get("Mapped_Department"))}

    # Optional addresses - using reusable helper
    _add_addr_block(payload, row_get, "BillAddr")
    _add_addr_block(payload, row_get, "ShipAddr")
    _add_addr_block(payload, row_get, "ShipFromAddr")

    # Build lines efficiently
    has_valid_line = False
    for _, ln in lines.iterrows():
        ln_get = ln.get if hasattr(ln, "get") else lambda k: ln[k] if k in ln else None
        detail_type = ln_get("DetailType")
        if detail_type == "SalesItemLineDetail":
            item = ln_get("Mapped_Item")
            if not item:
                item_src = ln_get("SalesItemLineDetail.ItemRef.value")
                item = item_dict.get(item_src)
            line = {
                "LineNum": int(float(ln_get("LineNum") or 1)),
                "Amount": safe_float(ln_get("Amount")),
                "Description": ln_get("Description"),
                "DetailType": "SalesItemLineDetail",
                "SalesItemLineDetail": {
                    "ItemRef": {"value": str(item)} if item else None,
                    "Qty": safe_float(ln_get("SalesItemLineDetail.Qty")),
                    "UnitPrice": safe_float(ln_get("SalesItemLineDetail.UnitPrice")),
                    "ServiceDate": ln_get("SalesItemLineDetail.ServiceDate"),
                    "TaxCodeRef": {"value": ln_get("SalesItemLineDetail.TaxCodeRef.value") or "NON"}
                }
            }
            class_val = ln_get("Mapped_Class")
            if not class_val:
                class_src = ln_get("SalesItemLineDetail.ClassRef.value")
                class_val = class_dict.get(class_src) if class_src else None
            if class_val:
                line["SalesItemLineDetail"]["ClassRef"] = {"value": str(class_val)}
            # Remove nulls from SalesItemLineDetail
            line["SalesItemLineDetail"] = {k: v for k, v in line["SalesItemLineDetail"].items() if v is not None}
            payload["Line"].append(line)
            has_valid_line = True
        elif detail_type == "AccountBasedExpenseLineDetail":
            detail = {
                "AccountRef": {"value": str(ln_get("Mapped_Account"))} if ln_get("Mapped_Account") else None,
                "TaxCodeRef": {"value": ln_get("SalesItemLineDetail.TaxCodeRef.value") or "NON"}
            }
            class_val = ln_get("Mapped_Class")
            if not class_val:
                class_src = ln_get("SalesItemLineDetail.ClassRef.value")
                class_val = class_dict.get(class_src) if class_src else None
            if class_val:
                detail["ClassRef"] = {"value": str(class_val)}
            # Remove nulls from detail
            detail = {k: v for k, v in detail.items() if v is not None}
            payload["Line"].append({
                "LineNum": int(float(ln_get("LineNum") or 1)),
                "Amount": safe_float(ln_get("Amount")),
                "Description": ln_get("Description"),
                "DetailType": "AccountBasedExpenseLineDetail",
                "AccountBasedExpenseLineDetail": detail
            })
            has_valid_line = True
        elif detail_type == "SubTotalLineDetail":
            payload["Line"].append({
                "DetailType": "SubTotalLineDetail",
                "Amount": safe_float(ln_get("Amount")),
                "SubTotalLineDetail": {}
            })
    if not has_valid_line:
        return None
    # Remove unsupported or null properties
    unsupported_fields = ["Balance", "PaymentRefNum", "CustomerMemo"]
    for field in unsupported_fields:
        if field in payload and (payload[field] is None or field == "Balance"):
            del payload[field]
    
    # NEW: Add tax detail if available
    add_txn_tax_detail_from_row(payload, row)
    
    # Remove null fields recursively
    return remove_null_fields(payload)

def generate_payloads(batch_size=500):
    """
    Iterates over all SalesReceipts with status 'Ready' and generates valid JSON payloads.
    Fetches line-level data, builds a payload using `build_payload()`, and stores the
    formatted JSON in Map_SalesReceipt.Payload_JSON.
    Args:
        batch_size (int): The number of records to process in each batch (default: 500).
    """
    logger.info("⚙️ Generating SalesReceipt payloads (optimized batch)...")
    while True:
        df = sql.fetch_table_with_params(f"""
            SELECT TOP {batch_size} * FROM [{MAPPING_SCHEMA}].[Map_SalesReceipt]
            WHERE Porter_Status = 'Ready'
              AND (Payload_JSON IS NULL OR Payload_JSON = '' OR Payload_JSON = 'null')
        """, ())
        if df.empty:
            logger.info("✅ All payloads generated.")
            break
        # Preload all lines for this batch for vectorized access
        sids = df["Source_Id"].tolist()
        all_lines = sql.fetch_table_with_params(f"SELECT * FROM [{SOURCE_SCHEMA}].[SalesReceipt_Line] WHERE Parent_Id IN ({','.join(['?']*len(sids))}) ORDER BY Parent_Id, LineNum", tuple(sids)) if sids else pd.DataFrame()
        lines_grouped = all_lines.groupby("Parent_Id") if not all_lines.empty else {}
        for idx, row in df.iterrows():
            sid = row["Source_Id"]
            lines = lines_grouped.get_group(sid) if sid in lines_grouped.groups else pd.DataFrame()
            if lines.empty:
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_SalesReceipt]
                    SET Porter_Status='Failed', Failure_Reason='No lines found'
                    WHERE Source_Id = ?
                """, (sid,))
                continue
            payload = build_payload(row, lines)
            if not payload:
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_SalesReceipt]
                    SET Porter_Status='Failed', Failure_Reason='No valid SalesItemLineDetail lines'
                    WHERE Source_Id = ?
                """, (sid,))
                continue
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_SalesReceipt]
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

# Convert entity endpoint → /batch endpoint
if "_derive_batch_url" not in globals():
    def _derive_batch_url(entity_url: str, entity_name: str) -> str:
        """
        entity_url: https://.../v3/company/<realm>/<Entity>?minorversion=XX
        entity_name: "SalesReceipt" | "Invoice" | "Bill" | ...
        -> https://.../v3/company/<realm>/batch?minorversion=XX
        """
        qpos = entity_url.find("?")
        query = entity_url[qpos:] if qpos != -1 else ""
        path = entity_url[:qpos] if qpos != -1 else entity_url

        seg = f"/{entity_name}".lower()
        lower = path.lower()
        if lower.endswith(seg):
            path = path[: -len(seg)]
        return f"{path}/batch{query}"

# Ensure a shared HTTP session exists
try:
    session
except NameError:
    import requests
    session = requests.Session()

# ============================ Batch Poster & Updates ==============================
def _post_batch_salesreceipts(eligible_batch, url, headers, timeout=40, post_batch_limit=20, max_manual_retries=1):
    """
    Single-threaded batch posting for SalesReceipts via QBO /batch.
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

    batch_url = _derive_batch_url(url, "SalesReceipt")

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
                    {"bId": str(sid), "operation": "create", "SalesReceipt": payload}
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
                        ent = item.get("SalesReceipt")
                        if ent and "Id" in ent:
                            qid = ent["Id"]
                            successes.append((qid, bid))
                            logger.info(f"✅ SalesReceipt {bid} → QBO {qid}")
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
                        logger.error(f"❌ SalesReceipt {bid} failed: {reason}")

                    # Any missing responses → mark failed
                    for sid, _ in chunk:
                        if str(sid) not in seen:
                            failures.append(("No response for bId", sid))
                            logger.error(f"❌ No response for SalesReceipt {sid}")
                    break

                elif sc in (401, 403) and not attempted_refresh:
                    logger.warning(f"🔐 {sc} on SalesReceipt batch ({len(chunk)} items); refreshing token and retrying once...")
                    try:
                        auto_refresh_token_if_needed()
                    except Exception:
                        pass
                    url, headers = get_qbo_auth()
                    batch_url = _derive_batch_url(url, "SalesReceipt")
                    attempted_refresh = True
                    continue

                else:
                    reason = (resp.text or f"HTTP {sc}")[:1000]
                    for sid, _ in chunk:
                        failures.append((reason, sid))
                    logger.error(f"❌ SalesReceipt batch failed ({len(chunk)} items): {reason}")
                    break

            except Exception as e:
                reason = f"Batch exception: {e}"
                for sid, _ in chunk:
                    failures.append((reason, sid))
                logger.exception(f"❌ Exception during SalesReceipt batch POST ({len(chunk)} items)")
                break

    return successes, failures

def _apply_batch_updates_salesreceipt(successes, failures):
    """
    Set-based updates for Map_SalesReceipt.
    """
    try:
        if successes:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_SalesReceipt] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                [(qid, sid) for qid, sid in successes]
            )
        if failures:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_SalesReceipt] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                [(reason, sid) for reason, sid in failures]
            )
    except NameError:
        # Fallback to per-row updates if executemany helper is not available
        for qid, sid in successes or []:
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_SalesReceipt] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                (qid, sid)
            )
        for reason, sid in failures or []:
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_SalesReceipt] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                (reason, sid)
            )

# ============================ Updated Posting APIs ===============================
def post_salesreceipt(row):
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
            f"UPDATE [{MAPPING_SCHEMA}].[Map_SalesReceipt] "
            f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count, 0) + 1, Failure_Reason = ? "
            f"WHERE Source_Id = ?",
            (f"Bad JSON: {e}", sid)
        )
        return

    try:
        resp = session.post(url, headers=headers, json=payload, timeout=20)
        if resp.status_code == 200:
            qid = (resp.json().get("SalesReceipt") or {}).get("Id")
            if qid:
                sql.run_query(
                    f"UPDATE [{MAPPING_SCHEMA}].[Map_SalesReceipt] "
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
            resp2 = session.post(url, headers=headers, json=payload, timeout=20)
            if resp2.status_code == 200:
                qid = (resp2.json().get("SalesReceipt") or {}).get("Id")
                if qid:
                    sql.run_query(
                        f"UPDATE [{MAPPING_SCHEMA}].[Map_SalesReceipt] "
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
            f"UPDATE [{MAPPING_SCHEMA}].[Map_SalesReceipt] "
            f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count, 0) + 1, Failure_Reason = ? "
            f"WHERE Source_Id = ?",
            (reason, sid)
        )
    except Exception as e:
        sql.run_query(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_SalesReceipt] "
            f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count, 0) + 1, Failure_Reason = ? "
            f"WHERE Source_Id = ?",
            (str(e), sid)
        )

def migrate_salesreceipts(SALESRECEIPT_DATE_FROM,SALESRECEIPT_DATE_TO):
    """
    Full end-to-end migration for SalesReceipts using QBO /batch.
    """
    print("\n🚀 Starting SalesReceipt Migration\n" + "=" * 40)
    ensure_mapping_table(SALESRECEIPT_DATE_FROM,SALESRECEIPT_DATE_TO)

    if 'ENABLE_GLOBAL_SALESRECEIPT_DOCNUMBER_DEDUP' in globals() and ENABLE_GLOBAL_SALESRECEIPT_DOCNUMBER_DEDUP:
        apply_duplicate_docnumber_strategy()

    generate_payloads()

    rows = sql.fetch_table("Map_SalesReceipt", MAPPING_SCHEMA)
    eligible = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("⚠️ No SalesReceipts to process.")
        return

    url, headers = get_qbo_auth()
    select_batch_size = 300   # DB slice size
    post_batch_limit  = 8    # items per QBO batch call (<=30). Set to 3 for exactly-3-at-a-time.
    timeout           = 40

    total = len(eligible)
    logger.info(f"📤 Posting {total} SalesReceipt(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        batch = eligible.iloc[i:i + select_batch_size]
        successes, failures = _post_batch_salesreceipts(
            batch, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates_salesreceipt(successes, failures)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    # Optional one-pass fallback retry for failed rows without Target_Id
    failed_df = sql.fetch_table("Map_SalesReceipt", MAPPING_SCHEMA)
    failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
    if not failed_records.empty:
        logger.info(f"🔁 Reprocessing {len(failed_records)} failed SalesReceipts (single-record fallback)...")
        for _, row in failed_records.iterrows():
            post_salesreceipt(row)

    print("\n🏁 SalesReceipt migration completed.")

def resume_or_post_salesreceipts(SALESRECEIPT_DATE_FROM,SALESRECEIPT_DATE_TO):
    """
    Resume posting pending/failed SalesReceipts using /batch.
    """
    print("\n🔁 Resuming SalesReceipt Migration\n" + "=" * 45)

    if not sql.table_exists("Map_SalesReceipt", MAPPING_SCHEMA):
        migrate_salesreceipts(SALESRECEIPT_DATE_FROM,SALESRECEIPT_DATE_TO)
        return

    mapped = sql.fetch_table("Map_SalesReceipt", MAPPING_SCHEMA)
    source = sql.fetch_table("SalesReceipt", SOURCE_SCHEMA)

    if len(mapped) != len(source):
        ensure_mapping_table(SALESRECEIPT_DATE_FROM,SALESRECEIPT_DATE_TO)

    generate_payloads()

    eligible = sql.fetch_table_with_params(
        f"""
        SELECT * FROM [{MAPPING_SCHEMA}].[Map_SalesReceipt]
        WHERE Porter_Status IN ('Ready', 'Failed')
          AND Payload_JSON IS NOT NULL AND Payload_JSON <> ''
        """,
        ()
    )
    if eligible.empty:
        logger.info("⚠️ No SalesReceipts to post.")
        return

    url, headers = get_qbo_auth()
    select_batch_size = 300
    post_batch_limit  = 8
    timeout           = 40

    total = len(eligible)
    logger.info(f"📤 Posting {total} SalesReceipt(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        batch = eligible.iloc[i:i + select_batch_size]
        successes, failures = _post_batch_salesreceipts(
            batch, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates_salesreceipt(successes, failures)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    print("\n🏁 SalesReceipt posting completed.")

# ============================ SMART SalesReceipt Migration ============================
def smart_salesreceipt_migration(SALESRECEIPT_DATE_FROM, SALESRECEIPT_DATE_TO):
    """
    Smart migration entrypoint for SalesReceipts:
    - If Map_SalesReceipt table exists and row count matches SalesReceipt table, resume/post.
    - If table missing or row count mismatch, perform full migration.
    - After either path, reprocess failed records with null Target_Id one more time.
    """
    logger.info("🔎 Running smart_salesreceipt_migration...")
    full_process = False

    if sql.table_exists("Map_SalesReceipt", MAPPING_SCHEMA):
        mapped_df = sql.fetch_table("Map_SalesReceipt", MAPPING_SCHEMA)
        source_df = sql.fetch_table("SalesReceipt", SOURCE_SCHEMA)

        if len(mapped_df) == len(source_df):
            logger.info("✅ Table exists and row count matches. Resuming/posting SalesReceipts.")
            resume_or_post_salesreceipts(SALESRECEIPT_DATE_FROM, SALESRECEIPT_DATE_TO)

            # Retry failed ones after resume
            failed_df = sql.fetch_table("Map_SalesReceipt", MAPPING_SCHEMA)
            failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
            if not failed_records.empty:
                logger.info(f"🔁 Reprocessing {len(failed_records)} failed SalesReceipts with null Target_Id after main migration...")
                for _, row in failed_records.iterrows():
                    post_salesreceipt(row)
            return
        else:
            logger.warning(f"❌ Row count mismatch: Map_SalesReceipt={len(mapped_df)}, SalesReceipt={len(source_df)}. Running full migration.")
            full_process = True
    else:
        logger.warning("❌ Map_SalesReceipt table does not exist. Running full migration.")
        full_process = True

    # ----- Full migration path -----
    if full_process:
        ensure_mapping_table(SALESRECEIPT_DATE_FROM, SALESRECEIPT_DATE_TO)

        if 'ENABLE_GLOBAL_SALESRECEIPT_DOCNUMBER_DEDUP' in globals() and ENABLE_GLOBAL_SALESRECEIPT_DOCNUMBER_DEDUP:
            apply_duplicate_docnumber_strategy()
            try:
                sql.clear_cache()
            except Exception:
                pass
            import time; time.sleep(1)

        generate_payloads()

        post_query = f"SELECT * FROM {MAPPING_SCHEMA}.Map_SalesReceipt WHERE Porter_Status = 'Ready'"
        post_rows = sql.fetch_table_with_params(post_query, tuple())
        if post_rows.empty:
            logger.warning("⚠️ No SalesReceipts with built JSON to post.")
            return

        url, headers = get_qbo_auth()
        select_batch_size = 300
        post_batch_limit  = 5
        timeout           = 40

        total = len(post_rows)
        logger.info(f"📤 Posting {total} SalesReceipt record(s) via QBO Batch API (limit {post_batch_limit}/call)...")

        for i in range(0, total, select_batch_size):
            slice_df = post_rows.iloc[i:i + select_batch_size]
            successes, failures = _post_batch_salesreceipts(
                slice_df, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
            )
            _apply_batch_updates_salesreceipt(successes, failures)

            done = min(i + select_batch_size, total)
            logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

        # Retry failed ones after full migration
        failed_df = sql.fetch_table("Map_SalesReceipt", MAPPING_SCHEMA)
        failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
        if not failed_records.empty:
            logger.info(f"🔁 Reprocessing {len(failed_records)} failed SalesReceipts with null Target_Id after main migration...")
            for _, row in failed_records.iterrows():
                post_salesreceipt(row)

        logger.info("🏁 SMART SalesReceipt migration complete.")


