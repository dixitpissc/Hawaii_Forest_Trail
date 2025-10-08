"""
Sequence : 12
Module: Invoice_migrator.py
Author: Dixit Prajapati
Created: 2025-09-12
Description: Handles migration of Invoice records from QBO to QBO.
Production : Ready
Phase : 02 - Multi User + Tax
"""

import os, json, requests, pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from storage.sqlserver.sql import insert_invoice_map_dataframe
from utils.token_refresher import auto_refresh_token_if_needed, get_qbo_context_migration,MINOR_VERSION 
from utils.apply_duplicate_docnumber import apply_duplicate_docnumber_strategy_dynamic
from utils.log_timer import global_logger as logger
from config.mapping.invoice_mapping import (
    INVOICE_HEADER_MAPPING,
    SALES_ITEM_LINE_MAPPING,
    GROUP_LINE_MAPPING,
    DISCOUNT_LINE_MAPPING,
    SUBTOTAL_LINE_MAPPING
)
from utils.payload_cleaner import clean_payload
import re

load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = "porter_entities_mapping"
ENABLE_TAXLINE_MAPPING   = False

# Centralized QBO context logic (similar to class migrator)
def get_qbo_auth_invoice():
    """
    Returns QBO API endpoints and headers for invoice migration using centralized context logic.
    """
    ctx = get_qbo_context_migration()
    base = ctx["BASE_URL"]
    realm_id = ctx["REALM_ID"]
    post_url = f"{base}/v3/company/{realm_id}/invoice?minorversion=75"
    headers = {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    return post_url, headers

def safe_float(val):
    """
    Safely converts a value to float. Returns 0.0 on failure.
    Args:
        val (Any): The value to convert.
    Returns:
        float: Converted float value or 0.0 if conversion fails.
    """
    try: return float(val)
    except: return 0.0

def ensure_mapping_table(INVOICE_DATE_FROM="1900-01-01",INVOICE_DATE_TO="2080-12-31"):
    """
    Ensures the Map_Invoice table is initialized with invoice data within a specified date range.
    Process:
    - Reads INVOICE_DATE_FROM and INVOICE_DATE_TO from the environment.
    - Filters Invoice records based on TxnDate.
    - Populates mapping columns including Target_Item_Refs, Mapped_CustomerRef, Mapped_TermRef, and Mapped_PaymentMethodRef.
    - Creates or resets the porter_entities_mapping.Map_Invoice table with enriched data.
    """

    date_from = INVOICE_DATE_FROM
    date_to = INVOICE_DATE_TO
    query = f"""
        SELECT * FROM [{SOURCE_SCHEMA}].[Invoice]
        WHERE 1=1
        {"AND TxnDate >= ?" if date_from else ""}
        {"AND TxnDate <= ?" if date_to else ""}
    """
    params = tuple(p for p in [date_from, date_to] if p)
    df = sql.fetch_table_with_params(query, params)
    if df.empty:
        logger.info("⚠️ No invoice records found for the specified date range.")
        return
    df["Source_Id"] = df["Id"]
    df["Target_Id"] = None
    df["Porter_Status"] = "Ready"
    df["Retry_Count"] = 0
    df["Failure_Reason"] = None
    required_cols = ["Payload_JSON", "Mapped_CustomerRef", "Mapped_TermRef", "Mapped_PaymentMethodRef", "Target_Item_Refs"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Bulk fetch mapping tables
    customer_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Customer]", tuple())
    customer_dict = dict(zip(customer_map["Source_Id"], customer_map["Target_Id"]))

    # term_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Term]", tuple())
    # term_dict = dict(zip(term_map["Source_Id"], term_map["Target_Id"]))

    # payment_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_PaymentMethod]", tuple())
    # payment_dict = dict(zip(payment_map["Source_Id"], payment_map["Target_Id"]))

    # Term mapping (optional)
    if sql.table_exists("Map_Term", MAPPING_SCHEMA):
        term_map = sql.fetch_table_with_params(
            f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Term]", tuple()
        )
        term_dict = dict(zip(term_map["Source_Id"], term_map["Target_Id"]))
    else:
        term_dict = {}

    # PaymentMethod mapping (optional)
    if sql.table_exists("Map_PaymentMethod", MAPPING_SCHEMA):
        payment_map = sql.fetch_table_with_params(
            f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_PaymentMethod]", tuple()
        )
        payment_dict = dict(zip(payment_map["Source_Id"], payment_map["Target_Id"]))
    else:
        payment_dict = {}

    item_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Item]", tuple())
    item_dict = dict(zip(item_map["Source_Id"], item_map["Target_Id"]))

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


    # NEW: Department map (optional)
    if sql.table_exists("Map_Department", MAPPING_SCHEMA):
        dept_map = sql.fetch_table_with_params(
            f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Department]", tuple()
        )
        dept_dict = dict(zip(dept_map["Source_Id"], dept_map["Target_Id"]))
    else:
        dept_dict = {}


    # Bulk fetch all invoice lines
    invoice_lines = sql.fetch_table_with_params(f"SELECT * FROM [{SOURCE_SCHEMA}].[Invoice_Line]", tuple())
    invoice_lines_grouped = invoice_lines.groupby("Parent_Id")

    # Map CustomerRef
    df["Mapped_CustomerRef"] = df[INVOICE_HEADER_MAPPING["CustomerRef.value"]].map(lambda x: customer_dict.get(x) if pd.notna(x) else None)

    # Map SalesTermRef
    df["Mapped_TermRef"] = df.get("SalesTermRef.value", None)
    if "Mapped_TermRef" in df.columns:
        df["Mapped_TermRef"] = df["Mapped_TermRef"].map(lambda x: term_dict.get(x) if pd.notna(x) else None)

    # Map PaymentMethodRef
    df["Mapped_PaymentMethodRef"] = df.get("PaymentMethodRef.value", None)
    if "Mapped_PaymentMethodRef" in df.columns:
        df["Mapped_PaymentMethodRef"] = df["Mapped_PaymentMethodRef"].map(lambda x: payment_dict.get(x) if pd.notna(x) else None)

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

    # NEW: Header DepartmentRef (source column DepartmentRef.value)
    src_dept_col = "DepartmentRef.value"
    if src_dept_col in df.columns:
        df["Mapped_DepartmentRef"] = df[src_dept_col].map(
            lambda x: dept_dict.get(x) if pd.notna(x) else None
        )
    else:
        df["Mapped_DepartmentRef"] = None

    # Map related items for each invoice
    def get_target_items_bulk(invoice_id):
        target_items = []
        if invoice_id in invoice_lines_grouped.groups:
            lines = invoice_lines_grouped.get_group(invoice_id)
            for _, ln in lines.iterrows():
                if ln.get("DetailType") == "SalesItemLineDetail":
                    item_ref = ln.get(SALES_ITEM_LINE_MAPPING["SalesItemLineDetail.ItemRef.value"])
                    item_id = item_dict.get(item_ref)
                    if item_id:
                        target_items.append(str(item_id))
        return ";".join(target_items)

    df["Target_Item_Refs"] = df["Id"].map(get_target_items_bulk)

    # Clear existing and insert fresh
    if sql.table_exists("Map_Invoice", MAPPING_SCHEMA):
        sql.run_query(f"TRUNCATE TABLE [{MAPPING_SCHEMA}].[Map_Invoice]")

    insert_invoice_map_dataframe(df, "Map_Invoice", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df)} rows into {MAPPING_SCHEMA}.Map_Invoice")
    
def get_lines(invoice_id):
    """
    Fetches all invoice line items related to a given invoice ID.
    Args:
        invoice_id (int or str): The source invoice ID.
    Returns:
        pd.DataFrame: DataFrame of invoice line records.
    """
    return sql.fetch_table_with_params(
        f"SELECT * FROM [{SOURCE_SCHEMA}].[Invoice_Line] WHERE Parent_Id = ?", (invoice_id,)
    )

def _first_nonnull(row, *names):
    """Return the first non-null value among the provided column names."""
    for n in names:
        if n in row.index:
            v = row.get(n)
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                return v
    return None

def _to_bool(v):
    if isinstance(v, bool): return v
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "t")

def _coerce_bool(v):
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):  return True
    if s in ("false", "0", "no"):  return False
    return None

def _coerce_float(v):
    try:
        if v is None or v == "" or str(v).lower() == "null":
            return None
        return float(v)
    except Exception:
        return None

def _value_str(v):
    if v is None or v == "" or str(v).lower() == "null":
        return None
    return str(v)

def add_txn_tax_detail_from_row(payload: dict, row) -> None:
    """
    Builds payload['TxnTaxDetail'] using header + up to 4 TaxLine components from Map_Invoice columns.
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

# Helper
def _maybe_bool(v):
    return None if v is None or (isinstance(v, float) and pd.isna(v)) else bool(v)

def _nonempty_str(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return s or None

def _first_present(ln: pd.Series, candidates: list[str]):
    """Return the first non-empty value from the candidate column names present on this line row."""
    for c in candidates:
        if c in ln.index:
            v = _nonempty_str(ln.get(c))
            if v is not None:
                return v
    return None

def _collect_linked_estimates(lines_df: pd.DataFrame, estimate_map: dict[str, str]) -> list[dict]:
    """
    Scan line columns like:
      [LinkedTxn[0]].TxnId], [LinkedTxn[0]].TxnType]
      [LinkedTxn[1]].TxnId], [LinkedTxn[1]].TxnType]
    (Also tolerates variants without trailing ']' or without the outer '[' ']'.)
    Map source Estimate Id -> Target_Id via Map_Estimate and return LinkedTxn list.
    """
    linked_pairs = set()  # to dedupe (TxnId, TxnType)

    # Find all LinkedTxn indices present in the columns
    idx_pat = re.compile(r'^\[?LinkedTxn\[(\d+)\]\]\.?Txn(Id|Type)\]?$|^LinkedTxn\[(\d+)\]\.(TxnId|TxnType)$')
    found_indices = set()
    for col in lines_df.columns:
        if not isinstance(col, str):
            continue
        m = idx_pat.match(col)
        if m:
            # index can appear in group(1) or group(3) depending on which branch matched
            idx = m.group(1) or m.group(3)
            if idx is not None:
                found_indices.add(int(idx))

    if not found_indices:
        return []

    # Build per-line, per-index pairs
    for _, ln in lines_df.iterrows():
        for i in sorted(found_indices):
            # be liberal with possible spellings
            id_candidates = [
                f"[LinkedTxn[{i}]].TxnId]",
                f"[LinkedTxn[{i}]].TxnId",
                f"LinkedTxn[{i}].TxnId",
            ]
            type_candidates = [
                f"[LinkedTxn[{i}]].TxnType]",
                f"[LinkedTxn[{i}]].TxnType",
                f"LinkedTxn[{i}].TxnType",
            ]

            src_est_id = _first_present(ln, id_candidates)
            if not src_est_id:
                continue

            txn_type = _first_present(ln, type_candidates) or "Estimate"  # default to Estimate

            tgt = estimate_map.get(src_est_id)
            if not tgt:
                # No mapping for this source Estimate Id -> skip but log
                logger.debug(f"🔎 Missing Map_Estimate for Source_Id={src_est_id}; skipping LinkedTxn")
                continue

            linked_pairs.add((str(tgt), txn_type))

    # Convert unique pairs into QBO LinkedTxn shape
    return [{"TxnId": txn_id, "TxnType": txn_type} for (txn_id, txn_type) in linked_pairs]

def build_payload(row, lines):
    """
    Builds a QBO-compatible JSON payload for a given invoice record and its associated lines.
    Args:
        row (pd.Series): Header row from the Map_Invoice table.
        lines (pd.DataFrame): Line items associated with the invoice.
    Returns:
        dict: JSON-serializable dictionary representing the QBO invoice payload.
    """
    # Pre-fetch all needed mappings for batch processing
    item_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Item]", tuple())
    item_dict = dict(zip(item_map["Source_Id"], item_map["Target_Id"]))

    # Optional Class map load
    if sql.table_exists("Map_Class", MAPPING_SCHEMA):
        class_map = sql.fetch_table_with_params(
            f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Class]", tuple()
        )
        class_dict = dict(zip(class_map["Source_Id"], class_map["Target_Id"]))
    else:
        class_dict = {}  # keep empty dict if table not present

    # class_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Class]", tuple())
    # class_dict = dict(zip(class_map["Source_Id"], class_map["Target_Id"]))
    
    account_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Account]", tuple())
    account_dict = dict(zip(account_map["Source_Id"], account_map["Target_Id"]))

    # ✅ NEW: Estimate map for LinkedTxn
    estimate_dict = {}
    if sql.table_exists("Map_Estimate", MAPPING_SCHEMA):
        est_map = sql.fetch_table_with_params(
            f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Estimate]",
            tuple()
        )
        estimate_dict = dict(zip(est_map["Source_Id"], est_map["Target_Id"]))

    payload = {
        "DocNumber": row.get("Duplicate_Docnumber") or row.get(INVOICE_HEADER_MAPPING["DocNumber"]),
        "TxnDate": row.get(INVOICE_HEADER_MAPPING["TxnDate"]),
        "DueDate": row.get(INVOICE_HEADER_MAPPING["DueDate"]),
        "CustomerRef": {"value": str(row.get("Mapped_CustomerRef"))},
        "EmailStatus": row.get(INVOICE_HEADER_MAPPING["EmailStatus"]),
        "ApplyTaxAfterDiscount": bool(row.get(INVOICE_HEADER_MAPPING["ApplyTaxAfterDiscount"])),
        "PrintStatus": row.get(INVOICE_HEADER_MAPPING["PrintStatus"]),
        "EmailStatus": row.get(INVOICE_HEADER_MAPPING["EmailStatus"]),
        "AllowOnlineACHPayment": bool(row.get(INVOICE_HEADER_MAPPING["AllowOnlineACHPayment"])),
        "AllowOnlineCreditCardPayment": bool(row.get(INVOICE_HEADER_MAPPING["AllowOnlineCreditCardPayment"])),
        "AllowIPNPayment": bool(row.get(INVOICE_HEADER_MAPPING["AllowIPNPayment"])),
        # "TotalAmt": safe_float(row.get("TotalAmt")),
        # "HomeTotalAmt": safe_float(row.get("HomeTotalAmt")),
        # "GlobalTaxCalculation": row.get("GlobalTaxCalculation", "")
    }
    # prune Nones
    payload = {k: v for k, v in payload.items() if v is not None}

    # Currency (include if you have it mapped)
    cur_val = _nonempty_str(row.get("CurrencyRef_value"))  # or your mapping
    if cur_val:
        payload["CurrencyRef"] = {"value": cur_val}

    # GlobalTaxCalculation is an enum string (TaxExcluded | TaxInclusive | NotApplicable)
    gtc = _nonempty_str(row.get("GlobalTaxCalculation"))
    if gtc:
        payload["GlobalTaxCalculation"] = gtc

    # BillEmail must be an object with Address
    if payload.get("EmailStatus") == "NeedToSend":
        bill_email = _nonempty_str(row.get("BillEmail_Address"))
        if bill_email:
            payload["BillEmail"] = {"Address": bill_email}



    def _nonempty(val):
        if val is None or pd.isna(val):
            return None
        s = str(val).strip()
        return s if s else None

    bill = {
        "Line1": _nonempty(row.get("BillAddr_Line1")),
        "Line2": _nonempty(row.get("BillAddr_Line2")),
        "Line3": _nonempty(row.get("BillAddr_Line3")),
        "Line4": _nonempty(row.get("BillAddr_Line4")),
        "Line5": _nonempty(row.get("BillAddr_Line5")),
        "City":  _nonempty(row.get("BillAddr_City")),
        "Country": _nonempty(row.get("BillAddr_Country")),
        "CountrySubDivisionCode": _nonempty(row.get("BillAddr_CountrySubDivisionCode")),
        "PostalCode": _nonempty(row.get("BillAddr_PostalCode")),
    }
    bill = {k: v for k, v in bill.items() if v is not None}
    if bill:
        payload["BillAddr"] = bill

    ship = {
        "Line1": _nonempty(row.get("ShipAddr_Line1")),   # ← string key, not list
        "Line2": _nonempty(row.get("ShipAddr_Line2")),
        "Line3": _nonempty(row.get("ShipAddr_Line3")),
        "Line4": _nonempty(row.get("ShipAddr_Line4")),
        "Line5": _nonempty(row.get("ShipAddr_Line5")),
        "City":  _nonempty(row.get("ShipAddr_City")),
        "Country": _nonempty(row.get("ShipAddr_Country")),
        "CountrySubDivisionCode": _nonempty(row.get("ShipAddr_CountrySubDivisionCode")),
        "PostalCode": _nonempty(row.get("ShipAddr_PostalCode")),
    }
    ship = {k: v for k, v in ship.items() if v is not None}
    if ship:
        payload["ShipAddr"] = ship

    if row.get("Mapped_TermRef"):
        payload["SalesTermRef"] = {"value": str(row.get("Mapped_TermRef"))}
    if row.get("Mapped_PaymentMethodRef"):
        payload["PaymentMethodRef"] = {"value": str(row.get("Mapped_PaymentMethodRef"))}

    # 👉 NEW: Add ExchangeRate if present in source
    if "ExchangeRate" in row.index and pd.notna(row.get("ExchangeRate")):
        payload["ExchangeRate"] = safe_float(row.get("ExchangeRate"))

    # if "GlobalTaxCalculation" in row.index and pd.notna(row.get("GlobalTaxCalculation")):
    #     payload["GlobalTaxCalculation"] = row.get("GlobalTaxCalculation")

    if "ShipDate" in row.index and pd.notna(row.get("ShipDate")):
            payload["ShipDate"] = safe_float(row.get("ShipDate"))
    
    if "TrackingNum" in row.index and pd.notna(row.get("TrackingNum")):
            payload["TrackingNum"] = safe_float(row.get("TrackingNum"))
            
    # NEW: Header-level TaxCodeRef (only if mapping exists)
    mapped_txn_tax_code = row.get("Mapped_TxnTaxCodeRef")
    if "TxnTaxDetail_TotalTax" in row.index and pd.notna(row.get("TxnTaxDetail_TotalTax")):
        TxnTaxDetail_TotalTax = safe_float(row.get("TxnTaxDetail_TotalTax"))
    if mapped_txn_tax_code:
        payload["TxnTaxDetail"] = {
            "TotalTax" :TxnTaxDetail_TotalTax,
            "TxnTaxCodeRef": {"value": str(mapped_txn_tax_code)}
            # You can include "TotalTax" later if you compute it; not required for setting the header tax code
        }

    # Existing payload dict already created above
    if ENABLE_TAXLINE_MAPPING:
        add_txn_tax_detail_from_row(payload, row)

    
    # NEW: Header DepartmentRef
    mapped_dept = row.get("Mapped_DepartmentRef")
    if mapped_dept:
        payload["DepartmentRef"] = {"value": str(mapped_dept)}

    payload["Line"] = []
    for _, ln in lines.iterrows():
        detail_type = ln.get("DetailType")
        if not detail_type:
            continue
        line_obj = {
            "DetailType": detail_type,
            "Amount": safe_float(ln.get("Amount")),
            "Description": ln.get("Description"),
        }
        detail = {}
        if detail_type == "SalesItemLineDetail":
            item_id = item_dict.get(ln.get(SALES_ITEM_LINE_MAPPING["SalesItemLineDetail.ItemRef.value"]))
            if not item_id:
                logger.warning(f"❗ Skipping line: unmapped item {ln.get(SALES_ITEM_LINE_MAPPING['SalesItemLineDetail.ItemRef.value'])}")
                continue
            class_id = class_dict.get(ln.get(SALES_ITEM_LINE_MAPPING["SalesItemLineDetail.ClassRef.value"]))
            qty = safe_float(ln.get(SALES_ITEM_LINE_MAPPING["SalesItemLineDetail.Qty"]))
            # if not qty and pd.isna(ln.get(SALES_ITEM_LINE_MAPPING["SalesItemLineDetail.UnitPrice"])):
                # logger.warning(f"⚠️ Cannot calculate UnitPrice for line in Invoice {row['Source_Id']} due to missing Qty.")
            unit_price = (
                safe_float(ln.get(SALES_ITEM_LINE_MAPPING["SalesItemLineDetail.UnitPrice"]))
                if pd.notna(ln.get(SALES_ITEM_LINE_MAPPING["SalesItemLineDetail.UnitPrice"]))
                else (safe_float(ln.get("Amount")) / qty if qty else None)
            )
            detail = {
                "ItemRef": {"value": str(item_id)},
                "Qty": qty,
                "UnitPrice": unit_price,
                "ServiceDate": ln.get(SALES_ITEM_LINE_MAPPING["SalesItemLineDetail.ServiceDate"]),
                "TaxCodeRef": {"value": ln.get(SALES_ITEM_LINE_MAPPING["SalesItemLineDetail.TaxCodeRef.value"]) or "NON"},
            }
            
            if class_id:
                detail["ClassRef"] = {"value": str(class_id)}
        elif detail_type == "SubTotalLineDetail":
            item_id = item_dict.get(ln.get(SUBTOTAL_LINE_MAPPING["SubTotalLineDetail.ItemRef.value"]))
            if item_id:
                detail = {
                    "ItemRef": {"value": str(item_id)}
                }
        elif detail_type == "DiscountLineDetail":
            discount_acct_id = account_dict.get(ln.get(DISCOUNT_LINE_MAPPING["DiscountLineDetail.DiscountAccountRef.value"]))
            class_id = class_dict.get(ln.get(DISCOUNT_LINE_MAPPING["DiscountLineDetail.ClassRef.value"]))
            detail = {
                "PercentBased": ln.get(DISCOUNT_LINE_MAPPING["DiscountLineDetail.PercentBased"]),
                "DiscountPercent": safe_float(ln.get(DISCOUNT_LINE_MAPPING["DiscountLineDetail.DiscountPercent"]))
            }
            if discount_acct_id:
                detail["DiscountAccountRef"] = {"value": str(discount_acct_id)}
            if class_id:
                detail["ClassRef"] = {"value": str(class_id)}
        elif detail_type == "GroupLineDetail":
            group_item_id = item_dict.get(ln.get(GROUP_LINE_MAPPING["GroupLineDetail.GroupItemRef.value"]))
            class_id = class_dict.get(ln.get(GROUP_LINE_MAPPING["GroupLineDetail.ClassRef.value"]))
            if group_item_id:
                detail = {
                    "GroupItemRef": {"value": str(group_item_id)},
                    "Quantity": safe_float(ln.get(GROUP_LINE_MAPPING["GroupLineDetail.Quantity"]))
                }
                if class_id:
                    detail["ClassRef"] = {"value": str(class_id)}
        else:
            logger.warning(f"⚠️ Unknown DetailType '{detail_type}' skipped")
            continue
        detail = {k: v for k, v in detail.items() if v is not None}
        if detail:
            line_obj[detail_type] = detail
            payload["Line"].append(line_obj)


    # Need to add linked for estimate
    # === Linked Estimate(s) ===
    linked_estimates = _collect_linked_estimates(lines, estimate_dict)
    if linked_estimates:
        payload["LinkedTxn"] = linked_estimates

    # payload["LinkedTxn"] = []


    return clean_payload(payload)

def generate_invoice_payloads_in_batches(batch_size=500):
    """
    Generates JSON payloads for all eligible invoices in batches
    and stores them in Map_Invoice table with status = 'Ready'.
    """
    logger.info("🔧 Generating JSON payloads for invoices...")
    while True:
        df = sql.fetch_table_with_params(
            f"""
            SELECT TOP {batch_size} * 
            FROM [{MAPPING_SCHEMA}].[Map_Invoice]
            WHERE Porter_Status = 'Ready' AND (Payload_JSON IS NULL OR Payload_JSON = '')
            """,
            ()
        )
        if df.empty:
            logger.info("✅ All invoice JSON payloads have been generated.")
            break
        # Pre-fetch all invoice lines for this batch
        invoice_ids = df["Source_Id"].tolist()
        all_lines = sql.fetch_table_with_params(
            f"SELECT * FROM [{SOURCE_SCHEMA}].[Invoice_Line] WHERE Parent_Id IN ({','.join(['?']*len(invoice_ids))})",
            tuple(invoice_ids)
        ) if invoice_ids else pd.DataFrame()
        lines_grouped = all_lines.groupby("Parent_Id") if not all_lines.empty else {}
        updates = []
        for _, row in df.iterrows():
            sid = row["Source_Id"]
            if not row.get("Mapped_CustomerRef"):
                logger.warning(f"⚠️ Skipped invoice {sid} — missing CustomerRef mapping.")
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_Invoice]
                    SET Porter_Status = 'Failed', Failure_Reason = 'Missing Mapped_CustomerRef'
                    WHERE Source_Id = ?
                """, (sid,))
                continue
            lines = lines_grouped.get_group(sid) if sid in getattr(lines_grouped, 'groups', {}) else pd.DataFrame()
            payload = build_payload(row, lines)
            if not payload["Line"]:
                logger.warning(f"⚠️ Skipped invoice {sid} — no valid line items.")
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_Invoice]
                    SET Porter_Status = 'Failed', Failure_Reason = 'No valid Line items'
                    WHERE Source_Id = ?
                """, (sid,))
                continue
            payload_json = json.dumps(payload, indent=2)
            updates.append((payload_json, sid))
        # Bulk update payloads
        if updates:
            for payload_json, sid in updates:
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_Invoice]
                    SET Payload_JSON = ?, Failure_Reason = NULL
                    WHERE Source_Id = ?
                """, (payload_json, sid))
        logger.info(f"✅ Generated payloads for {len(df)} invoices in this batch.")

session = requests.Session()

# ============================ Shared Helpers ============================
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
        entity_name: "Invoice" | "Bill" | "VendorCredit" | ...
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
  
# ============================ Batch Poster =============================
def _post_batch_invoices(eligible_batch, url, headers, timeout=40, post_batch_limit=20, max_manual_retries=1):
    """
    High-throughput, single-threaded batch posting for Invoices via QBO /batch.
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

    # Derive /batch endpoint for Invoice
    batch_url = _derive_batch_url(url, "Invoice")

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
                    {"bId": str(sid), "operation": "create", "Invoice": payload}
                    for (sid, payload) in chunk
                ]
            }
            return '' #session.post(batch_url, headers=_headers, json=body, timeout=timeout)

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
                        inv = item.get("Invoice")
                        if inv and "Id" in inv:
                            qid = inv["Id"]
                            successes.append((qid, bid))
                            logger.info(f"✅ Invoice {bid} → QBO {qid}")
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
                        logger.error(f"❌ Invoice {bid} failed: {reason}")

                    # Any missing responses → mark failed
                    for sid, _ in chunk:
                        if str(sid) not in seen:
                            failures.append(("No response for bId", sid))
                            logger.error(f"❌ No response for Invoice {sid}")
                    break

                elif sc in (401, 403) and not attempted_refresh:
                    logger.warning(f"🔐 {sc} on Invoice batch ({len(chunk)} items); refreshing token and retrying once...")
                    try:
                        auto_refresh_token_if_needed()
                    except Exception:
                        pass
                    url, headers = get_qbo_auth_invoice()
                    batch_url = _derive_batch_url(url, "Invoice")
                    attempted_refresh = True
                    continue

                else:
                    reason = (resp.text or f"HTTP {sc}")[:1000]
                    for sid, _ in chunk:
                        failures.append((reason, sid))
                    logger.error(f"❌ Invoice batch failed ({len(chunk)} items): {reason}")
                    break

            except Exception as e:
                reason = f"Batch exception: {e}"
                for sid, _ in chunk:
                    failures.append((reason, sid))
                logger.exception(f"❌ Exception during Invoice batch POST ({len(chunk)} items)")
                break

    return successes, failures

# ============================ Apply Updates ============================
def _apply_batch_updates_invoice(successes, failures):
    """
    Set-based updates for Map_Invoice; fast and idempotent.
    """
    try:
        if successes:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Invoice] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                [(qid, sid) for qid, sid in successes]
            )
        if failures:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Invoice] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                [(reason, sid) for reason, sid in failures]
            )
    except NameError:
        # Fallback to per-row updates if executemany helper is not available
        for qid, sid in successes or []:
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Invoice] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                (qid, sid)
            )
        for reason, sid in failures or []:
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Invoice] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                (reason, sid)
            )

def post_invoice_batch(rows, post_batch_limit: int = 10, timeout: int = 40):
    """
    Single-threaded, multi-item posting via QBO /batch.
    `post_batch_limit` controls items per HTTP call (<=30). Use 3 for “≥3 at a time”.
    """
    url, headers = get_qbo_auth_invoice()
    successes, failures = _post_batch_invoices(
        rows, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
    )
    _apply_batch_updates_invoice(successes, failures)

def migrate_invoices(INVOICE_DATE_FROM,INVOICE_DATE_TO):
    print("\n🚀 Starting Invoice Migration (full flow)\n" + "=" * 35)

    # 0. Ensure mapping table is initialized
    ensure_mapping_table(INVOICE_DATE_FROM,INVOICE_DATE_TO)

    # 1. Deduplicate DocNumber for all rows
    apply_duplicate_docnumber_strategy_dynamic(
        target_table="Map_Invoice",
        schema=MAPPING_SCHEMA,
        docnumber_column="DocNumber",
        source_id_column="Source_Id",
        duplicate_column="Duplicate_Docnumber",
        check_against_tables=[]
    )

    # 2. Generate payloads
    logger.info("🔧 Generating JSON payloads for all invoices...")
    generate_invoice_payloads_in_batches(batch_size=500)
    mapped_df = sql.fetch_table("Map_Invoice", MAPPING_SCHEMA)

    # 3. Migrate eligible
    eligible_rows = mapped_df[mapped_df["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible_rows.empty:
        logger.info("⚠️ No eligible invoices to process.")
        return

    logger.info("✅ All validations passed. Posting eligible invoices...")
    select_batch_size = 300     # DB slice size
    post_batch_limit  = 10      # items per QBO batch call (<=30). Set to 3 if you want exactly 3-at-a-time.
    timeout           = 40

    for i in range(0, len(eligible_rows), select_batch_size):
        batch = eligible_rows.iloc[i:i+select_batch_size]
        post_invoice_batch(batch, post_batch_limit=post_batch_limit, timeout=timeout)
        done = min(i + select_batch_size, len(eligible_rows))
        logger.info(f"⏱️ {done}/{len(eligible_rows)} processed ({done*100//len(eligible_rows)}%)")

    print("\n🏁 Invoice migration completed.")

def resume_or_post_invoices(INVOICE_DATE_FROM,INVOICE_DATE_TO):
    """
    Enhanced resume/post logic for invoice migration:
    1. Check Map_Invoice exists.
    2. Ensure Duplicate_Docnumber dedup if needed.
    3. Generate missing Payload_JSON only.
    4. Post only Ready/Failed via /batch.
    """
    print("\n🔁 Resuming Invoice Migration (enhanced mode)\n" + "=" * 50)

    # 1. Table existence check
    if not sql.table_exists("Map_Invoice", MAPPING_SCHEMA):
        logger.warning("❌ Map_Invoice table does not exist. Running full migration.")
        migrate_invoices(INVOICE_DATE_FROM,INVOICE_DATE_TO)
        return

    mapped_df = sql.fetch_table("Map_Invoice", MAPPING_SCHEMA)
    source_df = sql.fetch_table("Invoice", SOURCE_SCHEMA)

    # 2. Deduplicate DocNumber if needed
    if "Duplicate_Docnumber" not in mapped_df.columns or \
       mapped_df["Duplicate_Docnumber"].isnull().any() or (mapped_df["Duplicate_Docnumber"] == "").any():
        logger.info("🔍 Some Duplicate_Docnumber values missing. Running deduplication...")
        apply_duplicate_docnumber_strategy_dynamic(
            target_table="Map_Invoice",
            schema=MAPPING_SCHEMA,
            docnumber_column="DocNumber",
            source_id_column="Source_Id",
            duplicate_column="Duplicate_Docnumber",
            check_against_tables=[]
        )
        mapped_df = sql.fetch_table("Map_Invoice", MAPPING_SCHEMA)
    else:
        logger.info("✅ All Duplicate_Docnumber values present. Skipping deduplication.")

    # 3. Generate payloads only for missing
    payload_missing = mapped_df["Payload_JSON"].isnull() | (mapped_df["Payload_JSON"] == "")
    missing_count = int(payload_missing.sum())
    if missing_count > 0:
        logger.info(f"🔧 {missing_count} invoices missing Payload_JSON. Generating for those...")
        generate_invoice_payloads_in_batches(batch_size=500)
        mapped_df = sql.fetch_table("Map_Invoice", MAPPING_SCHEMA)
    else:
        logger.info("✅ All invoices have Payload_JSON.")

    # 4. Eligible only
    eligible_rows = mapped_df[mapped_df["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible_rows.empty:
        logger.info("⚠️ No eligible invoices to post.")
        return

    logger.info("✅ All validations passed. Posting eligible invoices...")
    select_batch_size = 300
    post_batch_limit  = 10
    timeout           = 40

    for i in range(0, len(eligible_rows), select_batch_size):
        batch = eligible_rows.iloc[i:i+select_batch_size]
        post_invoice_batch(batch, post_batch_limit=post_batch_limit, timeout=timeout)
        done = min(i + select_batch_size, len(eligible_rows))
        logger.info(f"⏱️ {done}/{len(eligible_rows)} processed ({done*100//len(eligible_rows)}%)")

    print("\n🏁 Invoice posting completed.")

def smrt_invoice_migration(INVOICE_DATE_FROM,INVOICE_DATE_TO):
    """
    Smart migration entrypoint:
    - If Map_Invoice table exists and row count matches Invoice table, resume/post invoices.
    - If table missing or row count mismatch, perform full migration.
    """
    logger.info("🔎 Running smart_invoice_migration...")
    if sql.table_exists("Map_Invoice", MAPPING_SCHEMA):
        mapped_df = sql.fetch_table("Map_Invoice", MAPPING_SCHEMA)
        source_df = sql.fetch_table("Invoice", SOURCE_SCHEMA)
        if len(mapped_df) == len(source_df):
            logger.info("✅ Table exists and row count matches. Resuming/posting invoices.")
            resume_or_post_invoices(INVOICE_DATE_FROM,INVOICE_DATE_TO)
            return
        else:
            logger.warning(f"❌ Row count mismatch: Map_Invoice={len(mapped_df)}, Invoice={len(source_df)}. Running full migration.")
    else:
        logger.warning("❌ Map_Invoice table does not exist. Running full migration.")
    migrate_invoices(INVOICE_DATE_FROM,INVOICE_DATE_TO)
