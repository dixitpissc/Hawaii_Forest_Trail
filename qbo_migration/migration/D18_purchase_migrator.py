
"""
Sequence : 18
Module: Purchase_migrator.py
Author: Dixit Prajapati
Created: 2025-09-16
Description: Handles migration of Purchase records from QBO to QBO.
Production : Working 
Development : Require when necessary
Phase : 02 - Multi User + Tax
"""

import os, json, requests, pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import global_logger as logger
from config.mapping.purchase_mapping import PURCHASE_HEADER_MAPPING as HEADER_MAP, PURCHASE_LINE_MAPPING as LINE_MAP
from utils.apply_duplicate_docnumber import apply_duplicate_docnumber_strategy_dynamic
from utils.token_refresher import get_qbo_context_migration
from storage.sqlserver.sql import executemany

load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
API_ENTITY = "purchase"


ENABLE_GLOBAL_JE_DOCNUMBER_DEDUP=True

def apply_global_docnumber_strategy_for_Purchase():
            apply_duplicate_docnumber_strategy_dynamic(
            target_table="Map_Purchase",
            schema=MAPPING_SCHEMA,
            check_against_tables=["Map_Bill","Map_Invoice","Map_VendorCredit","Map_JournalEntry","Map_Deposit"]
        )

def get_qbo_auth():
    env = os.getenv("QBO_ENVIRONMENT", "sandbox")
    base = "https://sandbox-quickbooks.api.intuit.com" if env == "sandbox" else "https://quickbooks.api.intuit.com"
    ctx = get_qbo_context_migration()
    realm = ctx["REALM_ID"]

    return f"{base}/v3/company/{realm}/{API_ENTITY}", {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

def safe_float(val, decimals=6):
    import math
    try:
        if val is None:
            return None
        num = float(str(val).replace(",", "").strip())
        if math.isinf(num) or math.isnan(num):
            return None
        return round(num, decimals)
    except Exception:
        return None

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
    Builds payload['TxnTaxDetail'] using header + up to 4 TaxLine components from Map_Purchase columns.
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

def insert_purchase_map_dataframe(df, table: str, schema: str = "dbo"):
    if df.empty:
        logger.warning(f"⚠️ Skipped insertion: DataFrame for table '{schema}.{table}' is empty.")
        return

    df = df.copy()

    # ---- helper: sanitize numeric-like columns to SQL-safe floats/NULLs ----
    def _sanitize_numeric_like_columns_for_sql(df_in: pd.DataFrame, decimals=6) -> pd.DataFrame:
        import re
        dfx = df_in.copy()

        tokens = ("amt", "amount", "rate", "qty", "quantity", "price", "tax", "total", "balance")
        num_cols = [c for c in dfx.columns if any(tok in c.lower() for tok in tokens)]
        for extra in ("ExchangeRate", "TotalAmt", "TxnTaxDetail.TotalTax"):
            if extra in dfx.columns and extra not in num_cols:
                num_cols.append(extra)

        for col in set(num_cols):
            if col not in dfx.columns:
                continue
            s = dfx[col]

            # Normalize object/str -> strip commas, blanks to None
            if s.dtype == object:
                s = s.astype(str).str.strip()
                s = s.replace({
                    "": None, "None": None, "none": None,
                    "null": None, "NaN": None, "nan": None,
                    "—": None, "–": None
                })
                s = s.apply(lambda x: None if x is None else re.sub(r",", "", x))

            # Coerce to numeric; invalid -> NaN
            s = pd.to_numeric(s, errors="coerce")
            # Round to avoid DECIMAL scale overflow
            s = s.round(decimals)
            # NaN -> None for SQL NULL
            dfx[col] = s.where(s.notna(), None)

        # Relax ExchangeRate precision a bit more
        if "ExchangeRate" in dfx.columns:
            ex = pd.to_numeric(dfx["ExchangeRate"], errors="coerce").round(10)
            dfx["ExchangeRate"] = ex.where(ex.notna(), None)

        return dfx

    # ---- header-level enrichment ----
    def map_payment_type(ptype):
        if pd.isna(ptype):
            return None
        ptype_clean = str(ptype).replace(" ", "").lower()
        return sql.fetch_single_value(
            f"SELECT Target_Id FROM {MAPPING_SCHEMA}.Map_PaymentMethod WHERE REPLACE(LOWER(Name), ' ', '') = ?",
            (ptype_clean,)
        )

    def map_account(source_id):
        if pd.isna(source_id):
            return None
        return sql.fetch_single_value(
            f"SELECT Target_Id FROM {MAPPING_SCHEMA}.Map_Account WHERE Source_Id=?",
            (source_id,)
        )

    df["Mapped_PaymentMethod"] = df["PaymentType"].apply(map_payment_type) if "PaymentType" in df.columns else None
    df["Mapped_AccountRef"]  = df["AccountRef.value"].apply(map_account) if "AccountRef.value" in df.columns else None

    # ---- line-level enrichment ----
    enriched = {
        "Mapped_Line_ClassRef": "AccountBasedExpenseLineDetail.ClassRef.value",
        "Mapped_Line_DepartmentRef": "AccountBasedExpenseLineDetail.DepartmentRef.value",
        "Mapped_Line_AccountRef": "AccountBasedExpenseLineDetail.AccountRef.value",
        "Mapped_Line_ItemRef": "ItemBasedExpenseLineDetail.ItemRef.value",
    }
    line_df = sql.fetch_table("Purchase_Line", SOURCE_SCHEMA)

    for target_col, src_col in enriched.items():
        if src_col in line_df.columns:
            values = line_df[[src_col, "Parent_Id"]].dropna().copy()

            mapping_table = {
                "AccountRef": "Map_Account",
                "ClassRef": "Map_Class",
                "DepartmentRef": "Map_Department",
                "ItemRef": "Map_Item",
            }
            key = src_col.split(".")[-2]  # e.g., "AccountRef"
            map_tbl = mapping_table.get(key)

            if map_tbl:
                values["Target"] = values[src_col].apply(
                    lambda x: sql.fetch_single_value(
                        f"SELECT Target_Id FROM {MAPPING_SCHEMA}.{map_tbl} WHERE Source_Id=?",
                        (x,),
                    )
                )
                merged = values.groupby("Parent_Id", as_index=False)["Target"].first()
                merged.columns = ["Id", target_col]
                df = df.merge(merged, on="Id", how="left")
            else:
                df[target_col] = None
        else:
            df[target_col] = None

    # ---- safe defaults (avoid tuple multi-assign) ----
    defaults = {
        "Target_Id": None,
        "Porter_Status": "Ready",
        "Retry_Count": 0,
        "Failure_Reason": None,
        "Payload_JSON": None,
    }
    for col, val in defaults.items():
        if col not in df.columns:
            df[col] = val
        else:
            df[col] = df[col].where(pd.notna(df[col]), val)

    # ---- sanitize numeric-like columns to prevent TDS float errors ----
    df = _sanitize_numeric_like_columns_for_sql(df, decimals=6)

    # ---- normalize mapped ID columns to strings/NULL (not numbers) ----
    for c in ["Mapped_PaymentMethod", "Mapped_AccountRef",
              "Mapped_Line_ClassRef", "Mapped_Line_DepartmentRef",
              "Mapped_Line_AccountRef", "Mapped_Line_ItemRef"]:
        if c in df.columns:
            df[c] = df[c].astype(object).where(df[c].notna(), None)
            df[c] = df[c].apply(lambda x: str(x) if x is not None else None)

    # ---- final: Pythonize types so pyodbc sees pure Python (no numpy types) ----
    def _to_python_scalar(x):
        import math
        import numpy as np
        try:
            # numpy floats/ints -> python
            if isinstance(x, (np.floating,)):
                return None if (np.isnan(x)) else float(x)
            if isinstance(x, (np.integer,)):
                return int(x)
        except Exception:
            pass
        # plain NaN handling
        if isinstance(x, float) and (math.isnan(x)):
            return None
        return x

    df = df.map(_to_python_scalar)

    # Optional quick diagnostic
    maybe_bad = [
        c for c in df.columns
        if (df[c].dtype == object and df[c].apply(lambda v: isinstance(v, str)).any())
        and any(tok in c.lower() for tok in ("amt", "amount", "rate", "qty", "quantity", "price", "tax", "total", "balance"))
    ]
    if maybe_bad:
        logger.warning(f"🔎 String values still present in numeric-like columns: {maybe_bad}")

    # ---- insert ----
    if hasattr(sql, "insert_map_dataframe"):
        sql.insert_map_dataframe(df, table, schema)   # preferred generic inserter
    else:
        sql.insert_invoice_map_dataframe(df, table, schema)  # fallback

def ensure_mapping_table(PURCHASE_DATE_FROM='1900-01-01',PURCHASE_DATE_TO='2080-12-31'):
    # 1. Date filter and only purchases with lines
    date_from = PURCHASE_DATE_FROM
    date_to = PURCHASE_DATE_TO
    query = f"""
        SELECT * FROM [{SOURCE_SCHEMA}].[Purchase]
        WHERE EXISTS (
            SELECT 1 FROM [{SOURCE_SCHEMA}].[Purchase_Line] l WHERE l.Parent_Id = [{SOURCE_SCHEMA}].[Purchase].Id
        )
        {"AND TxnDate >= ?" if date_from else ""}
        {"AND TxnDate <= ?" if date_to else ""}
    """
    params = tuple(p for p in [date_from, date_to] if p)
    df = sql.fetch_table_with_params(query, params)

    if df.empty:
        logger.warning("No purchase records found with lines in specified range.")
        return

    # 2. Core migration fields
    df["Source_Id"] = df["Id"]
    required_cols = [
        "Target_Id",
        "Porter_Status",
        "Retry_Count",
        "Failure_Reason",
        "Payload_JSON",
        # Add mapped fields as needed for your logic
        "Mapped_PaymentMethod",
        "Mapped_AccountRef",
        "Mapped_Line_ClassRef",
        "Mapped_Line_DepartmentRef",
        "Mapped_Line_AccountRef",
        "Mapped_Line_ItemRef",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    df["Target_Id"] = None
    df["Porter_Status"] = "Ready"
    df["Retry_Count"] = 0
    df["Failure_Reason"] = None
    df["Payload_JSON"] = None

    # 3. Bulk load mapping tables into dicts
    def load_map(table):
        t = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[{table}]", tuple())
        return dict(zip(t["Source_Id"], t["Target_Id"])) if not t.empty else {}

    account_dict = load_map("Map_Account")
    class_dict = load_map("Map_Class")
    dept_dict = load_map("Map_Department")
    item_dict = load_map("Map_Item")
    payment_dict = load_map("Map_PaymentMethod")

    # NEW: TaxCode and TaxRate mapping (optional but preferred)
    taxcode_dict = load_map("Map_TaxCode") if sql.table_exists("Map_TaxCode", MAPPING_SCHEMA) else {}
    taxrate_dict = load_map("Map_TaxRate") if sql.table_exists("Map_TaxRate", MAPPING_SCHEMA) else {}

    # 4. Bulk fetch Purchase_Line and group
    purchase_lines = sql.fetch_table_with_params(f"SELECT * FROM [{SOURCE_SCHEMA}].[Purchase_Line]", tuple())
    purchase_lines_grouped = purchase_lines.groupby("Parent_Id") if not purchase_lines.empty else {}


    # 5. Resolve line-level mappings (vectorized, optional)
    def get_refs(parent_id, col, mapping_dict):
        if col not in purchase_lines.columns:
            return None
        if parent_id not in purchase_lines_grouped.groups:
            return None
        vals = purchase_lines_grouped.get_group(parent_id)[col].dropna().unique()
        targets = [str(mapping_dict.get(v)) for v in vals if v in mapping_dict]
        return ";".join(sorted(set(targets))) if targets else None

    # Only map if the source column exists in Purchase_Line
    if "AccountBasedExpenseLineDetail.AccountRef.value" in purchase_lines.columns:
        df["Mapped_Line_AccountRef"] = df["Id"].map(lambda x: get_refs(x, "AccountBasedExpenseLineDetail.AccountRef.value", account_dict))
    if "AccountBasedExpenseLineDetail.ClassRef.value" in purchase_lines.columns:
        df["Mapped_Line_ClassRef"] = df["Id"].map(lambda x: get_refs(x, "AccountBasedExpenseLineDetail.ClassRef.value", class_dict))
    if "AccountBasedExpenseLineDetail.DepartmentRef.value" in purchase_lines.columns:
        df["Mapped_Line_DepartmentRef"] = df["Id"].map(lambda x: get_refs(x, "AccountBasedExpenseLineDetail.DepartmentRef.value", dept_dict))
    if "ItemBasedExpenseLineDetail.ItemRef.value" in purchase_lines.columns:
        df["Mapped_Line_ItemRef"] = df["Id"].map(lambda x: get_refs(x, "ItemBasedExpenseLineDetail.ItemRef.value", item_dict))

    # 6. Header-level mappings (keep logic as is)
    if "PaymentType" in df.columns:
        df["Mapped_PaymentMethod"] = df["PaymentType"].map(lambda x: payment_dict.get(x) if pd.notna(x) else None)
    if "AccountRef.value" in df.columns:
        df["Mapped_AccountRef"] = df["AccountRef.value"].map(lambda x: account_dict.get(x) if pd.notna(x) else None)

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

    # 7. Clear & insert
    if sql.table_exists("Map_Purchase", MAPPING_SCHEMA):
        sql.run_query(f"TRUNCATE TABLE [{MAPPING_SCHEMA}].[Map_Purchase]")

    insert_purchase_map_dataframe(df, "Map_Purchase", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df)} rows into {MAPPING_SCHEMA}.Map_Purchase")

def get_lines(purchase_id):
    return sql.fetch_table_with_params(f"SELECT * FROM {SOURCE_SCHEMA}.Purchase_Line WHERE Parent_Id=?", (purchase_id,))

def build_payload(row, lines):
    # --- Optimized, index-based, mapping-dict-driven logic (like deposit) ---
    # Preload all mapping dicts
    account_dict = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Account]", tuple())
    account_map = dict(zip(account_dict["Source_Id"], account_dict["Target_Id"])) if not account_dict.empty else {}
    class_dict = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Class]", tuple())
    class_map = dict(zip(class_dict["Source_Id"], class_dict["Target_Id"])) if not class_dict.empty else {}
    dept_dict = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Department]", tuple())
    dept_map = dict(zip(dept_dict["Source_Id"], dept_dict["Target_Id"])) if not dept_dict.empty else {}
    item_dict = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Item]", tuple())
    item_map = dict(zip(item_dict["Source_Id"], item_dict["Target_Id"])) if not item_dict.empty else {}
    payment_dict = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_PaymentMethod]", tuple())
    payment_map = dict(zip(payment_dict["Source_Id"], payment_dict["Target_Id"])) if not payment_dict.empty else {}
    customer_dict = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Customer]", tuple())
    customer_map = dict(zip(customer_dict["Source_Id"], customer_dict["Target_Id"])) if not customer_dict.empty else {}
    vendor_dict = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Vendor]", tuple())
    vendor_map = dict(zip(vendor_dict["Source_Id"], vendor_dict["Target_Id"])) if not vendor_dict.empty else {}
    # Load employee mapping only if table exists
    if sql.table_exists("Map_Employee", MAPPING_SCHEMA):
        employee_dict = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Employee]", tuple())
        employee_map = dict(zip(employee_dict["Source_Id"], employee_dict["Target_Id"])) if not employee_dict.empty else {}
    else:
        employee_map = {}

    # Header fields
    raw_doc = row.get(HEADER_MAP.get("DocNumber"))
    dedup_doc = row.get("Duplicate_DocNumber")
    doc_number = (
        str(dedup_doc).strip()
        if pd.notna(dedup_doc) and str(dedup_doc).strip() != ""
        else str(raw_doc).strip()
    )
    payload = {
        "DocNumber": doc_number,
        "CurrencyRef": {"value": row.get("CurrencyRef.value", "USD") or "USD"},
        "Line": []
    }
    # Add PaymentType if present
    payment_type = row.get("PaymentType")
    if pd.notna(payment_type):
        payload["PaymentType"] = payment_type
    if pd.notna(row.get("ExchangeRate")):
        payload["ExchangeRate"] = safe_float(row["ExchangeRate"])
    # Add mapped payment method
    if pd.notna(row.get("Mapped_PaymentMethod")):
        payload["PaymentMethodRef"] = {"value": str(row["Mapped_PaymentMethod"])}
    # Add mapped account ref
    acct_id = row.get("Mapped_AccountRef")
    if not acct_id and pd.notna(row.get("AccountRef.value")):
        acct_id = account_map.get(row["AccountRef.value"])
    if acct_id:
        payload["AccountRef"] = {"value": str(acct_id)}
    # EntityRef (Vendor/Customer/Employee) - dynamic
    entity_type = str(row.get("EntityRef.type") or row.get("EntityRef_type") or "").strip().lower()
    if entity_type in ["none", "null", "", "na", "n/a"]:
        entity_type = None
    entity_val = row.get("EntityRef.value") or row.get("EntityRef_value")
    entity_map = None
    if entity_type == "customer":
        entity_map = customer_map
    elif entity_type == "vendor":
        entity_map = vendor_map
    elif entity_type == "employee":
        entity_map = employee_map
    if pd.notna(entity_val) and entity_map is not None:
        entity_val_str = str(entity_val).strip()
        entity_id = entity_map.get(entity_val_str)
        if not entity_id and not entity_val_str.isdigit():
            # Optionally: implement name-based lookup if needed
            pass
        if entity_id:
            payload["EntityRef"] = {
                "type": entity_type.capitalize(),
                "value": str(entity_id)
            }
        else:
            logger.warning(f"⚠️ EntityRef NOT FOUND in '{entity_type}' for value: {entity_val}")
    elif entity_val and not entity_type:
        logger.warning(f"⚠️ Unknown EntityRef.type: '{entity_type}' — cannot resolve mapping.")

    # --- Line processing (index-based, robust, optional) ---
    if lines is None or lines.empty:
        return payload if payload["Line"] else None
    cols = lines.columns
    get_idx = cols.get_loc
    def _idx(key: str):
        col = LINE_MAP.get(key)
        return get_idx(col) if col and (col in cols) else None
    c_DetailType = _idx("DetailType")
    c_Amount = _idx("Amount")
    c_Desc = _idx("Description")
    c_Acct = _idx("AccountBasedExpenseLineDetail.AccountRef.value")
    c_Class = _idx("AccountBasedExpenseLineDetail.ClassRef.value")
    c_Dept = _idx("AccountBasedExpenseLineDetail.DepartmentRef.value")
    c_Item = _idx("ItemBasedExpenseLineDetail.ItemRef.value")
    c_Cust = _idx("AccountBasedExpenseLineDetail.CustomerRef.value")
    c_ItemCust = _idx("ItemBasedExpenseLineDetail.CustomerRef.value")
    c_ItemClass = _idx("ItemBasedExpenseLineDetail.ClassRef.value")
    c_TaxCode = _idx("AccountBasedExpenseLineDetail.TaxCodeRef.value")
    c_ItemTaxCode = _idx("ItemBasedExpenseLineDetail.TaxCodeRef.value")
    c_BillStatus = _idx("AccountBasedExpenseLineDetail.BillableStatus")
    c_Qty = _idx("ItemBasedExpenseLineDetail.Qty")
    c_UnitPrice = _idx("ItemBasedExpenseLineDetail.UnitPrice")

    for row_vals in lines.itertuples(index=False, name=None):
        detail_type = row_vals[c_DetailType] if c_DetailType is not None else None
        amount = safe_float(row_vals[c_Amount]) if c_Amount is not None else None
        if not amount or amount == 0:
            continue
        base_line = {
            "Amount": amount,
            "Description": row_vals[c_Desc] if c_Desc is not None else None,
            "DetailType": detail_type
        }
        detail = {}
        if detail_type == "AccountBasedExpenseLineDetail":
            if c_Acct is not None:
                acct_val = row_vals[c_Acct]
                if pd.notna(acct_val):
                    acct_id = account_map.get(acct_val)
                    if acct_id:
                        detail["AccountRef"] = {"value": str(acct_id)}
            if c_Cust is not None:
                cust_val = row_vals[c_Cust]
                if pd.notna(cust_val):
                    cust_id = customer_map.get(cust_val)
                    if cust_id:
                        detail["CustomerRef"] = {"value": str(cust_id)}
            if c_Class is not None:
                class_val = row_vals[c_Class]
                if pd.notna(class_val):
                    class_id = class_map.get(class_val)
                    if class_id:
                        detail["ClassRef"] = {"value": str(class_id)}
            if c_TaxCode is not None:
                tax_code = row_vals[c_TaxCode]
                if pd.notna(tax_code):
                    detail["TaxCodeRef"] = {"value": str(tax_code)}
            if c_BillStatus is not None:
                bill_status = row_vals[c_BillStatus]
                if pd.notna(bill_status):
                    detail["BillableStatus"] = bill_status
            if c_Dept is not None:
                dept_val = row_vals[c_Dept]
                if pd.notna(dept_val):
                    dept_id = dept_map.get(dept_val)
                    if dept_id:
                        detail["DepartmentRef"] = {"value": str(dept_id)}
            base_line["AccountBasedExpenseLineDetail"] = detail
        elif detail_type == "ItemBasedExpenseLineDetail":
            if c_Item is not None:
                item_val = row_vals[c_Item]
                if pd.notna(item_val):
                    item_id = item_map.get(item_val)
                    if item_id:
                        detail["ItemRef"] = {"value": str(item_id)}
            if c_ItemCust is not None:
                cust_val = row_vals[c_ItemCust]
                if pd.notna(cust_val):
                    cust_id = customer_map.get(cust_val)
                    if cust_id:
                        detail["CustomerRef"] = {"value": str(cust_id)}
            if c_ItemClass is not None:
                class_val = row_vals[c_ItemClass]
                if pd.notna(class_val):
                    class_id = class_map.get(class_val)
                    if class_id:
                        detail["ClassRef"] = {"value": str(class_id)}
            if c_ItemTaxCode is not None:
                tax_code = row_vals[c_ItemTaxCode]
                if pd.notna(tax_code):
                    detail["TaxCodeRef"] = {"value": str(tax_code)}
            if c_Qty is not None:
                qty = row_vals[c_Qty]
                if pd.notna(qty):
                    detail["Qty"] = safe_float(qty)
            if c_UnitPrice is not None:
                unit_price = row_vals[c_UnitPrice]
                if pd.notna(unit_price):
                    detail["UnitPrice"] = safe_float(unit_price)
            base_line["ItemBasedExpenseLineDetail"] = detail
        payload["Line"].append(base_line)

    # NEW: Add tax detail if available
    add_txn_tax_detail_from_row(payload, row)

    return payload if payload["Line"] else None

def generate_purchase_payloads_in_batches(batch_size=500):
    """
    Generates JSON payloads for purchases in batches (like deposit).
    Only processes records with Porter_Status='Ready' and missing Payload_JSON.
    """
    logger.info("🔧 Generating JSON payloads for purchases...")

    # Optional: pre-load Map_* dicts once so build_payload never hits DB
    try:
        pass  # No _ensure_maps_loaded in this file, but could add if needed
    except Exception:
        pass

    while True:
        df = sql.fetch_table_with_params(
            f"SELECT TOP {batch_size} * "
            f"FROM [{MAPPING_SCHEMA}].[Map_Purchase] "
            f"WHERE Porter_Status = 'Ready' "
            f"  AND (Payload_JSON IS NULL OR Payload_JSON = '')",
            ()
        )

        if df.empty:
            logger.info("✅ All purchase JSON payloads have been generated.")
            break

        for _, row in df.iterrows():
            sid = row["Source_Id"]

            lines = get_lines(sid)
            payload = build_payload(row, lines)

            if not payload:
                sql.run_query(
                    f"UPDATE {MAPPING_SCHEMA}.Map_Purchase SET Porter_Status='Failed', Failure_Reason=? WHERE Source_Id=?",
                    ("Empty line items or missing mapping", sid)
                )
                continue

            pretty_json = json.dumps(payload, indent=2)
            sql.run_query(
                f"UPDATE {MAPPING_SCHEMA}.Map_Purchase SET Payload_JSON=?, Porter_Status='Ready', Failure_Reason=NULL WHERE Source_Id=?",
                (pretty_json, sid)
            )

        logger.info(f"✅ Processed {len(df)} payloads in this batch.")

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
        entity_name: "Purchase" | "Invoice" | "Bill" | ...
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


# ============================ Batch poster & updates ==============================
def _post_batch_purchases(eligible_batch, url, headers, timeout=40, post_batch_limit=20, max_manual_retries=1):
    """
    High-throughput, single-threaded batch posting for Purchases via QBO /batch.
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

    batch_url = _derive_batch_url(url, "Purchase")

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
                    {"bId": str(sid), "operation": "create", "Purchase": payload}
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
                        ent = item.get("Purchase")
                        if ent and "Id" in ent:
                            qid = ent["Id"]
                            successes.append((qid, bid))
                            logger.info(f"✅ Purchase {bid} → QBO {qid}")
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
                        logger.error(f"❌ Purchase {bid} failed: {reason}")

                    # Any missing responses → mark failed
                    for sid, _ in chunk:
                        if str(sid) not in seen:
                            failures.append(("No response for bId", sid))
                            logger.error(f"❌ No response for Purchase {sid}")
                    break

                elif sc in (401, 403) and not attempted_refresh:
                    logger.warning(f"🔐 {sc} on Purchase batch ({len(chunk)} items); refreshing token and retrying once...")
                    try:
                        auto_refresh_token_if_needed()
                    except Exception:
                        pass
                    url, headers = get_qbo_auth()
                    batch_url = _derive_batch_url(url, "Purchase")
                    attempted_refresh = True
                    continue

                else:
                    reason = (resp.text or f"HTTP {sc}")[:1000]
                    for sid, _ in chunk:
                        failures.append((reason, sid))
                    logger.error(f"❌ Purchase batch failed ({len(chunk)} items): {reason}")
                    break

            except Exception as e:
                reason = f"Batch exception: {e}"
                for sid, _ in chunk:
                    failures.append((reason, sid))
                logger.exception(f"❌ Exception during Purchase batch POST ({len(chunk)} items)")
                break

    return successes, failures

def _apply_batch_updates_purchase(successes, failures):
    """
    Set-based updates for Map_Purchase; fast and idempotent.
    """
    try:

        if successes:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Purchase] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                [(qid, sid) for qid, sid in successes]
            )
        if failures:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Purchase] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                [(reason, sid) for reason, sid in failures]
            )
    except NameError:
        # Fallback to per-row updates if executemany helper is not available
        for qid, sid in successes or []:
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Purchase] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                (qid, sid)
            )
        for reason, sid in failures or []:
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Purchase] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                (reason, sid)
            )

# ============================ Updated purchase functions =========================
def post_purchase(row):
    """
    Single-record fallback; prefer batch path in migrate/resume.
    Keeps your Duplicate_DocNumber refresh + (re)build payload if needed.
    """
    if row.get("Porter_Status") == "Success":
        return

    # Refresh Duplicate_DocNumber from DB
    dedup_doc = sql.fetch_single_value(
        f"SELECT Duplicate_DocNumber FROM [{MAPPING_SCHEMA}].[Map_Purchase] WHERE Source_Id = ?",
        (row["Source_Id"],)
    )
    if dedup_doc:
        row["Duplicate_DocNumber"] = dedup_doc

    # Build payload if missing
    payload = None
    if not row.get("Payload_JSON"):
        lines = get_lines(row["Source_Id"])
        payload = build_payload(row, lines)
        if not payload:
            return
        payload_json = json.dumps(payload, indent=2)
        sql.run_query(
            f"UPDATE {MAPPING_SCHEMA}.Map_Purchase SET Payload_JSON=?, Porter_Status='Ready' WHERE Source_Id=?",
            (payload_json, row["Source_Id"])
        )
    else:
        try:
            payload = _fast_loads(row["Payload_JSON"])
        except Exception as e:
            sql.run_query(
                f"UPDATE {MAPPING_SCHEMA}.Map_Purchase SET Porter_Status='Failed', Retry_Count=ISNULL(Retry_Count,0)+1, Failure_Reason=? WHERE Source_Id=?",
                (f"Bad JSON: {e}", row["Source_Id"])
            )
            return

    url, headers = get_qbo_auth()
    try:
        resp = session.post(url, headers=headers, json=payload, timeout=20)
        if resp.status_code == 200:
            qid = (resp.json().get("Purchase") or {}).get("Id")
            if qid:
                sql.run_query(
                    f"UPDATE {MAPPING_SCHEMA}.Map_Purchase SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL WHERE Source_Id=?",
                    (qid, row["Source_Id"])
                )
                logger.info(f"✅ Purchase {row['Source_Id']} → QBO {qid}")
                return
            reason = "No Id in response"
        elif resp.status_code in (401, 403):
            logger.warning(f"🔐 {resp.status_code} on Purchase {row['Source_Id']}; refreshing token and retrying once...")
            try:
                auto_refresh_token_if_needed()
            except Exception:
                pass
            url, headers = get_qbo_auth()
            resp2 = session.post(url, headers=headers, json=payload, timeout=20)
            if resp2.status_code == 200:
                qid = (resp2.json().get("Purchase") or {}).get("Id")
                if qid:
                    sql.run_query(
                        f"UPDATE {MAPPING_SCHEMA}.Map_Purchase SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL WHERE Source_Id=?",
                        (qid, row["Source_Id"])
                    )
                    logger.info(f"✅ Purchase {row['Source_Id']} → QBO {qid}")
                    return
                reason = "No Id in response (after refresh)"
            else:
                reason = (resp2.text or f"HTTP {resp2.status_code}")[:500]
        else:
            reason = (resp.text or f"HTTP {resp.status_code}")[:500]

        sql.run_query(
            f"UPDATE {MAPPING_SCHEMA}.Map_Purchase SET Porter_Status='Failed', Retry_Count=ISNULL(Retry_Count,0)+1, Failure_Reason=? WHERE Source_Id=?",
            (reason, row["Source_Id"])
        )
        logger.error(f"❌ Failed Purchase {row['Source_Id']}: {reason}")

    except Exception as e:
        sql.run_query(
            f"UPDATE {MAPPING_SCHEMA}.Map_Purchase SET Porter_Status='Failed', Retry_Count=ISNULL(Retry_Count,0)+1, Failure_Reason=? WHERE Source_Id=?",
            (str(e), row["Source_Id"])
        )
        logger.exception(f"❌ Exception posting Purchase {row['Source_Id']}")

def migrate_purchases(PURCHASE_DATE_FROM,PURCHASE_DATE_TO,retry_only=False):
    logger.info("\n🚀 Starting Purchase Migration\n" + "=" * 40)
    ensure_mapping_table(PURCHASE_DATE_FROM,PURCHASE_DATE_TO)

    if ENABLE_GLOBAL_JE_DOCNUMBER_DEDUP:
        apply_global_docnumber_strategy_for_Purchase()
        try:
            sql.clear_cache()
        except Exception:
            pass
        import time; time.sleep(1)

    # Phase 1: Build JSON payloads for eligible records (batched)
    generate_purchase_payloads_in_batches()

    # Phase 2: Post via /batch
    post_query = f"SELECT * FROM {MAPPING_SCHEMA}.Map_Purchase WHERE Porter_Status = 'Ready'"
    post_params = []
    if PURCHASE_DATE_FROM and PURCHASE_DATE_TO:
        post_query += " AND TxnDate BETWEEN ? AND ?"
        post_params = [PURCHASE_DATE_FROM, PURCHASE_DATE_TO]
    post_rows = sql.fetch_table_with_params(post_query, tuple(post_params))
    if post_rows.empty:
        logger.warning("⚠️ No purchases with built JSON to post.")
        return

    url, headers = get_qbo_auth()
    select_batch_size = 300   # DB slice size
    post_batch_limit  = 8    # items per QBO batch call (<=30). Set to 3 for exactly-3-at-a-time.
    timeout           = 40

    total = len(post_rows)
    logger.info(f"📤 Posting {total} Purchase record(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        slice_df = post_rows.iloc[i:i + select_batch_size]
        successes, failures = _post_batch_purchases(
            slice_df, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates_purchase(successes, failures)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    logger.info("🏁 Purchase migration complete.")

def resume_or_post_purchases(PURCHASE_DATE_FROM,PURCHASE_DATE_TO):
    print("\n🔁 Resuming Purchase Migration (conditional mode)\n" + "=" * 50)

    if not sql.table_exists("Map_Purchase", MAPPING_SCHEMA):
        logger.warning("❌ Map_Purchase table does not exist. Running full migration.")
        migrate_purchases(PURCHASE_DATE_FROM,PURCHASE_DATE_TO)
        return

    mapped_df = sql.fetch_table("Map_Purchase", MAPPING_SCHEMA)
    source_df = sql.fetch_table("Purchase", SOURCE_SCHEMA)

    if len(mapped_df) != len(source_df):
        logger.warning(f"❌ Row count mismatch: Map_Purchase = {len(mapped_df)}, Source Purchase = {len(source_df)}")
        migrate_purchases(PURCHASE_DATE_FROM,PURCHASE_DATE_TO)
        return

    if mapped_df["Payload_JSON"].isnull().any() or (mapped_df["Payload_JSON"] == "").any():
        logger.warning("🔍 Some rows in Map_Purchase are missing Payload_JSON. Regenerating for missing only...")
        generate_purchase_payloads_in_batches()
        mapped_df = sql.fetch_table("Map_Purchase", MAPPING_SCHEMA)

    eligible = mapped_df[mapped_df["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("⚠️ No eligible purchases to post.")
        return

    url, headers = get_qbo_auth()
    select_batch_size = 300
    post_batch_limit  = 8
    timeout           = 40

    total = len(eligible)
    logger.info(f"📤 Posting {total} Purchase record(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        slice_df = eligible.iloc[i:i + select_batch_size]
        successes, failures = _post_batch_purchases(
            slice_df, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates_purchase(successes, failures)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    print("\n🏁 Purchase posting completed.")

# ============================ SMART PURCHASE MIGRATION ============================
def smart_purchase_migration(PURCHASE_DATE_FROM, PURCHASE_DATE_TO):
    """
    Smart migration entrypoint for Purchase:
    - If Map_Purchase table exists and row count matches Purchase table, resume/post Purchases.
    - If table missing or row count mismatch, perform full migration.
    """
    logger.info("🔎 Running smart_purchase_migration...")
    full_process = False

    if sql.table_exists("Map_Purchase", MAPPING_SCHEMA):
        mapped_df = sql.fetch_table("Map_Purchase", MAPPING_SCHEMA)
        source_df = sql.fetch_table("Purchase", SOURCE_SCHEMA)

        if len(mapped_df) == len(source_df):
            logger.info("✅ Table exists and row count matches. Resuming/posting Purchases.")
            resume_or_post_purchases(PURCHASE_DATE_FROM, PURCHASE_DATE_TO)

            # After resume_or_post, reprocess failed records one more time
            failed_df = sql.fetch_table("Map_Purchase", MAPPING_SCHEMA)
            failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
            if not failed_records.empty:
                logger.info(f"🔁 Reprocessing {len(failed_records)} failed Purchases with null Target_Id after main migration...")
                for _, row in failed_records.iterrows():
                    post_purchase(row)
            return
        else:
            logger.warning(f"❌ Row count mismatch: Map_Purchase={len(mapped_df)}, Purchase={len(source_df)}. Running full migration.")
            full_process = True
    else:
        logger.warning("❌ Map_Purchase table does not exist. Running full migration.")
        full_process = True

    # ----- Full migration path -----
    if full_process:
        ensure_mapping_table(PURCHASE_DATE_FROM, PURCHASE_DATE_TO)

        if ENABLE_GLOBAL_JE_DOCNUMBER_DEDUP:
            apply_global_docnumber_strategy_for_Purchase()
            try:
                sql.clear_cache()
            except Exception:
                pass
            import time; time.sleep(1)

        generate_purchase_payloads_in_batches()

        post_query = f"SELECT * FROM {MAPPING_SCHEMA}.Map_Purchase WHERE Porter_Status = 'Ready'"
        post_rows = sql.fetch_table_with_params(post_query, tuple())
        if post_rows.empty:
            logger.warning("⚠️ No purchases with built JSON to post.")
            return

        url, headers = get_qbo_auth()
        select_batch_size = 300
        post_batch_limit  = 8
        timeout           = 40

        total = len(post_rows)
        logger.info(f"📤 Posting {total} Purchase record(s) via QBO Batch API (limit {post_batch_limit}/call)...")

        for i in range(0, total, select_batch_size):
            slice_df = post_rows.iloc[i:i + select_batch_size]
            successes, failures = _post_batch_purchases(
                slice_df, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
            )
            _apply_batch_updates_purchase(successes, failures)

            done = min(i + select_batch_size, total)
            logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

        # After full migration, retry failed ones with null Target_Id
        failed_df = sql.fetch_table("Map_Purchase", MAPPING_SCHEMA)
        failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
        if not failed_records.empty:
            logger.info(f"🔁 Reprocessing {len(failed_records)} failed Purchases with null Target_Id after main migration...")
            for _, row in failed_records.iterrows():
                post_purchase(row)

        logger.info("🏁 SMART Purchase migration complete.")

