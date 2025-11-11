"""
Sequence : 14
Module: Bill_migrator.py (Optimized)
Author: Dixit Prajapati
Created: 2025-09-15
Description: Handles migration of Bill records from QBO to QBO.
Production : Ready
Development : Require when necessary
Phase : 02 - Multi User + Tax + ReimburgeID 
"""

#working good till payload generation

import os, json, requests, pandas as pd
import time, random
from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import global_logger as logger
from config.mapping.bill_mapping import BILL_HEADER_MAPPING, BILL_LINE_MAPPING
from utils.apply_duplicate_docnumber import apply_duplicate_docnumber_strategy_dynamic
from utils.token_refresher import get_qbo_context_migration
from dotenv import load_dotenv
from utils.mapping_updater import update_mapping_status
from storage.sqlserver.sql import executemany
from utils.payload_cleaner import deep_clean

# ---------------------------- ENV/INIT ----------------------------
load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")

# Session for API calls
session = requests.Session()

# Turn on/off optional duplicate DocNumber strategy for Bills
ENABLE_BILL_DOCNUMBER_DEDUP = True  # set True to activate


# --------------------------------- Batch posting helpers --------------------------------------
def _sleep_with_jitter(attempt=0):
    base = 0.5 + attempt * 0.5
    time.sleep(base + random.uniform(0, 0.5))

def _post_batch_bills(eligible_batch, url, headers, timeout=20, max_manual_retries=2):
    """
    Single-threaded batch poster for Bills:
    - Pre-decodes Payload_JSON with orjson/json once per row.
    - Handles 401/403 (token refresh once) and gentle manual retries for sporadic errors.
    Returns:
        successes: list[(qid, sid)]
        failures:  list[(reason, sid)]
    """
    successes, failures = [], []
    parsed = {}
    for _, r in eligible_batch.iterrows():
        sid = r["Source_Id"]
        if r.get("Porter_Status") == "Success" or int(r.get("Retry_Count") or 0) >= 5 or not r.get("Payload_JSON"):
            continue
        try:
            parsed[sid] = orjson.loads(r["Payload_JSON"]) if "orjson" in globals() else json.loads(r["Payload_JSON"])
        except Exception as e:
            failures.append((f"Bad JSON: {e}", sid))
    for _, r in eligible_batch.iterrows():
        sid = r["Source_Id"]
        payload = parsed.get(sid)
        if payload is None:
            continue
        attempted_refresh = False
        for attempt in range(max_manual_retries + 1):
            try:
                resp = session.post(url, headers=headers, json=payload, timeout=timeout)
                sc = resp.status_code
                if sc == 200:
                    qid = None
                    try:
                        qid = resp.json().get("Bill", {}).get("Id")
                    except Exception:
                        pass
                    if qid:
                        successes.append((qid, sid))
                        logger.info(f"✅ Posted Bill {sid} → Target ID {qid}")
                    else:
                        failures.append(("No Id in response", sid))
                        logger.info(f"❌ Posted Bill {sid} → No Id in response")
                    break
                elif sc in (401, 403) and not attempted_refresh:
                    attempted_refresh = True
                    try:
                        auto_refresh_token_if_needed(force=True)
                    except Exception:
                        pass
                    url, headers = get_qbo_auth()
                    _sleep_with_jitter(attempt=attempt)
                    continue
                elif sc in (429, 500, 502, 503, 504) and attempt < max_manual_retries:
                    _sleep_with_jitter(attempt=attempt)
                    continue
                else:
                    failures.append((resp.text[:300], sid))
                    logger.info(f"❌ Posted Bill {sid} → Failed with status {sc}")
                    break
            except requests.Timeout:
                if attempt < max_manual_retries:
                    _sleep_with_jitter(attempt=attempt)
                    continue
                failures.append(("Timeout", sid))
                break
            except Exception as e:
                if attempt < max_manual_retries:
                    _sleep_with_jitter(attempt=attempt)
                    continue
                failures.append((str(e), sid))
                break
    return successes, failures

def _apply_batch_updates_bill(successes, failures):
    if successes:
        executemany(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_Bill] SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL WHERE Source_Id=?",
            [(qid, sid) for qid, sid in successes]
        )
    if failures:
        executemany(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_Bill] SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? WHERE Source_Id=?",
            [(reason, sid) for reason, sid in failures]
        )

try:
    import orjson
    # orjson option for pretty print
    def dumps_fast(obj): 
        return orjson.dumps(obj, option=orjson.OPT_INDENT_2).decode("utf-8")
except Exception:
    def dumps_fast(obj): 
        return json.dumps(obj, indent=2, ensure_ascii=False)


# ---------------------------- SMALL HELPERS ----------------------------

def safe_float(val):
    try: return float(val)
    except: return 0.0

# ⬇️ ADD THIS HELPER RIGHT AFTER safe_float
def _norm_id(v):
    """
    Return a clean numeric ID string or None.
    Skips None / NaN / "" / "nan" / "null" / "none" and any non-digits.
    """
    if v is None:
        return None
    # pandas NaN handling
    try:
        import math
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    s = str(v).strip().lower()
    if s in ("", "nan", "null", "none"):
        return None
    return s if s.isdigit() else None

def _to_str_or_none(x):
    if pd.isna(x): return None
    return str(x)

def _requests_session():
    # reuse session for speed
    if not hasattr(_requests_session, "_s"):
        _requests_session._s = requests.Session()
    return _requests_session._s

def fetch_dataframe_with_params(query: str, params: tuple):
    conn = sql.get_sqlserver_connection()
    try:
        df = pd.read_sql_query(query, conn, params=params)
        return df
    finally:
        conn.close()

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
    Builds payload['TxnTaxDetail'] using header + up to 4 TaxLine components from Map_Bill columns.
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

# ---------------------------- AUTH ----------------------------
def get_qbo_auth():
    ctx = get_qbo_context_migration()
    base = ctx["BASE_URL"]
    realm = ctx["REALM_ID"]
    return f"{base}/v3/company/{realm}/bill", {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

# ---------------------------- MAP PRELOAD ----------------------------
def preload_maps(mapping_schema: str):
    """Load Map_* (Vendor/Account/Item/Class/Department/Term) into dicts once."""
    maps = {}
    def load_map(name):
        tbl = f"Map_{name}"
        if sql.table_exists(tbl, mapping_schema):
            dfm = sql.fetch_dataframe(
                f"SELECT CAST(Source_Id AS NVARCHAR(255)) AS Source_Id, "
                f"CAST(Target_Id AS NVARCHAR(255)) AS Target_Id "
                f"FROM [{mapping_schema}].[{tbl}] WITH (NOLOCK)"
            )
            if not dfm.empty:
                return dict(zip(dfm["Source_Id"], dfm["Target_Id"]))
        return {}
    for name in ["Vendor", "Account", "Item", "Class", "Department", "Term", "Customer"]:
        maps[name] = load_map(name)
    return maps

# ---------------------------- LINES PREFETCH ----------------------------
def fetch_and_prepare_lines(source_schema: str, mapping_schema: str, bill_ids: list | None):
    """
    Pull minimal Bill_Line columns, pre-map to target IDs, and group by Parent_Id.
    If bill_ids is None, fetch all; otherwise fetch only the requested Parent_Id set.
    """
    cols = [
        "Parent_Id",
        BILL_LINE_MAPPING["DetailType"],
        BILL_LINE_MAPPING["Amount"],
        BILL_LINE_MAPPING.get("Description"),
        BILL_LINE_MAPPING.get("AccountRef.value"),
        "ItemBasedExpenseLineDetail.ItemRef.value",
        "AccountBasedExpenseLineDetail.ClassRef.value",
        "AccountBasedExpenseLineDetail.DepartmentRef.value",
        "AccountBasedExpenseLineDetail.CustomerRef.value",
        "ItemBasedExpenseLineDetail.ClassRef.value",
        "ItemBasedExpenseLineDetail.DepartmentRef.value",
        BILL_LINE_MAPPING.get("MarkupInfo.Percent"),
        BILL_LINE_MAPPING.get("TaxCodeRef.value"),
        BILL_LINE_MAPPING.get("BillableStatus"),
        BILL_LINE_MAPPING.get("ItemBasedExpenseLineDetail.UnitPrice"),
        BILL_LINE_MAPPING.get("ItemBasedExpenseLineDetail.Qty"),
        BILL_LINE_MAPPING.get("ItemBasedExpenseLineDetail.TaxCodeRef.value"),
        BILL_LINE_MAPPING.get("ItemBasedExpenseLineDetail.BillableStatus"),
    ]
    cols = [c for c in cols if c and column_exists("Bill_Line", source_schema, c)]

    where = ""
    params = ()
    if bill_ids is not None and len(bill_ids) > 0:
        placeholders = ",".join(["?"] * len(bill_ids))
        where = f" WHERE Parent_Id IN ({placeholders})"
        params = tuple(bill_ids)

    q = f"SELECT {', '.join('[' + c + ']' for c in cols)} FROM [{source_schema}].[Bill_Line] WITH (NOLOCK){where}"
    lines = fetch_dataframe_with_params(q, params) if params else sql.fetch_dataframe(q)

    if lines.empty:
        return {}

    maps = preload_maps(mapping_schema)

    # Pre-map commonly used references
    acct_src_col = BILL_LINE_MAPPING.get("AccountRef.value")
    if acct_src_col and acct_src_col in lines.columns:
        lines["Mapped_AccountRef"] = lines[acct_src_col].astype(str).map(maps["Account"])

    if "ItemBasedExpenseLineDetail.ItemRef.value" in lines.columns:
        lines["Mapped_ItemRef"] = lines["ItemBasedExpenseLineDetail.ItemRef.value"].astype(str).map(maps["Item"])

    # Pre-map line-level class/department/customer (both ABELD and IBELD)
    mapping_targets = [
        ("AccountBasedExpenseLineDetail.ClassRef.value", "Mapped_ABE_ClassRef"),
        ("AccountBasedExpenseLineDetail.DepartmentRef.value", "Mapped_ABE_DepartmentRef"),
        ("AccountBasedExpenseLineDetail.CustomerRef.value", "Mapped_ABE_CustomerRef"),
        ("ItemBasedExpenseLineDetail.ClassRef.value", "Mapped_IBE_ClassRef"),
        ("ItemBasedExpenseLineDetail.DepartmentRef.value", "Mapped_IBE_DepartmentRef"),
    ]
    for src_col, out_col in mapping_targets:
        if src_col in lines.columns:
            if "ClassRef" in src_col:
                key = "Class"
            elif "DepartmentRef" in src_col:
                key = "Department"
            elif "CustomerRef" in src_col:
                key = "Customer"
            else:
                continue
            lines[out_col] = lines[src_col].astype(str).map(maps[key])

    # Group once
    grouped = {pid: g for pid, g in lines.groupby("Parent_Id", sort=False)}
    return grouped

# ---------------------------- MAPPING TABLE ----------------------------
def insert_bill_map_dataframe(df, table: str, schema: str = "dbo"):
    """Create/evolve Map_Bill and bulk insert rows with fast_executemany."""
    if df.empty:
        logger.warning(f"⚠️ Skipped insertion: DataFrame for table '{schema}.{table}' is empty.")
        return

    conn = sql.get_sqlserver_connection()
    cur = conn.cursor()
    try:
        full_table = f"[{schema}].[{table}]"
        original_columns = df.columns.tolist()
        sanitized_columns = [sql.sanitize_column_name(col) for col in original_columns]
        rename_mapping = dict(zip(original_columns, sanitized_columns))
        df2 = df.rename(columns=rename_mapping)

        col_defs = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df2.columns])
        if not sql.table_exists(table, schema):
            cur.execute(f"CREATE TABLE {full_table} ({col_defs})")
        else:
            existing_cols = set(col.column_name for col in cur.columns(table=table, schema=schema))
            new_cols = [c for c in df2.columns if c not in existing_cols]
            for col in new_cols:
                cur.execute(f"ALTER TABLE {full_table} ADD [{col}] NVARCHAR(MAX)")

        insert_cols = ", ".join([f"[{col}]" for col in df2.columns])
        placeholders = ", ".join(["?"] * len(df2.columns))
        params = [tuple(None if pd.isna(v) else v for v in row) for row in df2.itertuples(index=False, name=None)]

        try:
            cur.fast_executemany = True
        except Exception:
            pass

        cur.executemany(f"INSERT INTO {full_table} ({insert_cols}) VALUES ({placeholders})", params)
        cur.connection.commit()
    finally:
        cur.close()
        conn.close()


# -------------------- DDL helper (add once) --------------------
# def _ensure_reimburse_col():
#     """
#     Ensure porter_entities_mapping.Map_Bill has column:
#       target_reimbursecharge_id NVARCHAR(MAX) NULL
#     Idempotent; safe to call many times.
#     """
#     sql.executesql(f"""
#     IF NOT EXISTS (
#         SELECT 1
#         FROM INFORMATION_SCHEMA.COLUMNS
#         WHERE TABLE_SCHEMA = '{MAPPING_SCHEMA}'
#           AND TABLE_NAME   = 'Map_Bill'
#           AND COLUMN_NAME  = 'target_reimbursecharge_id'
#     )
#     BEGIN
#         ALTER TABLE [{MAPPING_SCHEMA}].[Map_Bill]
#         ADD [target_reimbursecharge_id] NVARCHAR(MAX) NULL;
#         -- Optional index if you plan to filter by this column:
#         -- CREATE NONCLUSTERED INDEX IX_Map_Bill_Target_ReimburseCharge
#         --   ON [{MAPPING_SCHEMA}].[Map_Bill]([target_reimbursecharge_id]);
#     END
#     """)

def ensure_mapping_table(BILL_DATE_FROM='1900-01-01',BILL_DATE_TO='2080-12-31'):
    """
    Build Map_Bill with minimal queries:
    - Pull eligible Bill headers (date filter + has lines) in SQL.
    - Map Vendor/Account/Class/Department/Term via preloaded dicts (no per-row queries).
    - Aggregate line keys once for diagnostics (optional).
    """
    # Push down filters to SQL
    date_filter = ""
    params = []
    if BILL_DATE_FROM and BILL_DATE_TO:
        date_filter = " AND b.TxnDate >= ? AND b.TxnDate <= ?"
        params = [BILL_DATE_FROM, BILL_DATE_TO]

    df = fetch_dataframe_with_params(f"""
        SELECT b.*
        FROM [{SOURCE_SCHEMA}].[Bill] b WITH (NOLOCK)
        WHERE EXISTS (SELECT 1 FROM [{SOURCE_SCHEMA}].[Bill_Line] bl WITH (NOLOCK) WHERE bl.Parent_Id = b.Id)
        {date_filter}
    """, tuple(params))

    if df.empty:
        logger.warning("No bills found with associated lines. Skipping mapping.")
        return

    df["Source_Id"] = df["Id"]
    df["Target_Id"] = None
    df["Porter_Status"] = "Ready"
    df["Retry_Count"] = 0
    df["Failure_Reason"] = None
    df["Payload_JSON"] = None
    df["Target_Reimbursecharge_Id"] = None

    maps = preload_maps(MAPPING_SCHEMA)

    # NEW: TaxCode mapping (optional but preferred)
    if sql.table_exists("Map_TaxCode", MAPPING_SCHEMA):
        taxcode_map = sql.fetch_dataframe(
            f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_TaxCode]"
        )
        if not taxcode_map.empty:
            taxcode_dict = dict(zip(taxcode_map["Source_Id"], taxcode_map["Target_Id"]))
        else:
            taxcode_dict = {}
    else:
        taxcode_dict = {}

    # NEW: Taxrate mapping (optional but preferred)
    if sql.table_exists("Map_TaxRate", MAPPING_SCHEMA):
        taxrate_map = sql.fetch_dataframe(
            f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_TaxRate]"
        )
        if not taxrate_map.empty:
            taxrate_dict = dict(zip(taxrate_map["Source_Id"], taxrate_map["Target_Id"]))
        else:
            taxrate_dict = {}
    else:
        taxrate_dict = {}

    # Map header references
    vend_src = BILL_HEADER_MAPPING.get("VendorRef.value")
    acct_src = BILL_HEADER_MAPPING.get("APAccountRef.value")
    dept_src = BILL_HEADER_MAPPING.get("DepartmentRef.value")
    class_src = BILL_HEADER_MAPPING.get("ClassRef.value")
    term_src = BILL_HEADER_MAPPING.get("SalesTermRef.value")

    if vend_src in df.columns:
        df["Mapped_Vendor"] = df[vend_src].astype(str).map(maps["Vendor"])
    else:
        df["Mapped_Vendor"] = None

    if acct_src in df.columns:
        df["Mapped_Account"] = df[acct_src].astype(str).map(maps["Account"])
    else:
        df["Mapped_Account"] = None

    df["Mapped_Department"] = df[dept_src].astype(str).map(maps["Department"]) if dept_src in df.columns else None
    df["Mapped_Class"] = df[class_src].astype(str).map(maps["Class"]) if class_src in df.columns else None
    df["Target_TermRef"] = df[term_src].astype(str).map(maps["Term"]) if term_src in df.columns else None

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

    # Optional: pre-aggregate line source/target ids for diagnostics (single fetch)
    line_cols = []
    for c in [BILL_LINE_MAPPING.get("AccountRef.value"),
              "ItemBasedExpenseLineDetail.ItemRef.value"]:
        if c and column_exists("Bill_Line", SOURCE_SCHEMA, c):
            line_cols.append(c)
    if line_cols:
        lc_sql = f"""
            SELECT Parent_Id, {', '.join('['+c+']' for c in line_cols)}
            FROM [{SOURCE_SCHEMA}].[Bill_Line] WITH (NOLOCK)
        """
        line_df = sql.fetch_dataframe(lc_sql)
        if not line_df.empty:
            # Map target ids
            if BILL_LINE_MAPPING.get("AccountRef.value") in line_df.columns:
                line_df["Mapped_AccountRef"] = line_df[BILL_LINE_MAPPING["AccountRef.value"]].astype(str).map(maps["Account"])
            if "ItemBasedExpenseLineDetail.ItemRef.value" in line_df.columns:
                line_df["Mapped_ItemRef"] = line_df["ItemBasedExpenseLineDetail.ItemRef.value"].astype(str).map(maps["Item"])

            agg_dict = {}
            if BILL_LINE_MAPPING.get("AccountRef.value"):
                agg_dict[BILL_LINE_MAPPING["AccountRef.value"]] = lambda x: "|".join(map(str, pd.unique(x.dropna())))
            if "Mapped_AccountRef" in line_df.columns:
                agg_dict["Mapped_AccountRef"] = lambda x: "|".join(map(str, pd.unique(x.dropna())))
            if "ItemBasedExpenseLineDetail.ItemRef.value" in line_df.columns:
                agg_dict["ItemBasedExpenseLineDetail.ItemRef.value"] = lambda x: "|".join(map(str, pd.unique(x.dropna())))
            if "Mapped_ItemRef" in line_df.columns:
                agg_dict["Mapped_ItemRef"] = lambda x: "|".join(map(str, pd.unique(x.dropna())))

            agg_map = line_df.groupby("Parent_Id", as_index=False).agg(agg_dict)
            agg_map = agg_map.rename(columns={"Parent_Id": "Source_Id"})
            renames = {}
            if BILL_LINE_MAPPING.get("AccountRef.value"):
                renames[BILL_LINE_MAPPING["AccountRef.value"]] = "Line_Account_Source_Id"
            if "Mapped_AccountRef" in agg_map.columns:
                renames["Mapped_AccountRef"] = "Line_Account_Target_Id"
            if "ItemBasedExpenseLineDetail.ItemRef.value" in agg_map.columns:
                renames["ItemBasedExpenseLineDetail.ItemRef.value"] = "Line_Item_Source_Id"
            if "Mapped_ItemRef" in agg_map.columns:
                renames["Mapped_ItemRef"] = "Line_Item_Target_Id"
            agg_map.rename(columns=renames, inplace=True)
            df = df.merge(agg_map, how="left", on="Source_Id")

    # Rebuild Map_Bill
    if sql.table_exists("Map_Bill", MAPPING_SCHEMA):
        sql.run_query(f"DELETE FROM [{MAPPING_SCHEMA}].[Map_Bill]")
    insert_bill_map_dataframe(df, "Map_Bill", MAPPING_SCHEMA)
    logger.info(f"✅ Mapped {len(df)} Bill records with valid lines")

def apply_duplicate_docnumber_strategy_for_bills():
    """
    Deduplicates DocNumbers in Map_Bill:
    - If DocNumber length ≤18, suffixes like -01, -02, etc.
    - If DocNumber length >18, replaces first 2 characters with 01, 02, etc.
    - First occurrence of each DocNumber is left unchanged.
    """
    if not column_exists("Map_Bill", MAPPING_SCHEMA, "DocNumber"):
        logger.info("ℹ️ Map_Bill.DocNumber not found; skipping deduplication.")
        return

    apply_duplicate_docnumber_strategy_dynamic(
            target_table="Map_Bill",
            schema=MAPPING_SCHEMA,
            docnumber_column="DocNumber",
            source_id_column="Source_Id",
            duplicate_column="Duplicate_DocNumber",
            check_against_tables=[]
        )

# ---------------------------- PAYLOAD ----------------------------
def build_payload(row: dict | pd.Series, lines_df: pd.DataFrame | None):
    """
    Build QBO Bill payload using pre-mapped columns; no DB calls here.
    """
    # Row accessor
    get = row.get if isinstance(row, dict) else row.__getitem__
    has = (lambda k: (k in row and pd.notna(get(k)))) if isinstance(row, dict) else (lambda k: (k in row.index and pd.notna(get(k))))

    if not has("Mapped_Vendor") or not has("Mapped_Account"):
        logger.warning(f"Skipping Bill {get('Source_Id')} due to missing Vendor or Account mapping")
        return None

    payload = {
        "VendorRef": {"value": str(get("Mapped_Vendor"))},
        "APAccountRef": {"value": str(get("Mapped_Account"))},
        "Line": []
    }

    # ✅ Add DepartmentRef / ClassRef / SalesTermRef only if clean numeric
    dep = _norm_id(row.get("Mapped_Department"))
    if dep:
        payload["DepartmentRef"] = {"value": dep}

    cls = _norm_id(row.get("Mapped_Class"))
    if cls:
        payload["ClassRef"] = {"value": cls}

    term = _norm_id(row.get("Target_TermRef"))
    if term:
        payload["SalesTermRef"] = {"value": term}

    # Header fields from mapping (skip refs handled above)
    for qbo_field, source_col in BILL_HEADER_MAPPING.items():
        if qbo_field in ("VendorRef.value", "APAccountRef.value", "DepartmentRef.value", "SalesTermRef.value"):
            continue
        if source_col in row and pd.notna(get(source_col)):
            if "." in qbo_field:
                parent, child = qbo_field.split(".")
                payload.setdefault(parent, {})[child] = get(source_col)
            else:
                payload[qbo_field] = get(source_col)

    # Optional: override DocNumber with deduped value if present
    if "Duplicate_DocNumber" in row and pd.notna(get("Duplicate_DocNumber")):
        payload["DocNumber"] = get("Duplicate_DocNumber")

    # Currency & ExchangeRate
    c_ref = BILL_HEADER_MAPPING.get("CurrencyRef.value")
    currency_value = get(c_ref) if c_ref and c_ref in row and pd.notna(get(c_ref)) else "USD"
    payload["CurrencyRef"] = {"value": currency_value}

    exch_col = BILL_HEADER_MAPPING.get("ExchangeRate")
    if exch_col and exch_col in row and pd.notna(get(exch_col)):
        payload["ExchangeRate"] = safe_float(get(exch_col))

    # PrivateNote fallback to Memo
    if not payload.get("PrivateNote") or str(payload.get("PrivateNote")).strip() == "":
        for k in ["Memo", "memo"]:
            if k in row and pd.notna(get(k)):
                payload["PrivateNote"] = get(k)
                break

    # Lines
    if lines_df is None or lines_df.empty:
        return None

    # Faster iteration with index tracking for DataFrame access
    it = lines_df.itertuples(index=True, name="Ln")  # Include index for DataFrame access
    detail_type_col = BILL_LINE_MAPPING["DetailType"]
    amount_col = BILL_LINE_MAPPING["Amount"]
    desc_col = BILL_LINE_MAPPING.get("Description")

    # Get the actual column names from the DataFrame for BillableStatus and MarkupInfo fields
    abe_billable_col = None
    ibe_billable_col = None
    abe_markup_col = None
    
    for col in lines_df.columns:
        if "AccountBasedExpenseLineDetail" in col and "BillableStatus" in col:
            abe_billable_col = col
        elif "ItemBasedExpenseLineDetail" in col and "BillableStatus" in col:
            ibe_billable_col = col
        elif "AccountBasedExpenseLineDetail" in col and "MarkupInfo" in col and "Percent" in col:
            abe_markup_col = col

    # Create a mapping from column names to namedtuple field positions
    sample_row = next(iter(lines_df.itertuples(index=True, name="Ln")), None)
    if sample_row:
        column_to_field = {}
        for i, col in enumerate(lines_df.columns):
            if i < len(sample_row._fields) - 1:  # -1 because first field is Index
                field_name = sample_row._fields[i + 1]  # +1 to skip Index field
                column_to_field[col] = field_name
        
        # Get the actual field names for our target columns
        abe_billable_field = column_to_field.get(abe_billable_col)
        ibe_billable_field = column_to_field.get(ibe_billable_col)
        abe_markup_field = column_to_field.get(abe_markup_col)
    else:
        abe_billable_field = None
        ibe_billable_field = None
        abe_markup_field = None

    for ln in it:
        detail_type = getattr(ln, detail_type_col)
        amount_val = getattr(ln, amount_col)
        desc_val = getattr(ln, desc_col) if (desc_col and hasattr(ln, desc_col)) else None

        base_line = {
            "DetailType": detail_type,
            "Amount": safe_float(amount_val),
            "Description": desc_val
        }

        if detail_type == "AccountBasedExpenseLineDetail":
            # Only add refs if they are clean numeric; skip line if AccountRef missing/invalid
            acct_target = _norm_id(getattr(ln, "Mapped_AccountRef", None))
            if not acct_target:
                continue
            detail = {"AccountRef": {"value": acct_target}}

            # Tax & billable
            tax_col = BILL_LINE_MAPPING.get("TaxCodeRef.value")
            if tax_col and hasattr(ln, tax_col):
                v = getattr(ln, tax_col)
                if pd.notna(v):
                    detail["TaxCodeRef"] = {"value": v}
            
            # Handle BillableStatus
            if abe_billable_field and hasattr(ln, abe_billable_field):
                abe_bill_status = getattr(ln, abe_billable_field)
                if pd.notna(abe_bill_status):
                    detail["BillableStatus"] = abe_bill_status
            elif abe_billable_col:
                # Fallback to DataFrame access using row index
                row_idx = ln.Index
                abe_bill_status = lines_df.iloc[row_idx][abe_billable_col]
                if pd.notna(abe_bill_status):
                    detail["BillableStatus"] = abe_bill_status

            # Pre-mapped class/department/customer (optional)
            cls_val = _norm_id(getattr(ln, "Mapped_ABE_ClassRef", None))
            if cls_val:
                detail["ClassRef"] = {"value": cls_val}
            dep_val = _norm_id(getattr(ln, "Mapped_ABE_DepartmentRef", None))
            if dep_val:
                detail["DepartmentRef"] = {"value": dep_val}
            cust_val = _norm_id(getattr(ln, "Mapped_ABE_CustomerRef", None))
            if cust_val:
                detail["CustomerRef"] = {"value": cust_val}

            # Handle MarkupInfo.Percent
            markup_percent = None
            if abe_markup_field and hasattr(ln, abe_markup_field):
                markup_percent = getattr(ln, abe_markup_field)
            elif abe_markup_col:
                # Fallback to DataFrame access using row index
                row_idx = ln.Index
                markup_percent = lines_df.iloc[row_idx][abe_markup_col]
            
            if markup_percent is not None and pd.notna(markup_percent):
                try:
                    markup_val = float(markup_percent)
                    detail["MarkupInfo"] = {"Percent": markup_val}
                except (ValueError, TypeError):
                    pass  # Skip invalid values

            base_line["AccountBasedExpenseLineDetail"] = detail


        elif detail_type == "ItemBasedExpenseLineDetail":
            # Only add refs if they are clean numeric; skip line if ItemRef missing/invalid
            item_target = _norm_id(getattr(ln, "Mapped_ItemRef", None))
            if not item_target:
                continue
            detail = {"ItemRef": {"value": item_target}}

            # Handle UnitPrice, Qty, TaxCodeRef, and BillableStatus
            for f in ["UnitPrice", "Qty", "TaxCodeRef.value"]:
                src = BILL_LINE_MAPPING.get(f"ItemBasedExpenseLineDetail.{f}")
                if src and hasattr(ln, src):
                    val = getattr(ln, src)
                    if pd.notna(val):
                        if f.endswith(".value"):
                            key = f.split(".")[0]
                            detail[key] = {"value": val}
                        else:
                            detail[f] = val
            
            # Handle BillableStatus
            if ibe_billable_field and hasattr(ln, ibe_billable_field):
                ibe_bill_status = getattr(ln, ibe_billable_field)
                if pd.notna(ibe_bill_status):
                    detail["BillableStatus"] = ibe_bill_status
            elif ibe_billable_col:
                # Fallback to DataFrame access using row index
                row_idx = ln.Index
                ibe_bill_status = lines_df.iloc[row_idx][ibe_billable_col]
                if pd.notna(ibe_bill_status):
                    detail["BillableStatus"] = ibe_bill_status

            # Optional class/department (only if clean numeric)
            cls_val = _norm_id(getattr(ln, "Mapped_IBE_ClassRef", None))
            if cls_val:
                detail["ClassRef"] = {"value": cls_val}
            dep_val = _norm_id(getattr(ln, "Mapped_IBE_DepartmentRef", None))
            if dep_val:
                detail["DepartmentRef"] = {"value": dep_val}

            base_line["ItemBasedExpenseLineDetail"] = detail

        else:
            # unsupported detail type
            continue

        payload["Line"].append(base_line)

    # NEW: Add tax detail if available
    add_txn_tax_detail_from_row(payload, row)

    return deep_clean(payload) if payload["Line"] else None

# ---------------------------- PAYLOAD GENERATION (BATCHED) ----------------------------
def generate_payloads_in_batches(batch_size=1000):
    """
    Generates payloads for Bills in batches.
    - Prefetch Bill_Line for the batch and use pre-mapped columns.
    - Bulk UPDATE Payload_JSON/Status using executemany (fast_executemany).
    """
    logger.info(f"📦 Starting batched payload generation for Bills (batch size = {batch_size})")

    rows = sql.fetch_dataframe(f"""
        SELECT * FROM [{MAPPING_SCHEMA}].[Map_Bill] WITH (NOLOCK)
        WHERE (Porter_Status IS NULL OR Porter_Status IN ('Ready', 'Failed'))
          AND Payload_JSON IS NULL
        ORDER BY Source_Id
    """)
    total = len(rows)
    if total == 0:
        logger.info("✅ No eligible Bill records found for payload generation.")
        return

    logger.info(f"📄 Found {total} Bills requiring payload generation.")

    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = rows.iloc[start:end]
        bill_ids = batch["Source_Id"].tolist()

        # Prefetch & pre-map lines for just this batch
        lines_by_parent = fetch_and_prepare_lines(SOURCE_SCHEMA, MAPPING_SCHEMA, bill_ids=bill_ids)

        updates_ok = []
        updates_fail = []

        for r in batch.itertuples(index=False, name="Row"):
            source_id = r.Source_Id
            lines = lines_by_parent.get(source_id)
            payload = build_payload(r._asdict(), lines)
            if payload:
                payload_json = dumps_fast(payload)
                updates_ok.append((payload_json, source_id))
            else:
                # Per your 2025‑08‑03 note: mark as Failed (not Skipped) when payload missing
                reason = "Failed to build payload — missing mapped header or valid lines"
                updates_fail.append((reason, source_id))

        if updates_ok:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Bill] SET Payload_JSON=?, Porter_Status='Ready', Failure_Reason=NULL WHERE Source_Id=?",
                updates_ok
            )
        if updates_fail:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Bill] SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? WHERE Source_Id=?",
                updates_fail
            )
        logger.info(f"🔄 Processed {start+1}–{end} of {total}")

    logger.info("✅ Finished batched payload generation for Bills.")

def remove_nulls_from_payload(data):
    """
    Recursively removes all keys with None values from a nested dictionary or list.
    Converts Python None → removes field from final payload.
    """
    if isinstance(data, dict):
        return {
            k: remove_nulls_from_payload(v)
            for k, v in data.items()
            if v is not None
        }
    elif isinstance(data, list):
        return [remove_nulls_from_payload(item) for item in data if item is not None]
    else:
        return data

def _extract_reimburse_ids_from_bill_obj(bill_obj: dict) -> str | None:
    """
    Returns a comma-separated string of ReimburseCharge IDs found in Bill.LinkedTxn
    (minorversion >= 55 allows ReimburseCharge as a LinkedTxn on Bill).
    """
    try:
        linked = bill_obj.get("LinkedTxn") or []
        ids = [str(x.get("TxnId")) for x in linked if (x.get("TxnType") == "ReimburseCharge" and x.get("TxnId"))]
        if ids:
            return ",".join(sorted(set(ids), key=lambda z: int(z) if str(z).isdigit() else z))
    except Exception:
        pass
    return None

# ---------------------------- POSTING ----------------------------
session = requests.Session()

# ============================ Shared Helpers ============================
# Add once near the top of your module; safe if already present.

# Fast JSON loader (uses orjson when available)
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
        entity_name: "Bill" | "VendorCredit" | "JournalEntry" | ...
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

# ============================ Batch Poster =============================
def _post_batch_bills(eligible_batch, url, headers, timeout=40, post_batch_limit=20, max_manual_retries=1):
    """
    High-throughput, single-threaded batch posting for Bills via QBO /batch.
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

    # Derive /batch endpoint for Bill
    batch_url = _derive_batch_url(url, "Bill")

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
                    {"bId": str(sid), "operation": "create", "Bill": payload}
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
                        ent = item.get("Bill")
                        if ent and "Id" in ent:
                            # qid = ent["Id"]
                            # successes.append((qid, bid))
                            qid = ent["Id"]
                            reimb = _extract_reimburse_ids_from_bill_obj(ent)  # <- NEW
                            successes.append((qid, bid, reimb))                # <- NEW shape

                            logger.info(f"✅ Bill {bid} → QBO {qid}")
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
                        logger.error(f"❌ Bill {bid} failed: {reason}")

                    # Any missing responses → mark failed
                    for sid, _ in chunk:
                        if str(sid) not in seen:
                            failures.append(("No response for bId", sid))
                            logger.error(f"❌ No response for Bill {sid}")
                    break

                elif sc in (401, 403) and not attempted_refresh:
                    logger.warning(f"🔐 {sc} on Bill batch ({len(chunk)} items); refreshing token and retrying once...")
                    auto_refresh_token_if_needed()
                    # refresh creds and batch_url
                    url, headers = get_qbo_auth()
                    batch_url = _derive_batch_url(url, "Bill")
                    attempted_refresh = True
                    continue

                else:
                    reason = (resp.text or f"HTTP {sc}")[:1000]
                    for sid, _ in chunk:
                        failures.append((reason, sid))
                    logger.error(f"❌ Bill batch failed ({len(chunk)} items): {reason}")
                    break

            except Exception as e:
                reason = f"Batch exception: {e}"
                for sid, _ in chunk:
                    failures.append((reason, sid))
                logger.exception(f"❌ Exception during Bill batch POST ({len(chunk)} items)")
                break

    return successes, failures

# ============================ Apply Updates ============================
# def _apply_batch_updates_bill(successes, failures):
#     """
#     Set-based updates for Map_Bill; fast and idempotent.
#     """
#     if successes:
#         executemany(
#             f"UPDATE [{MAPPING_SCHEMA}].[Map_Bill] "
#             f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
#             f"WHERE Source_Id=?",
#             [(qid, sid) for qid, sid in successes]
#         )
#     if failures:
#         executemany(
#             f"UPDATE [{MAPPING_SCHEMA}].[Map_Bill] "
#             f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
#             f"WHERE Source_Id=?",
#             [(reason, sid) for reason, sid in failures]
#         )

def _apply_batch_updates_bill(successes, failures):
    """
    Set-based updates for Map_Bill; fast and idempotent.
    successes: list of tuples (qid, sid, reimburse_csv_or_None)
    failures : list of tuples (reason, sid)
    """
    if successes:
        # Split updates: with/without reimburse ids to avoid wiping existing values.
        with_reimb   = [(qid, reimb, sid) for (qid, sid, reimb) in successes if reimb]
        without_reimb = [(qid, sid) for (qid, sid, reimb) in successes if not reimb]

        if with_reimb:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Bill] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL, "
                f"    Target_Reimbursecharge_Id=? "
                f"WHERE Source_Id=?",
                with_reimb
            )
        if without_reimb:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Bill] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                without_reimb
            )

    if failures:
        executemany(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_Bill] "
            f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
            f"WHERE Source_Id=?",
            [(reason, sid) for reason, sid in failures]
        )


# ============================ Single-record Fallback ====================
def post_bill(row):
    """
    Single-record fallback; prefer _post_batch_bills for throughput.
    """
    sid = row.get("Source_Id")
    if row.get("Porter_Status") == "Success":
        return
    if int(row.get("Retry_Count") or 0) >= 5:
        return

    pj = row.get("Payload_JSON")
    if not pj:
        update_mapping_status(MAPPING_SCHEMA, "Map_Bill", sid, "Failed",
                              failure_reason="Missing Payload_JSON", increment_retry=True)
        logger.warning(f"⚠️ Bill {sid} skipped — missing Payload_JSON")
        return

    try:
        payload = _fast_loads(pj)
    except Exception as e:
        update_mapping_status(MAPPING_SCHEMA, "Map_Bill", sid, "Failed",
                              failure_reason=f"Bad JSON: {e}", increment_retry=True)
        return

    url, headers = get_qbo_auth()
    try:
        resp =  session.post(url, headers=headers, json=payload, timeout=20)
        if resp.status_code == 200:
            # qid = (resp.json().get("Bill") or {}).get("Id")
            # if qid:
            #     update_mapping_status(MAPPING_SCHEMA, "Map_Bill", sid, "Success", target_id=qid)
            #     logger.info(f"✅ Bill {sid} → QBO {qid}")
            #     return
            bill_obj = (resp.json().get("Bill") or {})
            qid = bill_obj.get("Id")
            if qid:
                reimb = _extract_reimburse_ids_from_bill_obj(bill_obj)
                if reimb:
                    # lightweight, targeted update to include reimburse ids
                    executemany(
                        f"UPDATE [{MAPPING_SCHEMA}].[Map_Bill] "
                        f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL, "
                        f"    Target_Reimbursecharge_Id=? "
                        f"WHERE Source_Id=?",
                        [(qid, reimb, sid)]
                    )
                else:
                    update_mapping_status(MAPPING_SCHEMA, "Map_Bill", sid, "Success", target_id=qid)
                logger.info(f"✅ Bill {sid} → QBO {qid} "
                            f"{'(reimburse ids: ' + reimb + ')' if reimb else ''}")
                return
            
            reason = "No Id in response"
        elif resp.status_code in (401, 403):
            logger.warning(f"🔐 {resp.status_code} on Bill {sid}; refreshing token and retrying once...")
            auto_refresh_token_if_needed()
            url, headers = get_qbo_auth()
            resp2 = session.post(url, headers=headers, json=payload, timeout=20)
            if resp2.status_code == 200:
                qid = (resp2.json().get("Bill") or {}).get("Id")
                if qid:
                    update_mapping_status(MAPPING_SCHEMA, "Map_Bill", sid, "Success", target_id=qid)
                    logger.info(f"✅ Bill {sid} → QBO {qid}")
                    return
                reason = "No Id in response (after refresh)"
            else:
                reason = (resp2.text or f"HTTP {resp2.status_code}")[:500]
        else:
            reason = (resp.text or f"HTTP {resp.status_code}")[:500]

        update_mapping_status(MAPPING_SCHEMA, "Map_Bill", sid, "Failed",
                              failure_reason=reason, increment_retry=True)
        logger.error(f"❌ Failed Bill {sid}: {reason}")

    except Exception as e:
        update_mapping_status(MAPPING_SCHEMA, "Map_Bill", sid, "Failed",
                              failure_reason=str(e), increment_retry=True)
        logger.exception(f"❌ Exception posting Bill {sid}")

# ============================== Migrate ================================
def migrate_bills(BILL_DATE_FROM,BILL_DATE_TO,retry_only: bool = False):
    """
    Posts Bills using pre-generated Payload_JSON via the QBO Batch API.
    Single-threaded, high-throughput (multiple items per HTTP request).
    """
    print("\n🚀 Starting Bill Migration Phase\n" + "=" * 40)

    ensure_mapping_table(BILL_DATE_FROM,BILL_DATE_TO)

    # DocNumber dedup (as per your pipeline)
    apply_duplicate_docnumber_strategy_for_bills()

    # Build/generate payloads
    generate_payloads_in_batches(batch_size=1000)

    rows = sql.fetch_table("Map_Bill", MAPPING_SCHEMA)
    if retry_only:
        eligible = rows[rows["Porter_Status"].isin(["Failed"])].reset_index(drop=True)
    else:
        eligible = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)

    if eligible.empty:
        logger.info("⚠️ No eligible Bills to post.")
        return

    url, headers = get_qbo_auth()

    select_batch_size = 300   # DB slice size
    post_batch_limit  = 10    # Bills per QBO batch call (<=30). Set to 3 if you want exactly 3-at-a-time.

    total = len(eligible)
    logger.info(f"📤 Posting {total} Bill(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        slice_df = eligible.iloc[i:i + select_batch_size]
        successes, failures = _post_batch_bills(
            slice_df, url, headers, timeout=40, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates_bill(successes, failures)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    # Optional one-pass fallback retry via single-record POST for failures without Target_Id
    failed_df = sql.fetch_table("Map_Bill", MAPPING_SCHEMA)
    failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
    if not failed_records.empty:
        logger.info(f"🔁 Reprocessing {len(failed_records)} failed Bills (single-record fallback)...")
        for _, row in failed_records.iterrows():
            post_bill(row)

    print("\n🏁 Bill migration completed.")

# ============================ Resume/Continue ==========================
def resume_or_post_bills(BILL_DATE_FROM,BILL_DATE_TO,retry_only: bool = False):
    """
    Resumes/continues Bill migration:
    - Rebuild mapping if Map_Bill missing/incomplete.
    - Generate only missing Payload_JSON.
    - Post eligible rows via Batch API (single-threaded).
    """
    logger.info("🔁 Initiating resume_or_post_bills process...")
    print("\n🔁 Resuming Bill Migration (conditional mode)\n" + "=" * 50)

    # 1) Ensure table exists
    if not sql.table_exists("Map_Bill", MAPPING_SCHEMA):
        logger.warning("❌ Map_Bill table does not exist. Running full migration.")
        migrate_bills(BILL_DATE_FROM,BILL_DATE_TO,retry_only=retry_only)
        return

    mapped_df = sql.fetch_table("Map_Bill", MAPPING_SCHEMA)
    source_df = sql.fetch_table("Bill", SOURCE_SCHEMA)

    # 2) Ensure Duplicate_DocNumber present (dedup if needed)
    if "Duplicate_DocNumber" not in mapped_df.columns or \
       mapped_df["Duplicate_DocNumber"].isnull().any() or (mapped_df["Duplicate_DocNumber"] == "").any():
        logger.info("🔍 Running DocNumber deduplication for Bills...")
        apply_duplicate_docnumber_strategy_for_bills()
        mapped_df = sql.fetch_table("Map_Bill", MAPPING_SCHEMA)
    else:
        logger.info("✅ All Duplicate_DocNumber values present. Skipping deduplication.")

    # 3) Generate missing payloads only
    payload_missing = mapped_df["Payload_JSON"].isnull() | (mapped_df["Payload_JSON"] == "")
    missing_count = int(payload_missing.sum())
    if missing_count > 0:
        logger.info(f"🧰 {missing_count} Bills missing Payload_JSON. Generating for those...")
        generate_payloads_in_batches(batch_size=1000)
        mapped_df = sql.fetch_table("Map_Bill", MAPPING_SCHEMA)
    else:
        logger.info("✅ All Bills have Payload_JSON.")

    # 4) Choose eligible set
    if retry_only:
        eligible = mapped_df[mapped_df["Porter_Status"].isin(["Failed"])].reset_index(drop=True)
    else:
        eligible = mapped_df[mapped_df["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)

    if eligible.empty:
        logger.info("⚠️ No eligible Bills to post.")
        return

    url, headers = get_qbo_auth()

    select_batch_size = 300
    post_batch_limit  = 10

    total = len(eligible)
    logger.info(f"📤 Posting {total} Bill(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        slice_df = eligible.iloc[i:i + select_batch_size]
        successes, failures = _post_batch_bills(
            slice_df, url, headers, timeout=40, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates_bill(successes, failures)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    # Optional single-record fallback retry
    failed_df = sql.fetch_table("Map_Bill", MAPPING_SCHEMA)
    failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
    if not failed_records.empty:
        logger.info(f"🔁 Reprocessing {len(failed_records)} failed Bills (single-record fallback)...")
        for _, row in failed_records.iterrows():
            post_bill(row)

    print("\n🏁 Bill posting completed.")


# --- SMART BILL MIGRATION ---
def smart_bill_migration(BILL_DATE_FROM,BILL_DATE_TO):
    """
    Smart migration entrypoint for Bill:
    - If Map_Bill table exists and row count matches Bill table, resume/post Bills.
    - If table missing or row count mismatch, perform full migration.
    """
    logger.info("🔎 Running smart_bill_migration...")
    full_process = False
    if sql.table_exists("Map_Bill", MAPPING_SCHEMA):
        mapped_df = sql.fetch_table("Map_Bill", MAPPING_SCHEMA)
        source_df = sql.fetch_table("Bill", SOURCE_SCHEMA)
        if len(mapped_df) == len(source_df):
            logger.info("✅ Table exists and row count matches. Resuming/posting Bills.")
            resume_or_post_bills(BILL_DATE_FROM,BILL_DATE_TO)
            # After resume_or_post, reprocess failed records one more time
            failed_df = sql.fetch_table("Map_Bill", MAPPING_SCHEMA)
            failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
            if not failed_records.empty:
                logger.info(f"🔁 Reprocessing {len(failed_records)} failed Bills with null Target_Id after main migration...")
                for _, row in failed_records.iterrows():
                    post_bill(row)
            return
        else:
            logger.warning(f"❌ Row count mismatch: Map_Bill={len(mapped_df)}, Bill={len(source_df)}. Running full migration.")
            full_process = True
    else:
        logger.warning("❌ Map_Bill table does not exist. Running full migration.")
        full_process = True
    if full_process:
        migrate_bills(BILL_DATE_FROM,BILL_DATE_TO)
        # After full migration, reprocess failed records with null Target_Id one more time
        failed_df = sql.fetch_table("Map_Bill", MAPPING_SCHEMA)
        failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
        if not failed_records.empty:
            logger.info(f"🔁 Reprocessing {len(failed_records)} failed Bills with null Target_Id after main migration...")
            for _, row in failed_records.iterrows():
                post_bill(row)
