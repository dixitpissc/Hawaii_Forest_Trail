
"""
Sequence : 17
Module: Deposit_migrator.py
Author: Dixit Prajapati
Created: 2025-09-16
Description: Handles migration of Deposit records from QBO to QBO.
Production : Working 
Development : Require when necessary
Phase : 02 - Multi User + Tax
"""


import os, json, requests, pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import global_logger as logger
from utils.mapping_updater import update_mapping_status
from config.mapping.deposit_mapping import DEPOSIT_HEADER_MAPPING as HEADER_MAP, DEPOSIT_LINE_MAPPING as LINE_MAP
from utils.apply_duplicate_docnumber import apply_duplicate_docnumber_strategy_dynamic
from utils.token_refresher import get_qbo_context_migration
# from storage.sqlserver.sql import executemany

load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
API_ENTITY = "deposit"
ENABLE_GLOBAL_JE_DOCNUMBER_DEDUP = True

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

def safe_float(val):
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
    Builds payload['TxnTaxDetail'] using header + up to 4 TaxLine components from Map_Deposit columns.
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

def ensure_mapping_table(DEPOSIT_DATE_FROM='1900-01-01',DEPOSIT_DATE_TO='2080-12-31'):
    """
    Ensures the Map_Deposit table is initialized with deposit data.
    Optimized for millions of rows:
    - Bulk fetch of Deposit + Deposit_Line
    - Preload all Map_* tables into dictionaries
    - Vectorized pandas mappings
    - Groupby aggregation for line-level refs
    """

    # 1. Date filter
    date_from = DEPOSIT_DATE_FROM
    date_to = DEPOSIT_DATE_TO
    query = f"""
        SELECT * FROM [{SOURCE_SCHEMA}].[Deposit]
        WHERE EXISTS (
            SELECT 1 FROM [{SOURCE_SCHEMA}].[Deposit_Line] l WHERE l.Parent_Id = [{SOURCE_SCHEMA}].[Deposit].Id
        )
        {"AND TxnDate >= ?" if date_from else ""}
        {"AND TxnDate <= ?" if date_to else ""}
    """
    params = tuple(p for p in [date_from, date_to] if p)
    df = sql.fetch_table_with_params(query, params)

    if df.empty:
        logger.info("⚠️ No deposit records found with lines in specified range.")
        return

    # 2. Core migration fields
    df["Source_Id"] = df["Id"]
    df["Target_Id"] = None
    df["Porter_Status"] = "Ready"
    df["Retry_Count"] = 0
    df["Failure_Reason"] = None
    required_cols = [
        "Payload_JSON",
        "Mapped_EntityRefs",
        "Mapped_AccountRefs",
        "Mapped_ClassRefs",
        "Mapped_PaymentMethodRefs",
        "Mapped_DepartmentRef",
        "Mapped_DepositToAccount",
        "Mapped_CurrencyRef",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # 3. Bulk load mapping tables into dicts
    def load_map(table):
        t = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[{table}]", tuple())
        return dict(zip(t["Source_Id"], t["Target_Id"])) if not t.empty else {}

    customer_dict = load_map("Map_Customer")
    account_dict = load_map("Map_Account")
    class_dict = load_map("Map_Class")
    payment_dict = load_map("Map_PaymentMethod")
    dept_dict = load_map("Map_Department")

    # NEW: TaxCode and TaxRate mapping (optional but preferred)
    taxcode_dict = load_map("Map_TaxCode") if sql.table_exists("Map_TaxCode", MAPPING_SCHEMA) else {}
    taxrate_dict = load_map("Map_TaxRate") if sql.table_exists("Map_TaxRate", MAPPING_SCHEMA) else {}

    # 4. Bulk fetch Deposit_Line and group
    deposit_lines = sql.fetch_table_with_params(f"SELECT * FROM [{SOURCE_SCHEMA}].[Deposit_Line]", tuple())
    deposit_lines_grouped = deposit_lines.groupby("Parent_Id") if not deposit_lines.empty else {}

    # 5. Resolve line-level mappings
    def get_refs(parent_id, col, mapping_dict):
        if parent_id not in deposit_lines_grouped.groups:
            return None
        vals = deposit_lines_grouped.get_group(parent_id)[col].dropna().unique()
        targets = [str(mapping_dict.get(v)) for v in vals if v in mapping_dict]
        return ";".join(sorted(set(targets))) if targets else None

    df["Mapped_EntityRefs"] = df["Id"].map(
        lambda x: get_refs(x, LINE_MAP["DepositLineDetail.Entity.value"], customer_dict)
    )
    df["Mapped_AccountRefs"] = df["Id"].map(
        lambda x: get_refs(x, LINE_MAP["DepositLineDetail.AccountRef.value"], account_dict)
    )
    df["Mapped_ClassRefs"] = df["Id"].map(
        lambda x: get_refs(x, LINE_MAP["DepositLineDetail.ClassRef.value"], class_dict)
    )
    df["Mapped_PaymentMethodRefs"] = df["Id"].map(
        lambda x: get_refs(x, LINE_MAP["DepositLineDetail.PaymentMethodRef.value"], payment_dict)
    )

    # 6. Header-level mappings
    df["Mapped_DepositToAccount"] = df[HEADER_MAP["DepositToAccountRef.value"]].map(
        lambda x: account_dict.get(x) if pd.notna(x) else None
    )
    df["Mapped_CurrencyRef"] = df[HEADER_MAP["CurrencyRef.value"]].fillna("USD")
    df["Mapped_DepartmentRef"] = df.get("DepartmentRef.value", None).map(
        lambda x: dept_dict.get(x) if pd.notna(x) else None
    )

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
    if sql.table_exists("Map_Deposit", MAPPING_SCHEMA):
        sql.run_query(f"TRUNCATE TABLE [{MAPPING_SCHEMA}].[Map_Deposit]")

    sql.insert_invoice_map_dataframe(df, "Map_Deposit", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df)} rows into {MAPPING_SCHEMA}.Map_Deposit")

def apply_global_docnumber_strategy_for_deposit():
    apply_duplicate_docnumber_strategy_dynamic(
        target_table="Map_deposit",
        schema=MAPPING_SCHEMA,
        check_against_tables=["Map_Bill", "Map_Invoice", "Map_VendorCredit", "Map_JournalEntry"],
    )

def get_lines(deposit_id):
    """
    O(1) when warm_lines_cache() has been called for the batch;
    otherwise falls back to a single-Parent_Id query.
    """
    pid = int(deposit_id)
    if '_LINES_CACHE' in globals() and pid in _LINES_CACHE:
        return _LINES_CACHE[pid]
    return sql.fetch_table_with_params(
        f"SELECT * FROM [{SOURCE_SCHEMA}].[Deposit_Line] WHERE Parent_Id = ?",
        (pid,)
    )

# ---------- ultra-fast caches (optional but recommended) ----------
_LINES_CACHE = {}          # parent_id -> DataFrame of lines
_LINES_CACHE_KEYS = set()  # track what's warmed
_MAPS = None               # lazy-loaded mapping dicts

def _ensure_maps_loaded():
    """
    Load Map_* lookup dictionaries once to avoid any per-line SQL during payload build.
    """
    global _MAPS
    if _MAPS is not None:
        return

    def _to_dict(table):
        t = sql.fetch_table_with_params(
            f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[{table}]",
            tuple()
        )
        return dict(zip(t["Source_Id"], t["Target_Id"])) if not t.empty else {}

    _MAPS = {
        "Account":       _to_dict("Map_Account"),
        "Class":         _to_dict("Map_Class"),
        "Department":    _to_dict("Map_Department"),
        "PaymentMethod": _to_dict("Map_PaymentMethod"),
        "Customer":      _to_dict("Map_Customer"),
        "Vendor":        _to_dict("Map_Vendor"),
        "Employee":      _to_dict("Map_Employee"),
    }

# ---------- OPT: call these around your batch loop ----------
def warm_lines_cache(parent_ids, chunk_size=1000):
    """
    Preload Deposit_Line rows for many parents at once (chunked IN query).
    Then get_lines() becomes O(1) dict lookups.
    """
    global _LINES_CACHE, _LINES_CACHE_KEYS
    parent_ids = [int(x) for x in parent_ids if pd.notna(x)]
    to_fetch = [pid for pid in parent_ids if pid not in _LINES_CACHE_KEYS]
    if not to_fetch:
        return

    for i in range(0, len(to_fetch), chunk_size):
        chunk = to_fetch[i:i+chunk_size]
        placeholders = ",".join(["?"] * len(chunk))
        q = f"SELECT * FROM [{SOURCE_SCHEMA}].[Deposit_Line] WHERE Parent_Id IN ({placeholders})"
        lines_df = sql.fetch_table_with_params(q, tuple(chunk))
        if lines_df.empty:
            _LINES_CACHE_KEYS.update(chunk)
            continue
        grp = lines_df.groupby("Parent_Id")
        for pid, sub in grp:
            _LINES_CACHE[pid] = sub
            _LINES_CACHE_KEYS.add(pid)

def clear_lines_cache():
    global _LINES_CACHE, _LINES_CACHE_KEYS
    _LINES_CACHE.clear()
    _LINES_CACHE_KEYS.clear()

# ===========================
# 1) get_lines (optimized)
# ===========================
def get_lines(deposit_id):
    """
    O(1) hot path via cache if warm_lines_cache() was used.
    Falls back to a single-Parent_Id query on cache miss.
    """
    pid = int(deposit_id)
    if pid in _LINES_CACHE:
        return _LINES_CACHE[pid]
    return sql.fetch_table_with_params(
        f"SELECT * FROM [{SOURCE_SCHEMA}].[Deposit_Line] WHERE Parent_Id = ?",
        (pid,)
    )

# ===========================
# 2) build_payload (optimized)
# ===========================
def build_payload(row, lines):
    _ensure_maps_loaded()
    maps = _MAPS

    mapped_deposit_to_account = row.get("Mapped_DepositToAccount")
    if not mapped_deposit_to_account:
        logger.warning(f"Skipping Deposit {row['Source_Id']} due to missing account mapping")
        return None

    if lines is None or lines.empty:
        return None

    department_ref_value = row.get("Mapped_DepartmentRef")
    txn_tax_total = row.get("TxnTaxDetail.TotalTax")

    payload = {
        "DepositToAccountRef": {"value": str(mapped_deposit_to_account)},
        "TxnDate": row.get("TxnDate"),
        "DocNumber": row.get("DocNumber"),
        "PrivateNote": row.get("PrivateNote"),
        "CurrencyRef": {"value": (row.get("Mapped_CurrencyRef") if pd.notna(row.get("Mapped_CurrencyRef")) else (row.get("CurrencyRef.value") if pd.notna(row.get("CurrencyRef.value")) else row.get("currencyref_value")))},
        "GlobalTaxCalculation": row.get("GlobalTaxCalculation"),
        "TxnStatus": row.get("TxnStatus"),
        "DepartmentRef": {"value": department_ref_value} if pd.notna(department_ref_value) else None,
        "TxnTaxDetail": {"TotalTax": safe_float(txn_tax_total)} if pd.notna(txn_tax_total) else None,
        "Line": []
    }
    payload = {k: v for k, v in payload.items() if v is not None}

    # maps
    acct_map  = maps["Account"]
    class_map = maps["Class"]
    dept_map  = maps["Department"]
    paym_map  = maps["PaymentMethod"]
    cust_map  = maps["Customer"]
    vend_map  = maps["Vendor"]
    emp_map   = maps["Employee"]

    # column indexes (conditionally present)
    cols = lines.columns
    get_idx = cols.get_loc
    def _idx(key: str):
        col = LINE_MAP.get(key)
        return get_idx(col) if col and (col in cols) else None

    # always-used columns (assumed present)
    c_LineNum = get_idx(LINE_MAP["LineNum"])
    c_Amount  = get_idx(LINE_MAP["Amount"])
    c_Desc    = get_idx(LINE_MAP["Description"])

    # conditionally-used columns
    c_Acct       = _idx("DepositLineDetail.AccountRef.value")
    c_Class      = _idx("DepositLineDetail.ClassRef.value")
    c_Dept       = _idx("DepositLineDetail.DepartmentRef.value")
    c_Method     = _idx("DepositLineDetail.PaymentMethodRef.value")
    c_CheckNum   = _idx("DepositLineDetail.CheckNum")
    c_TaxAmount  = _idx("DepositLineDetail.TaxAmount")
    c_TaxCode    = _idx("DepositLineDetail.TaxCodeRef.value")
    c_EntityVal  = _idx("DepositLineDetail.Entity.value")
    c_EntityType = _idx("DepositLineDetail.Entity.type")

    for row_vals in lines.itertuples(index=False, name=None):
        line_num_val = row_vals[c_LineNum]
        amt_val      = row_vals[c_Amount]
        desc_val     = row_vals[c_Desc]

        line = {
            "DetailType": "DepositLineDetail",
            "Amount": safe_float(amt_val),
            "Description": desc_val
        }
        if pd.notna(line_num_val):
            line["LineNum"] = int(line_num_val)

        detail = {}

        # AccountRef
        if c_Acct is not None:
            acct_val = row_vals[c_Acct]
            if pd.notna(acct_val):
                acct_id = acct_map.get(acct_val)
                if acct_id:
                    detail["AccountRef"] = {"value": str(acct_id)}

        # ClassRef
        if c_Class is not None:
            class_val = row_vals[c_Class]
            if pd.notna(class_val):
                class_id = class_map.get(class_val)
                if class_id:
                    detail["ClassRef"] = {"value": str(class_id)}

        # DepartmentRef
        if c_Dept is not None:
            dept_val = row_vals[c_Dept]
            if pd.notna(dept_val):
                dept_id = dept_map.get(dept_val)
                if dept_id:
                    detail["DepartmentRef"] = {"value": str(dept_id)}

        # PaymentMethodRef
        if c_Method is not None:
            method_val = row_vals[c_Method]
            if pd.notna(method_val):
                method_id = paym_map.get(method_val)
                if method_id:
                    detail["PaymentMethodRef"] = {"value": str(method_id)}

        # CheckNum
        if c_CheckNum is not None:
            checknum = row_vals[c_CheckNum]
            if pd.notna(checknum):
                detail["CheckNum"] = checknum

        # TaxAmount
        if c_TaxAmount is not None:
            tax_amount = row_vals[c_TaxAmount]
            if pd.notna(tax_amount):
                detail["TaxAmount"] = safe_float(tax_amount)

        # TaxCodeRef
        if c_TaxCode is not None:
            tax_code = row_vals[c_TaxCode]
            if pd.notna(tax_code):
                detail["TaxCodeRef"] = {"value": str(tax_code)}

        # Entity (Customer/Vendor/Employee)
        if c_EntityVal is not None and c_EntityType is not None:
            entity_val  = row_vals[c_EntityVal]
            entity_type = row_vals[c_EntityType]
            if pd.notna(entity_val) and pd.notna(entity_type):
                et = str(entity_type).capitalize()
                if et == "Customer":
                    eid = cust_map.get(entity_val)
                elif et == "Vendor":
                    eid = vend_map.get(entity_val)
                elif et == "Employee":
                    eid = emp_map.get(entity_val)
                else:
                    eid = None
                if eid:
                    detail["Entity"] = {"type": et, "value": str(eid)}

        if detail:
            line["DepositLineDetail"] = detail

        payload["Line"].append(line)

    # NEW: Add tax detail if available
    add_txn_tax_detail_from_row(payload, row)

    return payload if payload["Line"] else None

def generate_deposit_payloads_in_batches(batch_size=500):
    logger.info("🔧 Generating JSON payloads for deposits...")

    # Optional: pre-load Map_* dicts once so build_payload never hits DB
    try:
        _ensure_maps_loaded()
    except Exception:
        # safe to ignore if helper isn't present in this file
        pass

    while True:
        # fetch next batch of Ready + missing payloads
        df = sql.fetch_table_with_params(
            f"SELECT TOP {batch_size} * "
            f"FROM [{MAPPING_SCHEMA}].[Map_Deposit] "
            f"WHERE Porter_Status = 'Ready' "
            f"  AND (Payload_JSON IS NULL OR Payload_JSON = '')",
            ()
        )

        if df.empty:
            logger.info("✅ All deposit JSON payloads have been generated.")
            break

        # warm the line cache for this batch only (then clear to bound memory)
        try:
            clear_lines_cache()
            warm_lines_cache(df["Source_Id"].tolist())
        except Exception:
            # if helpers aren't defined, generation still proceeds with cold get_lines()
            pass

        # build payloads for this batch
        for _, row in df.iterrows():
            sid = row["Source_Id"]

            # validate required mapping
            if not row.get("Mapped_DepositToAccount") or pd.isna(row.get("Mapped_DepositToAccount")):
                update_mapping_status(
                    MAPPING_SCHEMA, "Map_Deposit", sid, "Failed",
                    failure_reason="Missing DepositToAccount mapping"
                )
                continue

            lines = get_lines(sid)
            payload = build_payload(row, lines)

            if not payload:
                update_mapping_status(
                    MAPPING_SCHEMA, "Map_Deposit", sid, "Failed",
                    failure_reason="Empty line items"
                )
                continue

            # keep your existing pretty-print pattern (safe, but you can pass `payload` directly for speed)
            pretty_json = json.dumps(payload, indent=2)
            update_mapping_status(
                MAPPING_SCHEMA, "Map_Deposit", sid, "Ready",
                payload=json.loads(pretty_json)
            )

        logger.info(f"✅ Processed {len(df)} payloads in this batch.")

    # final cleanup
    try:
        clear_lines_cache()
    except Exception:
        pass

session = requests.Session()

# ---------- tiny helpers ----------
try:
    import orjson as _orjson
    def _fast_loads(s):
        if isinstance(s, (bytes, bytearray)):
            return _orjson.loads(s)
        if isinstance(s, str):
            return _orjson.loads(s.encode("utf-8"))
        return s  # already a dict
except Exception:
    def _fast_loads(s):
        return json.loads(s) if isinstance(s, (str, bytes, bytearray)) else s

def _derive_batch_url(entity_url: str) -> str:
    """
    Turn .../v3/company/<realm>/<entity>?minorversion=XX into .../v3/company/<realm>/batch?minorversion=XX
    Works even if there's no query string.
    """
    qpos = entity_url.find("?")
    query = entity_url[qpos:] if qpos != -1 else ""
    path = entity_url[:qpos] if qpos != -1 else entity_url
    # strip trailing entity segment if present
    lower = path.lower()
    if lower.endswith("/deposit"):
        path = path.rsplit("/", 1)[0]
    return f"{path}/batch{query}"

def post_deposit(row):
    """
    Single-record fallback (kept for compatibility and retries).
    """
    sid = row["Source_Id"]
    if row.get("Porter_Status") == "Success":
        return
    if int(row.get("Retry_Count") or 0) >= 5:
        return
    if not row.get("Payload_JSON"):
        logger.warning(f"⚠️ Deposit {sid} skipped — missing Payload_JSON")
        return

    url, headers = get_qbo_auth()  # entity endpoint
    payload = _fast_loads(row["Payload_JSON"])

    try:
        resp = session.post(url, headers=headers, json=payload, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            qid = data.get("Deposit", {}).get("Id")
            update_mapping_status(MAPPING_SCHEMA, "Map_Deposit", sid, "Success", target_id=qid)
            logger.info(f"✅ Deposit {sid} → QBO {qid}")
        else:
            reason = (resp.text or "")[:500]
            update_mapping_status(MAPPING_SCHEMA, "Map_Deposit", sid, "Failed", failure_reason=reason, increment_retry=True)
            logger.error(f"❌ Failed Deposit {sid}: {reason}")
    except Exception as e:
        update_mapping_status(MAPPING_SCHEMA, "Map_Deposit", sid, "Failed", failure_reason=str(e), increment_retry=True)
        logger.exception(f"❌ Exception posting Deposit {sid}")

def post_deposits_batch(batch_df, batch_limit: int = 20):
    """
    High-throughput single-request batch posting (no multithreading).
    QBO Batch supports up to 30 ops per call; 20 is a safe default.
    """
    if batch_df.empty:
        return

    # Filter eligible in this slice (same guards as post_deposit)
    work = []
    for _, row in batch_df.iterrows():
        sid = row["Source_Id"]
        if row.get("Porter_Status") == "Success":
            continue
        if int(row.get("Retry_Count") or 0) >= 5:
            continue
        pj = row.get("Payload_JSON")
        if not pj:
            update_mapping_status(MAPPING_SCHEMA, "Map_Deposit", sid, "Failed", failure_reason="Missing Payload_JSON", increment_retry=True)
            continue
        try:
            payload = _fast_loads(pj)
        except Exception as e:
            update_mapping_status(MAPPING_SCHEMA, "Map_Deposit", sid, "Failed", failure_reason=f"Bad JSON: {e}", increment_retry=True)
            continue
        work.append((sid, payload))

    if not work:
        return

    # Chunk into BatchItemRequest groups
    url, headers = get_qbo_auth()              # entity endpoint
    batch_url = _derive_batch_url(url)         # .../batch

    for i in range(0, len(work), batch_limit):
        auto_refresh_token_if_needed()
        chunk = work[i:i + batch_limit]
        batch_body = {
            "BatchItemRequest": [
                {"bId": str(sid), "operation": "create", "Deposit": payload}
                for (sid, payload) in chunk
            ]
        }

        try:
            
            resp = session.post(batch_url, headers=headers, json=batch_body, timeout=40)
            if resp.status_code != 200:
                reason = (resp.text or f"HTTP {resp.status_code}")[:1000]
                # mark all in chunk as failed (retryable)
                for sid, _ in chunk:
                    update_mapping_status(MAPPING_SCHEMA, "Map_Deposit", sid, "Failed", failure_reason=reason, increment_retry=True)
                logger.error(f"❌ Batch POST failed ({len(chunk)} items): {reason}")
                continue

            rj = resp.json()
            items = rj.get("BatchItemResponse", []) or []
            seen = set()

            for item in items:
                bid = item.get("bId")
                seen.add(bid)
                dep = item.get("Deposit")
                if dep and "Id" in dep:
                    qid = dep["Id"]
                    update_mapping_status(MAPPING_SCHEMA, "Map_Deposit", bid, "Success", target_id=qid)
                    logger.info(f"✅ Deposit {bid} → QBO {qid}")
                    continue

                # Fault path
                fault = item.get("Fault") or {}
                errs = fault.get("Error") or []
                if errs:
                    msg = errs[0].get("Message") or ""
                    det = errs[0].get("Detail") or ""
                    reason = (msg + " | " + det).strip()[:1000]
                else:
                    reason = "Unknown batch failure"
                update_mapping_status(MAPPING_SCHEMA, "Map_Deposit", bid, "Failed", failure_reason=reason, increment_retry=True)
                logger.error(f"❌ Failed Deposit {bid}: {reason}")

            # Any missing responses in this chunk → mark failed
            for sid, _ in chunk:
                if str(sid) not in seen:
                    update_mapping_status(MAPPING_SCHEMA, "Map_Deposit", sid, "Failed", failure_reason="No response for bId", increment_retry=True)
                    logger.error(f"❌ No response for Deposit {sid}")

        except Exception as e:
            reason = f"Batch exception: {e}"
            for sid, _ in chunk:
                update_mapping_status(MAPPING_SCHEMA, "Map_Deposit", sid, "Failed", failure_reason=reason, increment_retry=True)
            logger.exception(f"❌ Exception during batch POST ({len(chunk)} items)")

def migrate_deposits(DEPOSIT_DATE_FROM,DEPOSIT_DATE_TO):
    print("\n🚀 Starting Deposit Migration Phase\n" + "=" * 40)

    ensure_mapping_table(DEPOSIT_DATE_FROM,DEPOSIT_DATE_TO)
    try:
        sql.clear_cache()
    except Exception:
        pass

    generate_deposit_payloads_in_batches()

    rows = sql.fetch_table("Map_Deposit", MAPPING_SCHEMA)
    eligible = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("⚠️ No eligible deposits to post.")
        return

    # Outer DB-read batch (keeps memory bounded). Inner: batch POST to QBO.
    select_batch_size = 300   # how many rows we load from DB at once
    post_batch_limit  = 10    # how many we send in ONE QBO batch call (<=30)

    total = len(eligible)
    logger.info(f"📤 Posting {total} Deposit(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        slice_df = eligible.iloc[i:i + select_batch_size]
        # Post in batch (single-threaded, multiple per request)
        post_deposits_batch(slice_df, batch_limit=post_batch_limit)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    # Retry failed with null Target_Id once (single-record fallback)
    failed_df = sql.fetch_table("Map_Deposit", MAPPING_SCHEMA)
    failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
    if not failed_records.empty:
        logger.info(f"🔁 Reprocessing {len(failed_records)} failed Deposits (single-record fallback)...")
        for _, row in failed_records.iterrows():
            post_deposit(row)

    print("\n🏁 Deposit migration completed.")

def resume_or_post_deposits(DEPOSIT_DATE_FROM,DEPOSIT_DATE_TO):
    print("\n🔁 Resuming Deposit Migration (conditional mode)\n" + "=" * 50)

    if not sql.table_exists("Map_Deposit", MAPPING_SCHEMA):
        logger.warning("❌ Map_Deposit table does not exist. Running full migration.")
        migrate_deposits(DEPOSIT_DATE_FROM,DEPOSIT_DATE_TO)
        return

    mapped_df = sql.fetch_table("Map_Deposit", MAPPING_SCHEMA)
    source_df = sql.fetch_table("Deposit", SOURCE_SCHEMA)

    if len(mapped_df) != len(source_df):
        logger.warning(f"❌ Row count mismatch: Map_Deposit={len(mapped_df)}, Deposit={len(source_df)}. Running full migration.")
        migrate_deposits(DEPOSIT_DATE_FROM,DEPOSIT_DATE_TO)
        return

    if mapped_df["Payload_JSON"].isnull().any() or (mapped_df["Payload_JSON"] == "").any():
        logger.warning("🔍 Some Payload_JSON values missing. Generating for those...")
        generate_deposit_payloads_in_batches()
        mapped_df = sql.fetch_table("Map_Deposit", MAPPING_SCHEMA)
    else:
        logger.info("✅ All Deposits have Payload_JSON.")

    eligible = mapped_df[mapped_df["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("⚠️ No eligible Deposits to post.")
        return

    select_batch_size = 300
    post_batch_limit  = 10

    total = len(eligible)
    logger.info(f"📤 Posting {total} Deposit(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        slice_df = eligible.iloc[i:i + select_batch_size]
        post_deposits_batch(slice_df, batch_limit=post_batch_limit)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    # Retry failed with null Target_Id once (single-record fallback)
    failed_df = sql.fetch_table("Map_Deposit", MAPPING_SCHEMA)
    failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
    if not failed_records.empty:
        logger.info(f"🔁 Reprocessing {len(failed_records)} failed Deposits (single-record fallback)...")
        for _, row in failed_records.iterrows():
            post_deposit(row)

    print("\n🏁 Deposit posting completed.")

# --- SMART DEPOSIT MIGRATION ---
def smart_deposit_migration(DEPOSIT_DATE_FROM,DEPOSIT_DATE_TO):
    """
    Smart migration entrypoint for Deposit:
    - If Map_Deposit table exists and row count matches Deposit table, resume/post Deposits.
    - If table missing or row count mismatch, perform full migration.
    """
    logger.info("🔎 Running smart_deposit_migration...")
    full_process = False
    if sql.table_exists("Map_Deposit", MAPPING_SCHEMA):
        mapped_df = sql.fetch_table("Map_Deposit", MAPPING_SCHEMA)
        source_df = sql.fetch_table("Deposit", SOURCE_SCHEMA)
        if len(mapped_df) == len(source_df):
            logger.info("✅ Table exists and row count matches. Resuming/posting Deposits.")
            resume_or_post_deposits(DEPOSIT_DATE_FROM,DEPOSIT_DATE_TO)
            # After resume_or_post, reprocess failed records one more time
            failed_df = sql.fetch_table("Map_Deposit", MAPPING_SCHEMA)
            failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
            if not failed_records.empty:
                logger.info(f"🔁 Reprocessing {len(failed_records)} failed Deposits with null Target_Id after main migration...")
                for _, row in failed_records.iterrows():
                    post_deposit(row)
            return
        else:
            logger.warning(f"❌ Row count mismatch: Map_Deposit={len(mapped_df)}, Deposit={len(source_df)}. Running full migration.")
            full_process = True
    else:
        logger.warning("❌ Map_Deposit table does not exist. Running full migration.")
        full_process = True
    if full_process:
        migrate_deposits()
        # After full migration, reprocess failed records with null Target_Id one more time
        failed_df = sql.fetch_table("Map_Deposit", MAPPING_SCHEMA)
        failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
        if not failed_records.empty:
            logger.info(f"🔁 Reprocessing {len(failed_records)} failed Deposits with null Target_Id after main migration...")
            for _, row in failed_records.iterrows():
                post_deposit(row)

