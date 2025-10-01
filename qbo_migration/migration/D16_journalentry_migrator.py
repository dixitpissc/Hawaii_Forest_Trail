"""
Sequence : 16
Module: JournalEntry_migrator.py
Author: Dixit Prajapati
Created: 2025-09-15
Description: Handles migration of JournalEntry records from QBO to QBO.
Production : Ready
Development : Require when necessary
Phase : 02 - Multi User + Tax
"""


import os, json, requests, pandas as pd
from dotenv import load_dotenv
from utils.token_refresher import (
    auto_refresh_token_if_needed,
    get_qbo_context_migration,
)
from utils.log_timer import global_logger as logger
from utils.mapping_updater import update_mapping_status
from utils.apply_duplicate_docnumber import apply_duplicate_docnumber_strategy_dynamic
from storage.sqlserver import sql
from config.mapping.journalentry_mapping import (
    JOURNALENTRY_HEADER_MAPPING as HEADER_MAP,
    JOURNALENTRY_LINE_MAPPING as LINE_MAP,
)

# --------------------------- Global Init ---------------------------
load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA  = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
API_ENTITY     = "journalentry"


ENABLE_GLOBAL_JE_DOCNUMBER_DEDUP = True

# Tuned HTTP session (same posting logic, just faster & more robust connections)
session = requests.Session()
try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    _retry = Retry(
        total=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
        raise_on_status=False,
    )
    _adapter = HTTPAdapter(max_retries=_retry, pool_connections=64, pool_maxsize=64)
    session.mount("https://", _adapter)
    session.mount("http://", _adapter)
except Exception:
    pass

# Global, read-only lookup maps populated by preload_maps()
_g_maps = {}

# --------------------------- Small Utilities ---------------------------
def safe_float(val):
    try:
        return float(val)
    except Exception:
        return 0.0

def get_qbo_auth():
    """
    Returns (url, headers) just like your original function.
    """
    ctx = get_qbo_context_migration()
    base = ctx["BASE_URL"]
    realm = ctx["REALM_ID"]
    return f"{base}/v3/company/{realm}/{API_ENTITY}", {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def apply_global_docnumber_strategy_for_journalentry():
    apply_duplicate_docnumber_strategy_dynamic(
        target_table="Map_JournalEntry",
        schema=MAPPING_SCHEMA,
        check_against_tables=["Map_Bill", "Map_Invoice", "Map_VendorCredit", "Map_JournalEntry"],
    )

def _table_exists(map_table: str) -> bool:
    try:
        return sql.table_exists(map_table, MAPPING_SCHEMA)
    except Exception:
        return False

def _load_map(table_name: str):
    """
    Load a Map_* table into {Source_Id -> Target_Id} dict if it exists.
    """
    if not _table_exists(table_name):
        return {}
    df = sql.fetch_table(table_name, MAPPING_SCHEMA)
    if df.empty:
        return {}
    # normalize to str keys (source ids may be numeric or string)
    out = {}
    sid_col = "Source_Id"
    tid_col = "Target_Id"
    if sid_col in df.columns and tid_col in df.columns:
        for sid, tid in zip(df[sid_col], df[tid_col]):
            if pd.notna(sid) and pd.notna(tid):
                out[str(sid)] = str(tid)
    return out

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
    Builds payload['TxnTaxDetail'] using header + up to 4 TaxLine components from Map_JournalEntry columns.
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

def preload_maps():
    """
    Loads all commonly used Map_* tables once into memory
    so payload generation and enrichment do O(1) lookups
    instead of per-row SQL queries.
    """
    global _g_maps
    logger.info("🧭 Preloading Map_* lookups into memory...")
    _g_maps = {
        "Account":    _load_map("Map_Account"),
        "Class":      _load_map("Map_Class"),
        "Department": _load_map("Map_Department"),
        "TaxCode":    _load_map("Map_TaxCode"),
        "TaxRate":    _load_map("Map_TaxRate"),
        "Customer":   _load_map("Map_Customer"),
        "Vendor":     _load_map("Map_Vendor"),
        "Employee":   _load_map("Map_Employee"),
    }
    # lightweight metrics for visibility
    info = ", ".join([f"{k}:{len(v)}" for k, v in _g_maps.items()])
    logger.info(f"🧠 Lookup sizes → {info}")

def _map_id(map_name: str, source_id):
    """
    Fast in-memory lookup: returns Target_Id or None.
    """
    if source_id is None or pd.isna(source_id):
        return None
    d = _g_maps.get(map_name) or {}
    return d.get(str(source_id))

def _norm_cap(s: str):
    return str(s).strip().capitalize() if pd.notna(s) else None

# --------------------------- Data Access ---------------------------
def get_lines_for_parents(parent_ids):
    """
    Fetch all lines for a set of Parent_Id values in one round-trip.
    Returns a DataFrame; caller can groupby('Parent_Id').
    """
    if not parent_ids:
        return pd.DataFrame()

    # Build a temp table approach for very large IN lists to avoid parameter limits.
    # However, to avoid core logic changes, we keep it simple with IN chunks.
    chunks = []
    CHUNK = 999
    for i in range(0, len(parent_ids), CHUNK):
        part = parent_ids[i : i + CHUNK]
        placeholders = ",".join(["?"] * len(part))
        q = f"SELECT * FROM {SOURCE_SCHEMA}.JournalEntry_Line WHERE Parent_Id IN ({placeholders})"
        df = sql.fetch_table_with_params(q, tuple(part))
        if not df.empty:
            chunks.append(df)
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()

# --------------------------- Mapping Table Build ---------------------------
def ensure_mapping_table(JOURNALENTRY_DATE_FROM,JOURNALENTRY_DATE_TO):
    """
    Builds (or refreshes) porter_entities_mapping.Map_JournalEntry from source JournalEntry + JournalEntry_Line,
    storing the same enrichment fields you already used — but in a set-based, batched way.
    """
    logger.info("🧱 Ensuring Map_JournalEntry from source...")

    # 1) Load headers that have at least one line (set-based)
    hdr = sql.fetch_table("JournalEntry", SOURCE_SCHEMA)
    if hdr.empty:
        logger.warning("No JournalEntry headers found.")
        return

    valid_parents = sql.fetch_column(f"SELECT DISTINCT Parent_Id FROM {SOURCE_SCHEMA}.JournalEntry_Line")
    if not valid_parents:
        logger.warning("No JournalEntry lines found; skipping.")
        return

    hdr = hdr[hdr["Id"].isin(valid_parents)].copy()

    # Optional date filter (kept identical to your logic)
    if JOURNALENTRY_DATE_FROM and JOURNALENTRY_DATE_TO and "TxnDate" in hdr.columns:
        hdr = hdr[(hdr["TxnDate"] >= JOURNALENTRY_DATE_FROM) & (hdr["TxnDate"] <= JOURNALENTRY_DATE_TO)].copy()

    if hdr.empty:
        logger.warning("No journal entries found with lines after filtering.")
        return

    # 2) Initialize Map_JournalEntry DataFrame with required columns
    hdr["Source_Id"]     = hdr["Id"]
    hdr["Target_Id"]     = None
    hdr["Porter_Status"] = "Ready"
    hdr["Retry_Count"]   = 0
    hdr["Failure_Reason"]= None
    hdr["Payload_JSON"]  = None

    # Add mapped/enrichment columns (same names as your original)
    for field in [
        "Mapped_AccountRefs", "Mapped_ClassRefs", "Mapped_DepartmentRefs",
        "Mapped_EntityRefs", "Mapped_TaxCodeRefs", "Mapped_EntityTypes",
        "Mapped_CurrencyRef", "Mapped_TxnTaxAmount"
    ]:
        hdr[field] = None

    # CurrencyRef + TxnTax (same logic)
    hdr["Mapped_CurrencyRef"] = hdr[HEADER_MAP.get("CurrencyRef.value")].fillna("USD")
    tax_field = HEADER_MAP.get("TxnTaxDetail.TotalTax")
    if tax_field and tax_field in hdr.columns:
        hdr["Mapped_TxnTaxAmount"] = hdr[tax_field].apply(safe_float)
    else:
        hdr["Mapped_TxnTaxAmount"] = None

    # NEW: Map header-level TxnTaxCodeRef (from source header column)
    # Source column name provided: [TxnTaxDetail.TxnTaxCodeRef.value]
    source_tax_col = "TxnTaxDetail.TxnTaxCodeRef.value"
    if source_tax_col in hdr.columns:
        def _map_taxcode(src):
            if pd.isna(src):
                return None
            tgt = _g_maps.get("TaxCode", {}).get(str(src))
            if not tgt:
                # If mapping is missing, leave None (QBO will default/allow per-tenant settings)
                logger.debug(f"🔎 Missing TaxCode mapping for Source_Id={src}")
            return tgt
        hdr["Mapped_TxnTaxCodeRef"] = hdr[source_tax_col].map(_map_taxcode)
    else:
        hdr["Mapped_TxnTaxCodeRef"] = None
    
    #######################

    source_tax_col = "TxnTaxDetail.TaxLine[0].TaxLineDetail.TaxRateRef.value"
    if source_tax_col in hdr.columns:
        def _map_taxrate(src):
            if pd.isna(src):
                return None
            tgt = _g_maps.get("TaxRate", {}).get(str(src))
            if not tgt:
                # If mapping is missing, leave None (QBO will default/allow per-tenant settings)
                logger.debug(f"🔎 Missing TaxRate mapping for Source_Id={src}")
            return tgt
        hdr["Mapped_TxnTaxRateRef"] = hdr[source_tax_col].map(_map_taxrate)
    else:
        hdr["Mapped_TxnTaxRateRef"] = None

    # 3) Load all lines once for these parents
    parent_ids = hdr["Id"].tolist()
    lines_df = get_lines_for_parents(parent_ids)
    if lines_df.empty:
        logger.warning("JournalEntry lines not found for selected headers.")
        return

    # 4) Preload maps for fast in-memory lookups
    preload_maps()

    # Column shortcuts
    acct_col = LINE_MAP.get("JournalEntryLineDetail.AccountRef.value")
    cls_col  = LINE_MAP.get("JournalEntryLineDetail.ClassRef.value")
    dep_col  = LINE_MAP.get("JournalEntryLineDetail.DepartmentRef.value")
    tax_col  = LINE_MAP.get("JournalEntryLineDetail.TaxCodeRef.value")
    ent_val  = LINE_MAP.get("JournalEntryLineDetail.Entity.EntityRef.value")
    ent_typ  = LINE_MAP.get("JournalEntryLineDetail.Entity.Type")

    # 5) Build per-parent aggregates (unique Target_Ids per category)
    # Map source ids -> target ids using vectorized .map where possible
    if acct_col and acct_col in lines_df.columns:
        lines_df["_tgt_acct"] = lines_df[acct_col].astype(str).map(_g_maps.get("Account", {}))
    else:
        lines_df["_tgt_acct"] = None

    if cls_col and cls_col in lines_df.columns:
        lines_df["_tgt_class"] = lines_df[cls_col].astype(str).map(_g_maps.get("Class", {}))
    else:
        lines_df["_tgt_class"] = None

    if dep_col and dep_col in lines_df.columns:
        lines_df["_tgt_dept"] = lines_df[dep_col].astype(str).map(_g_maps.get("Department", {}))
    else:
        lines_df["_tgt_dept"] = None

    if tax_col and tax_col in lines_df.columns:
        lines_df["_tgt_tax"] = lines_df[tax_col].astype(str).map(_g_maps.get("TaxCode", {}))
    else:
        lines_df["_tgt_tax"] = None

    # Dynamic Entity: detect per-row type and map via the correct dictionary
    if ent_val and ent_val in lines_df.columns and ent_typ and ent_typ in lines_df.columns:
        # normalize type to capitalized Customer/Vendor/Employee (same behavior)
        lines_df["_norm_type"] = lines_df[ent_typ].apply(_norm_cap)
        _cust = _g_maps.get("Customer", {})
        _vend = _g_maps.get("Vendor", {})
        _empl = _g_maps.get("Employee", {})
        def _map_entity(row):
            if pd.isna(row.get(ent_val)) or pd.isna(row.get("_norm_type")):
                return None
            sid = str(row[ent_val])
            t = row["_norm_type"]
            if t == "Customer":
                return _cust.get(sid)
            if t == "Vendor":
                return _vend.get(sid)
            if t == "Employee":
                return _empl.get(sid)
            return None
        lines_df["_tgt_entity"] = lines_df.apply(_map_entity, axis=1)
    else:
        lines_df["_norm_type"]  = None
        lines_df["_tgt_entity"] = None

    # Group and aggregate unique IDs as comma-joined strings (same stored semantics)
    def _uniq_join(series):
        vals = [str(x) for x in series.dropna().unique().tolist() if str(x) != "None"]
        return ",".join(sorted(vals)) if vals else None

    agg = lines_df.groupby("Parent_Id", dropna=False).agg(
        Mapped_AccountRefs    = ("_tgt_acct", _uniq_join),
        Mapped_ClassRefs      = ("_tgt_class", _uniq_join),
        Mapped_DepartmentRefs = ("_tgt_dept", _uniq_join),
        Mapped_TaxCodeRefs    = ("_tgt_tax", _uniq_join),
        Mapped_EntityRefs     = ("_tgt_entity", _uniq_join),
        Mapped_EntityTypes    = ("_norm_type", _uniq_join),
    ).reset_index()

    # 6) Merge aggregates back to header
    hdr = hdr.merge(agg, left_on="Id", right_on="Parent_Id", how="left")
    for col in [
        "Mapped_AccountRefs","Mapped_ClassRefs","Mapped_DepartmentRefs",
        "Mapped_EntityRefs","Mapped_TaxCodeRefs","Mapped_EntityTypes"
    ]:
        # prefer aggregated (right) values when present
        hdr[col] = hdr[f"{col}_y"].combine_first(hdr[f"{col}_x"] if f"{col}_x" in hdr.columns else None)
        # cleanup helper columns if existed
        for suffix in ("_x", "_y"):
            c = f"{col}{suffix}"
            if c in hdr.columns:
                hdr.drop(columns=[c], inplace=True)

    if "Parent_Id" in hdr.columns:
        hdr.drop(columns=["Parent_Id"], inplace=True, errors="ignore")

    # 7) Replace/refresh mapping table atomically
    if sql.table_exists("Map_JournalEntry", MAPPING_SCHEMA):
        sql.run_query(f"DELETE FROM [{MAPPING_SCHEMA}].[Map_JournalEntry]")
    # Keep your existing helper to insert (aligns with your codebase)
    sql.insert_invoice_map_dataframe(hdr, "Map_JournalEntry", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(hdr)} rows into {MAPPING_SCHEMA}.Map_JournalEntry")

# --------------------------- Payload Builder ---------------------------
def build_payload(row, lines):
    """
    Constructs a QBO-compatible JournalEntry payload based on mapped header and line-level data.
    Skips any line with Amount == 0.0 to avoid validation errors.
    NOTE: Signature and logic shape preserved; now uses in-memory maps for speed.
    """
    payload = {"Line": []}

    # ---------- Header fields ----------
    for qbo_field, src_col in HEADER_MAP.items():
        if qbo_field == "DocNumber":
            val = row.get("Duplicate_DocNumber") or row.get("DocNumber")
        else:
            val = row.get(src_col)

        if pd.notna(val):
            if "." in qbo_field:
                parent, child = qbo_field.split(".")
                payload.setdefault(parent, {})[child] = val
            else:
                payload[qbo_field] = val

    # ---------- Fallbacks and defaults ----------
    if not payload.get("CurrencyRef"):
        payload["CurrencyRef"] = {"value": row.get("Mapped_CurrencyRef", "USD")}

    if pd.notna(row.get("ExchangeRate")):
        payload["ExchangeRate"] = safe_float(row["ExchangeRate"])

    payload["Adjustment"]   = False
    payload["TxnTaxDetail"] = {}

    # ---------- Column shortcuts ----------
    amount_col = LINE_MAP.get("Amount")
    desc_col   = LINE_MAP.get("Description")
    acct_col   = LINE_MAP.get("JournalEntryLineDetail.AccountRef.value")
    class_col  = LINE_MAP.get("JournalEntryLineDetail.ClassRef.value")
    dept_col   = LINE_MAP.get("JournalEntryLineDetail.DepartmentRef.value")
    tax_col    = LINE_MAP.get("JournalEntryLineDetail.TaxCodeRef.value")
    ent_val    = LINE_MAP.get("JournalEntryLineDetail.Entity.EntityRef.value")
    ent_typ    = LINE_MAP.get("JournalEntryLineDetail.Entity.Type")
    post_col   = LINE_MAP.get("JournalEntryLineDetail.PostingType")

    # ---------- Line items ----------
    for _, ln in lines.iterrows():
        amount = safe_float(ln.get(amount_col)) if amount_col else 0.0
        if amount == 0.0:
            continue  # Skip zero-amount lines

        line = {
            "Amount": amount,
            "Description": ln.get(desc_col) if desc_col else None,
            "DetailType": "JournalEntryLineDetail",
        }
        detail = {}

        # Fast in-memory mappings (no per-line SQL)
        if acct_col:
            t = _map_id("Account", ln.get(acct_col))
            if t:
                detail["AccountRef"] = {"value": t}
        if class_col:
            t = _map_id("Class", ln.get(class_col))
            if t:
                detail["ClassRef"] = {"value": t}
        if dept_col:
            t = _map_id("Department", ln.get(dept_col))
            if t:
                detail["DepartmentRef"] = {"value": t}
        if tax_col:
            t = _map_id("TaxCode", ln.get(tax_col))
            if t:
                detail["TaxCodeRef"] = {"value": t}

        if ent_val and ent_typ:
            ev = ln.get(ent_val)
            et = _norm_cap(ln.get(ent_typ))
            if pd.notna(ev) and et in ("Customer", "Vendor", "Employee"):
                t = _map_id(et, ev)
                if t:
                    detail["Entity"] = {"Type": et, "EntityRef": {"value": t}}

        if post_col:
            posting_type = ln.get(post_col)
            if pd.notna(posting_type):
                detail["PostingType"] = posting_type

        if detail:
            line["JournalEntryLineDetail"] = detail

        payload["Line"].append(line)

    # NEW: Add tax detail if available
    add_txn_tax_detail_from_row(payload, row)

    return payload if payload["Line"] else None

# --------------------------- Payload Generation ---------------------------
def generate_journalentry_payloads_in_batches(batch_size=1000):
    """
    Generates JSON payloads for journal entries in batches.
    Same semantics as your function, but:
      - Grabs a batch of headers
      - Fetches ALL their lines in one query
      - Uses in-memory maps
      - Writes back using update_mapping_status (unchanged)
    """
    logger.info("🔧 Generating JSON payloads for journal entries...")

    while True:
        df = sql.fetch_table_with_params(
            f"""
            SELECT TOP {batch_size} *
            FROM [{MAPPING_SCHEMA}].[Map_JournalEntry]
            WHERE Porter_Status = 'Ready'
              AND (Payload_JSON IS NULL OR Payload_JSON = '')
            """,
            (),
        )
        if df.empty:
            logger.info("✅ All journal entry JSON payloads have been generated.")
            break

        parent_ids = df["Source_Id"].tolist()

        # Refresh Duplicate_DocNumber column values in-memory for the batch
        dup_df = sql.fetch_table_with_params(
            f"""
            SELECT Source_Id, Duplicate_DocNumber
            FROM [{MAPPING_SCHEMA}].[Map_JournalEntry]
            WHERE Source_Id IN ({",".join(["?"] * len(parent_ids))})
            """,
            tuple(parent_ids),
        )
        dup_map = {r["Source_Id"]: r["Duplicate_DocNumber"] for _, r in dup_df.iterrows()} if not dup_df.empty else {}

        # Pull all lines at once for this batch
        lines_df = get_lines_for_parents(parent_ids)
        grouped = dict(tuple(lines_df.groupby("Parent_Id"))) if not lines_df.empty else {}

        # Build payloads
        for _, row in df.iterrows():
            sid = row["Source_Id"]
            if sid in dup_map and pd.notna(dup_map[sid]):
                row["Duplicate_DocNumber"] = dup_map[sid]

            sub_lines = grouped.get(sid, pd.DataFrame())
            payload = build_payload(row, sub_lines)
            if not payload:
                update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Failed", failure_reason="Empty line items")
                continue
            update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Ready", payload=payload)

        logger.info(f"📦 Batch processed: {len(df)} payloads")

# def _sleep_with_jitter(attempt=0):
#     base = 0.5 + attempt * 0.5
#     time.sleep(base + random.uniform(0, 0.5))

# def _post_batch_journalentries(eligible_batch, url, headers, timeout=20, max_manual_retries=2):
#     successes, failures = [], []
#     parsed = {}
#     for _, r in eligible_batch.iterrows():
#         sid = r["Source_Id"]
#         if r.get("Porter_Status") == "Success" or int(r.get("Retry_Count") or 0) >= 5 or not r.get("Payload_JSON"):
#             continue
#         try:
#             parsed[sid] = json.loads(r["Payload_JSON"])
#         except Exception as e:
#             failures.append((f"Bad JSON: {e}", sid))
#     for _, r in eligible_batch.iterrows():
#         sid = r["Source_Id"]
#         payload = parsed.get(sid)
#         if payload is None:
#             continue
#         attempted_refresh = False
#         for attempt in range(max_manual_retries + 1):
#             try:
#                 resp = session.post(url, headers=headers, json=payload, timeout=timeout)
#                 sc = resp.status_code
#                 if sc == 200:
#                     qid = None
#                     try:
#                         qid = resp.json().get("JournalEntry", {}).get("Id")
#                     except Exception:
#                         pass
#                     if qid:
#                         successes.append((qid, sid))
#                         logger.info(f"✅ Posted JournalEntry {sid} → Target ID {qid}")
#                     else:
#                         failures.append(("No Id in response", sid))
#                         logger.info(f"❌ Posted JournalEntry {sid} → No Id in response")
#                     break
#                 elif sc in (401, 403) and not attempted_refresh:
#                     attempted_refresh = True
#                     try:
#                         auto_refresh_token_if_needed(force=True)
#                     except Exception:
#                         pass
#                     url, headers = get_qbo_auth()
#                     _sleep_with_jitter(attempt=attempt)
#                     continue
#                 elif sc in (429, 500, 502, 503, 504) and attempt < max_manual_retries:
#                     _sleep_with_jitter(attempt=attempt)
#                     continue
#                 else:
#                     failures.append((resp.text[:300], sid))
#                     logger.info(f"❌ Posted JournalEntry {sid} → Failed with status {sc}")
#                     break
#             except requests.Timeout:
#                 if attempt < max_manual_retries:
#                     _sleep_with_jitter(attempt=attempt)
#                     continue
#                 failures.append(("Timeout", sid))
#                 break
#             except Exception as e:
#                 if attempt < max_manual_retries:
#                     _sleep_with_jitter(attempt=attempt)
#                     continue
#                 failures.append((str(e), sid))
#                 break
#     return successes, failures

# def _apply_batch_updates_journalentry(successes, failures):
#     from sys import modules
#     mod = modules[__name__]
#     if successes:
#         mod.executemany(
#             f"UPDATE [{MAPPING_SCHEMA}].[Map_JournalEntry] SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL WHERE Source_Id=?",
#             [(qid, sid) for qid, sid in successes]
#         )
#     if failures:
#         mod.executemany(
#             f"UPDATE [{MAPPING_SCHEMA}].[Map_JournalEntry] SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? WHERE Source_Id=?",
#             [(reason, sid) for reason, sid in failures]
#         )

# ---------- helpers (shared) ----------
try:
    import orjson as _orjson
    def _fast_loads(s):
        """
        Fast JSON loader: accepts str/bytes/dict. Uses orjson when available.
        """
        if isinstance(s, dict):
            return s
        if isinstance(s, (bytes, bytearray)):
            return _orjson.loads(s)
        if isinstance(s, str):
            return _orjson.loads(s.encode("utf-8"))
        # last resort
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


def _derive_batch_url(entity_url: str, entity_name: str) -> str:
    """
    Convert an entity endpoint to its QBO /batch endpoint.
    Example:
      entity_url: https://.../v3/company/<realm>/JournalEntry?minorversion=65
      entity_name: "JournalEntry"
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

def post_journalentry(row):
    """
    Single-record fallback (kept for compatibility and retries).
    Uses shared `session` and short timeout; no threading.
    """
    sid = row.get("Source_Id")
    if row.get("Porter_Status") == "Success":
        return
    if int(row.get("Retry_Count") or 0) >= 5:
        return
    if not row.get("Payload_JSON"):
        logger.warning(f"⚠️ JournalEntry {sid} skipped — missing Payload_JSON")
        return

    url, headers = get_qbo_auth()  # entity endpoint (JournalEntry)
    batch_url = _derive_batch_url(url, "JournalEntry")
    
    # fast JSON load if orjson is available
    try:
        payload = _fast_loads(row["Payload_JSON"])  # deposit helpers assumed present
    except NameError:
        payload = json.loads(row["Payload_JSON"])

    try:
        resp = session.post(url, headers=headers, json=payload, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            qid = data.get("JournalEntry", {}).get("Id")
            update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Success", target_id=qid)
            logger.info(f"✅ JournalEntry {sid} → QBO {qid}")
        else:
            reason = (resp.text or "")[:500]
            update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Failed",
                                  failure_reason=reason, increment_retry=True)
            logger.error(f"❌ Failed JournalEntry {sid}: {reason}")
    except Exception as e:
        update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Failed",
                              failure_reason=str(e), increment_retry=True)
        logger.exception(f"❌ Exception posting JournalEntry {sid}")

def migrate_journalentries(JOURNALENTRY_DATE_FROM,JOURNALENTRY_DATE_TO):
    print("\n🚀 Starting JournalEntry Migration Phase\n" + "=" * 40)

    ensure_mapping_table(JOURNALENTRY_DATE_FROM,JOURNALENTRY_DATE_TO)
    if ENABLE_GLOBAL_JE_DOCNUMBER_DEDUP:
        logger.info("🌐 Applying global DocNumber deduplication strategy...")
        apply_global_docnumber_strategy_for_journalentry()
    try:
        sql.clear_cache()
    except Exception:
        pass

    generate_journalentry_payloads_in_batches()

    rows = sql.fetch_table("Map_JournalEntry", MAPPING_SCHEMA)
    eligible = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("⚠️ No eligible journal entries to post.")
        return

    # Convert entity URL to /batch endpoint
    def _batch_url(entity_url: str) -> str:
        qpos = entity_url.find("?")
        query = entity_url[qpos:] if qpos != -1 else ""
        path = entity_url[:qpos] if qpos != -1 else entity_url
        if path.lower().endswith("/journalentry"):
            path = path.rsplit("/", 1)[0]
        return f"{path}/batch{query}"

    url, headers = get_qbo_auth()
    batch_url = _batch_url(url)

    select_batch_size = 300   # DB slice size
    post_batch_limit  = 10    # QBO batch ops per request (<=30). Set to 3 if you want exactly 3-at-a-time.

    total = len(eligible)
    logger.info(f"📤 Posting {total} JournalEntry(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        slice_df = eligible.iloc[i:i + select_batch_size]

        # Prepare worklist (filter guards here)
        work = []
        for _, row in slice_df.iterrows():
            sid = row["Source_Id"]
            if row.get("Porter_Status") == "Success":
                continue
            if int(row.get("Retry_Count") or 0) >= 5:
                continue
            pj = row.get("Payload_JSON")
            if not pj:
                update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Failed",
                                      failure_reason="Missing Payload_JSON", increment_retry=True)
                continue
            try:
                try:
                    payload = _fast_loads(pj)  # deposit helper assumed
                except NameError:
                    payload = json.loads(pj)
            except Exception as e:
                update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Failed",
                                      failure_reason=f"Bad JSON: {e}", increment_retry=True)
                continue
            work.append((sid, payload))

        # Post in chunks to QBO batch
        for j in range(0, len(work), post_batch_limit):
            auto_refresh_token_if_needed()
            chunk = work[j:j + post_batch_limit]
            if not chunk:
                continue

            batch_body = {
                "BatchItemRequest": [
                    {"bId": str(sid), "operation": "create", "JournalEntry": payload}
                    for (sid, payload) in chunk
                ]
            }

            try:
                resp = session.post(batch_url, headers=headers, json=batch_body, timeout=40)
                if resp.status_code != 200:
                    reason = (resp.text or f"HTTP {resp.status_code}")[:1000]
                    for sid, _ in chunk:
                        update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Failed",
                                              failure_reason=reason, increment_retry=True)
                    logger.error(f"❌ Batch POST failed ({len(chunk)} items): {reason}")
                    continue

                rj = resp.json()
                items = rj.get("BatchItemResponse", []) or []
                seen = set()

                for item in items:
                    bid = item.get("bId")
                    seen.add(bid)
                    je = item.get("JournalEntry")
                    if je and "Id" in je:
                        qid = je["Id"]
                        update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", bid, "Success", target_id=qid)
                        logger.info(f"✅ JournalEntry {bid} → QBO {qid}")
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
                    update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", bid, "Failed",
                                          failure_reason=reason, increment_retry=True)
                    logger.error(f"❌ Failed JournalEntry {bid}: {reason}")

                # Any missing responses in this chunk → mark failed
                for sid, _ in chunk:
                    if str(sid) not in seen:
                        update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Failed",
                                              failure_reason="No response for bId", increment_retry=True)
                        logger.error(f"❌ No response for JournalEntry {sid}")

            except Exception as e:
                reason = f"Batch exception: {e}"
                for sid, _ in chunk:
                    update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Failed",
                                          failure_reason=reason, increment_retry=True)
                logger.exception(f"❌ Exception during batch POST ({len(chunk)} items)")

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    # Retry once for failed with null Target_Id via single-record fallback
    failed_df = sql.fetch_table("Map_JournalEntry", MAPPING_SCHEMA)
    failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
    if not failed_records.empty:
        logger.info(f"🔁 Reprocessing {len(failed_records)} failed JournalEntries (single-record fallback)...")
        for _, row in failed_records.iterrows():
            post_journalentry(row)

    print("\n🏁 JournalEntry migration completed.")

def resume_or_post_journalentries(JOURNALENTRY_DATE_FROM,JOURNALENTRY_DATE_TO):
    print("\n🔁 Resuming JournalEntry Migration (conditional mode)\n" + "=" * 50)

    if not sql.table_exists("Map_JournalEntry", MAPPING_SCHEMA):
        logger.warning("❌ Map_JournalEntry table does not exist. Running full migration.")
        migrate_journalentries(JOURNALENTRY_DATE_FROM,JOURNALENTRY_DATE_TO)
        return

    mapped_df = sql.fetch_table("Map_JournalEntry", MAPPING_SCHEMA)
    source_df = sql.fetch_table("JournalEntry", SOURCE_SCHEMA)
    if len(mapped_df) != len(source_df):
        logger.warning(f"❌ Row count mismatch: Map_JournalEntry={len(mapped_df)}, JournalEntry={len(source_df)}. Running full migration.")
        migrate_journalentries(JOURNALENTRY_DATE_FROM,JOURNALENTRY_DATE_TO)
        return

    if mapped_df["Payload_JSON"].isnull().any() or (mapped_df["Payload_JSON"] == "").any():
        logger.warning("🔍 Some Payload_JSON values missing. Generating for those...")
        generate_journalentry_payloads_in_batches()
        mapped_df = sql.fetch_table("Map_JournalEntry", MAPPING_SCHEMA)
    else:
        logger.info("✅ All JournalEntries have Payload_JSON.")

    eligible = mapped_df[mapped_df["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("⚠️ No eligible JournalEntries to post.")
        return

    # Local helper to derive /batch URL
    def _batch_url(entity_url: str) -> str:
        qpos = entity_url.find("?")
        query = entity_url[qpos:] if qpos != -1 else ""
        path = entity_url[:qpos] if qpos != -1 else entity_url
        if path.lower().endswith("/journalentry"):
            path = path.rsplit("/", 1)[0]
        return f"{path}/batch{query}"

    url, headers = get_qbo_auth()
    batch_url = _batch_url(url)

    select_batch_size = 300
    post_batch_limit  = 10

    total = len(eligible)
    logger.info(f"📤 Posting {total} JournalEntry(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        slice_df = eligible.iloc[i:i + select_batch_size]

        # Prepare worklist
        work = []
        for _, row in slice_df.iterrows():
            sid = row["Source_Id"]
            if row.get("Porter_Status") == "Success":
                continue
            if int(row.get("Retry_Count") or 0) >= 5:
                continue
            pj = row.get("Payload_JSON")
            if not pj:
                update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Failed",
                                      failure_reason="Missing Payload_JSON", increment_retry=True)
                continue
            try:
                try:
                    payload = _fast_loads(pj)
                except NameError:
                    payload = json.loads(pj)
            except Exception as e:
                update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Failed",
                                      failure_reason=f"Bad JSON: {e}", increment_retry=True)
                continue
            work.append((sid, payload))

        # Batch POST to QBO
        for j in range(0, len(work), post_batch_limit):
            chunk = work[j:j + post_batch_limit]
            if not chunk:
                continue

            batch_body = {
                "BatchItemRequest": [
                    {"bId": str(sid), "operation": "create", "JournalEntry": payload}
                    for (sid, payload) in chunk
                ]
            }

            try:
                resp = session.post(batch_url, headers=headers, json=batch_body, timeout=40)
                if resp.status_code != 200:
                    reason = (resp.text or f"HTTP {resp.status_code}")[:1000]
                    for sid, _ in chunk:
                        update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Failed",
                                              failure_reason=reason, increment_retry=True)
                    logger.error(f"❌ Batch POST failed ({len(chunk)} items): {reason}")
                    continue

                rj = resp.json()
                items = rj.get("BatchItemResponse", []) or []
                seen = set()

                for item in items:
                    bid = item.get("bId")
                    seen.add(bid)
                    je = item.get("JournalEntry")
                    if je and "Id" in je:
                        qid = je["Id"]
                        update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", bid, "Success", target_id=qid)
                        logger.info(f"✅ JournalEntry {bid} → QBO {qid}")
                        continue

                    fault = item.get("Fault") or {}
                    errs = fault.get("Error") or []
                    if errs:
                        msg = errs[0].get("Message") or ""
                        det = errs[0].get("Detail") or ""
                        reason = (msg + " | " + det).strip()[:1000]
                    else:
                        reason = "Unknown batch failure"
                    update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", bid, "Failed",
                                          failure_reason=reason, increment_retry=True)
                    logger.error(f"❌ Failed JournalEntry {bid}: {reason}")

                for sid, _ in chunk:
                    if str(sid) not in seen:
                        update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Failed",
                                              failure_reason="No response for bId", increment_retry=True)
                        logger.error(f"❌ No response for JournalEntry {sid}")

            except Exception as e:
                reason = f"Batch exception: {e}"
                for sid, _ in chunk:
                    update_mapping_status(MAPPING_SCHEMA, "Map_JournalEntry", sid, "Failed",
                                          failure_reason=reason, increment_retry=True)
                logger.exception(f"❌ Exception during batch POST ({len(chunk)} items)")

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    # Retry once (single-record fallback)
    failed_df = sql.fetch_table("Map_JournalEntry", MAPPING_SCHEMA)
    failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
    if not failed_records.empty:
        logger.info(f"🔁 Reprocessing {len(failed_records)} failed JournalEntries (single-record fallback)...")
        for _, row in failed_records.iterrows():
            post_journalentry(row)

    print("\n🏁 JournalEntry posting completed.")

# --- SMART JOURNALENTRY MIGRATION ---
def smart_journalentry_migration(JOURNALENTRY_DATE_FROM,JOURNALENTRY_DATE_TO):
    """
    Smart migration entrypoint for JournalEntry:
    - If Map_JournalEntry table exists and row count matches JournalEntry table, resume/post JournalEntries.
    - If table missing or row count mismatch, perform full migration.
    """
    logger.info("🔎 Running smart_journalentry_migration...")
    full_process = False
    if sql.table_exists("Map_JournalEntry", MAPPING_SCHEMA):
        mapped_df = sql.fetch_table("Map_JournalEntry", MAPPING_SCHEMA)
        source_df = sql.fetch_table("JournalEntry", SOURCE_SCHEMA)
        if len(mapped_df) == len(source_df):
            logger.info("✅ Table exists and row count matches. Resuming/posting JournalEntries.")
            resume_or_post_journalentries(JOURNALENTRY_DATE_FROM,JOURNALENTRY_DATE_TO)
            # After resume_or_post, reprocess failed records one more time
            failed_df = sql.fetch_table("Map_JournalEntry", MAPPING_SCHEMA)
            failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
            if not failed_records.empty:
                logger.info(f"🔁 Reprocessing {len(failed_records)} failed JournalEntries with null Target_Id after main migration...")
                for _, row in failed_records.iterrows():
                    post_journalentry(row)
            return
        else:
            logger.warning(f"❌ Row count mismatch: Map_JournalEntry={len(mapped_df)}, JournalEntry={len(source_df)}. Running full migration.")
            full_process = True
    else:
        logger.warning("❌ Map_JournalEntry table does not exist. Running full migration.")
        full_process = True
    if full_process:
        migrate_journalentries(JOURNALENTRY_DATE_FROM,JOURNALENTRY_DATE_TO)
        # After full migration, reprocess failed records with null Target_Id one more time
        failed_df = sql.fetch_table("Map_JournalEntry", MAPPING_SCHEMA)
        failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
        if not failed_records.empty:
            logger.info(f"🔁 Reprocessing {len(failed_records)} failed JournalEntries with null Target_Id after main migration...")
            for _, row in failed_records.iterrows():
                post_journalentry(row)

