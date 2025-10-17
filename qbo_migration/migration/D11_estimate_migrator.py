"""
Sequence : 24
Author: Dixit Prajapati
Created: 2025-10-03
Description: Handles migration of estimate records from source system to QBO.
Production : Ready
Development : Require when necessary
Phase : 02 - MultiUser + Tax + Currency
"""

import os, json, requests, pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from storage.sqlserver.sql import insert_invoice_map_dataframe
from utils.token_refresher import auto_refresh_token_if_needed,get_qbo_context_migration
from utils.log_timer import global_logger as logger, ProgressTimer
from utils.apply_duplicate_docnumber import apply_duplicate_docnumber_strategy_dynamic
from utils.payload_cleaner import deep_clean

# ──────────────────────────────────────────────────────────────
# Boot / Config
# ──────────────────────────────────────────────────────────────
load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA    = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA   = "porter_entities_mapping"
POST_BATCH_SIZE  = int(os.getenv("POST_BATCH_SIZE", "500"))
QBO_TAX_MODEL    = os.getenv("QBO_TAX_MODEL", "US").upper()
# By default we DO NOT send CurrencyRef or ExchangeRate to avoid multi-currency violation.
QBO_ALLOW_CURRENCYREF = os.getenv("QBO_ALLOW_CURRENCYREF", "false").strip().lower() in ("1","true","yes")


# ──────────────────────────────────────────────────────────────
# QBO auth
# ──────────────────────────────────────────────────────────────
def get_qbo_auth():
    env   = os.getenv("QBO_ENVIRONMENT", "sandbox")
    base  = "https://sandbox-quickbooks.api.intuit.com" if env == "sandbox" else "https://quickbooks.api.intuit.com"
    ctx = get_qbo_context_migration()
    base = ctx["BASE_URL"]
    realm = ctx["REALM_ID"]
    return f"{base}/v3/company/{realm}/estimate", {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

def safe_float(v):
    try: return float(v)
    except: return 0.0

# ──────────────────────────────────────────────────────────────
# Dual-name utilities (raw vs sanitized column names)
# ──────────────────────────────────────────────────────────────
def pick_col(df_or_row, *candidates):
    for c in candidates:
        try:
            if isinstance(df_or_row, pd.Series):
                if c in df_or_row.index and pd.notna(df_or_row.get(c)):
                    return df_or_row.get(c)
            else:
                if c in df_or_row.columns:
                    val = df_or_row.get(c)
                    if val is not None:
                        return val
        except Exception:
            continue
    return None

def _addr_from_row(row, prefix):
    out = {}
    for k in ["Line1","Line2","Line3","Line4","Line5","City","CountrySubDivisionCode","Country","PostalCode"]:
        raw = f"[{prefix}.{k}]"
        san = f"{prefix}_{k}"
        v = row.get(raw) if raw in row.index else row.get(san)
        if pd.notna(v):
            out[k] = v
    return out

# ──────────────────────────────────────────────────────────────
# ID normalization
# ──────────────────────────────────────────────────────────────
def _norm_id(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip().strip('"').strip("'")
    if s.endswith(".0"):
        s = s[:-2]
    return s

def _to_dict(rows):
    d = {}
    for s, t in rows:
        ks = _norm_id(s); kt = _norm_id(t)
        if ks is not None and kt is not None:
            d[ks] = kt
    return d

def _to_dict_name(rows):
    d = {}
    for s, n in rows:
        ks = _norm_id(s)
        if ks is not None and n is not None:
            d[ks] = str(n)
    return d

# Mapping caches (add presence trackers)
_MAP_ITEM        = {}   # Source_Id -> Target_Id
_MAP_ITEM_ACTIVE = {}   # Source_Id -> "true"/"false"/"1"/"0"
_ITEM_EXISTS     = set()
_ITEM_WITH_TARGET= set()
_MAP_CLASS       = {}
_MAP_TAX         = {}
_MAP_TAX_NAME    = {}
_MAP_DEPT        = {}
_MAP_CUST_CURR   = {}

def load_mapping_caches(mapping_schema="porter_entities_mapping"):
    global _MAP_ITEM, _MAP_ITEM_ACTIVE, _ITEM_EXISTS, _ITEM_WITH_TARGET
    global _MAP_CLASS, _MAP_TAX, _MAP_TAX_NAME, _MAP_DEPT, _MAP_CUST_CURR

    # Pull Source_Id, Target_Id, Active in one pass to build presence sets
    rows = sql.fetch_all(f"SELECT Source_Id, Target_Id, Active FROM [{mapping_schema}].[Map_Item]")
    _MAP_ITEM = {}
    _MAP_ITEM_ACTIVE = {}
    _ITEM_EXISTS = set()
    _ITEM_WITH_TARGET = set()

    for s, t, a in rows:
        ks = _norm_id(s)
        kt = _norm_id(t)
        _ITEM_EXISTS.add(ks)
        if kt is not None:
            _MAP_ITEM[ks] = kt
            _ITEM_WITH_TARGET.add(ks)
        _MAP_ITEM_ACTIVE[ks] = (str(a).strip().lower() if a is not None else "")

    _MAP_CLASS = _to_dict(sql.fetch_all(f"SELECT Source_Id, Target_Id FROM [{mapping_schema}].[Map_Class]"))
    _MAP_TAX   = _to_dict(sql.fetch_all(f"SELECT Source_Id, Target_Id FROM [{mapping_schema}].[Map_TaxCode]"))
    _MAP_TAX_NAME = _to_dict_name(sql.fetch_all(f"SELECT Source_Id, Name FROM [{mapping_schema}].[Map_TaxCode]"))
    _MAP_DEPT  = _to_dict(sql.fetch_all(f"SELECT Source_Id, Target_Id FROM [{mapping_schema}].[Map_Department]"))

    # Optional customer currency for validating CurrencyRef (still default off)
    try:
        _MAP_CUST_CURR = _to_dict(sql.fetch_all(f"SELECT Source_Id, CurrencyRef_Target_Id FROM [{mapping_schema}].[Map_Customer]"))
    except Exception:
        _MAP_CUST_CURR = {}

def map_from_cache(cache: dict, src):
    return cache.get(_norm_id(src))

# ──────────────────────────────────────────────────────────────
# Ensure/Replicate mapping table (header only)
# ──────────────────────────────────────────────────────────────
def ensure_mapping_table():
    logger.info("🔧 Ensuring Map_Estimate from SOURCE.Estimate ...")
    df = sql.fetch_table("Estimate", SOURCE_SCHEMA)
    if df.empty:
        logger.info("⚠️ No Estimate records found in source.")
        return

    # Migration columns
    df["Source_Id"] = df["Id"]
    for col, default in [
        ("Target_Id", None), ("Porter_Status", "Ready"), ("Retry_Count", 0),
        ("Failure_Reason", None), ("Payload_JSON", None), ("Duplicate_Docnumber", None),
        ("Mapped_CustomerRef", None), ("Mapped_DepartmentRef", None), ("Mapped_CurrencyRef", None),
    ]:
        if col not in df.columns:
            df[col] = default

    # CustomerRef → Map_Customer.Target_Id
    cust_col = None
    for c in ("[CustomerRef.value]", "CustomerRef.value", "CustomerRef_value"):
        if c in df.columns:
            cust_col = c; break
    if cust_col:
        df["Mapped_CustomerRef"] = df[cust_col].map(
            lambda x: sql.fetch_single_value(
                f"SELECT Target_Id FROM [{MAPPING_SCHEMA}].[Map_Customer] WHERE Source_Id = ?",
                (x,)
            ) if pd.notna(x) else None
        )

    # DepartmentRef (header)
    dept_col = None
    for c in ("[DepartmentRef.value]", "DepartmentRef.value", "DepartmentRef_value"):
        if c in df.columns:
            dept_col = c; break
    if dept_col:
        df["Mapped_DepartmentRef"] = df[dept_col].map(lambda x: map_from_cache(_MAP_DEPT, x))

    # CurrencyRef (optional enrichment; we will **not** send it unless allowed & validated)
    curr_col = None
    for c in ("[CurrencyRef.value]", "CurrencyRef.value", "CurrencyRef_value"):
        if c in df.columns:
            curr_col = c; break
    if curr_col:
        df["Mapped_CurrencyRef"] = df[curr_col]  # keep raw; validation happens later if ever used

    # Reset and insert
    if sql.table_exists("Map_Estimate", MAPPING_SCHEMA):
        sql.run_query(f"DELETE FROM [{MAPPING_SCHEMA}].[Map_Estimate]")
    insert_invoice_map_dataframe(df, "Map_Estimate", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df)} rows into {MAPPING_SCHEMA}.Map_Estimate")

# ──────────────────────────────────────────────────────────────
# Lines loader
# ──────────────────────────────────────────────────────────────
def get_lines(estimate_id):
    return sql.fetch_table_with_params(
        f"""SELECT * FROM [{SOURCE_SCHEMA}].[Estimate_Line] WHERE Parent_Id = ? """,(estimate_id,))

# ──────────────────────────────────────────────────────────────
# Tax code resolver (US: only TAX/NON allowed)
# ──────────────────────────────────────────────────────────────
def resolve_us_taxcode_value(src_taxcode, row=None):
    ks = _norm_id(src_taxcode)
    tgt = _MAP_TAX.get(ks)
    if tgt in ("TAX", "NON"):
        return tgt
    name = (_MAP_TAX_NAME.get(ks, "") or "").lower()
    if any(k in name for k in ("non", "exempt", "out of scope", "zero", "nontax", "no tax")):
        return "NON"
    if any(k in name for k in ("tax", "standard", "gst", "vat", "sales")):
        return "TAX"
    total_tax = pick_col(row, "[TxnTaxDetail.TotalTax]", "TxnTaxDetail.TotalTax", "TxnTaxDetail_TotalTax") if row is not None else None
    try:
        if total_tax is not None and float(total_tax) > 0:
            return "TAX"
    except:
        pass
    return "NON"

# ──────────────────────────────────────────────────────────────
# Payload builder
# ──────────────────────────────────────────────────────────────
def build_payload(row, lines):
    """
    - Do NOT send CurrencyRef/ExchangeRate/TotalAmt/HomeTotalAmt by default (let QBO infer from Customer).
    - Line ItemRef/ClassRef/TaxCodeRef mapped via Map_*; US tax normalized to TAX/NON.
    - Skip lines where item is unmapped or inactive.
    """
    payload = {
        "DocNumber": row.get("Duplicate_Docnumber") or row.get("DocNumber"),
        "TxnDate": row.get("TxnDate"),
        # "TxnStatus": row.get("TxnStatus") or "Pending",
        "sparse": False
    }

    # CustomerRef (required)
    cust_val = row.get("Mapped_CustomerRef") or pick_col(row, "[CustomerRef.value]", "CustomerRef.value", "CustomerRef_value")
    if not cust_val:
        return None
    payload["CustomerRef"] = {"value": str(cust_val)}

    # Email / Memo
    email = pick_col(row, "[BillEmail.Address]", "BillEmail.Address", "BillEmail_Address")
    if email:
        payload["BillEmail"] = {"Address": email}
    memo = pick_col(row, "[CustomerMemo.value]", "CustomerMemo.value", "CustomerMemo_value")
    if memo:
        payload["CustomerMemo"] = {"value": memo}

    # Addresses
    bill = _addr_from_row(row, "BillAddr")
    ship = _addr_from_row(row, "ShipAddr")
    if bill: payload["BillAddr"] = bill
    if ship: payload["ShipAddr"] = ship

    # Header DepartmentRef (mapped)
    dept_tid = row.get("Mapped_DepartmentRef")
    if dept_tid:
        payload["DepartmentRef"] = {"value": str(dept_tid)}

    # ⚠️ Currency handling:
    # By default, omit CurrencyRef and ExchangeRate so QBO uses the customer's currency.
    if QBO_ALLOW_CURRENCYREF:
        # Only include if we can validate it equals the customer's currency
        header_curr = row.get("Mapped_CurrencyRef") or pick_col(row, "[CurrencyRef.value]", "CurrencyRef.value", "CurrencyRef_value")
        if header_curr:
            payload["CurrencyRef"] = {"value": str(header_curr)}
        else:
            logger.info(f"ℹ️ CurrencyRef suppressed for Estimate {row['Source_Id']} (mismatch or unknown).")

    # Status flags
    ps = pick_col(row, "PrintStatus");  es = pick_col(row, "EmailStatus")
    if ps: payload["PrintStatus"] = ps
    if es: payload["EmailStatus"] = es

    # Optional overall tax total (not required)
    total_tax = pick_col(row, "[TxnTaxDetail.TotalTax]", "TxnTaxDetail.TotalTax", "TxnTaxDetail_TotalTax")
    if total_tax is not None and pd.notna(total_tax):
        payload["TxnTaxDetail"] = {"TotalTax": safe_float(total_tax)}

    # Header-level TxnTaxCodeRef mapping (from source header column)
    header_tax_src = pick_col(row, "[TxnTaxDetail.TxnTaxCodeRef.value]", "TxnTaxDetail.TxnTaxCodeRef.value", "TxnTaxDetail_TxnTaxCodeRef_value")
    if header_tax_src:
        header_tax_tid = map_from_cache(_MAP_TAX, header_tax_src)
        if header_tax_tid:
            if "TxnTaxDetail" not in payload:
                payload["TxnTaxDetail"] = {}
            payload["TxnTaxDetail"]["TxnTaxCodeRef"] = {"value": str(header_tax_tid)}
        else:
            logger.debug(f"🔎 Missing TaxCode mapping for header Source_Id={header_tax_src}")

    # Tracking / misc (harmless)
    trn = pick_col(row, "TrackingNum")
    if trn: payload["TrackingNum"] = trn
    exp = pick_col(row, "ExpirationDate")
    if exp: payload["ExpirationDate"] = exp

    # ── Lines
    payload["Line"] = []
    for _, ln in lines.iterrows():
        dt = ln.get("DetailType")
        if not dt:
            continue

        line = {
            "DetailType": dt,
            "Amount": safe_float(ln.get("Amount")),
            "Description": ln.get("Description"),
        }

        if dt == "SalesItemLineDetail":
            item_src  = (ln.get("[SalesItemLineDetail.ItemRef.value]") or ln.get("SalesItemLineDetail.ItemRef.value") or ln.get("SalesItemLineDetail_ItemRef_value"))
            class_src = (ln.get("[SalesItemLineDetail.ClassRef.value]") or ln.get("SalesItemLineDetail.ClassRef.value") or ln.get("SalesItemLineDetail_ClassRef_value"))
            tax_src   = (ln.get("[SalesItemLineDetail.TaxCodeRef.value]") or ln.get("SalesItemLineDetail.TaxCodeRef.value") or ln.get("SalesItemLineDetail_TaxCodeRef_value"))

            k_item = _norm_id(item_src)
            item_tid = map_from_cache(_MAP_ITEM, item_src)

            if not item_tid:
                if k_item in _ITEM_EXISTS and k_item not in _ITEM_WITH_TARGET:
                    logger.warning(f"❗ Skipping line for Estimate {row['Source_Id']} — item {k_item} present in Map_Item but Target_Id is NULL")
                else:
                    logger.warning(f"❗ Skipping line for Estimate {row['Source_Id']} — item {k_item} not mapped in Map_Item")
                continue

            active = _MAP_ITEM_ACTIVE.get(k_item, "")
            if active in ("0","false","no"):
                logger.warning(f"❗ Skipping line for Estimate {row['Source_Id']} — item {k_item} is inactive")
                continue

            class_tid = map_from_cache(_MAP_CLASS, class_src) if class_src is not None else None
            tax_value = "NON" if QBO_TAX_MODEL != "US" else resolve_us_taxcode_value(tax_src, row=row)

            qty = (ln.get("[SalesItemLineDetail.Qty]") or ln.get("SalesItemLineDetail.Qty") or ln.get("SalesItemLineDetail_Qty"))
            up  = (ln.get("[SalesItemLineDetail.UnitPrice]") or ln.get("SalesItemLineDetail.UnitPrice") or ln.get("SalesItemLineDetail_UnitPrice"))

            detail = {
                "ItemRef": {"value": str(item_tid)},
                "Qty": safe_float(qty) if pd.notna(qty) else None,
                "UnitPrice": safe_float(up) if pd.notna(up) else None,
                "ServiceDate": (ln.get("[SalesItemLineDetail.ServiceDate]") or ln.get("SalesItemLineDetail.ServiceDate") or ln.get("SalesItemLineDetail_ServiceDate")),
                "TaxCodeRef": {"value": tax_value}
            }
            if class_tid:
                detail["ClassRef"] = {"value": str(class_tid)}

            line["SalesItemLineDetail"] = {k: v for k, v in detail.items() if v is not None}


        elif dt == "SubTotalLineDetail":
            line["SubTotalLineDetail"] = {}

        elif dt == "DiscountLineDetail":
            line["DiscountLineDetail"] = {}

        else:
            line[dt] = {}

        payload["Line"].append(line)

    # ───────────────────────────────────────────────────────────────────────────
    # Dynamic TxnStatus block — only send if user supplied a supported value.
    # 'Converted' is system-set and will be ignored here.
    # Also attach AcceptedBy / AcceptedDate only when TxnStatus == 'Accepted'.
    # ───────────────────────────────────────────────────────────────────────────
    _status_raw = (str(row.get("TxnStatus"))).strip().lower()
    # normalize common variants
    _status_map = {
        "pending": "Pending",
        "open": "Pending",        # normalize 'Open' to 'Pending' (QBO doesn't accept 'Open')
        "accepted": "Accepted",
        "declined": "Rejected",   # UI says Declined; API uses Rejected
        "rejected": "Rejected",
        "closed": "Closed",
        "converted": "converted"         # never send; QBO sets this automatically
    }
    _normalized = _status_map.get(_status_raw)

    qbo_allowed = {"Pending", "Accepted", "Rejected", "Closed","converted"}

    if _normalized in qbo_allowed:
        payload["TxnStatus"] = _normalized

        if _normalized == "Accepted":
            # attach optional AcceptedBy / AcceptedDate when provided
            accepted_by = pick_col(row, "AcceptedBy", "Accepted_By", "Estimate_AcceptedBy")
            if accepted_by:
                payload["AcceptedBy"] = str(accepted_by)

            accepted_date = pick_col(row, "AcceptedDate", "Accepted_Date", "Estimate_AcceptedDate")
            if accepted_date:
                # pass through as provided; ensure it's an ISO date string upstream if needed
                payload["AcceptedDate"] = str(accepted_date)
    else:
        # Do not send TxnStatus (QBO will default to Pending)
        if _status_raw:
            logger.info(f"ℹ️ Suppressing unsupported TxnStatus '{_status_raw}' for Estimate {row.get('Source_Id')}")    

    return deep_clean(payload)

# ──────────────────────────────────────────────────────────────
# Batch JSON generation
# ──────────────────────────────────────────────────────────────
def generate_estimate_payloads_in_batches(batch_size=POST_BATCH_SIZE):
    logger.info("🔧 Generating JSON payloads for estimates...")
    while True:
        df = sql.fetch_table_with_params(
            f"""
            SELECT TOP {batch_size} *
            FROM [{MAPPING_SCHEMA}].[Map_Estimate]
            WHERE Porter_Status = 'Ready' AND (Payload_JSON IS NULL OR Payload_JSON = '')
            """,
            ()
        )
        if df.empty:
            logger.info("✅ All estimate JSON payloads have been generated.")
            break

        for _, row in df.iterrows():
            sid = row["Source_Id"]
            has_customer = (
                pd.notna(row.get("Mapped_CustomerRef")) or
                pd.notna(pick_col(row, "[CustomerRef.value]", "CustomerRef.value", "CustomerRef_value"))
            )
            if not has_customer:
                logger.warning(f"⚠️ Skipped estimate {sid} — missing CustomerRef.")
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_Estimate]
                    SET Porter_Status='Failed', Failure_Reason='Missing CustomerRef'
                    WHERE Source_Id = ?
                """, (sid,))
                continue

            lines = get_lines(sid)
            payload = build_payload(row, lines)
            if not payload or not payload.get("Line"):
                logger.warning(f"⚠️ Skipped estimate {sid} — no valid Line items.")
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_Estimate]
                    SET Porter_Status='Failed', Failure_Reason='No valid Line items'
                    WHERE Source_Id = ?
                """, (sid,))
                continue

            payload_json = json.dumps(payload, indent=2)
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_Estimate]
                SET Payload_JSON = ?, Failure_Reason = NULL
                WHERE Source_Id = ?
            """, (payload_json, sid))

        logger.info(f"✅ Generated payloads for {len(df)} estimates in this batch.")

def log_missing_item_targetids(limit=25):
    """
    Prints a quick summary of Estimate lines whose items exist in Map_Item
    but do NOT have a Target_Id (i.e., cannot be posted).
    """
    rows = sql.fetch_all(f"""
        SELECT DISTINCT CAST(el.[SalesItemLineDetail.ItemRef.value] AS NVARCHAR(100)) AS ItemSourceId
        FROM [{SOURCE_SCHEMA}].[Estimate_Line] el
        LEFT JOIN [{MAPPING_SCHEMA}].[Map_Item] mi
          ON CAST(el.[SalesItemLineDetail.ItemRef.value] AS NVARCHAR(100)) = CAST(mi.Source_Id AS NVARCHAR(100))
        WHERE mi.Source_Id IS NOT NULL AND mi.Target_Id IS NULL
    """)
    missing = [ _norm_id(r[0]) for r in rows if r and r[0] is not None ]
    if missing:
        show = ", ".join(missing[:limit])
        more = "" if len(missing) <= limit else f" … (+{len(missing)-limit} more)"
        logger.warning(f"🚫 {len(missing)} item(s) have NULL Target_Id in Map_Item. Examples: {show}{more}")
    else:
        logger.info("✅ All items referenced by Estimate lines have Target_Id in Map_Item.")

# ──────────────────────────────────────────────────────────────
# Posting
# ──────────────────────────────────────────────────────────────
session = requests.Session()

def post_estimate(row):
    sid = row["Source_Id"]
    if row.get("Porter_Status") == "Success":
        return
    if int(row.get("Retry_Count") or 0) >= 5:
        logger.warning(f"⚠️ Estimate {sid} skipped — exceeded retry limit")
        return
    if not row.get("Payload_JSON"):
        logger.warning("Missing Payload_JSON — cannot post")
        return

    auto_refresh_token_if_needed()

    url, headers = get_qbo_auth()
    payload = json.loads(row["Payload_JSON"])

    try:
        resp = session.post(url, headers=headers, json=payload)
        if resp.status_code in (200, 201):
            qid = (resp.json().get("Estimate") or {}).get("Id")
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_Estimate]
                SET Target_Id = ?, Porter_Status='Success', Failure_Reason=NULL
                WHERE Source_Id = ?
            """, (qid, sid))
            logger.info(f"✅ Estimate {sid} → QBO {qid}")
        else:
            reason = resp.text[:1000]
            logger.error(f"❌ Estimate {sid} failed: {resp.status_code} {reason}")
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_Estimate]
                SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=?
                WHERE Source_Id = ?
            """, (reason, sid))
    except Exception as e:
        logger.exception(f"❌ Exception posting Estimate {sid}")
        sql.run_query(f"""
            UPDATE [{MAPPING_SCHEMA}].[Map_Estimate]
            SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=?
            WHERE Source_Id = ?
        """, (str(e), sid))

# ──────────────────────────────────────────────────────────────
# Entry points
# ──────────────────────────────────────────────────────────────
def migrate_estimates():
    print("\n🚀 Starting Estimate Migration\n" + "=" * 36)
    load_mapping_caches(MAPPING_SCHEMA)
    ensure_mapping_table()
    # apply_duplicate_docnumber_strategy()
    apply_duplicate_docnumber_strategy_dynamic(
    target_table="Map_Estimate",
    schema=MAPPING_SCHEMA,
    docnumber_column="DocNumber",
    source_id_column="Source_Id",
    duplicate_column="Duplicate_Docnumber",
    check_against_tables=[]
    )
    load_mapping_caches(MAPPING_SCHEMA)
    log_missing_item_targetids()   # ← add this
    generate_estimate_payloads_in_batches(batch_size=POST_BATCH_SIZE)

    rows = sql.fetch_table("Map_Estimate", MAPPING_SCHEMA)
    eligible = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("⚠️ No eligible estimates to process.")
        return

    pt = ProgressTimer(len(eligible))
    for _, row in eligible.iterrows():
        post_estimate(row)
        pt.update()

    print("\n🏁 Estimate migration completed.")

def resume_or_post_estimates():
    print("\n🔁 Resuming Estimate Migration (conditional mode)\n" + "=" * 52)

    if not sql.table_exists("Map_Estimate", MAPPING_SCHEMA):
        logger.info("📂 Map_Estimate missing — building fresh.")
        migrate_estimates()
        return

    mapped_df = sql.fetch_table("Map_Estimate", MAPPING_SCHEMA)
    source_df = sql.fetch_table("Estimate", SOURCE_SCHEMA)

    if len(mapped_df) != len(source_df):
        logger.warning(f"❌ Row count mismatch: Map_Estimate = {len(mapped_df)}, Source Estimate = {len(source_df)}")
        migrate_estimates()
        return

    if mapped_df["Payload_JSON"].isnull().any():
        logger.info("📦 Missing payloads — regenerating…")
        load_mapping_caches(MAPPING_SCHEMA)
        generate_estimate_payloads_in_batches(batch_size=POST_BATCH_SIZE)
        mapped_df = sql.fetch_table("Map_Estimate", MAPPING_SCHEMA)

    eligible = mapped_df[mapped_df["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("⚠️ No eligible estimates to post.")
        return

    logger.info("🚚 Posting Ready/Failed estimates…")
    pt = ProgressTimer(len(eligible))
    for _, row in eligible.iterrows():
        post_estimate(row)
        pt.update()

    print("\n🏁 Estimate posting completed.")
