"""
Sequence : 13
Module: CreditMemo_migrator.py
Author: Dixit Prajapati
Created: 2025-09-12
Description: Handles migration of CreditMemo records from QBO to QBO.
Production : Currency
Development : Require when necessary
Phase : 02 -  Multiuser + Tax 
""" 
import os, json, requests, pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import global_logger as logger
from config.mapping.creditmemo_mapping import CREDITMEMO_HEADER_MAPPING, CREDITMEMO_LINE_MAPPING
from utils.mapping_updater import update_mapping_status
# ================== Single-thread, optimized posting (no ThreadPoolExecutor) ==================
import time, random, orjson, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")

# Turn on/off optional duplicate DocNumber strategy for Bills
ENABLE_VC_DOCNUMBER_DEDUP = False  # set True to activate
ENABLE_VC_DOCNUMBER_UNIVERSAL_DEDUP = True  # set True to activate

def get_qbo_auth():
    from utils.token_refresher import get_qbo_context_migration, MINOR_VERSION
    ctx = get_qbo_context_migration()
    base = ctx["BASE_URL"]
    realm_id = ctx["REALM_ID"]
    post_url = f"{base}/v3/company/{realm_id}/creditmemo?minorversion={MINOR_VERSION}"
    headers = {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    return post_url, headers

def safe_float(val):
    try: return float(val)
    except: return 0.0

def ensure_mapping_table(CREDITMEMO_DATE_FROM="1900-01-01",CREDITMEMO_DATE_TO="2080-12-31"):
    # Enhanced mapping logic with date filtering and required columns
    date_from = CREDITMEMO_DATE_FROM
    date_to = CREDITMEMO_DATE_TO
    query = f"SELECT * FROM [{SOURCE_SCHEMA}].[CreditMemo] WHERE 1=1"
    if date_from:
        query += " AND TxnDate >= ?"
    if date_to:
        query += " AND TxnDate <= ?"
    params = tuple(p for p in [date_from, date_to] if p)
    df = sql.fetch_table_with_params(query, params)
    if df.empty:
        logger.info("⚠️ No CreditMemo records found for the specified date range.")
        return
    df["Source_Id"] = df["Id"]
    df["Target_Id"] = None
    df["Porter_Status"] = "Ready"
    df["Retry_Count"] = 0
    df["Failure_Reason"] = None
    required_cols = ["Payload_JSON", "Mapped_CustomerRef", "Mapped_ClassRef", "Mapped_ItemRef", "Mapped_AccountRef"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Bulk fetch mapping tables
    customer_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Customer]", tuple())
    customer_dict = dict(zip(customer_map["Source_Id"], customer_map["Target_Id"]))

    item_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Item]", tuple())
    item_dict = dict(zip(item_map["Source_Id"], item_map["Target_Id"]))

    class_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Class]", tuple())
    class_dict = dict(zip(class_map["Source_Id"], class_map["Target_Id"]))

    account_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Account]", tuple())
    account_dict = dict(zip(account_map["Source_Id"], account_map["Target_Id"]))

    # Map CustomerRef
    df["Mapped_CustomerRef"] = df[CREDITMEMO_HEADER_MAPPING["CustomerRef.value"]].map(lambda x: customer_dict.get(x) if pd.notna(x) else None)


    # Bulk fetch all CreditMemo_Line rows and group by Parent_Id
    all_lines = sql.fetch_table_with_params(f"SELECT * FROM [{SOURCE_SCHEMA}].[CreditMemo_Line]", tuple())
    lines_grouped = all_lines.groupby("Parent_Id")

    def get_mapped_line_refs_bulk(txn_id, map_dict, source_fields: list):
        mapped_ids = []
        if txn_id in lines_grouped.groups:
            lines = lines_grouped.get_group(txn_id)
            for _, ln in lines.iterrows():
                for source_field in source_fields:
                    val = ln.get(source_field)
                    if pd.notna(val):
                        tgt = map_dict.get(val)
                        if tgt:
                            mapped_ids.append(str(tgt))
        return ",".join(sorted(set(mapped_ids))) if mapped_ids else None

    # Line-level mappings using bulk lines
    df["Mapped_ItemRef"] = df["Id"].map(lambda txn_id: get_mapped_line_refs_bulk(
        txn_id, item_dict, [
            CREDITMEMO_LINE_MAPPING["SalesItemLineDetail.ItemRef.value"],
            CREDITMEMO_LINE_MAPPING["ItemBasedExpenseLineDetail.ItemRef.value"]
        ]
    ))

    df["Mapped_ClassRef"] = df["Id"].map(lambda txn_id: get_mapped_line_refs_bulk(
        txn_id, class_dict, [
            CREDITMEMO_LINE_MAPPING["SalesItemLineDetail.ClassRef.value"]
        ]
    ))

    df["Mapped_AccountRef"] = df["Id"].map(lambda txn_id: get_mapped_line_refs_bulk(
        txn_id, account_dict, [
            CREDITMEMO_LINE_MAPPING["AccountBasedExpenseLineDetail.AccountRef.value"],
            CREDITMEMO_LINE_MAPPING["ItemBasedExpenseLineDetail.AccountRef.value"],
            CREDITMEMO_LINE_MAPPING["JournalEntryLineDetail.AccountRef.value"]
        ]
    ))

#---------------------------------------------- TAXCODE MAPPING ---------------------------------------------------------#
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

#-------------------------------------------------------------TAXCODE MAPPING DONE ---------------------------------------------------------

    # Clear existing and insert fresh
    if sql.table_exists("Map_CreditMemo", MAPPING_SCHEMA):
        sql.run_query(f"TRUNCATE TABLE [{MAPPING_SCHEMA}].[Map_CreditMemo]")

    sql.insert_invoice_map_dataframe(df, "Map_CreditMemo", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df)} CreditMemo records into Map_CreditMemo")

def get_lines(creditmemo_id):
    return sql.fetch_table_with_params(
        f"SELECT * FROM [{SOURCE_SCHEMA}].[CreditMemo_Line] WHERE Parent_Id = ?", (creditmemo_id,)
    )

#need to apply in dataframe while doing testing phase 2
def apply_duplicate_docnumber_strategy_universal():
    from utils.apply_duplicate_docnumber import apply_duplicate_docnumber_strategy_dynamic
    apply_duplicate_docnumber_strategy_dynamic(
        target_table="Map_CreditMemo",
        schema=MAPPING_SCHEMA,
        docnumber_column="DocNumber",
        source_id_column="Source_Id",
        duplicate_column="Duplicate_Docnumber",
        check_against_tables=["Map_Invoice", "Map_CreditMemo"]
    )

def generate_creditmemo_payloads_in_batches(batch_size=500):
    logger.info("⚙️ Generating payloads for CreditMemo records...")

    # Prefetch all lines and mapping tables for speed
    all_lines = sql.fetch_table_with_params(f"SELECT * FROM [{SOURCE_SCHEMA}].[CreditMemo_Line]", tuple())
    lines_grouped = all_lines.groupby("Parent_Id")

    item_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Item]", tuple())
    item_dict = dict(zip(item_map["Source_Id"], item_map["Target_Id"]))
    class_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Class]", tuple())
    class_dict = dict(zip(class_map["Source_Id"], class_map["Target_Id"]))
    account_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Account]", tuple())
    account_dict = dict(zip(account_map["Source_Id"], account_map["Target_Id"]))

    def build_payload_fast(row, lines, final_docnumber):
        payload = {
            "DocNumber": final_docnumber,
            "TxnDate": row.get(CREDITMEMO_HEADER_MAPPING["TxnDate"]),
            "CustomerRef": {"value": str(row.get("Mapped_CustomerRef"))},
            "CurrencyRef": {"value": row.get(CREDITMEMO_HEADER_MAPPING["CurrencyRef.value"]) or "USD"},
            "ApplyTaxAfterDiscount": bool(row.get(CREDITMEMO_HEADER_MAPPING["ApplyTaxAfterDiscount"])),
            "PrintStatus": row.get(CREDITMEMO_HEADER_MAPPING["PrintStatus"]),
            "EmailStatus": row.get(CREDITMEMO_HEADER_MAPPING["EmailStatus"]),
            "PrivateNote": row.get(CREDITMEMO_HEADER_MAPPING["PrivateNote"])
        }
        if row.get(CREDITMEMO_HEADER_MAPPING["CustomerMemo.value"]):
            payload["CustomerMemo"] = {"value": row.get(CREDITMEMO_HEADER_MAPPING["CustomerMemo.value"])}
        if row.get(CREDITMEMO_HEADER_MAPPING["BillEmail.Address"]):
            payload["BillEmail"] = {"Address": row.get(CREDITMEMO_HEADER_MAPPING["BillEmail.Address"])}
        if row.get(CREDITMEMO_HEADER_MAPPING["BillAddr.Line1"]):
            payload["BillAddr"] = {
                "Line1": row.get(CREDITMEMO_HEADER_MAPPING["BillAddr.Line1"]),
                "Line2": row.get(CREDITMEMO_HEADER_MAPPING.get("BillAddr.Line2")),
                "City": row.get(CREDITMEMO_HEADER_MAPPING["BillAddr.City"]),
                "Country": row.get(CREDITMEMO_HEADER_MAPPING["BillAddr.Country"]),
                "CountrySubDivisionCode": row.get(CREDITMEMO_HEADER_MAPPING["BillAddr.CountrySubDivisionCode"]),
                "PostalCode": row.get(CREDITMEMO_HEADER_MAPPING["BillAddr.PostalCode"])
            }
        
#------------------------------ NEW: Header-level TaxCodeRef (only if mapping exists) ---------------------------------------------
        mapped_txn_tax_code = row.get("Mapped_TxnTaxCodeRef")
        if "TxnTaxDetail_TotalTax" in row.index and pd.notna(row.get("TxnTaxDetail_TotalTax")):
            TxnTaxDetail_TotalTax = safe_float(row.get("TxnTaxDetail_TotalTax"))
        if mapped_txn_tax_code:
            payload["TxnTaxDetail"] = {
                "TotalTax" :TxnTaxDetail_TotalTax,
                "TxnTaxCodeRef": {"value": str(mapped_txn_tax_code)}
                # You can include "TotalTax" later if you compute it; not required for setting the header tax code
            }
#------------------------------ NEW: Header-level TaxCodeRef (only if mapping exists) ---------------------------------------------

        payload["Line"] = []
        for _, ln in lines.iterrows():
            detail_type = ln.get("DetailType")
            if not detail_type:
                continue
            line = {
                "DetailType": detail_type,
                "Amount": safe_float(ln.get("Amount")),
                "Description": ln.get("Description")
            }
            if detail_type == "SalesItemLineDetail":
                item_id = item_dict.get(ln.get(CREDITMEMO_LINE_MAPPING["SalesItemLineDetail.ItemRef.value"]))
                if not item_id:
                    continue
                detail = {
                    "ItemRef": {"value": str(item_id)},
                    "Qty": safe_float(ln.get(CREDITMEMO_LINE_MAPPING["SalesItemLineDetail.Qty"])),
                    "UnitPrice": safe_float(ln.get(CREDITMEMO_LINE_MAPPING["SalesItemLineDetail.UnitPrice"])),
                    "TaxCodeRef": {"value": ln.get(CREDITMEMO_LINE_MAPPING["SalesItemLineDetail.TaxCodeRef.value"]) or "NON"},
                    "ServiceDate": ln.get(CREDITMEMO_LINE_MAPPING["SalesItemLineDetail.ServiceDate"])
                }
                class_id = class_dict.get(ln.get(CREDITMEMO_LINE_MAPPING["SalesItemLineDetail.ClassRef.value"]))
                if class_id:
                    detail["ClassRef"] = {"value": str(class_id)}
                line["SalesItemLineDetail"] = detail
            elif detail_type == "SubTotalLineDetail":
                line["SubTotalLineDetail"] = {}
            elif detail_type == "DiscountLineDetail":
                detail = {
                    "PercentBased": ln.get("DiscountLineDetail.PercentBased") == True,
                    "DiscountPercent": safe_float(ln.get("DiscountLineDetail.DiscountPercent"))
                }
                line["DiscountLineDetail"] = detail
            elif detail_type == "AccountBasedExpenseLineDetail":
                account_id = account_dict.get(ln.get(CREDITMEMO_LINE_MAPPING["AccountBasedExpenseLineDetail.AccountRef.value"]))
                if not account_id:
                    continue
                line["AccountBasedExpenseLineDetail"] = {
                    "AccountRef": {"value": str(account_id)}
                }
            elif detail_type == "ItemBasedExpenseLineDetail":
                item_id = item_dict.get(ln.get(CREDITMEMO_LINE_MAPPING["ItemBasedExpenseLineDetail.ItemRef.value"]))
                account_id = account_dict.get(ln.get(CREDITMEMO_LINE_MAPPING["ItemBasedExpenseLineDetail.AccountRef.value"]))
                if not item_id and not account_id:
                    continue
                detail = {
                    "Qty": safe_float(ln.get(CREDITMEMO_LINE_MAPPING["ItemBasedExpenseLineDetail.Qty"])),
                    "UnitPrice": safe_float(ln.get(CREDITMEMO_LINE_MAPPING["ItemBasedExpenseLineDetail.UnitPrice"]))
                }
                if item_id:
                    detail["ItemRef"] = {"value": str(item_id)}
                if account_id:
                    detail["AccountRef"] = {"value": str(account_id)}
                line["ItemBasedExpenseLineDetail"] = detail
            elif detail_type == "JournalEntryLineDetail":
                account_id = account_dict.get(ln.get(CREDITMEMO_LINE_MAPPING["JournalEntryLineDetail.AccountRef.value"]))
                if not account_id:
                    continue
                detail = {
                    "AccountRef": {"value": str(account_id)},
                    "PostingType": ln.get(CREDITMEMO_LINE_MAPPING["JournalEntryLineDetail.PostingType"])
                }
                line["JournalEntryLineDetail"] = detail
            elif detail_type == "TaxLineDetail":
                detail = {
                    "TaxRateRef": {"value": ln.get(CREDITMEMO_LINE_MAPPING["TaxLineDetail.TaxRateRef.value"])} ,
                    "PercentBased": ln.get(CREDITMEMO_LINE_MAPPING["TaxLineDetail.PercentBased"]) == True,
                    "TaxPercent": safe_float(ln.get(CREDITMEMO_LINE_MAPPING["TaxLineDetail.TaxPercent"]))
                }
                line["TaxLineDetail"] = detail
            elif detail_type == "DescriptionOnly":
                line["DescriptionOnly"] = ln.get(CREDITMEMO_LINE_MAPPING["DescriptionOnly"])
            elif detail_type == "TDSLineDetail":
                line["TDSLineDetail"] = ln.get("TDSLineDetail") or {}
            else:
                logger.warning(f"⚠️ Unknown DetailType: {detail_type} — skipped")
                continue
            payload["Line"].append(line)
        return payload

    while True:
        df = sql.fetch_table_with_params(f"""
            SELECT TOP {batch_size} * FROM [{MAPPING_SCHEMA}].[Map_CreditMemo]
            WHERE Porter_Status = 'Ready' AND (Payload_JSON IS NULL OR Payload_JSON = '')
        """, ())
        if df.empty:
            logger.info("✅ All payloads generated.")
            break
        docnumber_map = {}
        for _, row in df.iterrows():
            sid = row["Source_Id"]
            raw_docnum = row.get("Duplicate_Docnumber") or row.get("DocNumber")
            raw_docnum = str(raw_docnum).strip() if raw_docnum is not None else ""
            if raw_docnum.lower() == "null" or raw_docnum == "":
                docnumber_map[sid] = None
            else:
                docnumber_map[sid] = raw_docnum[:21]
        for _, row in df.iterrows():
            sid = row["Source_Id"]
            if not row.get("Mapped_CustomerRef"):
                update_mapping_status(MAPPING_SCHEMA, "Map_CreditMemo", sid, "Failed", failure_reason="Missing CustomerRef")
                continue
            # Use prefetched lines
            lines = lines_grouped.get_group(sid) if sid in lines_grouped.groups else pd.DataFrame()
            final_docnumber = docnumber_map[sid]
            payload = build_payload_fast(row, lines, final_docnumber)
            if not payload.get("Line"):
                update_mapping_status(MAPPING_SCHEMA, "Map_CreditMemo", sid, "Failed", failure_reason="No Line items")
                continue
            if final_docnumber is None and "DocNumber" in payload:
                del payload["DocNumber"]
            pretty_json = json.dumps(payload, indent=2)
            update_mapping_status(MAPPING_SCHEMA, "Map_CreditMemo", sid, "Ready", payload=json.loads(pretty_json))
        logger.info(f"✅ Processed {len(df)} payloads in this batch.")

session = requests.Session()

# ---- Reusable session with HTTP-level retries for 429/5xx and keep-alive
try:
    session  # reuse if already created elsewhere
except NameError:
    session = requests.Session()
    _retry = Retry(
        total=5,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["POST"])
    )
    adapter = HTTPAdapter(max_retries=_retry, pool_connections=64, pool_maxsize=64)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

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

def _sleep_with_jitter(base=0.5, factor=2.0, attempt=0, cap=10.0):
    delay = min(cap, base * (factor ** attempt)) + random.random() * 0.2
    time.sleep(delay)

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
        entity_name: "CreditMemo" | "Bill" | "VendorCredit" | ...
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

# ============================ One-off poster (fallback) ============================
def post_creditmemo(row):
    exit()
    sid = row["Source_Id"]
    if row.get("Porter_Status") == "Success" or int(row.get("Retry_Count") or 0) >= 5 or not row.get("Payload_JSON"):
        return

    url, headers = get_qbo_auth()
    try:
        payload = _fast_loads(row["Payload_JSON"])
    except Exception as e:
        update_mapping_status(MAPPING_SCHEMA, "Map_CreditMemo", sid, "Failed",
                              failure_reason=f"Bad JSON: {e}", increment_retry=True)
        return

    try:
        resp = session.post(url, headers=headers, json=payload, timeout=20)
        if resp.status_code == 200:
            data = resp.json()
            qid = data.get("CreditMemo", {}).get("Id")
            update_mapping_status(MAPPING_SCHEMA, "Map_CreditMemo", sid, "Success", target_id=qid)
            logger.info(f"✅ CreditMemo {sid} migrated successfully. Target ID: {qid}")
        elif resp.status_code in (401, 403):
            # One-shot refresh and retry
            try:
                auto_refresh_token_if_needed(force=True)
            except Exception:
                pass
            url, headers = get_qbo_auth()
            resp2 = session.post(url, headers=headers, json=payload, timeout=20)
            if resp2.status_code == 200:
                qid = resp2.json().get("CreditMemo", {}).get("Id")
                update_mapping_status(MAPPING_SCHEMA, "Map_CreditMemo", sid, "Success", target_id=qid)
                logger.info(f"✅ CreditMemo {sid} migrated successfully. Target ID: {qid}")
            else:
                update_mapping_status(MAPPING_SCHEMA, "Map_CreditMemo", sid, "Failed",
                                      failure_reason=(resp2.text or f"HTTP {resp2.status_code}")[:500],
                                      increment_retry=True)
        else:
            update_mapping_status(MAPPING_SCHEMA, "Map_CreditMemo", sid, "Failed",
                                  failure_reason=(resp.text or f"HTTP {resp.status_code}")[:500],
                                  increment_retry=True)
    except Exception as e:
        update_mapping_status(MAPPING_SCHEMA, "Map_CreditMemo", sid, "Failed",
                              failure_reason=str(e), increment_retry=True)

# ============================ Batch posting helper ===============================
def _post_batch_creditmemos(eligible_batch, url, headers, timeout=40, post_batch_limit=20, max_manual_retries=1):
    """
    Single-threaded batch poster (uses QBO /batch to send many in one HTTP call).
    - Pre-decodes Payload_JSON once per row (fast loader).
    - Handles 401/403 with one token refresh; gentle retry on 429/5xx.
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

    batch_url = _derive_batch_url(url, "CreditMemo")

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
                    {"bId": str(sid), "operation": "create", "CreditMemo": payload}
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
                        cm = item.get("CreditMemo")
                        if cm and "Id" in cm:
                            qid = cm["Id"]
                            successes.append((qid, bid))
                            logger.info(f"✅ CreditMemo {bid} → QBO {qid}")
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
                        logger.error(f"❌ CreditMemo {bid} failed: {reason}")

                    # Any missing responses → mark failed
                    for sid, _ in chunk:
                        if str(sid) not in seen:
                            failures.append(("No response for bId", sid))
                            logger.error(f"❌ No response for CreditMemo {sid}")
                    break

                elif sc in (401, 403) and not attempted_refresh:
                    logger.warning(f"🔐 {sc} on CreditMemo batch ({len(chunk)} items); refreshing token and retrying once...")
                    try:
                        auto_refresh_token_if_needed(force=True)
                    except Exception:
                        pass
                    url, headers = get_qbo_auth()
                    batch_url = _derive_batch_url(url, "CreditMemo")
                    _sleep_with_jitter(attempt=attempt)
                    attempted_refresh = True
                    continue

                elif sc in (429, 500, 502, 503, 504) and attempt < max_manual_retries:
                    _sleep_with_jitter(attempt=attempt)
                    continue

                else:
                    reason = (resp.text or f"HTTP {sc}")[:1000]
                    for sid, _ in chunk:
                        failures.append((reason, sid))
                    logger.error(f"❌ CreditMemo batch failed ({len(chunk)} items): {reason}")
                    break

            except requests.Timeout:
                if attempt < max_manual_retries:
                    _sleep_with_jitter(attempt=attempt)
                    continue
                for sid, _ in chunk:
                    failures.append(("Timeout", sid))
                break
            except Exception as e:
                if attempt < max_manual_retries:
                    _sleep_with_jitter(attempt=attempt)
                    continue
                reason = f"Batch exception: {e}"
                for sid, _ in chunk:
                    failures.append((reason, sid))
                logger.exception(f"❌ Exception during CreditMemo batch POST ({len(chunk)} items)")
                break

    return successes, failures

# ============================ Apply updates (set-based) ===========================
def _apply_batch_updates(successes, failures):
    """
    Status updates for Map_CreditMemo.
    Uses executemany if available; falls back to row-by-row.
    """
    try:
        if successes:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_CreditMemo] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                [(qid, sid) for qid, sid in successes]
            )
        if failures:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_CreditMemo] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                [(reason, sid) for reason, sid in failures]
            )
    except NameError:
        # Fallback to per-row updates if executemany helper is not available
        if successes:
            for qid, sid in successes:
                sql.run_query(
                    f"UPDATE [{MAPPING_SCHEMA}].[Map_CreditMemo] "
                    f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                    f"WHERE Source_Id=?",
                    (qid, sid)
                )
        if failures:
            for reason, sid in failures:
                sql.run_query(
                    f"UPDATE [{MAPPING_SCHEMA}].[Map_CreditMemo] "
                    f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                    f"WHERE Source_Id=?",
                    (reason, sid)
                )

# ============================ Full migration path ================================
def migrate_creditmemos(CREDITMEMO_DATE_FROM,CREDITMEMO_DATE_TO):
    print("\n🚀 Starting CreditMemo Migration Phase\n" + "=" * 40)
    ensure_mapping_table(CREDITMEMO_DATE_FROM,CREDITMEMO_DATE_TO)

    # Optional DocNumber dedup
    if 'ENABLE_VC_DOCNUMBER_UNIVERSAL_DEDUP' in globals() and ENABLE_VC_DOCNUMBER_UNIVERSAL_DEDUP:
        apply_duplicate_docnumber_strategy_universal()

    # Ensure payloads exist
    generate_creditmemo_payloads_in_batches()

    # Pull eligible
    rows = sql.fetch_table("Map_CreditMemo", MAPPING_SCHEMA)
    eligible = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("⚠️ No eligible CreditMemos to post.")
        return

    url, headers = get_qbo_auth()

    # Tunables
    select_batch_size = 300        # DB slice size
    post_batch_limit  = 10         # items per /batch call (≤30). Set to 3 for "≥3 at a time".
    timeout = 40

    total = len(eligible)
    logger.info(f"📤 Posting {total} CreditMemo(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        slice_df = eligible.iloc[i:i+select_batch_size]
        successes, failures = _post_batch_creditmemos(
            slice_df, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates(successes, failures)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    # Optional one-pass fallback retry for failures without Target_Id
    failed_df = sql.fetch_table("Map_CreditMemo", MAPPING_SCHEMA)
    failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
    if not failed_records.empty:
        logger.info(f"🔁 Reprocessing {len(failed_records)} failed CreditMemos (single-record fallback)...")
        for _, row in failed_records.iterrows():
            post_creditmemo(row)

    print("\n🏁 CreditMemo migration completed.")

# ============================ Resume / Post path ================================
def resume_or_post_creditmemos(CREDITMEMO_DATE_FROM,CREDITMEMO_DATE_TO):
    """
    Smart resume for CreditMemo migration:
    - Rebuild mapping if missing.
    - Regenerate only missing payloads.
    - Post eligible Ready/Failed via Batch API.
    """
    print("\n🔁 Resuming CreditMemo Migration (conditional mode)\n" + "=" * 50)

    # 1) Table existence
    if not sql.table_exists("Map_CreditMemo", MAPPING_SCHEMA):
        logger.warning("❌ Map_CreditMemo table does not exist. Running full migration.")
        migrate_creditmemos(CREDITMEMO_DATE_FROM,CREDITMEMO_DATE_TO)
        return

    mapped_df = sql.fetch_table("Map_CreditMemo", MAPPING_SCHEMA)
    source_df = sql.fetch_table("CreditMemo", SOURCE_SCHEMA)

    # 2) Deduplicate Docnumber if needed
    from utils.apply_duplicate_docnumber import apply_duplicate_docnumber_strategy_dynamic
    if mapped_df["Duplicate_Docnumber"].isnull().any() or (mapped_df["Duplicate_Docnumber"] == "").any():
        logger.info("🔍 Some Duplicate_Docnumber values missing. Running deduplication...")
        apply_duplicate_docnumber_strategy_dynamic(
            target_table="Map_CreditMemo",
            schema=MAPPING_SCHEMA,
            docnumber_column="DocNumber",
            source_id_column="Source_Id",
            duplicate_column="Duplicate_Docnumber",
            check_against_tables=["Map_Invoice", "Map_CreditMemo"]
        )
        mapped_df = sql.fetch_table("Map_CreditMemo", MAPPING_SCHEMA)
    else:
        logger.info("✅ All Duplicate_Docnumber values present. Skipping deduplication.")

    # 3) Payloads: generate only missing
    payload_missing = mapped_df["Payload_JSON"].isnull() | (mapped_df["Payload_JSON"] == "")
    missing_count = int(payload_missing.sum())
    if missing_count > 0:
        logger.info(f"🔧 {missing_count} CreditMemos missing Payload_JSON. Generating for those...")
        generate_creditmemo_payloads_in_batches(batch_size=500)
        mapped_df = sql.fetch_table("Map_CreditMemo", MAPPING_SCHEMA)
    else:
        logger.info("✅ All CreditMemos have Payload_JSON.")

    # 4) Eligible rows
    eligible = mapped_df[mapped_df["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("⚠️ No eligible CreditMemos to post.")
        return

    url, headers = get_qbo_auth()

    select_batch_size = 300
    post_batch_limit  = 10
    timeout           = 40

    total = len(eligible)
    logger.info(f"📤 Posting {total} CreditMemo(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        slice_df = eligible.iloc[i:i+select_batch_size]
        successes, failures = _post_batch_creditmemos(
            slice_df, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates(successes, failures)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    print("\n🏁 CreditMemo posting completed.")

# -------------------------------- Smart entrypoint wrapper ------------------------------------
def smart_creditmemo_migration(CREDITMEMO_DATE_FROM,CREDITMEMO_DATE_TO):
    """
    Smart migration entrypoint for CreditMemo:
    - If Map_CreditMemo exists and row count matches CreditMemo → resume/post
    - Else → full migration
    """
    logger.info("🔎 Running smart_creditmemo_migration...")
    if sql.table_exists("Map_CreditMemo", MAPPING_SCHEMA):
        mapped_df = sql.fetch_table("Map_CreditMemo", MAPPING_SCHEMA)
        source_df = sql.fetch_table("CreditMemo", SOURCE_SCHEMA)
        if len(mapped_df) == len(source_df):
            logger.info("✅ Table exists and row count matches. Resuming/posting CreditMemos.")
            resume_or_post_creditmemos(CREDITMEMO_DATE_FROM,CREDITMEMO_DATE_TO)
            return
        else:
            logger.warning(f"❌ Row count mismatch: Map_CreditMemo={len(mapped_df)}, CreditMemo={len(source_df)}. Running full migration.")
    else:
        logger.warning("❌ Map_CreditMemo table does not exist. Running full migration.")
    migrate_creditmemos(CREDITMEMO_DATE_FROM,CREDITMEMO_DATE_TO)
