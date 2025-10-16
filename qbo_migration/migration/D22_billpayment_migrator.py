"""
Sequence : 22
Author: Dixit Prajapati
Created: 2025-09-16
Description: Handles migration of BillPayment records from source system to QBO.
Production : Ready
Development : Require when necessary
Phase : 02 - Multi User
"""

import os, json, requests, pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import global_logger as logger
from config.mapping.billpayment_mapping import BILLPAYMENT_HEADER_MAPPING as HEADER_MAP, BILLPAYMENT_LINE_MAPPING as LINE_MAP
from utils.apply_duplicate_docnumber import apply_duplicate_docnumber_strategy_dynamic
from utils.token_refresher import get_qbo_context_migration
from storage.sqlserver.sql import executemany
from utils.payload_cleaner import deep_clean

load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
ENABLE_GLOBAL_BILLPAYMENT_DOCNUMBER_DEDUP=True
API_ENTITY = "billpayment"


def apply_duplicate_docnumber_strategy():
    apply_duplicate_docnumber_strategy_dynamic(
    target_table="Map_BillPayment",
    docnumber_column= "DocNumber",
    duplicate_column= "Duplicate_DocNumber",
    schema=MAPPING_SCHEMA,
    check_against_tables=["Map_Bill","Map_Invoice","Map_VendorCredit","Map_JournalEntry","Map_Deposit","Map_Purchase","Map_Salesreceipt","Map_Refundreceipt","Map_Payment"])

def get_qbo_auth():
    env = os.getenv("QBO_ENVIRONMENT", "production")
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

def ensure_mapping_table(BILLPAYMENT_DATE_FROM,BILLPAYMENT_DATE_TO):
    """
    Ultra-optimized BillPayment → Map_BillPayment initializer (no threading).
    Targets ~50k headers / ~200k lines efficiently by:
      - Filtering headers in SQL (DATE_FROM / DATE_TO).
      - Bulk-loading all required mapping tables once.
      - Building a single UNION-style in-memory "map index" keyed by (TxnType, Source_Id).
      - Loading BillPayment_Line once (narrow columns), filtering in-memory by Parent_Id.
      - Vectorized join from lines → target ids (no per-row SQL).
      - Per-parent JSON aggregation done in grouped apply (fast enough at this scale).
    Core mapping and payload structure are unchanged.
    """
    DATE_FROM = BILLPAYMENT_DATE_FROM
    DATE_TO = BILLPAYMENT_DATE_TO

    # ---------- helper for safe SQL Server identifier quoting (e.g. LinkedTxn[0].TxnId) ----------
    def _qident(colname: str) -> str:
        return f"[{str(colname).replace(']', ']]')}]"

    # ---------- 1) Pull headers with optional date filter (in SQL) ----------
    params, where = [], []
    if DATE_FROM:
        where.append("TxnDate >= ?")
        params.append(DATE_FROM)
    if DATE_TO:
        where.append("TxnDate <= ?")
        params.append(DATE_TO)

    sql_headers = f"SELECT * FROM [{SOURCE_SCHEMA}].[BillPayment]"
    if where:
        sql_headers += " WHERE " + " AND ".join(where)

    df = sql.fetch_table_with_params(sql_headers, tuple(params))
    if df.empty:
        logger.warning("⚠️ No BillPayment records found in range.")
        return

    # ---------- 2) Base columns ----------
    df["Source_Id"]      = df["Id"]
    df["Target_Id"]      = None
    df["Porter_Status"]  = "Ready"
    df["Retry_Count"]    = 0
    df["Failure_Reason"] = None
    df["Payload_JSON"]   = None

    # ---------- 3) Bulk map master refs (Vendor, Department, Accounts) ----------
    def _to_dict(tbl):
        t = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[{tbl}]", tuple())
        return dict(zip(t["Source_Id"], t["Target_Id"])) if not t.empty else {}

    vendor_dict = _to_dict("Map_Vendor")
    dept_dict   = _to_dict("Map_Department")
    acct_dict   = _to_dict("Map_Account")

    ven_col = HEADER_MAP.get("VendorRef.value")
    if ven_col and ven_col in df.columns:
        df["Mapped_Vendor"] = df[ven_col].map(lambda x: vendor_dict.get(x) if pd.notna(x) else None)
    else:
        df["Mapped_Vendor"] = None

    dept_field = HEADER_MAP.get("DepartmentRef.value")
    if dept_field and dept_field in df.columns:
        df["Mapped_Department"] = df[dept_field].map(lambda x: dept_dict.get(x) if pd.notna(x) else None)
    else:
        df["Mapped_Department"] = None
        logger.info("ℹ️ DepartmentRef.value not found in source — skipping department mapping.")

    bank_col = HEADER_MAP.get("CheckPayment.BankAccountRef.value")
    if bank_col and bank_col in df.columns:
        df["Mapped_BankAccount"] = df[bank_col].map(lambda x: acct_dict.get(x) if pd.notna(x) else None)
    else:
        df["Mapped_BankAccount"] = None

    cc_field = HEADER_MAP.get("CreditCardPayment.CCAccountRef.value")
    if cc_field and cc_field in df.columns:
        df["Mapped_CCAccount"] = df[cc_field].map(lambda x: acct_dict.get(x) if pd.notna(x) else None)
    else:
        df["Mapped_CCAccount"] = None
        logger.warning("⚠️ CreditCardPayment.CCAccountRef.value not found in BillPayment source — skipping CC account mapping.")

    # ---------- 4) Build a unified mapping index for LinkedTxn lookups ----------
    # Source TxnType -> mapping table name
    txn_type_to_map = {
        "Bill":             "Map_Bill",
        "VendorCredit":     "Map_VendorCredit",
        "JournalEntry":     "Map_JournalEntry",
        "Purchase":         "Map_Purchase",    # also used for Check/Expense/CreditCardCredit
        "Check":            "Map_Purchase",
        "Expense":          "Map_Purchase",
        "CreditCardCredit": "Map_Purchase",
        "Deposit":          "Map_Deposit",
    }
    # Override of TxnType when writing payload
    txn_type_override = {
        "Expense":          "Cash",
        "Check":            "Check",
        "CreditCardCredit": "CreditCardCredit",
    }

    # Load each referenced mapping table once and stamp a TxnType column so we can join by (TxnType, Source_Id)
    map_frames = []
    loaded_tables = {}  # cache base DF per table to reuse for different TxnTypes that share a table
    for src_type, map_tbl in txn_type_to_map.items():
        if map_tbl not in loaded_tables:
            base = sql.fetch_table_with_params(
                f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[{map_tbl}]",
                tuple()
            )
            # Ensure dtype consistency (strings) to avoid join misses
            if not base.empty:
                base = base.copy()
                base["Source_Id"] = base["Source_Id"].astype(str).str.strip()
                base["Target_Id"] = base["Target_Id"].astype(str).str.strip()
            loaded_tables[map_tbl] = base

        base = loaded_tables[map_tbl]
        if base is None or base.empty:
            continue

        frame = base.copy()
        frame["TxnType"]      = src_type
        frame["ResolvedType"] = txn_type_override.get(src_type, src_type)
        map_frames.append(frame[["TxnType", "Source_Id", "Target_Id", "ResolvedType"]])

    map_union_df = pd.concat(map_frames, ignore_index=True) if map_frames else pd.DataFrame(
        columns=["TxnType", "Source_Id", "Target_Id", "ResolvedType"]
    )

    # ---------- 5) Load BillPayment_Line once (narrow columns), filter in-memory ----------
    parent_ids = set(df["Id"].tolist())
    if not parent_ids:
        df["LinkedTxns_JSON"] = "[]"
    else:
        # Column names (escaped)
        txnid_col_src   = LINE_MAP["LinkedTxn[0].TxnId"]
        txntype_col_src = LINE_MAP["LinkedTxn[0].TxnType"]
        q_txnid   = _qident(txnid_col_src)
        q_txntype = _qident(txntype_col_src)
        q_amount  = _qident("Amount")

        # Pull just needed columns; avoid 50k-parameter IN by loading all then filtering in-memory.
        line_q = (
            f"SELECT Parent_Id, {q_txnid} AS TxnIdCol, {q_txntype} AS TxnTypeCol, {q_amount} AS AmountCol "
            f"FROM [{SOURCE_SCHEMA}].[BillPayment_Line]"
        )
        line_df = sql.fetch_table_with_params(line_q, tuple())
        if not line_df.empty:
            # Filter to current batch of parents
            line_df = line_df[line_df["Parent_Id"].isin(parent_ids)]
        else:
            line_df = pd.DataFrame(columns=["Parent_Id","TxnIdCol","TxnTypeCol","AmountCol"])

        # ---------- 6) Vectorized resolve: join lines -> unified mapping index ----------
        if not line_df.empty and not map_union_df.empty:
            # Clean keys
            line_df = line_df.dropna(subset=["TxnIdCol", "TxnTypeCol"]).copy()
            if not line_df.empty:
                line_df["TxnIdKey"]   = line_df["TxnIdCol"].astype(str).str.strip()
                line_df["TxnTypeKey"] = line_df["TxnTypeCol"].astype(str).str.strip()

                map_union_df = map_union_df.copy()
                map_union_df["Source_Key"] = map_union_df["Source_Id"].astype(str).str.strip()

                merged = line_df.merge(
                    map_union_df[["TxnType", "Source_Key", "Target_Id", "ResolvedType"]],
                    left_on=["TxnTypeKey", "TxnIdKey"],
                    right_on=["TxnType", "Source_Key"],
                    how="left",
                    copy=False
                )

                # Keep only rows where a Target_Id was found
                merged = merged[merged["Target_Id"].notna()]

                if not merged.empty:
                    # Normalize amount
                    merged["AmountCol"] = pd.to_numeric(merged["AmountCol"], errors="coerce").fillna(0.0)

                    # Group → JSON per Parent_Id
                    def _pack(g):
                        # local vars speed
                        tids = g["Target_Id"].values
                        rtyp = g["ResolvedType"].values
                        amts = g["AmountCol"].values
                        return json.dumps(
                            [{"TxnId": str(tids[i]), "TxnType": str(rtyp[i]), "Amount": float(amts[i])}
                             for i in range(len(tids))],
                            separators=(",", ":")
                        )

                    linked_json_map = merged.groupby("Parent_Id", sort=False).apply(_pack).to_dict()
                else:
                    linked_json_map = {}
            else:
                linked_json_map = {}
        else:
            linked_json_map = {}

        # Map back (headers with no resolved lines get "[]")
        df["LinkedTxns_JSON"] = df["Id"].map(lambda pid: linked_json_map.get(pid, "[]"))

    # ---------- 7) Replace existing table contents and enforce column ----------
    if sql.table_exists("Map_BillPayment", MAPPING_SCHEMA):
        sql.run_query(f"DELETE FROM [{MAPPING_SCHEMA}].[Map_BillPayment]")

    sql.insert_dataframe(df, "Map_BillPayment", MAPPING_SCHEMA)

    sql.ensure_column_exists(
        table="Map_BillPayment",
        column="LinkedTxns_JSON",
        col_type="NVARCHAR(MAX)",
        schema=MAPPING_SCHEMA
    )

    logger.info(f"✅ Initialized Map_BillPayment with {len(df)} records.")

def get_lines(payment_id):
    return sql.fetch_table_with_params(f"SELECT * FROM {SOURCE_SCHEMA}.BillPayment_Line WHERE Parent_Id=?", (payment_id,))

def build_payload(row, lines):
    """
    Builds a BillPayment payload with mapped references and resolved linked transactions.
    Skips payload generation if TotalAmt and all LinkedTxn amounts are 0.
    Allows generation of empty 'Line' list for open/advance payments (but later we skip posting if no linked txns).
    NOTE: `lines` is unused by design — all linked info comes from LinkedTxns_JSON on the header row.
    """
    # --- fast json loader (local) ---
    try:
        import orjson as _orj
        _loads = lambda s: _orj.loads(s) if isinstance(s, (bytes, bytearray)) else _orj.loads(str(s).encode("utf-8"))
    except Exception:
        import json as _json
        _loads = lambda s: _json.loads(s)

    header_map = HEADER_MAP  # shorthand

    # Pre-derive docnumber once
    raw_doc   = row.get(header_map.get("DocNumber"))
    dedup_doc = row.get("Duplicate_DocNumber")
    doc_number = (str(dedup_doc).strip() if (pd.notna(dedup_doc) and str(dedup_doc).strip() != "") else str(raw_doc or "").strip())

    # Vendor mapping is mandatory
    mapped_vendor = row.get("Mapped_Vendor")
    if not mapped_vendor:
        logger.warning(f"Skipping BillPayment {row['Source_Id']} due to missing vendor mapping")
        return None

    # Total amount (used for zero checks)
    total_amt = safe_float(row.get("TotalAmt", 0))

    # Parse linked transactions (header-level JSON produced in mapping step)
    linked_txns = []
    ljson = row.get("LinkedTxns_JSON")
    if pd.notna(ljson) and str(ljson).strip() != "":
        try:
            linked_txns = _loads(ljson)
        except Exception as e:
            logger.warning(f"⚠️ Failed to parse LinkedTxns_JSON for BillPayment {row['Source_Id']}: {e}")
            return None

    # Skip if total and all linked amounts are 0
    if total_amt == 0 and linked_txns:
        # quick vector-like check
        all_zero = True
        for t in linked_txns:
            if safe_float(t.get("Amount", 0)) != 0:
                all_zero = False
                break
        if all_zero:
            logger.warning(f"⏭️ Skipping BillPayment {row['Source_Id']} — TotalAmt and all LinkedTxn amounts are 0")
            return None

    # ---- payload scaffold
    payload = {
        "DocNumber": doc_number,
        "VendorRef": {"value": str(mapped_vendor)},
        "Line": []
    }

    # Optional mapped values
    mapped_dept = row.get("Mapped_Department")
    if pd.notna(mapped_dept):
        payload["DepartmentRef"] = {"value": str(mapped_dept)}

    pay_type = row.get("PayType")
    if pd.notna(pay_type):
        payload["PayType"] = pay_type
        if pay_type == "Check":
            mba = row.get("Mapped_BankAccount")
            if pd.notna(mba):
                payload["CheckPayment"] = {"BankAccountRef": {"value": str(mba)}}
                cps = row.get("CheckPayment.PrintStatus")
                if pd.notna(cps):
                    payload["CheckPayment"]["PrintStatus"] = cps
        elif pay_type == "CreditCard":
            mcc = row.get("Mapped_CCAccount")
            if pd.notna(mcc):
                payload["CreditCardPayment"] = {"CCAccountRef": {"value": str(mcc)}}

    # Apply other header fields (excluding ones handled above)
    skip_fields = {
        "VendorRef.value",
        "DepartmentRef.value",
        "CheckPayment.BankAccountRef.value",
        "CreditCardPayment.CCAccountRef.value",
        "DocNumber",
    }
    for qbo_field, src_col in header_map.items():
        if qbo_field in skip_fields:
            continue
        val = row.get(src_col)
        if pd.notna(val):
            parts = qbo_field.split(".")
            if len(parts) == 1:
                payload[qbo_field] = val
            elif len(parts) == 2:
                parent, child = parts
                payload.setdefault(parent, {})[child] = val
            else:
                pass
                # logger.warning(f"⚠️ Skipping deeply nested field {qbo_field}")

    # Currency and exchange rate
    currency_val = row.get("CurrencyRef.value") or "USD"
    payload["CurrencyRef"] = {"value": currency_val}
    exr = row.get("ExchangeRate")
    if pd.notna(exr):
        payload["ExchangeRate"] = safe_float(exr)

    # Line-level LinkedTxn from header JSON
    if linked_txns:
        # prebuild list faster than repeated appends in some cases
        plines = []
        for txn in linked_txns:
            plines.append({
                "Amount": safe_float(txn.get("Amount", 0)),
                "LinkedTxn": [{
                    "TxnId": txn.get("TxnId"),
                    "TxnType": txn.get("TxnType")
                }]
            })
        payload["Line"] = plines

    # If no linked txns, treat as unsupported advance/zero payment per original logic
    if not linked_txns:
        if total_amt > 0:
            logger.warning(f"⏭️ Skipping BillPayment {row['Source_Id']} — advance payment without LinkedTxn is not supported by QBO API")
        else:
            logger.warning(f"⏭️ Skipping BillPayment {row['Source_Id']} — no LinkedTxn and amount is zero")
        return None

    return deep_clean(payload)

def generate_billpayment_payloads_in_batches(batch_size=1000):
    """
    Optimized payload generator (no threading):
      - Processes headers in slices.
      - Avoids per-row line queries (LinkedTxns come from header JSON).
      - Uses fast JSON dumps and executemany updates.
      - Preserves original mapping and validation behavior.
    """
    # Fast JSON dumps
    try:
        import orjson as _orj
        _dumps = lambda obj: _orj.dumps(obj, option=_orj.OPT_INDENT_2).decode("utf-8")
    except Exception:
        import json as _json
        _dumps = lambda obj: _json.dumps(obj, indent=2)

    rows = sql.fetch_dataframe(
        f"""
        SELECT * FROM [{MAPPING_SCHEMA}].[Map_BillPayment]
        WHERE Porter_Status IN ('Ready', 'Failed')
          AND (Payload_JSON IS NULL OR Payload_JSON = '')
          AND (Retry_Count IS NULL OR Retry_Count < 3)
        """
    )
    total = len(rows)
    if total == 0:
        logger.info("✅ No pending BillPayments needing payload generation.")
        return

    logger.info(f"⚙️ Generating payloads for {total} BillPayments...")

    processed = 0
    for start in range(0, total, batch_size):
        batch = rows.iloc[start:start + batch_size]

        success_updates = []  # (payload_json, sid)
        fail_updates    = []  # (reason, sid)

        # Build payloads
        for _, row in batch.iterrows():
            sid = row["Source_Id"]
            try:
                # LinkedTxns_JSON is already on header; lines are not required
                payload = build_payload(row, lines=None)
                if payload:
                    success_updates.append((_dumps(payload), sid))
                    processed += 1
                    if processed % 1000 == 0:
                        logger.info(f"✅ Successfully generated JSON payloads for {processed} BillPayments.")
                else:
                    fail_updates.append(("Invalid or empty payload", sid))
            except Exception as e:
                fail_updates.append((str(e), sid))
                logger.exception(f"❌ Exception while generating payload for BillPayment {sid}")

        # Bulk apply updates
        try:
            executemany(
                f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_BillPayment]
                SET Payload_JSON=?, Porter_Status='Ready'
                WHERE Source_Id=?
                """,
                success_updates
            )
        except NameError:
            for pj, sid in success_updates:
                sql.run_query(
                    f"UPDATE [{MAPPING_SCHEMA}].[Map_BillPayment] "
                    f"SET Payload_JSON=?, Porter_Status='Ready' "
                    f"WHERE Source_Id=?",
                    (pj, sid)
                )

        try:
            executemany(
                f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_BillPayment]
                SET Porter_Status='Failed',
                    Failure_Reason=?,
                    Retry_Count = ISNULL(Retry_Count, 0) + 1
                WHERE Source_Id=?
                """,
                fail_updates
            )
        except NameError:
            for reason, sid in fail_updates:
                sql.run_query(
                    f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_BillPayment]
                    SET Porter_Status='Failed',
                        Failure_Reason=?,
                        Retry_Count = ISNULL(Retry_Count, 0) + 1
                    WHERE Source_Id=?
                    """,
                    (reason, sid)
                )

session = requests.Session()

# -------------------- Helpers (defined once; safe to re-import) --------------------
if "_fast_loads" not in globals():
    try:
        import orjson as _orjson
        def _fast_loads(s):
            if isinstance(s, dict): return s
            if isinstance(s, (bytes, bytearray)): return _orjson.loads(s)
            if isinstance(s, str): return _orjson.loads(s.encode("utf-8"))
            return s
        def _fast_dumps(obj):  # pretty
            return _orjson.dumps(obj, option=_orjson.OPT_INDENT_2).decode("utf-8")
    except Exception:
        import json as _json
        def _fast_loads(s):
            if isinstance(s, dict): return s
            if isinstance(s, (bytes, bytearray)):
                try: return _json.loads(s.decode("utf-8"))
                except Exception: return _json.loads(s)
            if isinstance(s, str): return _json.loads(s)
            return s
        def _fast_dumps(obj):
            return _json.dumps(obj, indent=2)

if "_derive_batch_url" not in globals():
    def _derive_batch_url(entity_url: str, entity_name: str) -> str:
        qpos  = entity_url.find("?")
        query = entity_url[qpos:] if qpos != -1 else ""
        path  = entity_url[:qpos] if qpos != -1 else entity_url
        seg   = f"/{entity_name}".lower()
        lower = path.lower()
        if lower.endswith(seg):
            path = path[: -len(seg)]
        else:
            parts = path.rsplit("/", 1)
            if len(parts) == 2 and parts[1]:
                path = parts[0]
        marker = "/v3/company/"
        i = path.lower().find(marker)
        if i != -1:
            j = path.find("/", i + len(marker))
            if j != -1:
                path = path[:j]
        return f"{path}/batch{query}"

# Best-effort vectorized DB update; falls back to per-row if `executemany` missing
def _apply_batch_updates_billpayment(successes, failures):
    try:
        if successes:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_BillPayment] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                [(qid, sid) for qid, sid in successes]
            )
        if failures:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_BillPayment] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                [(reason, sid) for reason, sid in failures]
            )
    except NameError:
        for qid, sid in successes or []:
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_BillPayment] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                (qid, sid)
            )
        for reason, sid in failures or []:
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_BillPayment] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                (reason, sid)
            )

def _post_batch_billpayments(eligible_batch, url, headers, timeout=40, post_batch_limit=20, max_manual_retries=1):
    """
    Single-threaded batch poster for BillPayments:
    - Pre-decodes Payload_JSON once per row (orjson/json).
    - Uses QBO /batch endpoint.
    - Handles 401/403 (token refresh once) and basic manual retry.
    Returns:
      successes: list[(qid, sid)]
      failures:  list[(reason, sid)]
    """
    successes, failures = [], []
    if eligible_batch is None or eligible_batch.empty:
        return successes, failures

    # Filter & parse payloads
    items = []
    for _, r in eligible_batch.iterrows():
        sid = r["Source_Id"]
        if r.get("Porter_Status") == "Success": continue
        if int(r.get("Retry_Count") or 0) >= 3: continue
        pj = r.get("Payload_JSON")
        if not pj:
            failures.append(("Missing Payload_JSON", sid))
            continue
        try:
            payload = _fast_loads(pj)
        except Exception as e:
            failures.append((f"Bad JSON: {e}", sid))
            continue
        items.append((sid, payload))
    if not items:
        return successes, failures

    batch_url = _derive_batch_url(url, "BillPayment")

    # Chunked /batch posting
    for i in range(0, len(items), post_batch_limit):
        auto_refresh_token_if_needed()
        chunk = items[i:i + post_batch_limit]
        if not chunk:
            continue

        def _do_post(_headers, _items):
            body = {
                "BatchItemRequest": [
                    {"bId": str(sid), "operation": "create", "BillPayment": payload}
                    for (sid, payload) in _items
                ]
            }
            return session.post(batch_url, headers=_headers, json=body, timeout=timeout)

        attempted_refresh = False
        for attempt in range(max_manual_retries + 1):
            try:
                resp = _do_post(headers, chunk)
                sc = resp.status_code

                if sc == 200:
                    data = resp.json()
                    arr  = data.get("BatchItemResponse", []) or []
                    seen = set()

                    for it in arr:
                        bid = it.get("bId")
                        seen.add(bid)
                        ent = it.get("BillPayment")
                        if ent and "Id" in ent:
                            qid = ent["Id"]
                            successes.append((qid, bid))
                            logger.info(f"✅ BillPayment {bid} → QBO {qid}")
                        else:
                            fault = it.get("Fault") or {}
                            errs  = fault.get("Error") or []
                            if errs:
                                msg = (errs[0].get("Message") or "")
                                det = (errs[0].get("Detail")  or "")
                                reason = (msg + " | " + det).strip()[:1000]
                            else:
                                reason = "Unknown batch failure"
                            failures.append((reason, bid))
                            logger.error(f"❌ BillPayment {bid} failed: {reason}")

                    # mark any missing bIds as failed
                    for sid, _ in chunk:
                        if str(sid) not in seen:
                            failures.append(("No response for bId", sid))
                            logger.error(f"❌ No response for BillPayment {sid}")
                    break

                elif sc in (401, 403) and not attempted_refresh:
                    logger.warning(f"🔐 {sc} on BillPayment batch ({len(chunk)} items); refreshing token and retrying once...")
                    try:
                        auto_refresh_token_if_needed()
                    except Exception:
                        pass
                    url, headers = get_qbo_auth()
                    batch_url = _derive_batch_url(url, "BillPayment")
                    attempted_refresh = True
                    continue

                else:
                    reason = (resp.text or f"HTTP {sc}")[:1000]
                    for sid, _ in chunk:
                        failures.append((reason, sid))
                    logger.error(f"❌ BillPayment batch failed ({len(chunk)} items): {reason}")
                    break

            except Exception as e:
                reason = f"Batch exception: {e}"
                for sid, _ in chunk:
                    failures.append((reason, sid))
                logger.exception(f"❌ Exception during BillPayment batch POST ({len(chunk)} items)")
                break

    return successes, failures

# ----------------------------- UPDATED FUNCTION 1/2 --------------------------------
def post_billpayment(row):
    """
    Single-record fallback; prefer batch path in resume_or_post_billpayments().
    """
    sid = row["Source_Id"]
    if row.get("Porter_Status") == "Success": return
    if int(row.get("Retry_Count") or 0) >= 3: return

    pj = row.get("Payload_JSON")
    if not pj:
        logger.warning(f"⚠️ BillPayment {sid} skipped — missing Payload_JSON")
        return

    try:
        payload = _fast_loads(pj)
    except Exception as e:
        reason = f"Invalid JSON: {e}"
        sql.run_query(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_BillPayment] "
            f"SET Porter_Status='Failed', Retry_Count=ISNULL(Retry_Count,0)+1, Failure_Reason=? "
            f"WHERE Source_Id=?",
            (reason, sid)
        )
        return

    url, headers = get_qbo_auth()
    try:
        resp = session.post(url, headers=headers, json=payload, timeout=20)
        if resp.status_code == 200:
            qid = (resp.json().get("BillPayment") or {}).get("Id")
            if qid:
                sql.run_query(
                    f"UPDATE [{MAPPING_SCHEMA}].[Map_BillPayment] "
                    f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                    f"WHERE Source_Id=?",
                    (qid, sid)
                )
                logger.info(f"✅ BillPayment {sid} → QBO {qid}")
                return
            reason = "No Id in response"
        elif resp.status_code in (401, 403):
            # one-shot refresh & retry
            try:
                auto_refresh_token_if_needed()
            except Exception:
                pass
            url, headers = get_qbo_auth()
            resp2 = session.post(url, headers=headers, json=payload, timeout=20)
            if resp2.status_code == 200:
                qid2 = (resp2.json().get("BillPayment") or {}).get("Id")
                if qid2:
                    sql.run_query(
                        f"UPDATE [{MAPPING_SCHEMA}].[Map_BillPayment] "
                        f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                        f"WHERE Source_Id=?",
                        (qid2, sid)
                    )
                    logger.info(f"✅ BillPayment {sid} → QBO {qid2}")
                    return
                reason = "No Id in response (after refresh)"
            else:
                reason = (resp2.text or f"HTTP {resp2.status_code}")[:500]
        else:
            reason = (resp.text or f"HTTP {resp.status_code}")[:500]

        sql.run_query(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_BillPayment] "
            f"SET Porter_Status='Failed', Retry_Count=ISNULL(Retry_Count,0)+1, Failure_Reason=? "
            f"WHERE Source_Id=?",
            (reason, sid)
        )
    except Exception as e:
        sql.run_query(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_BillPayment] "
            f"SET Porter_Status='Failed', Retry_Count=ISNULL(Retry_Count,0)+1, Failure_Reason=? "
            f"WHERE Source_Id=?",
            (str(e), sid)
        )

def resume_or_post_billpayments(BILLPAYMENT_DATE_FROM,BILLPAYMENT_DATE_TO):
    """
    Resumes/initiates BillPayment posting using QBO /batch API (single-threaded).
    - Rebuilds Map_BillPayment if missing or mismatched.
    - Generates missing payloads.
    - Posts only eligible (Ready/Failed), Retry_Count < 3, has Payload_JSON.
    """
    if not sql.table_exists("Map_BillPayment", MAPPING_SCHEMA):
        logger.warning("🛠️ Map_BillPayment table not found. Initializing from scratch...")
        ensure_mapping_table(BILLPAYMENT_DATE_FROM,BILLPAYMENT_DATE_TO)

    if 'ENABLE_GLOBAL_BILLPAYMENT_DOCNUMBER_DEDUP' in globals() and ENABLE_GLOBAL_BILLPAYMENT_DOCNUMBER_DEDUP:
        logger.info("🔁 Applying global duplicate DocNumber strategy to Map_BillPayment...")
        apply_duplicate_docnumber_strategy()
        try: sql.clear_cache()
        except Exception: pass

    source_count = sql.fetch_single_value(f"SELECT COUNT(*) FROM [{SOURCE_SCHEMA}].[BillPayment]", ())
    mapped_count = sql.fetch_single_value(f"SELECT COUNT(*) FROM [{MAPPING_SCHEMA}].[Map_BillPayment]", ())

    if (source_count or 0) != (mapped_count or 0):
        logger.warning(f"⚠️ Source vs Mapping count mismatch: {source_count} ≠ {mapped_count}. Regenerating mapping...")
        ensure_mapping_table(BILLPAYMENT_DATE_FROM,BILLPAYMENT_DATE_TO)
        if 'ENABLE_GLOBAL_BILLPAYMENT_DOCNUMBER_DEDUP' in globals() and ENABLE_GLOBAL_BILLPAYMENT_DOCNUMBER_DEDUP:
            logger.info("🔁 Re-applying duplicate DocNumber strategy after rebuild...")
            apply_duplicate_docnumber_strategy()
            try: sql.clear_cache()
            except Exception: pass

    # Generate missing payloads only
    missing_payloads = sql.fetch_single_value(
        f"""
        SELECT COUNT(*) FROM [{MAPPING_SCHEMA}].[Map_BillPayment]
        WHERE (Payload_JSON IS NULL OR Payload_JSON = '')
          AND Porter_Status IN ('Ready', 'Failed')
          AND (Retry_Count IS NULL OR Retry_Count < 3)
        """,
        ()
    ) or 0
    if missing_payloads > 0:
        logger.info(f"🔄 {missing_payloads} BillPayments missing JSON payloads. Generating...")
        generate_billpayment_payloads_in_batches(batch_size=1000)

    # Eligible rows
    eligible = sql.fetch_dataframe(
        f"""
        SELECT * FROM [{MAPPING_SCHEMA}].[Map_BillPayment]
        WHERE Porter_Status IN ('Ready', 'Failed')
          AND (Retry_Count IS NULL OR Retry_Count < 3)
          AND Payload_JSON IS NOT NULL AND Payload_JSON <> ''
        """
    )
    if eligible.empty:
        logger.info("🎉 All BillPayments already processed or skipped.")
        return

    url, headers = get_qbo_auth()
    select_batch_size = 300   # DB slice size
    post_batch_limit  = 20    # Items per QBO /batch call (≤30). Set to 3 if you want exactly three per call.
    timeout           = 40

    total = len(eligible)
    logger.info(f"🚚 Posting {total} eligible BillPayments via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        slice_df = eligible.iloc[i:i + select_batch_size]
        successes, failures = _post_batch_billpayments(
            slice_df, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates_billpayment(successes, failures)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done*100//total}%)")

    # Optional final pass on failed-without-id using single-record poster
    failed_df = sql.fetch_table("Map_BillPayment", MAPPING_SCHEMA)
    failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
    if not failed_records.empty:
        logger.info(f"🔁 Reprocessing {len(failed_records)} failed BillPayments (single-record fallback)...")
        for _, row in failed_records.iterrows():
            post_billpayment(row)

    logger.info("✅ BillPayment posting completed.")

# ----------------------------- SMART MIGRATOR LOGIC --------------------------------
def smart_billpayment_migration(BILLPAYMENT_DATE_FROM,BILLPAYMENT_DATE_TO):
    """
    One-call smart migration for BillPayments: mapping, payload, and batch post.
    Checks all prerequisites and calls resume_or_post_billpayments if any are missing.
    """
    needs_mapping = not sql.table_exists("Map_BillPayment", MAPPING_SCHEMA)
    mapped = sql.fetch_table("Map_BillPayment", MAPPING_SCHEMA) if not needs_mapping else None
    source = sql.fetch_table("BillPayment", SOURCE_SCHEMA)
    needs_remap = not needs_mapping and (len(mapped) != len(source))
    needs_dedup = (
        not needs_mapping and
        'Duplicate_DocNumber' in mapped.columns and
        mapped['Duplicate_DocNumber'].isnull().any()
    )
    needs_payload = (
        not needs_mapping and
        ((mapped["Payload_JSON"].isnull()) |
         (mapped["Payload_JSON"] == "") |
         (mapped["Payload_JSON"] == "null")).any()
    )

    if needs_mapping or needs_remap or needs_dedup or needs_payload:
        logger.info("🔄 Detected pending mapping/docnumber/payload work; running resume_or_post_billpayments...")
        resume_or_post_billpayments(BILLPAYMENT_DATE_FROM,BILLPAYMENT_DATE_TO)
        return

    # All prerequisites met, post eligible records directly
    eligible = mapped[(mapped["Porter_Status"].isin(["Ready", "Failed"]))
                     & (mapped["Payload_JSON"].notna())
                     & (mapped["Payload_JSON"] != "")
                     & (mapped["Payload_JSON"] != "null")].reset_index(drop=True)
    if eligible.empty:
        logger.info("🎉 All BillPayments already processed or skipped.")
        return

    url, headers = get_qbo_auth()
    select_batch_size = 300
    post_batch_limit  = 5
    timeout           = 40

    total = len(eligible)
    logger.info(f"📤 Posting {total} BillPayment record(s) via QBO Batch API (limit {post_batch_limit}/call)...")
    for i in range(0, total, select_batch_size):
        batch = eligible.iloc[i:i + select_batch_size]
        successes, failures = _post_batch_billpayments(
            batch, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates_billpayment(successes, failures)
        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")
    logger.info("🏁 BillPayment posting completed.")

