"""
Sequence : 21
Module: Payment_migrator.py
Author: Dixit Prajapati
Created: 2025-09-16
Description: Handles migration of Payment records from QBO to QBO.
Production : Ready
Development : Require when necessary
Phase : 02 - Multi User 
"""

import os, json, requests, pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import global_logger as logger
from config.mapping.payment_mapping import PAYMENT_HEADER_MAPPING, PAYMENT_LINE_MAPPING
from utils.apply_duplicate_docnumber import apply_duplicate_docnumber_strategy_dynamic
from utils.token_refresher import get_qbo_context_migration
from storage.sqlserver.sql import executemany

ENABLE_GLOBAL_REFUNDRECEIPT_DOCNUMBER_DEDUP=True

load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
ENABLE_GLOBAL_PAYMENT_DOCNUMBER_DEDUP=True

def apply_duplicate_docnumber_strategy():
    apply_duplicate_docnumber_strategy_dynamic(
    target_table="Map_Payment",
    docnumber_column= "PaymentRefNum",
    duplicate_column= "Duplicate_PaymentRefNum",
    schema=MAPPING_SCHEMA,
    check_against_tables=["Map_Bill","Map_Invoice","Map_VendorCredit","Map_JournalEntry","Map_Deposit","Map_Purchase","Map_Salesreceipt","Map_Refundreceipt"])

def get_qbo_auth():
    """
    Constructs QBO API base URL and headers for authentication.
    Returns:
        Tuple[str, Dict[str, str]]: API endpoint and headers.
    """
    env = os.getenv("QBO_ENVIRONMENT", "sandbox")
    base = "https://sandbox-quickbooks.api.intuit.com" if env == "sandbox" else "https://quickbooks.api.intuit.com"
    ctx = get_qbo_context_migration()
    realm = ctx["REALM_ID"]
    return f"{base}/v3/company/{realm}/payment", {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

def invoice_exists_in_qbo(invoice_id):
    """
    Checks if a given invoice ID exists in QBO and retrieves its metadata.
    Args:
        invoice_id (str): Target QBO Invoice ID.
    Returns:
        dict or None: Invoice metadata with TxnId, DocNumber, Balance; None if not found.
    """
    base_url, headers = get_qbo_auth()
    invoice_url = base_url.replace("/payment", f"/invoice/{invoice_id}")
    resp = requests.get(invoice_url, headers=headers)
    logger.debug(f"Invoice check [{invoice_id}] → status {resp.status_code}, body: {resp.text[:200]}")
    if resp.status_code != 200:
        return None
    try:
        data = resp.json()["Invoice"]
        return {
            "TxnId": str(invoice_id),
            "DocNumber": data.get("DocNumber"),
            "Balance": str(data.get("Balance"))
        }
    except Exception as e:
        logger.error(f"Failed to parse invoice response for {invoice_id}: {e}")
        return None

def safe_float(val):
    """
    Safely converts a value to float.
    Args:
        val (Any): Value to convert.
    Returns:
        float: Float value or 0.0 if invalid.
    """
    try: return float(val)
    except: return 0.0

def ensure_mapping_table(PAYMENT_DATE_FROM,PAYMENT_DATE_TO):
    """
    Optimized Payment → Map_Payment initializer (bulk lookups, chunked reads).
    - Includes all Payment headers (even without lines).
    - Preserves original mapping logic; only performance improved.
    """
    DATE_FROM = PAYMENT_DATE_FROM
    DATE_TO = PAYMENT_DATE_TO

    # --- Helper: safely quote SQL Server identifiers that may contain ']' (e.g., LinkedTxn[0].TxnId)
    def _qident(colname: str) -> str:
        return f"[{str(colname).replace(']', ']]')}]"

    # -------- 1) Pull headers with optional date filter (done in SQL) --------
    params, where = [], []
    if DATE_FROM:
        where.append("TxnDate >= ?")
        params.append(DATE_FROM)
    if DATE_TO:
        where.append("TxnDate <= ?")
        params.append(DATE_TO)

    sql_headers = f"SELECT * FROM [{SOURCE_SCHEMA}].[Payment]"
    if where:
        sql_headers += " WHERE " + " AND ".join(where)

    header_df = sql.fetch_table_with_params(sql_headers, tuple(params))
    if header_df.empty:
        logger.warning("⚠️ No payment headers found in given range. Skipping mapping.")
        return

    # -------- 2) Base dataframe & core columns --------
    df = header_df.copy()
    df["Source_Id"]      = df["Id"]
    df["Target_Id"]      = None
    df["Porter_Status"]  = "Ready"
    df["Retry_Count"]    = 0
    df["Failure_Reason"] = None
    df["Payload_JSON"]   = None

    # -------- 3) Bulk map master values (Customer, Account) --------
    # Load once as dicts to avoid per-row SQL
    def _to_dict(tbl):
        t = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[{tbl}]", tuple())
        return dict(zip(t["Source_Id"], t["Target_Id"])) if not t.empty else {}

    customer_dict = _to_dict("Map_Customer")
    account_dict  = _to_dict("Map_Account")

    cust_col = PAYMENT_HEADER_MAPPING.get("CustomerRef.value")
    acct_col = PAYMENT_HEADER_MAPPING.get("DepositToAccountRef.value")

    if cust_col in df.columns:
        df["Mapped_Customer"] = df[cust_col].map(lambda x: customer_dict.get(x) if pd.notna(x) else None)
    else:
        df["Mapped_Customer"] = None

    if acct_col in df.columns:
        df["Mapped_Account"] = df[acct_col].map(lambda x: account_dict.get(x) if pd.notna(x) and str(x).strip() != "" else None)
    else:
        df["Mapped_Account"] = None

    # -------- 4) Prepare LinkedTxn resolvers (dicts per TxnType) --------
    type_to_map_table = {
        "Invoice":          "Map_Invoice",
        "CreditMemo":       "Map_CreditMemo",
        "JournalEntry":     "Map_JournalEntry",
        "Deposit":          "Map_Deposit",
        "Bill":             "Map_Bill",
        "Check":            "Map_Purchase",
        "Expense":          "Map_Purchase",
        "CreditCardCredit": "Map_Purchase",
    }
    txn_type_override = {
        "Expense":          "Cash",
        "Check":            "Check",
        "CreditCardCredit": "CreditCardCredit",
    }

    # Bulk-load mapping tables once
    needed_tables = set(type_to_map_table.values())
    map_dicts = {}
    for tname in needed_tables:
        try:
            tdf = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[{tname}]", tuple())
            map_dicts[tname] = dict(zip(tdf["Source_Id"], tdf["Target_Id"])) if not tdf.empty else {}
        except Exception:
            map_dicts[tname] = {}

    # -------- 5) Bulk load all Payment_Line rows for selected parents (chunked IN) --------
    parent_ids = df["Id"].tolist()
    linked_json_by_parent = {}

    if parent_ids:
        # Safely quote the mapped column names (they may look like LinkedTxn[0].TxnId)
        txnid_col_src   = PAYMENT_LINE_MAPPING["TxnId"]
        txntype_col_src = PAYMENT_LINE_MAPPING["TxnType"]
        q_txnid   = _qident(txnid_col_src)
        q_txntype = _qident(txntype_col_src)

        chunk_size = 2000
        all_parts = []
        for i in range(0, len(parent_ids), chunk_size):
            chunk = parent_ids[i:i + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            q = (
                f"SELECT Parent_Id, {q_txnid} AS TxnIdCol, "
                f"COALESCE({q_txntype}, 'Invoice') AS TxnTypeCol "
                f"FROM [{SOURCE_SCHEMA}].[Payment_Line] "
                f"WHERE Parent_Id IN ({placeholders})"
            )
            part = sql.fetch_table_with_params(q, tuple(chunk))
            if not part.empty:
                all_parts.append(part)

        line_df = (
            pd.concat(all_parts, ignore_index=True)
            if all_parts else
            pd.DataFrame(columns=["Parent_Id", "TxnIdCol", "TxnTypeCol"])
        )

        if not line_df.empty:
            # Group once; compute using dict lookups (no per-line SQL)
            for pid, g in line_df.groupby("Parent_Id", sort=False):
                arr = g[["TxnIdCol", "TxnTypeCol"]].to_numpy()
                linked = []
                for src_txn_id, orig_txn_type in arr:
                    if pd.isna(src_txn_id) or pd.isna(orig_txn_type):
                        continue
                    otype = str(orig_txn_type)
                    map_tbl = type_to_map_table.get(otype)
                    if not map_tbl:
                        continue
                    # Try exact and string-key lookup
                    tgt = map_dicts.get(map_tbl, {}).get(src_txn_id) or map_dicts.get(map_tbl, {}).get(str(src_txn_id))
                    if not tgt:
                        continue
                    resolved_type = txn_type_override.get(otype, otype)
                    linked.append({"TxnId": str(tgt), "TxnType": resolved_type})
                linked_json_by_parent[pid] = json.dumps(linked)
        else:
            # No lines at all → empty arrays
            linked_json_by_parent = {pid: "[]" for pid in parent_ids}

    # Map back (headers with no lines get "[]")
    df["LinkedTxns_JSON"] = df["Id"].map(lambda pid: linked_json_by_parent.get(pid, "[]"))

    # -------- 6) Replace existing table contents and enforce column --------
    if sql.table_exists("Map_Payment", MAPPING_SCHEMA):
        sql.run_query(f"DELETE FROM [{MAPPING_SCHEMA}].[Map_Payment]")

    sql.insert_dataframe(df, "Map_Payment", MAPPING_SCHEMA)

    sql.ensure_column_exists(
        table="Map_Payment",
        column="LinkedTxns_JSON",
        col_type="NVARCHAR(MAX)",
        schema=MAPPING_SCHEMA
    )

    logger.info(f"✅ Initialized Map_Payment with {len(df)} headers, including advances (no lines).")

def get_payment_lines(payment_id):
    """
    Retrieves line-level payment data for a given Payment ID.
    Args:
        payment_id (int): Source Payment ID.
    Returns:
        pd.DataFrame: Line-level records.
    """
    return sql.fetch_table_with_params(f"SELECT * FROM {SOURCE_SCHEMA}.Payment_Line WHERE Parent_Id=?", (payment_id,))

# ---------- Optimized, mapping-preserving ----------
# Notes:
# - Avoids per-line SQL by caching map tables in-memory (once per process).
# - Vectorizes zero-amount checks.
# - Uses fast column access (.iat) inside tight loops.
# - Keeps all original fields/logic; only performance improved.

# Module-level cache for linked Txn map tables (loaded on first use)
_PAYMENT_LINK_MAPS = None

def build_payload(row, lines):
    """
    Constructs a QBO-compatible JSON payload for the given payment.
    Skips records where both TotalAmt and all Line amounts are zero.
    """
    global _PAYMENT_LINK_MAPS

    issues = []
    if not row.get("Mapped_Customer"):
        issues.append("Missing Customer mapping")

    # Header values
    currency_val = row.get(PAYMENT_HEADER_MAPPING.get("CurrencyRef.value"), "USD") or "USD"
    total_amt    = safe_float(row.get(PAYMENT_HEADER_MAPPING.get("TotalAmt")))

    txn_date     = row.get(PAYMENT_HEADER_MAPPING.get("TxnDate"))

    # Fast zero-check for lines
    amount_col = PAYMENT_LINE_MAPPING.get("Amount")
    if lines is None or lines.empty or amount_col not in lines.columns:
        all_line_amounts_zero = True
    else:
        amt_series = pd.to_numeric(lines[amount_col], errors="coerce").fillna(0.0)
        all_line_amounts_zero = (amt_series == 0).all()

    # Skip the entire payload if both TotalAmt and all line amounts are 0
    if total_amt == 0 and all_line_amounts_zero:
        issues.append("TotalAmt and all Line amounts are zero")

    if issues:
        logger.warning(f"⛔ Payment {row['Source_Id']} skipped due to: {', '.join(issues)}")
        return None

    # Build payload (preserve original fields)
    payload = {
        "CustomerRef": {"value": str(row["Mapped_Customer"])},
        "TotalAmt": total_amt,
        "TxnDate": txn_date,
        # "ProcessPayment": True,
        "CurrencyRef": {"value": currency_val},
        "Line": []
    }

    # Keep DepartmentRef logic as in original (variable may be defined by caller)
    _DepartmentRef_val = globals().get("DepartmentRef", None)
    if _DepartmentRef_val is not None:
        payload["DepartmentRef"] = {"value": _DepartmentRef_val}

    if pd.notna(row.get("Mapped_Account")):
        payload["DepositToAccountRef"] = {"value": str(row["Mapped_Account"])}

    # Deduped PaymentRefNum (preserve behavior)
    dedup = row.get("Duplicate_PaymentRefNum")
    original = row.get("PaymentRefNum")
    payment_ref = (str(dedup).strip() if (pd.notna(dedup) and str(dedup).strip() != "") else str(original or "").strip())
    if payment_ref:
        payload["PaymentRefNum"] = payment_ref

    if pd.notna(row.get("PrivateNote")):
        payload["PrivateNote"] = str(row["PrivateNote"])

    # Mapping: TxnType -> map table (unchanged)
    type_to_map_table = {
        "Invoice": "Map_Invoice",
        "CreditMemo": "Map_CreditMemo",
        "JournalEntry": "Map_JournalEntry",
        "Deposit": "Map_Deposit",
        "Bill": "Map_Bill",
        "Check": "Map_Purchase",
        "Expense": "Map_Purchase",
        "CreditCardCredit": "Map_Purchase"
    }
    txn_type_override = {
        "Expense": "Cash",
        "Check": "Check",
        "CreditCardCredit": "CreditCardCredit"
    }

    # Load all mapping tables once per process (dictionary lookups are O(1))
    if _PAYMENT_LINK_MAPS is None:
        _PAYMENT_LINK_MAPS = {}
        needed = set(type_to_map_table.values())
        for tname in needed:
            try:
                tdf = sql.fetch_table_with_params(
                    f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[{tname}]",
                    tuple()
                )
                _PAYMENT_LINK_MAPS[tname] = dict(zip(tdf["Source_Id"], tdf["Target_Id"])) if not tdf.empty else {}
            except Exception:
                _PAYMENT_LINK_MAPS[tname] = {}

    # Fast column index access
    if lines is not None and not lines.empty:
        colpos = {c: i for i, c in enumerate(lines.columns)}
        arr = lines.to_numpy()

        c_txn_id  = colpos.get(PAYMENT_LINE_MAPPING.get("TxnId"))
        c_txn_typ = colpos.get(PAYMENT_LINE_MAPPING.get("TxnType"))
        c_amt     = colpos.get(PAYMENT_LINE_MAPPING.get("Amount"))

        n = arr.shape[0]
        for i in range(n):
            src_txn_id   = arr[i, c_txn_id]  if c_txn_id is not None else None
            orig_txn_type= arr[i, c_txn_typ] if c_txn_typ is not None else "Invoice"
            line_amt     = safe_float(arr[i, c_amt]) if c_amt is not None else 0.0

            if pd.isna(src_txn_id) or pd.isna(orig_txn_type) or line_amt == 0:
                continue

            map_table = type_to_map_table.get(orig_txn_type)
            if not map_table:
                logger.warning(f"⚠️ Unsupported TxnType '{orig_txn_type}' in Payment_Line for Source_Id {row['Source_Id']}")
                continue

            mdict = _PAYMENT_LINK_MAPS.get(map_table, {})
            target_id = mdict.get(src_txn_id) or mdict.get(str(src_txn_id))
            if not target_id:
                logger.warning(f"⚠️ No mapped Target_Id for TxnId={src_txn_id} in {map_table}")
                continue

            payload_txn_type = txn_type_override.get(orig_txn_type, orig_txn_type)
            payload["Line"].append({
                "Amount": line_amt,
                "LinkedTxn": [{
                    "TxnId": str(target_id),
                    "TxnType": payload_txn_type
                }]
            })

    return payload

def generate_payment_payloads_in_batches(batch_size=500):
    """
    Generates QBO-compatible JSON payloads for Payment records in batches.
    Stores them in Payload_JSON. Skips Retry_Count >= 3.
    Optimizations:
      - Bulk-fetch lines for each batch (single IN query).
      - Bulk-fetch dedup PaymentRefNum for batch IDs.
      - Use executemany updates to reduce round-trips.
    """
    # Fetch eligible header rows
    rows = sql.fetch_dataframe(f"""
        SELECT * FROM [{MAPPING_SCHEMA}].[Map_Payment]
        WHERE Porter_Status IN ('Ready', 'Failed')
          AND (Payload_JSON IS NULL OR Payload_JSON = '')
          AND (Retry_Count IS NULL OR Retry_Count < 3)
    """)
    total = len(rows)
    if total == 0:
        logger.info("✅ No pending payments needing payload generation.")
        return

    logger.info(f"⚙️ Generating payloads for {total} payments...")

    # Use orjson if available for faster dumps
    try:
        import orjson
        def _dumps(obj): return orjson.dumps(obj, option=orjson.OPT_INDENT_2).decode("utf-8")
    except Exception:
        import json as _json
        def _dumps(obj): return _json.dumps(obj, indent=2)

    processed = 0
    for start in range(0, total, batch_size):
        batch = rows.iloc[start:start + batch_size]

        # ---------- Bulk fetch dedup PaymentRefNum for the batch ----------
        sids = batch["Source_Id"].tolist()
        dedup_map = {}
        if sids:
            placeholders = ",".join(["?"] * len(sids))
            q = (
                f"SELECT Source_Id, Duplicate_PaymentRefNum "
                f"FROM [{MAPPING_SCHEMA}].[Map_Payment] "
                f"WHERE Source_Id IN ({placeholders})"
            )
            ddf = sql.fetch_table_with_params(q, tuple(sids))
            if not ddf.empty:
                dedup_map = dict(zip(ddf["Source_Id"], ddf["Duplicate_PaymentRefNum"]))

        # ---------- Bulk fetch all lines for the batch ----------
        line_map = {}
        if sids:
            # Pull only the lines we need for these parents
            placeholders = ",".join(["?"] * len(sids))
            q = (
                f"SELECT * FROM [{SOURCE_SCHEMA}].[Payment_Line] "
                f"WHERE Parent_Id IN ({placeholders})"
            )
            ldf = sql.fetch_table_with_params(q, tuple(sids))
            if not ldf.empty:
                # groupby once
                for pid, g in ldf.groupby("Parent_Id"):
                    line_map[pid] = g

        # ---------- Build payloads & collect updates ----------
        success_updates = []  # (payload_json, sid)
        fail_updates    = []  # (reason, sid)

        for _, row in batch.iterrows():
            try:
                sid = row["Source_Id"]

                # Patch deduped PaymentRefNum for this row (if present)
                ded = dedup_map.get(sid)
                if ded is not None:
                    row["Duplicate_PaymentRefNum"] = ded

                # Get lines for this parent from the map; default empty DataFrame
                lines = line_map.get(sid, pd.DataFrame())

                payload = build_payload(row, lines)

                if payload:
                    payload_json = _dumps(payload)
                    success_updates.append((payload_json, sid))
                    processed += 1
                    if processed % 500 == 0:
                        logger.info(f"✅ Successfully generated JSON payloads for {processed} payments.")
                else:
                    fail_updates.append(("Invalid or empty payload", sid))
                    logger.warning(f"❌ Skipped payment {sid} due to empty or invalid payload.")
            except Exception as e:
                fail_updates.append((str(e), row.get("Source_Id")))
                logger.exception(f"❌ Exception while generating payload for Payment {row.get('Source_Id')}")

        # ---------- Apply updates in bulk (executemany if available) ----------
        try:
            executemany(
                f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_Payment]
                SET Payload_JSON=?, Porter_Status='Ready'
                WHERE Source_Id=?
                """,
                success_updates
            )
        except NameError:
            # fallback per-row
            for payload_json, sid in success_updates:
                sql.run_query(
                    f"UPDATE [{MAPPING_SCHEMA}].[Map_Payment] "
                    f"SET Payload_JSON=?, Porter_Status='Ready' "
                    f"WHERE Source_Id=?",
                    (payload_json, sid)
                )

        try:
            executemany(
                f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_Payment]
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
                    UPDATE [{MAPPING_SCHEMA}].[Map_Payment]
                    SET Porter_Status='Failed',
                        Failure_Reason=?,
                        Retry_Count = ISNULL(Retry_Count, 0) + 1
                    WHERE Source_Id=?
                    """,
                    (reason, sid)
                )

session = requests.Session()

# ======================= Optimized Payment Posting (Batch) =======================

# helpers (defined once safely)
if "_fast_loads" not in globals():
    try:
        import orjson as _orjson
        def _fast_loads(s):
            if isinstance(s, dict): return s
            if isinstance(s, (bytes, bytearray)): return _orjson.loads(s)
            if isinstance(s, str): return _orjson.loads(s.encode("utf-8"))
            return s
        def _fast_dumps(obj):  # pretty, like json.dumps(..., indent=2)
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

def _post_batch_payments(eligible_batch, url, headers, timeout=40, post_batch_limit=20, max_manual_retries=1):
    """
    Batch POST Payments via QBO /batch. Preserves special header-only retry:
    If an item fails with Business Validation 'Amount Received ... can't be less ...',
    it will be retried (once) with the same payload but without the 'Line' array.
    Returns:
      successes:               list[(qid, sid)]
      failures:                list[(reason, sid)]
      header_only_successes:   list[(qid, sid, new_payload_json)]
    """
    successes, failures, header_only_successes = [], [], []
    if eligible_batch is None or eligible_batch.empty:
        return successes, failures, header_only_successes

    # Build worklist & preload/patch dedup PaymentRefNum for the slice
    work = []
    sids = []
    for _, r in eligible_batch.iterrows():
        sid = r["Source_Id"]
        if r.get("Porter_Status") == "Success":         continue
        if int(r.get("Retry_Count") or 0) >= 3:         continue
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
        sids.append(sid)

    if not work:
        return successes, failures, header_only_successes

    # Optional: refresh dedup PaymentRefNum for the batch (same logic as single post)
    dedup_map = {}
    try:
        if sids:
            placeholders = ",".join(["?"] * len(sids))
            q = (
                f"SELECT Source_Id, Duplicate_PaymentRefNum "
                f"FROM [{MAPPING_SCHEMA}].[Map_Payment] "
                f"WHERE Source_Id IN ({placeholders})"
            )
            ddf = sql.fetch_table_with_params(q, tuple(sids))
            if not ddf.empty:
                dedup_map = dict(zip(ddf["Source_Id"], ddf["Duplicate_PaymentRefNum"]))
    except Exception:
        pass

    # Patch payloads if dedup present
    patched_work = []
    for sid, payload in work:
        dedup_val = dedup_map.get(sid)
        if dedup_val is not None and str(dedup_val).strip() != "":
            payload = dict(payload)  # shallow copy
            payload["PaymentRefNum"] = str(dedup_val).strip()
        patched_work.append((sid, payload))
    work = patched_work

    batch_url = _derive_batch_url(url, "Payment")

    # Core post loop in chunks
    idx = 0
    while idx < len(work):
        auto_refresh_token_if_needed()
        chunk = work[idx:idx + post_batch_limit]
        idx   += post_batch_limit
        if not chunk:
            continue

        def _do_post(_headers, _items):
            body = {
                "BatchItemRequest": [
                    {"bId": str(sid), "operation": "create", "Payment": payload}
                    for (sid, payload) in _items
                ]
            }
            return session.post(batch_url, headers=_headers, json=body, timeout=timeout)

        attempted_refresh = False
        header_only_candidates = []  # (sid, payload_no_lines)

        for attempt in range(max_manual_retries + 1):
            try:
                resp = _do_post(headers, chunk)
                sc = resp.status_code

                if sc == 200:
                    rj = resp.json()
                    items = rj.get("BatchItemResponse", []) or []
                    seen  = set()

                    for item in items:
                        bid = item.get("bId")
                        seen.add(bid)
                        ent = item.get("Payment")
                        if ent and "Id" in ent:
                            qid = ent["Id"]
                            successes.append((qid, bid))
                            logger.info(f"✅ Payment {bid} → QBO {qid}")
                            continue

                        # analyze fault for potential header-only retry
                        fault = item.get("Fault") or {}
                        errs = fault.get("Error") or []
                        reason = "Unknown batch failure"
                        want_header_only_retry = False
                        if errs:
                            msg = ( errs[0].get("Message") or "" )
                            det = ( errs[0].get("Detail")  or "" )
                            reason = (msg + " | " + det).strip()[:1000]
                            if "Business Validation Error" in (msg + det) and \
                               "Amount Received (plus credits) can't be less than the selected charges" in (msg + det):
                                want_header_only_retry = True

                        if want_header_only_retry:
                            # find original payload and strip lines
                            for sid, payload in chunk:
                                if str(sid) == str(bid):
                                    p2 = dict(payload)
                                    p2.pop("Line", None)
                                    header_only_candidates.append((sid, p2))
                                    logger.warning(f"⚠️ Payment {bid} header-only retry queued...")
                                    break
                        else:
                            failures.append((reason, bid))
                            logger.error(f"❌ Payment {bid} failed: {reason}")

                    # mark any missing bIds as failed
                    for sid, _ in chunk:
                        if str(sid) not in seen:
                            failures.append(("No response for bId", sid))
                            logger.error(f"❌ No response for Payment {sid}")

                    # If we have header-only candidates, fire ONE more batch request for them
                    if header_only_candidates:
                        try:
                            resp2 = _do_post(headers, header_only_candidates)
                            if resp2.status_code == 200:
                                rj2 = resp2.json()
                                items2 = rj2.get("BatchItemResponse", []) or []
                                seen2 = set()
                                for item2 in items2:
                                    bid2 = item2.get("bId")
                                    seen2.add(bid2)
                                    ent2 = item2.get("Payment")
                                    if ent2 and "Id" in ent2:
                                        qid2 = ent2["Id"]
                                        # capture new payload JSON (header-only) for DB update
                                        # find modified payload object
                                        for _sid, p2 in header_only_candidates:
                                            if str(_sid) == str(bid2):
                                                header_only_successes.append((qid2, bid2, _fast_dumps(p2)))
                                                logger.info(f"✅ Retry success: Payment {bid2} → QBO {qid2} (Header-only)")
                                                break
                                    else:
                                        f2 = item2.get("Fault") or {}
                                        e2 = f2.get("Error") or []
                                        if e2:
                                            msg = ( e2[0].get("Message") or "" )
                                            det = ( e2[0].get("Detail")  or "" )
                                            failures.append(((msg + " | " + det).strip()[:1000], bid2))
                                        else:
                                            failures.append(("Unknown batch failure (header-only)", bid2))
                                for sid2, _ in header_only_candidates:
                                    if str(sid2) not in seen2:
                                        failures.append(("No response for bId (header-only)", sid2))
                            else:
                                reason2 = (resp2.text or f"HTTP {resp2.status_code}")[:1000]
                                for sid2, _ in header_only_candidates:
                                    failures.append((reason2, sid2))
                                logger.error(f"❌ Header-only batch failed ({len(header_only_candidates)}): {reason2}")
                        except Exception as e:
                            reason2 = f"Header-only batch exception: {e}"
                            for sid2, _ in header_only_candidates:
                                failures.append((reason2, sid2))
                            logger.exception("❌ Exception during header-only Payment batch")
                    break

                elif sc in (401, 403) and not attempted_refresh:
                    logger.warning(f"🔐 {sc} on Payment batch ({len(chunk)} items); refreshing token and retrying once...")
                    try:
                        auto_refresh_token_if_needed()
                    except Exception:
                        pass
                    url, headers = get_qbo_auth()
                    batch_url = _derive_batch_url(url, "Payment")
                    attempted_refresh = True
                    continue

                else:
                    reason = (resp.text or f"HTTP {sc}")[:1000]
                    for sid, _ in chunk:
                        failures.append((reason, sid))
                    logger.error(f"❌ Payment batch failed ({len(chunk)} items): {reason}")
                    break

            except Exception as e:
                reason = f"Batch exception: {e}"
                for sid, _ in chunk:
                    failures.append((reason, sid))
                logger.exception(f"❌ Exception during Payment batch POST ({len(chunk)} items)")
                break

    return successes, failures, header_only_successes

def _apply_batch_updates_payment(successes, failures, header_only_successes=None):
    """
    Apply DB updates for Payment batch results.
    - successes: set Target_Id, Success
    - failures : set Failed, bump Retry_Count, Failure_Reason
    - header_only_successes: also persist the modified header-only Payload_JSON
    """
    header_only_successes = header_only_successes or []
    try:
        if successes:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Payment] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                [(qid, sid) for qid, sid in successes]
            )
        if header_only_successes:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Payment] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL, Payload_JSON=? "
                f"WHERE Source_Id=?",
                [(qid, pj, sid) for (qid, sid, pj) in ((q, s, p) for q, s, p in header_only_successes)]
            )
        if failures:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Payment] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                [(reason, sid) for reason, sid in failures]
            )
    except NameError:
        # Fall back to per-row updates
        for qid, sid in successes or []:
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Payment] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                (qid, sid)
            )
        for qid, sid, pj in header_only_successes or []:
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Payment] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL, Payload_JSON=? "
                f"WHERE Source_Id=?",
                (qid, pj, sid)
            )
        for reason, sid in failures or []:
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Payment] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                (reason, sid)
            )

# ------------------------------- UPDATED 1/3 ------------------------------------
def post_payment(row):
    """
    Single-record fallback; prefer batch path in migrate/resume.
    Preserves header-only retry on specific Business Validation error.
    """
    sid = row["Source_Id"]
    if row.get("Porter_Status") == "Success": return
    if int(row.get("Retry_Count") or 0) >= 3: return

    # refresh dedup PaymentRefNum like original
    try:
        dedup = sql.fetch_single_value(
            f"SELECT Duplicate_PaymentRefNum FROM [{MAPPING_SCHEMA}].[Map_Payment] WHERE Source_Id = ?",
            (sid,)
        )
    except Exception:
        dedup = None

    pj = row.get("Payload_JSON")
    if not pj:
        logger.warning(f"⚠️ Payment {sid} skipped — missing Payload_JSON")
        return

    try:
        payload = _fast_loads(pj)
    except Exception as e:
        reason = f"Invalid JSON: {e}"
        sql.run_query(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_Payment] "
            f"SET Porter_Status='Failed', Retry_Count=ISNULL(Retry_Count,0)+1, Failure_Reason=? "
            f"WHERE Source_Id=?",
            (reason, sid)
        )
        return

    if dedup is not None and str(dedup).strip() != "":
        payload = dict(payload)
        payload["PaymentRefNum"] = str(dedup).strip()

    url, headers = get_qbo_auth()
    try:
        resp = session.post(url, headers=headers, json=payload, timeout=20)
        if resp.status_code == 200:
            qid = (resp.json().get("Payment") or {}).get("Id")
            if qid:
                sql.run_query(
                    f"UPDATE [{MAPPING_SCHEMA}].[Map_Payment] "
                    f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                    f"WHERE Source_Id=?",
                    (qid, sid)
                )
                logger.info(f"✅ Payment {sid} → QBO {qid}")
                return
            reason = "No Id in response"
        else:
            error_text = resp.text or ""
            # header-only retry path
            if ("Business Validation Error" in error_text and
                "Amount Received (plus credits) can't be less than the selected charges" in error_text):
                p2 = dict(payload); p2.pop("Line", None)
                resp2 = session.post(url, headers=headers, json=p2, timeout=20)
                if resp2.status_code == 200:
                    qid2 = (resp2.json().get("Payment") or {}).get("Id")
                    if qid2:
                        sql.run_query(
                            f"UPDATE [{MAPPING_SCHEMA}].[Map_Payment] "
                            f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL, Payload_JSON=? "
                            f"WHERE Source_Id=?",
                            (qid2, _fast_dumps(p2), sid)
                        )
                        logger.info(f"✅ Retry success: Payment {sid} → QBO {qid2} (Header-only)")
                        return
                    reason = "No Id in response (header-only)"
                else:
                    reason = (resp2.text or f"HTTP {resp2.status_code}")[:500]
            else:
                reason = (error_text or f"HTTP {resp.status_code}")[:500]

        sql.run_query(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_Payment] "
            f"SET Porter_Status='Failed', Retry_Count=ISNULL(Retry_Count,0)+1, Failure_Reason=?, Payload_JSON=? "
            f"WHERE Source_Id=?",
            (reason, _fast_dumps(payload), sid)
        )
    except Exception as e:
        sql.run_query(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_Payment] "
            f"SET Porter_Status='Failed', Retry_Count=ISNULL(Retry_Count,0)+1, Failure_Reason=? "
            f"WHERE Source_Id=?",
            (str(e), sid)
        )

# ------------------------------- UPDATED 2/3 ------------------------------------
def migrate_payments(PAYMENT_DATE_FROM,PAYMENT_DATE_TO,retry_only=False):
    """
    Orchestrates full migration for Payment records using QBO /batch.
    Skips records with Retry_Count >= 3. Preserves header-only retry behavior per item.
    """
    logger.info("🚀 Starting Payment Migration")
    ensure_mapping_table(PAYMENT_DATE_FROM,PAYMENT_DATE_TO)

    if 'ENABLE_GLOBAL_PAYMENT_DOCNUMBER_DEDUP' in globals() and ENABLE_GLOBAL_PAYMENT_DOCNUMBER_DEDUP:
        logger.info("🌐 Applying global PaymentRefNum deduplication strategy...")
        apply_duplicate_docnumber_strategy()
        try: sql.clear_cache()
        except Exception: pass

    # Ensure payloads exist (only generate for missing)
    missing_count = sql.fetch_single_value(
        f"""
        SELECT COUNT(*) FROM [{MAPPING_SCHEMA}].[Map_Payment]
        WHERE (Payload_JSON IS NULL OR Payload_JSON = '')
          AND Porter_Status IN ('Ready','Failed')
          AND (Retry_Count IS NULL OR Retry_Count < 3)
        """,
        ()
    ) or 0
    if missing_count:
        logger.info(f"🔧 {missing_count} payments missing JSON payloads. Generating...")
        generate_payment_payloads_in_batches(batch_size=500)

    rows = sql.fetch_dataframe(
        f"""
        SELECT * FROM [{MAPPING_SCHEMA}].[Map_Payment]
        WHERE Porter_Status {'= \'Failed\'' if retry_only else 'IN (\'Ready\', \'Failed\')'}
          AND (Retry_Count IS NULL OR Retry_Count < 3)
          AND Payload_JSON IS NOT NULL AND Payload_JSON <> ''
        """
    )
    if rows.empty:
        logger.warning("⚠️ No eligible payments to migrate.")
        return

    url, headers = get_qbo_auth()
    select_batch_size = 300   # DB slice size
    post_batch_limit  = 5    # items per QBO batch call (≤30). Set 3 to post exactly three per call.
    timeout           = 40

    total = len(rows)
    logger.info(f"📤 Posting {total} Payment(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        batch = rows.iloc[i:i + select_batch_size]
        successes, failures, hdr_only = _post_batch_payments(
            batch, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates_payment(successes, failures, header_only_successes=hdr_only)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done*100//total}%)")

    # Optional: single-record fallback retry for failed rows without Target_Id
    failed_df = sql.fetch_table("Map_Payment", MAPPING_SCHEMA)
    failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
    if not failed_records.empty:
        logger.info(f"🔁 Reprocessing {len(failed_records)} failed Payments (single-record fallback)...")
        for _, row in failed_records.iterrows():
            post_payment(row)

    logger.info("🏁 Payment migration completed.")

# ------------------------------- UPDATED 3/3 ------------------------------------
def resume_or_post_payments(PAYMENT_DATE_FROM,PAYMENT_DATE_TO):
    """
    Resumes or initiates Payment migration using /batch:
    - Rebuilds mapping if missing or mismatched.
    - Regenerates payloads if missing.
    - Posts only eligible records with Retry_Count < 3.
    """
    if not sql.table_exists("Map_Payment", MAPPING_SCHEMA):
        logger.warning("🛠️ Map_Payment table not found. Initializing from scratch...")
        ensure_mapping_table(PAYMENT_DATE_FROM,PAYMENT_DATE_TO)

    if 'ENABLE_GLOBAL_PAYMENT_DOCNUMBER_DEDUP' in globals() and ENABLE_GLOBAL_PAYMENT_DOCNUMBER_DEDUP:
        logger.info("🌐 Applying global PaymentRefNum deduplication strategy...")
        apply_duplicate_docnumber_strategy()
        try: sql.clear_cache()
        except Exception: pass

    source_count = sql.fetch_single_value(f"SELECT COUNT(*) FROM [{SOURCE_SCHEMA}].[Payment]", ())
    mapped_count = sql.fetch_single_value(f"SELECT COUNT(*) FROM [{MAPPING_SCHEMA}].[Map_Payment]", ())
    if (source_count or 0) != (mapped_count or 0):
        logger.warning(f"⚠️ Source vs Mapping count mismatch: {source_count} ≠ {mapped_count}. Regenerating mapping...")
        ensure_mapping_table(PAYMENT_DATE_FROM,PAYMENT_DATE_TO)

    # Generate missing payloads only
    missing_count = sql.fetch_single_value(
        f"""
        SELECT COUNT(*) FROM [{MAPPING_SCHEMA}].[Map_Payment]
        WHERE (Payload_JSON IS NULL OR Payload_JSON = '')
          AND Porter_Status IN ('Ready','Failed')
          AND (Retry_Count IS NULL OR Retry_Count < 3)
        """,
        ()
    ) or 0
    if missing_count:
        logger.info(f"🔄 {missing_count} payments missing JSON payloads. Generating...")
        generate_payment_payloads_in_batches(batch_size=500)

    eligible = sql.fetch_dataframe(
        f"""
        SELECT * FROM [{MAPPING_SCHEMA}].[Map_Payment]
        WHERE Porter_Status IN ('Ready','Failed')
          AND (Retry_Count IS NULL OR Retry_Count < 3)
          AND Payload_JSON IS NOT NULL AND Payload_JSON <> ''
        """
    )
    if eligible.empty:
        logger.info("🎉 No eligible payments to post.")
        return

    url, headers = get_qbo_auth()
    select_batch_size = 300
    post_batch_limit  = 5
    timeout           = 40

    total = len(eligible)
    logger.info(f"🚚 Posting {total} Payment(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        batch = eligible.iloc[i:i + select_batch_size]
        successes, failures, hdr_only = _post_batch_payments(
            batch, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
        )
        _apply_batch_updates_payment(successes, failures, header_only_successes=hdr_only)

        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done*100//total}%)")

    logger.info("✅ Payment posting completed.")

def smart_payment_migration(PAYMENT_DATE_FROM, PAYMENT_DATE_TO):
    """
    Smart migration entrypoint for Payments:
    - If Map_Payment table exists and row count matches Payment table, resume/post.
    - If table missing or row count mismatch, perform full migration.
    - After either path, reprocess failed records with null Target_Id one more time.
    """
    logger.info("🔎 Running smart_payment_migration...")
    full_process = False

    if sql.table_exists("Map_Payment", MAPPING_SCHEMA):
        mapped_df = sql.fetch_table("Map_Payment", MAPPING_SCHEMA)
        source_df = sql.fetch_table("Payment", SOURCE_SCHEMA)

        if len(mapped_df) == len(source_df):
            logger.info("✅ Table exists and row count matches. Resuming/posting Payments.")
            resume_or_post_payments(PAYMENT_DATE_FROM, PAYMENT_DATE_TO)

            # Retry failed ones after resume
            failed_df = sql.fetch_table("Map_Payment", MAPPING_SCHEMA)
            failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
            if not failed_records.empty:
                logger.info(f"🔁 Reprocessing {len(failed_records)} failed Payments with null Target_Id after main migration...")
                for _, row in failed_records.iterrows():
                    post_payment(row)
            return
        else:
            logger.warning(f"❌ Row count mismatch: Map_Payment={len(mapped_df)}, Payment={len(source_df)}. Running full migration.")
            full_process = True
    else:
        logger.warning("❌ Map_Payment table does not exist. Running full migration.")
        full_process = True

    # ----- Full migration path -----
    if full_process:
        ensure_mapping_table(PAYMENT_DATE_FROM, PAYMENT_DATE_TO)

        if 'ENABLE_GLOBAL_PAYMENT_DOCNUMBER_DEDUP' in globals() and ENABLE_GLOBAL_PAYMENT_DOCNUMBER_DEDUP:
            apply_duplicate_docnumber_strategy()
            try:
                sql.clear_cache()
            except Exception:
                pass
            import time; time.sleep(1)

        generate_payment_payloads_in_batches()

        post_query = f"SELECT * FROM {MAPPING_SCHEMA}.Map_Payment WHERE Porter_Status = 'Ready'"
        post_rows = sql.fetch_table_with_params(post_query, tuple())
        if post_rows.empty:
            logger.warning("⚠️ No Payments with built JSON to post.")
            return

        url, headers = get_qbo_auth()
        select_batch_size = 300
        post_batch_limit  = 5
        timeout           = 40

        total = len(post_rows)
        logger.info(f"📤 Posting {total} Payment record(s) via QBO Batch API (limit {post_batch_limit}/call)...")

        for i in range(0, total, select_batch_size):
            slice_df = post_rows.iloc[i:i + select_batch_size]
            successes, failures, hdr_only = _post_batch_payments(
                slice_df, url, headers, timeout=timeout, post_batch_limit=post_batch_limit, max_manual_retries=1
            )
            _apply_batch_updates_payment(successes, failures, header_only_successes=hdr_only)

            done = min(i + select_batch_size, total)
            logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

        # Retry failed ones after full migration
        failed_df = sql.fetch_table("Map_Payment", MAPPING_SCHEMA)
        failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
        if not failed_records.empty:
            logger.info(f"🔁 Reprocessing {len(failed_records)} failed Payments with null Target_Id after main migration...")
            for _, row in failed_records.iterrows():
                post_payment(row)

        logger.info("🏁 SMART Payment migration complete.")

