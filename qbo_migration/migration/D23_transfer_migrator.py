"""
Sequence : 23
Author: Dixit Prajapati
Created: 2025-09-17
Description: Handles migration of Transfer records from source system to QBO.
Production : Ready
Development : Require when necessary
Phase : 02 - Multi User
"""

import os
import json
import pandas as pd
import requests
from dotenv import load_dotenv
from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import global_logger as logger
from config.mapping.transfer_mapping import TRANSFER_HEADER_MAPPING
from utils.token_refresher import get_qbo_context_migration
from storage.sqlserver.sql import executemany
 
load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")

def get_qbo_auth():
    """
    Returns the QBO API endpoint and authentication headers for the Transfer entity.
    Returns:
        tuple:
            str: QBO API endpoint URL for Transfer.
            dict: HTTP headers including Authorization and Content-Type.
    """    
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
    """
    Safely converts a value to float.
    Args:
        val (Any): The value to convert.
    Returns:
        float: The converted float value, or 0.0 if conversion fails.
    """
    try: return float(val)
    except: return 0.0

def coerce_retry(series):
    """
    Converts a pandas Series to integer retry counts, coercing invalid entries to 0.
    Args:
        series (pd.Series): Input series containing retry counts.
    Returns:
        pd.Series: Integer series with invalid or missing values replaced with 0.
    """
    return pd.to_numeric(series, errors="coerce").fillna(0).astype(int)

def safe_int(val, default=0):
    """
    Safely converts a value to int, with a default fallback.
    Args:
        val (Any): The value to convert.
        default (int): Default integer value if conversion fails.
    Returns:
        int: Converted integer or default value.
    """    
    try:
        # handle blanks/None cleanly
        if val is None or (isinstance(val, str) and val.strip() == ""):
            return default
        return int(val)
    except Exception:
        return default

def ensure_mapping_table(TRANSFER_DATE_FROM='1900-01-01',TRANSFER_DATE_TO='2080-12-31'):
    """
    Optimized, single-pass builder for Map_Transfer (no per-row SQL lookups).

    - Pulls Transfer headers in one query with optional date bounds.
    - Bulk-loads Map_Account once and vectorizes From/To account mapping.
    - Initializes standard porter columns.
    - Replaces existing Map_Transfer rows.
    """
    # 1) Source pull with optional bounds (keeps original semantics)
    date_from = TRANSFER_DATE_FROM
    date_to   = TRANSFER_DATE_TO
    query = f"""
        SELECT * FROM [{SOURCE_SCHEMA}].[Transfer]
        WHERE (TxnDate >= ? OR ? IS NULL)
          AND (TxnDate <= ? OR ? IS NULL)
    """
    params = (date_from, date_from, date_to, date_to)
    df = sql.fetch_table_with_params(query, params)

    if df.empty:
        logger.info("⚠️ No Transfer records found for the specified date range.")
        return

    # 2) Standard porter columns (idempotent)
    df = df.copy()
    df["Source_Id"]      = df["Id"]
    df["Target_Id"]      = None
    df["Porter_Status"]  = "Ready"
    df["Retry_Count"]    = 0
    df["Failure_Reason"] = None
    if "Payload_JSON" not in df.columns:
        df["Payload_JSON"] = None

    # 3) Bulk-load account mapping once, then vectorize the lookups
    acct_map_df = sql.fetch_table_with_params(
        f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Account]", tuple()
    )
    acct_dict = dict(zip(acct_map_df["Source_Id"], acct_map_df["Target_Id"])) if not acct_map_df.empty else {}

    from_col = TRANSFER_HEADER_MAPPING.get("FromAccountRef.value")
    to_col   = TRANSFER_HEADER_MAPPING.get("ToAccountRef.value")

    if from_col and from_col in df.columns:
        df["Mapped_FromAccountRef"] = df[from_col].map(lambda x: acct_dict.get(x) if pd.notna(x) else None)
    else:
        df["Mapped_FromAccountRef"] = None
        if from_col:
            logger.warning("ℹ️ FromAccountRef source column not found; Mapped_FromAccountRef set to NULL.")

    if to_col and to_col in df.columns:
        df["Mapped_ToAccountRef"] = df[to_col].map(lambda x: acct_dict.get(x) if pd.notna(x) else None)
    else:
        df["Mapped_ToAccountRef"] = None
        if to_col:
            logger.warning("ℹ️ ToAccountRef source column not found; Mapped_ToAccountRef set to NULL.")

    # 4) Replace existing rows (TRUNCATE if possible for speed, else DELETE)
    if sql.table_exists("Map_Transfer", MAPPING_SCHEMA):
        try:
            sql.run_query(f"TRUNCATE TABLE [{MAPPING_SCHEMA}].[Map_Transfer]")
        except Exception:
            sql.run_query(f"DELETE FROM [{MAPPING_SCHEMA}].[Map_Transfer]")

    # 5) Bulk insert (convert NaN→None for NVARCHAR/INT NULLs)
    sql.insert_invoice_map_dataframe(df.where(pd.notnull, None), "Map_Transfer", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df)} records into {MAPPING_SCHEMA}.Map_Transfer")

# 1) Keep this set, but the wrapper will go away
FORBIDDEN_CREATE_FIELDS = {"domain", "sparse", "Id", "SyncToken"}

def build_payload(row):
    """
    Optimized (allocation/light lookup) Transfer payload builder.
    Preserves original logic & mappings.
    """
    sid = row.get("Source_Id")

    # Fast local refs
    _map     = TRANSFER_HEADER_MAPPING
    _get     = row.get
    _sf      = safe_float
    _notna   = pd.notna
    _str     = str

    from_acc_val = _get("Mapped_FromAccountRef")
    to_acc_val   = _get("Mapped_ToAccountRef")

    # Core fields (avoid repeated dict lookups)
    amt_key   = _map.get("Amount")
    date_key  = _map.get("TxnDate")
    note_key  = _map.get("PrivateNote")
    curr_key  = _map.get("CurrencyRef.value")
    xrt_key   = _map.get("ExchangeRate")

    transfer = {
        "Amount": _sf(_get(amt_key) or 0.0) if amt_key else 0.0,
        "FromAccountRef": {"value": _str(from_acc_val)} if from_acc_val else None,
        "ToAccountRef":   {"value": _str(to_acc_val)}   if to_acc_val   else None,
    }

    # Optional header fields
    if date_key:
        txn_date = _get(date_key)
        if _notna(txn_date):
            transfer["TxnDate"] = _str(txn_date)

    if note_key:
        private_note = _get(note_key)
        if _notna(private_note):
            transfer["PrivateNote"] = _str(private_note)

    # Multicurrency (only if provided)
    if curr_key:
        cur_val = _get(curr_key)
        mapped_cur_val = None
        # Try to get mapped currency if mapping table exists and value is present
        try:
            if "Mapped_CurrencyRef" in row and _notna(row["Mapped_CurrencyRef"]):
                mapped_cur_val = row["Mapped_CurrencyRef"]
            elif "CurrencyRef_value" in row and _notna(row["CurrencyRef_value"]):
                mapped_cur_val = row["CurrencyRef_value"]
        except Exception:
            mapped_cur_val = None
        if _notna(mapped_cur_val):
            transfer["CurrencyRef"] = {"value": _str(mapped_cur_val)}
        elif _notna(cur_val):
            transfer["CurrencyRef"] = {"value": _str(cur_val)}
    if xrt_key:
        ex_rate = _get(xrt_key)
        if _notna(ex_rate):
            try:
                transfer["ExchangeRate"] = float(ex_rate)
            except Exception:
                pass

    # Strip forbidden + None
    for k in list(transfer.keys()):
        if k in FORBIDDEN_CREATE_FIELDS:
            transfer.pop(k, None)
    transfer = {k: v for k, v in transfer.items() if v is not None}

    # Sanity warnings (behavior unchanged)
    if not transfer.get("FromAccountRef") or not transfer.get("ToAccountRef"):
        logger.warning(f"⚠️ Missing From/To AccountRef for {sid}")
    if transfer.get("Amount", 0) <= 0:
        logger.warning(f"⚠️ Non-positive Amount for {sid}")

    return transfer

def generate_transfer_payloads_in_batches(batch_size=500):
    """
    Optimized batch payload generator for Transfers (no per-row SQL on success path).
    - Pulls batches of Ready rows missing Payload_JSON.
    - Skips rows missing mapped From/To.
    - Uses fast serializer when available.
    - Applies batched DB updates via executemany (fallback to per-row).
    """
    # Lightweight local serializer
    try:
        import orjson as _oj
        def _dumps(obj): return _oj.dumps(obj, option=_oj.OPT_INDENT_2).decode("utf-8")
    except Exception:
        def _dumps(obj): return json.dumps(obj, indent=2)

    logger.info("🔧 Generating JSON payloads for Transfers in batches...")

    select_sql = (
        f"SELECT TOP {batch_size} * "
        f"FROM [{MAPPING_SCHEMA}].[Map_Transfer] "
        f"WHERE Porter_Status = 'Ready' AND (Payload_JSON IS NULL OR Payload_JSON = '')"
    )

    while True:
        df = sql.fetch_table_with_params(select_sql, tuple())
        if df.empty:
            logger.info("✅ All Transfer payloads generated.")
            break

        # Build payloads in Python; collect updates & failures
        success_updates = []  # (payload_json, sid)
        fail_updates    = []  # (reason, sid)

        # Local bindings for speed
        _iterrows = df.iterrows
        _get      = getattr
        _pp       = _dumps

        for _, row in _iterrows():
            sid = row["Source_Id"]
            if not row.get("Mapped_FromAccountRef") or not row.get("Mapped_ToAccountRef"):
                fail_updates.append(("Missing mapped FromAccountRef or ToAccountRef", sid))
                continue

            try:
                payload = build_payload(row)
                # Always produce payload (even if warnings); original logic serialized regardless
                payload_json = _pp(payload)
                success_updates.append((payload_json, sid))
            except Exception as e:
                fail_updates.append((f"Build error: {e}", sid))

        # Apply batch updates
        if success_updates:
            try:
                executemany(
                    f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_Transfer]
                       SET Payload_JSON = ?, Failure_Reason = NULL
                     WHERE Source_Id = ?
                    """,
                    success_updates
                )
            except NameError:
                for payload_json, sid in success_updates:
                    sql.run_query(
                        f"""
                        UPDATE [{MAPPING_SCHEMA}].[Map_Transfer]
                           SET Payload_JSON = ?, Failure_Reason = NULL
                         WHERE Source_Id = ?
                        """,
                        (payload_json, sid)
                    )

        if fail_updates:
            try:
                executemany(
                    f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_Transfer]
                       SET Porter_Status = 'Failed',
                           Failure_Reason = ?
                     WHERE Source_Id = ?
                    """,
                    fail_updates
                )
            except NameError:
                for reason, sid in fail_updates:
                    sql.run_query(
                        f"""
                        UPDATE [{MAPPING_SCHEMA}].[Map_Transfer]
                           SET Porter_Status = 'Failed',
                               Failure_Reason = ?
                         WHERE Source_Id = ?
                        """,
                        (reason, sid)
                    )

        logger.info(f"✅ Generated payloads for {len(success_updates)} Transfers; "
                    f"marked {len(fail_updates)} as Failed in this batch.")

API_ENTITY = "transfer"
session = requests.Session()

# ---------- Helper(s) for optimized single-thread batch posting ----------

def _fast_loads(s):
    try:
        import orjson
        if isinstance(s, (bytes, bytearray)):  # guard mixed storage types
            return orjson.loads(s)
        return orjson.loads(s.encode("utf-8")) if isinstance(s, str) else s
    except Exception:
        if isinstance(s, (bytes, bytearray)):
            try:
                return json.loads(s.decode("utf-8"))
            except Exception:
                return json.loads(s)
        return json.loads(s) if isinstance(s, str) else s

def _derive_batch_url(entity_url: str, entity_name: str) -> str:
    """
    Turn .../v3/company/{realm}/{Entity}?minorversion=xx  ->  .../v3/company/{realm}/batch?minorversion=xx
    Works even if the entity segment casing differs.
    """
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

def _post_batch_transfers(eligible_batch, url, headers, timeout=25, post_batch_limit=25, max_manual_retries=1):
    """
    Single-threaded batch POST for Transfers.
    - Pre-decodes Payload_JSON once per row.
    - Uses QBO /batch with 'Transfer' entities.
    - On 401/403, refreshes token once and retries batch.
    Returns: (successes, failures) where
        successes: list[(qid, sid)]
        failures : list[(reason, sid)]
    """
    successes, failures = [], []
    if eligible_batch is None or eligible_batch.empty:
        return successes, failures

    # Pre-parse payloads, filter out ineligible rows
    items = []  # (sid, payload)
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
            items.append((sid, payload))
        except Exception as e:
            failures.append((f"Bad JSON: {e}", sid))

    if not items:
        return successes, failures

    batch_url = _derive_batch_url(url, "Transfer")

    def _do_post(_headers, _items):
        body = {
            "BatchItemRequest": [
                {"bId": str(sid), "operation": "create", "Transfer": payload}
                for (sid, payload) in _items
            ]
        }
        return session.post(batch_url, headers=_headers, json=body, timeout=timeout)

    # Chunk into multiple /batch calls
    for i in range(0, len(items), post_batch_limit):
        auto_refresh_token_if_needed()
        chunk = items[i : i + post_batch_limit]
        attempted_refresh = False

        for attempt in range(max_manual_retries + 1):
            try:
                resp = _do_post(headers, chunk)
                sc = resp.status_code

                if sc == 200:
                    data = resp.json() or {}
                    arr  = data.get("BatchItemResponse", []) or []
                    seen = set()

                    for it in arr:
                        bid = it.get("bId")
                        seen.add(bid)
                        ent = it.get("Transfer")
                        if ent and "Id" in ent:
                            successes.append((ent["Id"], bid))
                            logger.info(f"✅ Transfer {bid} → QBO {ent['Id']}")
                        else:
                            fault = it.get("Fault") or {}
                            errs  = fault.get("Error") or []
                            msg   = (errs[0].get("Message") if errs else "") or ""
                            det   = (errs[0].get("Detail")  if errs else "") or ""
                            reason = (msg + " | " + det).strip()[:1000] or "Unknown error"
                            failures.append((reason, bid))
                            logger.error(f"❌ Transfer {bid} failed: {reason}")

                    # Any missing responses get marked failed
                    for sid2, _ in chunk:
                        if str(sid2) not in seen:
                            failures.append(("No response for bId", sid2))
                            logger.error(f"❌ No response for Transfer {sid2}")
                    break

                elif sc in (401, 403) and not attempted_refresh:
                    logger.warning(f"🔐 {sc} on Transfer batch ({len(chunk)} items); refreshing token and retrying once...")
                    try:
                        auto_refresh_token_if_needed()
                    except Exception:
                        pass
                    url, headers = get_qbo_auth()  # refresh creds
                    batch_url = _derive_batch_url(url, "Transfer")
                    attempted_refresh = True
                    continue

                else:
                    reason = (resp.text or f"HTTP {sc}")[:1000]
                    for sid2, _ in chunk:
                        failures.append((reason, sid2))
                    logger.error(f"❌ Transfer batch failed ({len(chunk)} items): {reason}")
                    break

            except requests.Timeout:
                if attempt < max_manual_retries:
                    continue
                reason = "Timeout"
                for sid2, _ in chunk:
                    failures.append((reason, sid2))
                logger.error(f"⏱️ Timeout on Transfer batch of {len(chunk)}")
                break
            except Exception as e:
                reason = f"Batch exception: {e}"
                for sid2, _ in chunk:
                    failures.append((reason, sid2))
                logger.exception("❌ Exception during Transfer batch POST")
                break

    return successes, failures

def _apply_batch_updates_transfer(successes, failures):
    """
    Apply results to Map_Transfer efficiently.
    """
    if successes:
        try:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Transfer] "
                f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                f"WHERE Source_Id=?",
                [(qid, sid) for qid, sid in successes]
            )
        except NameError:
            for qid, sid in successes:
                sql.run_query(
                    f"UPDATE [{MAPPING_SCHEMA}].[Map_Transfer] "
                    f"SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL "
                    f"WHERE Source_Id=?",
                    (qid, sid)
                )
    if failures:
        try:
            executemany(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Transfer] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                [(reason, sid) for reason, sid in failures]
            )
        except NameError:
            for reason, sid in failures:
                sql.run_query(
                    f"UPDATE [{MAPPING_SCHEMA}].[Map_Transfer] "
                    f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                    f"WHERE Source_Id=?",
                    (reason, sid)
                )

# ---------- Updated posting functions (optimized, batch-first) ----------

def post_transfer(row):
    """
    One-off optimized poster for a single Transfer (kept for compatibility).
    Uses shared `session`, short timeouts, and a single auth refresh on 401/403.
    """
    sid = row["Source_Id"]
    if row.get("Porter_Status") == "Success":
        return
    if int(row.get("Retry_Count") or 0) >= 5:
        logger.warning(f"⚠️ Skipping Transfer {sid} - exceeded retry limit")
        return
    if not row.get("Payload_JSON"):
        logger.warning(f"⚠️ Skipping Transfer {sid} - missing Payload_JSON")
        return

    url, headers = get_qbo_auth()
    payload = _fast_loads(row["Payload_JSON"])

    try:
        resp = session.post(url, headers=headers, json=payload, timeout=25)
        if resp.status_code == 200:
            qid = (resp.json() or {}).get("Transfer", {}).get("Id")
            if qid:
                sql.run_query(
                    f"UPDATE [{MAPPING_SCHEMA}].[Map_Transfer] "
                    f"SET Target_Id = ?, Porter_Status = 'Success', Failure_Reason = NULL "
                    f"WHERE Source_Id = ?",
                    (qid, sid)
                )
                logger.info(f"✅ Transfer {sid} → QBO {qid}")
                return
            # rare: no Id
            reason = "No Id in 200 response"
            logger.error(f"❌ Transfer {sid}: {reason}")
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Transfer] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                (reason, sid)
            )
            return

        if resp.status_code in (401, 403):
            logger.warning(f"🔐 {resp.status_code} on Transfer {sid}; refreshing token and retrying once...")
            try:
                auto_refresh_token_if_needed()
            except Exception:
                pass
            url, headers = get_qbo_auth()
            resp2 = session.post(url, headers=headers, json=payload, timeout=25)
            if resp2.status_code == 200:
                qid = (resp2.json() or {}).get("Transfer", {}).get("Id")
                if qid:
                    sql.run_query(
                        f"UPDATE [{MAPPING_SCHEMA}].[Map_Transfer] "
                        f"SET Target_Id = ?, Porter_Status = 'Success', Failure_Reason = NULL "
                        f"WHERE Source_Id = ?",
                        (qid, sid)
                    )
                    logger.info(f"✅ Transfer {sid} → QBO {qid}")
                    return
            reason = (resp2.text or "")[:300]
            logger.error(f"❌ Failed to post Transfer {sid}: {reason}")
            sql.run_query(
                f"UPDATE [{MAPPING_SCHEMA}].[Map_Transfer] "
                f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
                f"WHERE Source_Id=?",
                (reason, sid)
            )
            return

        # generic non-200
        reason = (resp.text or f"HTTP {resp.status_code}")[:300]
        logger.error(f"❌ Failed to post Transfer {sid}: {reason}")
        sql.run_query(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_Transfer] "
            f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
            f"WHERE Source_Id=?",
            (reason, sid)
        )

    except requests.Timeout:
        reason = "Timeout"
        sql.run_query(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_Transfer] "
            f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
            f"WHERE Source_Id=?",
            (reason, sid)
        )
        logger.error(f"⏱️ Timeout posting Transfer {sid}")
    except Exception as e:
        sql.run_query(
            f"UPDATE [{MAPPING_SCHEMA}].[Map_Transfer] "
            f"SET Porter_Status='Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason=? "
            f"WHERE Source_Id=?",
            (str(e), sid)
        )
        logger.exception(f"❌ Exception posting Transfer {sid}")

def migrate_transfers(TRANSFER_DATE_FROM,TRANSFER_DATE_TO):
    """
    Full Transfer migration:
    - ensure mapping
    - generate payloads
    - post in single-threaded batches via QBO /batch
    """
    print("\n🚀 Starting Transfer Migration\n" + "=" * 40)
    ensure_mapping_table(TRANSFER_DATE_FROM,TRANSFER_DATE_TO)
    generate_transfer_payloads_in_batches()

    rows = sql.fetch_table("Map_Transfer", MAPPING_SCHEMA)
    if rows.empty:
        logger.info("⚠️ Map_Transfer has no rows.")
        return

    retry_s = coerce_retry(rows["Retry_Count"])
    eligible = rows[
        rows["Porter_Status"].isin(["Ready", "Failed"]) & (retry_s < 5)
    ].reset_index(drop=True)

    if eligible.empty:
        logger.info("⚠️ No Transfers ready for posting.")
        return

    # One auth fetch; batch path handles 401/403 refresh internally
    url, headers = get_qbo_auth()

    batch_select = 300  # DB slice
    post_limit   = 5   # items per /batch call
    timeout      = 25
    total        = len(eligible)
    logger.info(f"📤 Posting {total} Transfer(s) in single-threaded /batch batches of ≤{post_limit}...")

    for i in range(0, total, batch_select):
        slice_df = eligible.iloc[i:i+batch_select]
        successes, failures = _post_batch_transfers(
            slice_df, url, headers, timeout=timeout, post_batch_limit=post_limit, max_manual_retries=1
        )
        _apply_batch_updates_transfer(successes, failures)

    print("\n🏁 Transfer Migration Completed.\n")

def resume_or_post_transfers(TRANSFER_DATE_FROM,TRANSFER_DATE_TO):
    """
    Resume/continue Transfer migration:
    - Ensure mapping table exists.
    - Generate missing payloads only.
    - Batch-post Ready/Failed with Retry_Count < 5.
    """
    print("\n🔁 Resuming or Starting Transfer Migration\n" + "=" * 40)

    if not sql.table_exists("Map_Transfer", MAPPING_SCHEMA):
        logger.warning("🛠️ Map_Transfer table not found. Initializing from source...")
        ensure_mapping_table(TRANSFER_DATE_FROM,TRANSFER_DATE_TO)

    logger.info("🔧 Generating any missing Transfer payloads...")
    generate_transfer_payloads_in_batches()

    rows = sql.fetch_table("Map_Transfer", MAPPING_SCHEMA)
    if rows.empty:
        logger.info("✅ No transfers found to post.")
        return

    retry_s = coerce_retry(rows["Retry_Count"])
    eligible = rows[
        rows["Porter_Status"].isin(["Ready", "Failed"]) & (retry_s < 5)
    ].reset_index(drop=True)

    if eligible.empty:
        logger.info("✅ All eligible Transfers have been posted.")
        return

    url, headers = get_qbo_auth()
    batch_select = 300
    post_limit   = 5
    timeout      = 25
    total        = len(eligible)
    logger.info(f"📤 Posting {total} Transfer(s) in single-threaded /batch batches of ≤{post_limit}...")

    for i in range(0, total, batch_select):
        slice_df = eligible.iloc[i:i+batch_select]
        successes, failures = _post_batch_transfers(
            slice_df, url, headers, timeout=timeout, post_batch_limit=post_limit, max_manual_retries=1
        )
        _apply_batch_updates_transfer(successes, failures)

    print("\n🏁 Transfer Migration Completed.\n")

def smrt_transfer_migration(TRANSFER_DATE_FROM,TRANSFER_DATE_TO):
    """
    Smart migration entrypoint for Transfer:
    - If Map_Transfer table exists and row count matches Transfer table, resume/post transfers.
    - If table missing or row count mismatch, perform full migration.
    """
    logger.info("🔎 Running smart_transfer_migration...")
    if sql.table_exists("Map_Transfer", MAPPING_SCHEMA):
        mapped_df = sql.fetch_table("Map_Transfer", MAPPING_SCHEMA)
        source_df = sql.fetch_table("Transfer", SOURCE_SCHEMA)
        if len(mapped_df) == len(source_df):
            logger.info("✅ Table exists and row count matches. Resuming/posting transfers.")
            resume_or_post_transfers(TRANSFER_DATE_FROM,TRANSFER_DATE_TO)
            return
        else:
            logger.warning(f"❌ Row count mismatch: Map_Transfer={len(mapped_df)}, Transfer={len(source_df)}. Running full migration.")
    else:
        logger.warning("❌ Map_Transfer table does not exist. Running full migration.")
    migrate_transfers(TRANSFER_DATE_FROM,TRANSFER_DATE_TO)

