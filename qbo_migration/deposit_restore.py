import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv

from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed, get_qbo_context_migration
from utils.log_timer import global_logger as logger
from utils.mapping_updater import update_mapping_status
from utils.payload_cleaner import deep_clean

# -------------------------------------------------------------------
# ENV + BASIC
# -------------------------------------------------------------------
load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
API_ENTITY = "deposit"

session = requests.Session()


# -------------------------------------------------------------------
# QBO HELPERS
# -------------------------------------------------------------------
def get_qbo_auth():
    env = os.getenv("QBO_ENVIRONMENT", "sandbox")
    base = "https://sandbox-quickbooks.api.intuit.com" if env == "sandbox" else "https://quickbooks.api.intuit.com"

    ctx = get_qbo_context_migration()
    realm = ctx["REALM_ID"]

    url = f"{base}/v3/company/{realm}/{API_ENTITY}"
    headers = {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    return url, headers


def safe_float(val):
    try:
        if pd.isna(val):
            return None
        return float(val)
    except Exception:
        return None


# -------------------------------------------------------------------
# BUILD PAYLOAD (NO MAPPING – DIRECT FROM SOURCE COLUMNS)
# -------------------------------------------------------------------
def build_deposit_payload(header_row: pd.Series, lines_df: pd.DataFrame) -> dict | None:
    """
    Build QBO Deposit payload using ONLY these columns:

    Header (Deposit):
      [DepositToAccountRef.value], [DepositToAccountRef.name], TotalAmt, TxnDate,
      [CurrencyRef.value], [CurrencyRef.name], PrivateNote, DocNumber,
      [CashBack.AccountRef.value], [CashBack.AccountRef.name],
      [CashBack.Amount], [CashBack.Memo]

    Lines (Deposit_Line):
      Amount,
      [LinkedTxn[0].TxnId], [LinkedTxn[0].TxnType], [LinkedTxn[0].TxnLineId],
      [DepositLineDetail.PaymentMethodRef.value], [DepositLineDetail.CheckNum],
      Parent_Id, TxnDate, DocNumber, Id, LineNum, DetailType,
      [DepositLineDetail.AccountRef.value], [DepositLineDetail.AccountRef.name],
      Description,
      [DepositLineDetail.ClassRef.value], [DepositLineDetail.ClassRef.name],
      [DepositLineDetail.Entity.value], [DepositLineDetail.Entity.name],
      [DepositLineDetail.Entity.type],
      [DepositLineDetail.PaymentMethodRef.name],
      [ProjectRef.value], Parent_Entity

    NO Map_* tables are used. We simply pass the source values straight to QBO.
    """

    # --- Header ---
    deposit_to_acct_val = header_row.get("DepositToAccountRef.value")
    if pd.isna(deposit_to_acct_val) or deposit_to_acct_val is None:
        logger.warning(f"Skipping Deposit Id={header_row.get('Id')} – missing DepositToAccountRef.value")
        return None

    payload = {
        "DepositToAccountRef": {
            "value": str(deposit_to_acct_val)
        },
        "TxnDate": header_row.get("TxnDate"),
        "PrivateNote": header_row.get("PrivateNote"),
        "DocNumber": header_row.get("DocNumber"),
        "Line": []
    }

    # CurrencyRef (optional)
    curr_val = header_row.get("CurrencyRef.value")
    curr_name = header_row.get("CurrencyRef.name")
    if pd.notna(curr_val) or pd.notna(curr_name):
        cref = {}
        if pd.notna(curr_val):
            cref["value"] = str(curr_val)
        if pd.notna(curr_name):
            cref["name"] = str(curr_name)
        payload["CurrencyRef"] = cref

    # TotalAmt (optional)
    total_amt = safe_float(header_row.get("TotalAmt"))
    if total_amt is not None:
        payload["TotalAmt"] = total_amt

    # CashBack (optional)
    cb_acct_val = header_row.get("CashBack.AccountRef.value")
    cb_amt = safe_float(header_row.get("CashBack.Amount"))
    cb_memo = header_row.get("CashBack.Memo")
    cb_name = header_row.get("CashBack.AccountRef.name")

    if (pd.notna(cb_acct_val) and cb_acct_val) or (cb_amt is not None):
        cashback = {}
        if pd.notna(cb_acct_val):
            cashback["AccountRef"] = {
                "value": str(cb_acct_val)
            }
            if pd.notna(cb_name):
                cashback["AccountRef"]["name"] = str(cb_name)
        if cb_amt is not None:
            cashback["Amount"] = cb_amt
        if pd.notna(cb_memo):
            cashback["Memo"] = str(cb_memo)
        if cashback:
            payload["CashBack"] = cashback

    # --- Lines ---
    if lines_df is None or lines_df.empty:
        logger.warning(f"Deposit Id={header_row.get('Id')} has no lines – skipping.")
        return None

    line_cols = set(lines_df.columns)

    for _, line_row in lines_df.iterrows():
        amount = safe_float(line_row.get("Amount"))
        if amount is None:
            logger.warning(f"Skipping line for Parent_Id={line_row.get('Parent_Id')} – Amount is NULL/invalid")
            continue

        detail_type = line_row.get("DetailType")
        if pd.isna(detail_type) or detail_type is None:
            detail_type = "DepositLineDetail"

        line_obj = {
            "Amount": amount,
            "DetailType": str(detail_type)
        }

        # LineNum (optional)
        ln = line_row.get("LineNum")
        if pd.notna(ln):
            try:
                line_obj["LineNum"] = int(float(ln))
            except Exception:
                logger.warning(f"Invalid LineNum={ln} for Parent_Id={line_row.get('Parent_Id')}")

        # Description (optional)
        desc = line_row.get("Description")
        if pd.notna(desc):
            line_obj["Description"] = str(desc)

        # --- DepositLineDetail ---
        detail = {}

        # AccountRef
        acct_val = line_row.get("DepositLineDetail.AccountRef.value")
        acct_name = line_row.get("DepositLineDetail.AccountRef.name")
        if pd.notna(acct_val):
            detail["AccountRef"] = {"value": str(acct_val)}
            if pd.notna(acct_name):
                detail["AccountRef"]["name"] = str(acct_name)

        # ClassRef
        class_val = line_row.get("DepositLineDetail.ClassRef.value")
        class_name = line_row.get("DepositLineDetail.ClassRef.name")
        if pd.notna(class_val):
            detail["ClassRef"] = {"value": str(class_val)}
            if pd.notna(class_name):
                detail["ClassRef"]["name"] = str(class_name)

        # Entity
        ent_val = line_row.get("DepositLineDetail.Entity.value")
        ent_name = line_row.get("DepositLineDetail.Entity.name")
        ent_type = line_row.get("DepositLineDetail.Entity.type")
        if pd.notna(ent_val):
            ent_obj = {"value": str(ent_val)}
            if pd.notna(ent_name):
                ent_obj["name"] = str(ent_name)
            if pd.notna(ent_type):
                ent_obj["type"] = str(ent_type)
            detail["Entity"] = ent_obj

        # PaymentMethodRef
        pm_val = line_row.get("DepositLineDetail.PaymentMethodRef.value")
        pm_name = line_row.get("DepositLineDetail.PaymentMethodRef.name")
        if pd.notna(pm_val):
            pm_obj = {"value": str(pm_val)}
            if pd.notna(pm_name):
                pm_obj["name"] = str(pm_name)
            detail["PaymentMethodRef"] = pm_obj

        # CheckNum
        check_num = line_row.get("DepositLineDetail.CheckNum")
        if pd.notna(check_num):
            detail["CheckNum"] = str(check_num)

        # ProjectRef (if present in extract)
        if "ProjectRef.value" in line_cols:
            proj_val = line_row.get("ProjectRef.value")
            if pd.notna(proj_val):
                detail["ProjectRef"] = {"value": str(proj_val)}

        if detail:
            line_obj["DepositLineDetail"] = detail

        # --- LinkedTxn (from [LinkedTxn[0].*] columns) ---
        linked_txn_list = []

        col_txn_id = "LinkedTxn[0].TxnId"
        col_txn_type = "LinkedTxn[0].TxnType"
        col_txn_line = "LinkedTxn[0].TxnLineId"

        if col_txn_id in line_cols or col_txn_type in line_cols or col_txn_line in line_cols:
            l_id = line_row.get(col_txn_id) if col_txn_id in line_cols else None
            l_type = line_row.get(col_txn_type) if col_txn_type in line_cols else None
            l_lineid = line_row.get(col_txn_line) if col_txn_line in line_cols else None

            if (pd.notna(l_id) and l_id) or (pd.notna(l_type) and l_type) or (pd.notna(l_lineid) and l_lineid):
                lt = {}
                if pd.notna(l_id):
                    lt["TxnId"] = str(l_id)
                if pd.notna(l_type):
                    lt["TxnType"] = str(l_type)
                if pd.notna(l_lineid):
                    lt["TxnLineId"] = str(l_lineid)
                if lt:
                    linked_txn_list.append(lt)

        if linked_txn_list:
            line_obj["LinkedTxn"] = linked_txn_list

        payload["Line"].append(line_obj)

    if not payload["Line"]:
        logger.warning(f"Deposit Id={header_row.get('Id')} has no valid lines – skipping.")
        return None

    return deep_clean(payload)


# -------------------------------------------------------------------
# Ensure Map_Deposit exists AND seed rows for each Deposit.Id
# -------------------------------------------------------------------
def ensure_map_deposit_table_exists():
    """
    Ensure porter_entities_mapping.Map_Deposit exists with a minimal schema
    and that there is one row per Deposit.Id so update_mapping_status can UPDATE.
    """

    # Ensure schema exists
    create_schema_sql = f"""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{MAPPING_SCHEMA}')
    BEGIN
        EXEC('CREATE SCHEMA {MAPPING_SCHEMA}');
    END;
    """
    sql.run_query(create_schema_sql, ())

    # Ensure table exists
    create_table_sql = f"""
    IF OBJECT_ID('{MAPPING_SCHEMA}.Map_Deposit','U') IS NULL
    BEGIN
        CREATE TABLE [{MAPPING_SCHEMA}].[Map_Deposit] (
            [Source_Id]     NVARCHAR(50) NOT NULL PRIMARY KEY,
            [Target_Id]     NVARCHAR(50) NULL,
            [Porter_Status] NVARCHAR(20) NULL,
            [Retry_Count]   INT NOT NULL CONSTRAINT DF_Map_Deposit_RetryCount DEFAULT(0),
            [Failure_Reason] NVARCHAR(MAX) NULL,
            [Payload_JSON]   NVARCHAR(MAX) NULL,
            [CreatedAtUtc]   DATETIME2(3) NOT NULL 
                CONSTRAINT DF_Map_Deposit_CreatedAtUtc DEFAULT (SYSUTCDATETIME()),
            [UpdatedAtUtc]   DATETIME2(3) NOT NULL 
                CONSTRAINT DF_Map_Deposit_UpdatedAtUtc DEFAULT (SYSUTCDATETIME())
        );
    END;
    """
    sql.run_query(create_table_sql, ())

    # 🔹 Seed one row per Deposit.Id if not already present
    seed_sql = f"""
    INSERT INTO [{MAPPING_SCHEMA}].[Map_Deposit] (
        Source_Id, Target_Id, Porter_Status, Retry_Count, Failure_Reason, Payload_JSON
    )
    SELECT
        CAST(d.Id AS NVARCHAR(50)) AS Source_Id,
        NULL AS Target_Id,
        NULL AS Porter_Status,
        0    AS Retry_Count,
        NULL AS Failure_Reason,
        NULL AS Payload_JSON
    FROM [{SOURCE_SCHEMA}].[Deposit] d
    WHERE NOT EXISTS (
        SELECT 1
        FROM [{MAPPING_SCHEMA}].[Map_Deposit] m
        WHERE m.Source_Id = CAST(d.Id AS NVARCHAR(50))
    );
    """
    sql.run_query(seed_sql, ())


# -------------------------------------------------------------------
# STEP 1: GENERATE PAYLOADS → Map_Deposit.Payload_JSON
# -------------------------------------------------------------------
def generate_all_deposit_payloads(DEPOSIT_DATE_FROM=None, DEPOSIT_DATE_TO=None):
    """
    1) Read Deposit + Deposit_Line from SOURCE_SCHEMA
    2) Build payloads (no mapping) for each Deposit with lines
    3) Store in porter_entities_mapping.Map_Deposit.Payload_JSON with Porter_Status='Ready'
    """
    logger.info("🔧 Generating Deposit payloads (no mapping)...")

    # Make sure Map_Deposit exists and is seeded
    ensure_map_deposit_table_exists()

    # Build WHERE conditions for optional date range
    conds = []
    params = []

    if DEPOSIT_DATE_FROM:
        conds.append("TxnDate >= ?")
        params.append(DEPOSIT_DATE_FROM)
    if DEPOSIT_DATE_TO:
        conds.append("TxnDate <= ?")
        params.append(DEPOSIT_DATE_TO)

    # Headers
    header_sql = f"""
        SELECT
            [DepositToAccountRef.value],
            [DepositToAccountRef.name],
            TotalAmt,
            [domain],
            sparse,
            Id,
            SyncToken,
            [MetaData.CreateTime],
            [MetaData.LastUpdatedTime],
            TxnDate,
            [CurrencyRef.value],
            [CurrencyRef.name],
            PrivateNote,
            DocNumber,
            [CashBack.AccountRef.value],
            [CashBack.AccountRef.name],
            [CashBack.Amount],
            [CashBack.Memo]
        FROM [{SOURCE_SCHEMA}].[Deposit]
        WHERE EXISTS (
            SELECT 1 FROM [{SOURCE_SCHEMA}].[Deposit_Line] l
            WHERE l.Parent_Id = [{SOURCE_SCHEMA}].[Deposit].Id
        )
        {("AND " + " AND ".join(conds)) if conds else ""}
    """

    headers_df = sql.fetch_table_with_params(header_sql, tuple(params))
    if headers_df.empty:
        logger.warning("⚠️ No Deposit headers found for given filters.")
        return

    # Lines – join back to Deposit to reuse same date conditions
    line_conds = []
    line_params = []

    if DEPOSIT_DATE_FROM:
        line_conds.append("d.TxnDate >= ?")
        line_params.append(DEPOSIT_DATE_FROM)
    if DEPOSIT_DATE_TO:
        line_conds.append("d.TxnDate <= ?")
        line_params.append(DEPOSIT_DATE_TO)

    line_where = ""
    if line_conds:
        line_where = "WHERE " + " AND ".join(line_conds)

    line_sql = f"""
        SELECT
            l.Amount,
            l."LinkedTxn[0].TxnId",
            l."LinkedTxn[0].TxnType",
            l."LinkedTxn[0].TxnLineId",
            l.[DepositLineDetail.PaymentMethodRef.value],
            l.[DepositLineDetail.CheckNum],
            l.Parent_Id,
            l.TxnDate,
            l.DocNumber,
            l.Id,
            l.LineNum,
            l.DetailType,
            l.[DepositLineDetail.AccountRef.value],
            l.[DepositLineDetail.AccountRef.name],
            l.Description,
            l.[DepositLineDetail.ClassRef.value],
            l.[DepositLineDetail.ClassRef.name],
            l.[DepositLineDetail.Entity.value],
            l.[DepositLineDetail.Entity.name],
            l.[DepositLineDetail.Entity.type],
            l.[DepositLineDetail.PaymentMethodRef.name],
            l.[ProjectRef.value],
            l.Parent_Entity
        FROM [{SOURCE_SCHEMA}].[Deposit_Line] l
        JOIN [{SOURCE_SCHEMA}].[Deposit] d
          ON d.Id = l.Parent_Id
        {line_where}
    """
    lines_df = sql.fetch_table_with_params(line_sql, tuple(line_params))
    if lines_df.empty:
        logger.warning("⚠️ No Deposit_Line rows found for given filters.")
        return

    lines_grouped = lines_df.groupby("Parent_Id")

    total = len(headers_df)
    success_payload = 0
    failed_payload = 0

    for idx, (_, hrow) in enumerate(headers_df.iterrows(), start=1):
        src_id = hrow.get("Id")
        logger.info(f"➡️ [{idx}/{total}] Building payload for Deposit Source_Id={src_id}")

        if src_id not in lines_grouped.groups:
            logger.warning(f"Deposit Id={src_id} has no lines – marking Failed.")
            update_mapping_status(
                MAPPING_SCHEMA,
                "Map_Deposit",
                src_id,
                "Failed",
                failure_reason="No lines found for Deposit",
                payload=None
            )
            failed_payload += 1
            continue

        dep_lines = lines_grouped.get_group(src_id)
        payload = build_deposit_payload(hrow, dep_lines)
        if not payload:
            logger.warning(f"Deposit Id={src_id}: payload is None/empty – marking Failed.")
            update_mapping_status(
                MAPPING_SCHEMA,
                "Map_Deposit",
                src_id,
                "Failed",
                failure_reason="Empty payload",
                payload=None
            )
            failed_payload += 1
            continue

        # Store pretty JSON in Map_Deposit.Payload_JSON
        pretty_json = json.dumps(payload, indent=2, default=str)
        update_mapping_status(
            MAPPING_SCHEMA,
            "Map_Deposit",
            src_id,
            "Ready",
            payload=json.loads(pretty_json),
            failure_reason=None
        )
        success_payload += 1

    logger.info(f"✅ Payload generation complete. Ready={success_payload}, Failed={failed_payload}")


# -------------------------------------------------------------------
# STEP 2: POST FROM Map_Deposit.Payload_JSON → QBO
# -------------------------------------------------------------------
try:
    import orjson as _orjson

    def _fast_loads(s):
        if isinstance(s, (bytes, bytearray)):
            return _orjson.loads(s)
        if isinstance(s, str):
            return _orjson.loads(s.encode("utf-8"))
        return s
except Exception:  # fallback
    def _fast_loads(s):
        return json.loads(s) if isinstance(s, (str, bytes, bytearray)) else s


def _derive_batch_url(entity_url: str) -> str:
    """
    Turn .../v3/company/<realm>/deposit?minorversion=XX
    into .../v3/company/<realm>/batch?minorversion=XX
    """
    qpos = entity_url.find("?")
    query = entity_url[qpos:] if qpos != -1 else ""
    path = entity_url[:qpos] if qpos != -1 else entity_url
    lower = path.lower()
    if lower.endswith("/deposit"):
        path = path.rsplit("/", 1)[0]
    return f"{path}/batch{query}"


def post_deposit(row):
    """Single-record fallback."""
    sid = row["Source_Id"]

    if row.get("Porter_Status") == "Success":
        return

    retry = int(row.get("Retry_Count") or 0)
    if retry >= 5:
        return

    pj = row.get("Payload_JSON")
    if not pj:
        logger.warning(f"⚠️ Deposit {sid} skipped — missing Payload_JSON")
        return

    try:
        payload = _fast_loads(pj)
    except Exception as e:
        update_mapping_status(
            MAPPING_SCHEMA,
            "Map_Deposit",
            sid,
            "Failed",
            failure_reason=f"Bad JSON: {e}",
            increment_retry=True
        )
        logger.error(f"❌ Deposit {sid}: invalid JSON in Payload_JSON: {e}")
        return

    url, headers = get_qbo_auth()

    try:
        auto_refresh_token_if_needed()
        resp = session.post(url, headers=headers, json=payload, timeout=30)

        if resp.status_code == 200:
            data = resp.json()
            qid = data.get("Deposit", {}).get("Id")
            update_mapping_status(
                MAPPING_SCHEMA,
                "Map_Deposit",
                sid,
                "Success",
                target_id=qid
            )
            logger.info(f"✅ Deposit {sid} → QBO {qid}")
        else:
            reason = (resp.text or "")[:1000]
            update_mapping_status(
                MAPPING_SCHEMA,
                "Map_Deposit",
                sid,
                "Failed",
                failure_reason=reason,
                increment_retry=True
            )
            logger.error(f"❌ Failed Deposit {sid}: {reason}")
    except Exception as e:
        update_mapping_status(
            MAPPING_SCHEMA,
            "Map_Deposit",
            sid,
            "Failed",
            failure_reason=str(e),
            increment_retry=True
        )
        logger.exception(f"❌ Exception posting Deposit {sid}")


def post_deposits_batch(batch_df: pd.DataFrame, batch_limit: int = 20):
    """
    High-throughput QBO Batch posting from Map_Deposit.
    Reads Payload_JSON, sends as Deposit create operations.
    """
    if batch_df.empty:
        return

    # gather work
    work = []
    for _, row in batch_df.iterrows():
        sid = row["Source_Id"]
        status = row.get("Porter_Status")
        retry = int(row.get("Retry_Count") or 0)

        if status == "Success" or retry >= 5:
            continue

        pj = row.get("Payload_JSON")
        if not pj:
            update_mapping_status(
                MAPPING_SCHEMA,
                "Map_Deposit",
                sid,
                "Failed",
                failure_reason="Missing Payload_JSON",
                increment_retry=True
            )
            continue

        try:
            payload = _fast_loads(pj)
        except Exception as e:
            update_mapping_status(
                MAPPING_SCHEMA,
                "Map_Deposit",
                sid,
                "Failed",
                failure_reason=f"Bad JSON: {e}",
                increment_retry=True
            )
            logger.error(f"❌ Deposit {sid}: invalid JSON in batch: {e}")
            continue

        work.append((sid, payload))

    if not work:
        return

    url, headers = get_qbo_auth()
    batch_url = _derive_batch_url(url)

    for i in range(0, len(work), batch_limit):
        chunk = work[i:i + batch_limit]
        batch_body = {
            "BatchItemRequest": [
                {"bId": str(sid), "operation": "create", "Deposit": payload}
                for (sid, payload) in chunk
            ]
        }

        try:
            auto_refresh_token_if_needed()
            resp = session.post(batch_url, headers=headers, json=batch_body, timeout=40)

            if resp.status_code != 200:
                reason = (resp.text or f"HTTP {resp.status_code}")[:1000]
                for sid, _ in chunk:
                    update_mapping_status(
                        MAPPING_SCHEMA,
                        "Map_Deposit",
                        sid,
                        "Failed",
                        failure_reason=reason,
                        increment_retry=True
                    )
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
                    update_mapping_status(
                        MAPPING_SCHEMA,
                        "Map_Deposit",
                        bid,
                        "Success",
                        target_id=qid
                    )
                    logger.info(f"✅ Deposit {bid} → QBO {qid}")
                    continue

                fault = item.get("Fault") or {}
                errs = fault.get("Error") or []
                if errs:
                    msg = errs[0].get("Message") or ""
                    det = errs[0].get("Detail") or ""
                    reason = (msg + " | " + det).strip()[:1000]
                else:
                    reason = "Unknown batch failure"

                update_mapping_status(
                    MAPPING_SCHEMA,
                    "Map_Deposit",
                    bid,
                    "Failed",
                    failure_reason=reason,
                    increment_retry=True
                )
                logger.error(f"❌ Failed Deposit {bid}: {reason}")

            # Any chunk items missing response
            for sid, _ in chunk:
                if str(sid) not in seen:
                    update_mapping_status(
                        MAPPING_SCHEMA,
                        "Map_Deposit",
                        sid,
                        "Failed",
                        failure_reason="No response for bId",
                        increment_retry=True
                    )
                    logger.error(f"❌ No response for Deposit {sid}")
        except Exception as e:
            reason = f"Batch exception: {e}"
            for sid, _ in chunk:
                update_mapping_status(
                    MAPPING_SCHEMA,
                    "Map_Deposit",
                    sid,
                    "Failed",
                    failure_reason=reason,
                    increment_retry=True
                )
            logger.exception(f"❌ Exception during batch POST ({len(chunk)} items)")


def post_deposits_from_mapping(select_batch_size: int = 300, post_batch_limit: int = 20):
    """
    Read Map_Deposit and post all Ready/Failed rows to QBO.
    """
    rows = sql.fetch_table("Map_Deposit", MAPPING_SCHEMA)
    if rows.empty:
        logger.info("⚠️ Map_Deposit is empty – nothing to post.")
        return

    eligible = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("⚠️ No eligible Deposits to post (Ready/Failed).")
        return

    total = len(eligible)
    logger.info(f"📤 Posting {total} Deposit(s) via QBO Batch API (limit {post_batch_limit}/call)...")

    for i in range(0, total, select_batch_size):
        slice_df = eligible.iloc[i:i + select_batch_size]
        post_deposits_batch(slice_df, batch_limit=post_batch_limit)
        done = min(i + select_batch_size, total)
        logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

    # Final single-record fallback for any remaining Failed with null Target_Id
    failed_df = sql.fetch_table("Map_Deposit", MAPPING_SCHEMA)
    failed_records = failed_df[(failed_records["Porter_Status"] == "Failed") & (failed_records["Target_Id"].isnull())]
    if not failed_records.empty:
        logger.info(f"🔁 Reprocessing {len(failed_records)} failed Deposits (single-record fallback)...")
        for _, row in failed_records.iterrows():
            post_deposit(row)

    logger.info("🏁 Deposit posting completed.")


# -------------------------------------------------------------------
# ENTRYPOINT: FIRST GENERATE PAYLOAD, THEN POST
# -------------------------------------------------------------------
def migrate_deposits(DEPOSIT_DATE_FROM=None, DEPOSIT_DATE_TO=None):
    """
    Full flow:
    1) Generate payloads (no mapping) into porter_entities_mapping.Map_Deposit
    2) Post to QBO using those payloads
    """
    print("\n🚀 Starting Deposit Migration (direct restore)\n" + "=" * 50)
    # generate_all_deposit_payloads(DEPOSIT_DATE_FROM, DEPOSIT_DATE_TO)
    post_deposits_from_mapping()
    print("\n🏁 Deposit Migration completed.")


if __name__ == "__main__":
    # Example: migrate all deposits
    migrate_deposits()
    # Or with date filters:
    # migrate_deposits(DEPOSIT_DATE_FROM="2024-01-01", DEPOSIT_DATE_TO="2025-12-31")


#############################
# import os
# import json
# import requests
# import pandas as pd
# from dotenv import load_dotenv

# from storage.sqlserver import sql
# from utils.token_refresher import auto_refresh_token_if_needed, get_qbo_context_migration
# from utils.log_timer import global_logger as logger
# from utils.mapping_updater import update_mapping_status
# from utils.payload_cleaner import deep_clean

# # -------------------------------------------------------------------
# # ENV + BASIC
# # -------------------------------------------------------------------
# load_dotenv()
# auto_refresh_token_if_needed()

# SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
# MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
# API_ENTITY = "deposit"

# session = requests.Session()


# # -------------------------------------------------------------------
# # QBO HELPERS
# # -------------------------------------------------------------------
# def get_qbo_auth():
#     env = os.getenv("QBO_ENVIRONMENT", "sandbox")
#     base = "https://sandbox-quickbooks.api.intuit.com" if env == "sandbox" else "https://quickbooks.api.intuit.com"

#     ctx = get_qbo_context_migration()
#     realm = ctx["REALM_ID"]

#     url = f"{base}/v3/company/{realm}/{API_ENTITY}"
#     headers = {
#         "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
#         "Accept": "application/json",
#         "Content-Type": "application/json"
#     }
#     return url, headers


# def safe_float(val):
#     try:
#         if pd.isna(val):
#             return None
#         return float(val)
#     except Exception:
#         return None


# # -------------------------------------------------------------------
# # BUILD PAYLOAD (NO MAPPING – DIRECT FROM SOURCE COLUMNS)
# # -------------------------------------------------------------------
# def build_deposit_payload(header_row: pd.Series, lines_df: pd.DataFrame) -> dict | None:
#     """
#     Build QBO Deposit payload using ONLY these columns:

#     Header (Deposit):
#       [DepositToAccountRef.value], [DepositToAccountRef.name], TotalAmt, TxnDate,
#       [CurrencyRef.value], [CurrencyRef.name], PrivateNote, DocNumber,
#       [CashBack.AccountRef.value], [CashBack.AccountRef.name],
#       [CashBack.Amount], [CashBack.Memo]

#     Lines (Deposit_Line):
#       Amount,
#       [LinkedTxn[0].TxnId], [LinkedTxn[0].TxnType], [LinkedTxn[0].TxnLineId],
#       [DepositLineDetail.PaymentMethodRef.value], [DepositLineDetail.CheckNum],
#       Parent_Id, TxnDate, DocNumber, Id, LineNum, DetailType,
#       [DepositLineDetail.AccountRef.value], [DepositLineDetail.AccountRef.name],
#       Description,
#       [DepositLineDetail.ClassRef.value], [DepositLineDetail.ClassRef.name],
#       [DepositLineDetail.Entity.value], [DepositLineDetail.Entity.name],
#       [DepositLineDetail.Entity.type],
#       [DepositLineDetail.PaymentMethodRef.name],
#       [ProjectRef.value], Parent_Entity

#     NO Map_* tables are used. We simply pass the source values straight to QBO.
#     """

#     # --- Header ---
#     deposit_to_acct_val = header_row.get("DepositToAccountRef.value")
#     if pd.isna(deposit_to_acct_val) or deposit_to_acct_val is None:
#         logger.warning(f"Skipping Deposit Id={header_row.get('Id')} – missing DepositToAccountRef.value")
#         return None

#     payload = {
#         "DepositToAccountRef": {
#             "value": str(deposit_to_acct_val)
#         },
#         "TxnDate": header_row.get("TxnDate"),
#         "PrivateNote": header_row.get("PrivateNote"),
#         "DocNumber": header_row.get("DocNumber"),
#         "Line": []
#     }

#     # CurrencyRef (optional)
#     curr_val = header_row.get("CurrencyRef.value")
#     curr_name = header_row.get("CurrencyRef.name")
#     if pd.notna(curr_val) or pd.notna(curr_name):
#         cref = {}
#         if pd.notna(curr_val):
#             cref["value"] = str(curr_val)
#         if pd.notna(curr_name):
#             cref["name"] = str(curr_name)
#         payload["CurrencyRef"] = cref

#     # TotalAmt (optional)
#     total_amt = safe_float(header_row.get("TotalAmt"))
#     if total_amt is not None:
#         payload["TotalAmt"] = total_amt

#     # CashBack (optional)
#     cb_acct_val = header_row.get("CashBack.AccountRef.value")
#     cb_amt = safe_float(header_row.get("CashBack.Amount"))
#     cb_memo = header_row.get("CashBack.Memo")
#     cb_name = header_row.get("CashBack.AccountRef.name")

#     if (pd.notna(cb_acct_val) and cb_acct_val) or (cb_amt is not None):
#         cashback = {}
#         if pd.notna(cb_acct_val):
#             cashback["AccountRef"] = {
#                 "value": str(cb_acct_val)
#             }
#             if pd.notna(cb_name):
#                 cashback["AccountRef"]["name"] = str(cb_name)
#         if cb_amt is not None:
#             cashback["Amount"] = cb_amt
#         if pd.notna(cb_memo):
#             cashback["Memo"] = str(cb_memo)
#         if cashback:
#             payload["CashBack"] = cashback

#     # --- Lines ---
#     if lines_df is None or lines_df.empty:
#         logger.warning(f"Deposit Id={header_row.get('Id')} has no lines – skipping.")
#         return None

#     line_cols = set(lines_df.columns)

#     for _, line_row in lines_df.iterrows():
#         amount = safe_float(line_row.get("Amount"))
#         if amount is None:
#             logger.warning(f"Skipping line for Parent_Id={line_row.get('Parent_Id')} – Amount is NULL/invalid")
#             continue

#         detail_type = line_row.get("DetailType")
#         if pd.isna(detail_type) or detail_type is None:
#             detail_type = "DepositLineDetail"

#         line_obj = {
#             "Amount": amount,
#             "DetailType": str(detail_type)
#         }

#         # LineNum (optional)
#         ln = line_row.get("LineNum")
#         if pd.notna(ln):
#             try:
#                 line_obj["LineNum"] = int(float(ln))
#             except Exception:
#                 logger.warning(f"Invalid LineNum={ln} for Parent_Id={line_row.get('Parent_Id')}")

#         # Description (optional)
#         desc = line_row.get("Description")
#         if pd.notna(desc):
#             line_obj["Description"] = str(desc)

#         # --- DepositLineDetail ---
#         detail = {}

#         # AccountRef
#         acct_val = line_row.get("DepositLineDetail.AccountRef.value")
#         acct_name = line_row.get("DepositLineDetail.AccountRef.name")
#         if pd.notna(acct_val):
#             detail["AccountRef"] = {"value": str(acct_val)}
#             if pd.notna(acct_name):
#                 detail["AccountRef"]["name"] = str(acct_name)

#         # ClassRef
#         class_val = line_row.get("DepositLineDetail.ClassRef.value")
#         class_name = line_row.get("DepositLineDetail.ClassRef.name")
#         if pd.notna(class_val):
#             detail["ClassRef"] = {"value": str(class_val)}
#             if pd.notna(class_name):
#                 detail["ClassRef"]["name"] = str(class_name)

#         # Entity
#         ent_val = line_row.get("DepositLineDetail.Entity.value")
#         ent_name = line_row.get("DepositLineDetail.Entity.name")
#         ent_type = line_row.get("DepositLineDetail.Entity.type")
#         if pd.notna(ent_val):
#             ent_obj = {"value": str(ent_val)}
#             if pd.notna(ent_name):
#                 ent_obj["name"] = str(ent_name)
#             if pd.notna(ent_type):
#                 ent_obj["type"] = str(ent_type)
#             detail["Entity"] = ent_obj

#         # PaymentMethodRef
#         pm_val = line_row.get("DepositLineDetail.PaymentMethodRef.value")
#         pm_name = line_row.get("DepositLineDetail.PaymentMethodRef.name")
#         if pd.notna(pm_val):
#             pm_obj = {"value": str(pm_val)}
#             if pd.notna(pm_name):
#                 pm_obj["name"] = str(pm_name)
#             detail["PaymentMethodRef"] = pm_obj

#         # CheckNum
#         check_num = line_row.get("DepositLineDetail.CheckNum")
#         if pd.notna(check_num):
#             detail["CheckNum"] = str(check_num)

#         # ProjectRef (if present in extract)
#         if "ProjectRef.value" in line_cols:
#             proj_val = line_row.get("ProjectRef.value")
#             if pd.notna(proj_val):
#                 detail["ProjectRef"] = {"value": str(proj_val)}

#         if detail:
#             line_obj["DepositLineDetail"] = detail

#         # --- LinkedTxn (from [LinkedTxn[0].*] columns) ---
#         linked_txn_list = []

#         # NOTE: your SELECT had slightly broken brackets; using *corrected* names here:
#         col_txn_id = "LinkedTxn[0].TxnId"
#         col_txn_type = "LinkedTxn[0].TxnType"
#         col_txn_line = "LinkedTxn[0].TxnLineId"

#         if col_txn_id in line_cols or col_txn_type in line_cols or col_txn_line in line_cols:
#             l_id = line_row.get(col_txn_id) if col_txn_id in line_cols else None
#             l_type = line_row.get(col_txn_type) if col_txn_type in line_cols else None
#             l_lineid = line_row.get(col_txn_line) if col_txn_line in line_cols else None

#             if (pd.notna(l_id) and l_id) or (pd.notna(l_type) and l_type) or (pd.notna(l_lineid) and l_lineid):
#                 lt = {}
#                 if pd.notna(l_id):
#                     lt["TxnId"] = str(l_id)
#                 if pd.notna(l_type):
#                     lt["TxnType"] = str(l_type)
#                 if pd.notna(l_lineid):
#                     lt["TxnLineId"] = str(l_lineid)
#                 if lt:
#                     linked_txn_list.append(lt)

#         if linked_txn_list:
#             line_obj["LinkedTxn"] = linked_txn_list

#         payload["Line"].append(line_obj)

#     if not payload["Line"]:
#         logger.warning(f"Deposit Id={header_row.get('Id')} has no valid lines – skipping.")
#         return None

#     return deep_clean(payload)

# def ensure_map_deposit_table_exists():
#     """
#     Ensure porter_entities_mapping.Map_Deposit exists with a minimal schema
#     compatible with update_mapping_status (Source_Id, Target_Id, Porter_Status,
#     Retry_Count, Failure_Reason, Payload_JSON).
#     """

#     # Ensure schema exists
#     create_schema_sql = f"""
#     IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{MAPPING_SCHEMA}')
#     BEGIN
#         EXEC('CREATE SCHEMA {MAPPING_SCHEMA}');
#     END;
#     """
#     sql.run_query(create_schema_sql, ())

#     # Ensure table exists
#     create_table_sql = f"""
#     IF OBJECT_ID('{MAPPING_SCHEMA}.Map_Deposit','U') IS NULL
#     BEGIN
#         CREATE TABLE [{MAPPING_SCHEMA}].[Map_Deposit] (
#             [Source_Id]     NVARCHAR(50) NOT NULL PRIMARY KEY,
#             [Target_Id]     NVARCHAR(50) NULL,
#             [Porter_Status] NVARCHAR(20) NULL,
#             [Retry_Count]   INT NOT NULL CONSTRAINT DF_Map_Deposit_RetryCount DEFAULT(0),
#             [Failure_Reason] NVARCHAR(MAX) NULL,
#             [Payload_JSON]   NVARCHAR(MAX) NULL,
#             [CreatedAtUtc]   DATETIME2(3) NOT NULL 
#                 CONSTRAINT DF_Map_Deposit_CreatedAtUtc DEFAULT (SYSUTCDATETIME()),
#             [UpdatedAtUtc]   DATETIME2(3) NOT NULL 
#                 CONSTRAINT DF_Map_Deposit_UpdatedAtUtc DEFAULT (SYSUTCDATETIME())
#         );
#     END;
#     """
#     sql.run_query(create_table_sql, ())

# # -------------------------------------------------------------------
# # STEP 1: GENERATE PAYLOADS → Map_Deposit.Payload_JSON
# # -------------------------------------------------------------------
# def generate_all_deposit_payloads(DEPOSIT_DATE_FROM=None, DEPOSIT_DATE_TO=None):
#     """
#     1) Read Deposit + Deposit_Line from SOURCE_SCHEMA
#     2) Build payloads (no mapping) for each Deposit with lines
#     3) Store in porter_entities_mapping.Map_Deposit.Payload_JSON with Porter_Status='Ready'
#     """
#     logger.info("🔧 Generating Deposit payloads (no mapping)...")

#     ensure_map_deposit_table_exists()
#     # Build WHERE conditions for optional date range
#     conds = []
#     params = []

#     if DEPOSIT_DATE_FROM:
#         conds.append("TxnDate >= ?")
#         params.append(DEPOSIT_DATE_FROM)
#     if DEPOSIT_DATE_TO:
#         conds.append("TxnDate <= ?")
#         params.append(DEPOSIT_DATE_TO)

#     where_clause = ""
#     if conds:
#         where_clause = "WHERE " + " AND ".join(conds)

#     # Headers
#     header_sql = f"""
#         SELECT
#             "DepositToAccountRef.value",
#             "DepositToAccountRef.name",
#             TotalAmt,
#             "domain",
#             sparse,
#             Id,
#             SyncToken,
#             "MetaData.CreateTime",
#             "MetaData.LastUpdatedTime",
#             TxnDate,
#             "CurrencyRef.value",
#             "CurrencyRef.name",
#             PrivateNote,
#             DocNumber,
#             "CashBack.AccountRef.value",
#             "CashBack.AccountRef.name",
#             "CashBack.Amount",
#             "CashBack.Memo"
#         FROM [{SOURCE_SCHEMA}].[Deposit]
#         WHERE EXISTS (
#             SELECT 1 FROM [{SOURCE_SCHEMA}].[Deposit_Line] l
#             WHERE l.Parent_Id = [{SOURCE_SCHEMA}].[Deposit].Id
#         )
#         {("AND " + " AND ".join(conds)) if conds else ""}
#     """

#     headers_df = sql.fetch_table_with_params(header_sql, tuple(params))
#     if headers_df.empty:
#         logger.warning("⚠️ No Deposit headers found for given filters.")
#         return

#     # Lines – join back to Deposit to reuse same date conditions
#     line_conds = []
#     line_params = []

#     if DEPOSIT_DATE_FROM:
#         line_conds.append("d.TxnDate >= ?")
#         line_params.append(DEPOSIT_DATE_FROM)
#     if DEPOSIT_DATE_TO:
#         line_conds.append("d.TxnDate <= ?")
#         line_params.append(DEPOSIT_DATE_TO)

#     line_where = ""
#     if line_conds:
#         line_where = "WHERE " + " AND ".join(line_conds)

#     line_sql = f"""
#         SELECT
#             l.Amount,
#             l."LinkedTxn[0].TxnId",
#             l."LinkedTxn[0].TxnType",
#             l."LinkedTxn[0].TxnLineId",
#             l."DepositLineDetail.PaymentMethodRef.value",
#             l."DepositLineDetail.CheckNum",
#             l.Parent_Id,
#             l.TxnDate,
#             l.DocNumber,
#             l.Id,
#             l.LineNum,
#             l.DetailType,
#             l.[DepositLineDetail.AccountRef.value],
#             l.[DepositLineDetail.AccountRef.name],
#             l.Description,
#             l.[DepositLineDetail.ClassRef.value],
#             l.[DepositLineDetail.ClassRef.name],
#             l.[DepositLineDetail.Entity.value],
#             l.[DepositLineDetail.Entity.name],
#             l.[DepositLineDetail.Entity.type],
#             l.[DepositLineDetail.PaymentMethodRef.name],
#             l.[ProjectRef.value],
#             l.Parent_Entity
#         FROM [{SOURCE_SCHEMA}].[Deposit_Line] l
#         JOIN [{SOURCE_SCHEMA}].[Deposit] d
#           ON d.Id = l.Parent_Id
#         {line_where}
#     """
#     lines_df = sql.fetch_table_with_params(line_sql, tuple(line_params))
#     if lines_df.empty:
#         logger.warning("⚠️ No Deposit_Line rows found for given filters.")
#         return

#     lines_grouped = lines_df.groupby("Parent_Id")

#     total = len(headers_df)
#     success_payload = 0
#     failed_payload = 0

#     for idx, (_, hrow) in enumerate(headers_df.iterrows(), start=1):
#         src_id = hrow.get("Id")
#         logger.info(f"➡️ [{idx}/{total}] Building payload for Deposit Source_Id={src_id}")

#         if src_id not in lines_grouped.groups:
#             logger.warning(f"Deposit Id={src_id} has no lines – marking Failed.")
#             update_mapping_status(
#                 MAPPING_SCHEMA,
#                 "Map_Deposit",
#                 src_id,
#                 "Failed",
#                 failure_reason="No lines found for Deposit",
#                 payload=None
#             )
#             failed_payload += 1
#             continue

#         dep_lines = lines_grouped.get_group(src_id)
#         payload = build_deposit_payload(hrow, dep_lines)
#         if not payload:
#             logger.warning(f"Deposit Id={src_id}: payload is None/empty – marking Failed.")
#             update_mapping_status(
#                 MAPPING_SCHEMA,
#                 "Map_Deposit",
#                 src_id,
#                 "Failed",
#                 failure_reason="Empty payload",
#                 payload=None
#             )
#             failed_payload += 1
#             continue

#         # Store pretty JSON in Map_Deposit.Payload_JSON (same style as your earlier scripts)
#         pretty_json = json.dumps(payload, indent=2, default=str)
#         update_mapping_status(
#             MAPPING_SCHEMA,
#             "Map_Deposit",
#             src_id,
#             "Ready",
#             payload=json.loads(pretty_json),
#             failure_reason=None
#         )
#         success_payload += 1

#     logger.info(f"✅ Payload generation complete. Ready={success_payload}, Failed={failed_payload}")


# # -------------------------------------------------------------------
# # STEP 2: POST FROM Map_Deposit.Payload_JSON → QBO
# # -------------------------------------------------------------------
# # tiny JSON helper
# try:
#     import orjson as _orjson

#     def _fast_loads(s):
#         if isinstance(s, (bytes, bytearray)):
#             return _orjson.loads(s)
#         if isinstance(s, str):
#             return _orjson.loads(s.encode("utf-8"))
#         return s
# except Exception:  # fallback
#     def _fast_loads(s):
#         return json.loads(s) if isinstance(s, (str, bytes, bytearray)) else s


# def _derive_batch_url(entity_url: str) -> str:
#     """
#     Turn .../v3/company/<realm>/deposit?minorversion=XX
#     into .../v3/company/<realm>/batch?minorversion=XX
#     """
#     qpos = entity_url.find("?")
#     query = entity_url[qpos:] if qpos != -1 else ""
#     path = entity_url[:qpos] if qpos != -1 else entity_url
#     lower = path.lower()
#     if lower.endswith("/deposit"):
#         path = path.rsplit("/", 1)[0]
#     return f"{path}/batch{query}"


# def post_deposit(row):
#     """Single-record fallback."""
#     sid = row["Source_Id"]

#     if row.get("Porter_Status") == "Success":
#         return

#     retry = int(row.get("Retry_Count") or 0)
#     if retry >= 5:
#         return

#     pj = row.get("Payload_JSON")
#     if not pj:
#         logger.warning(f"⚠️ Deposit {sid} skipped — missing Payload_JSON")
#         return

#     try:
#         payload = _fast_loads(pj)
#     except Exception as e:
#         update_mapping_status(
#             MAPPING_SCHEMA,
#             "Map_Deposit",
#             sid,
#             "Failed",
#             failure_reason=f"Bad JSON: {e}",
#             increment_retry=True
#         )
#         logger.error(f"❌ Deposit {sid}: invalid JSON in Payload_JSON: {e}")
#         return

#     url, headers = get_qbo_auth()

#     try:
#         auto_refresh_token_if_needed()
#         resp = session.post(url, headers=headers, json=payload, timeout=30)

#         if resp.status_code == 200:
#             data = resp.json()
#             qid = data.get("Deposit", {}).get("Id")
#             update_mapping_status(
#                 MAPPING_SCHEMA,
#                 "Map_Deposit",
#                 sid,
#                 "Success",
#                 target_id=qid
#             )
#             logger.info(f"✅ Deposit {sid} → QBO {qid}")
#         else:
#             reason = (resp.text or "")[:1000]
#             update_mapping_status(
#                 MAPPING_SCHEMA,
#                 "Map_Deposit",
#                 sid,
#                 "Failed",
#                 failure_reason=reason,
#                 increment_retry=True
#             )
#             logger.error(f"❌ Failed Deposit {sid}: {reason}")
#     except Exception as e:
#         update_mapping_status(
#             MAPPING_SCHEMA,
#             "Map_Deposit",
#             sid,
#             "Failed",
#             failure_reason=str(e),
#             increment_retry=True
#         )
#         logger.exception(f"❌ Exception posting Deposit {sid}")


# def post_deposits_batch(batch_df: pd.DataFrame, batch_limit: int = 20):
#     """
#     High-throughput QBO Batch posting from Map_Deposit.
#     Reads Payload_JSON, sends as Deposit create operations.
#     """
#     if batch_df.empty:
#         return

#     # gather work
#     work = []
#     for _, row in batch_df.iterrows():
#         sid = row["Source_Id"]
#         status = row.get("Porter_Status")
#         retry = int(row.get("Retry_Count") or 0)

#         if status == "Success" or retry >= 5:
#             continue

#         pj = row.get("Payload_JSON")
#         if not pj:
#             update_mapping_status(
#                 MAPPING_SCHEMA,
#                 "Map_Deposit",
#                 sid,
#                 "Failed",
#                 failure_reason="Missing Payload_JSON",
#                 increment_retry=True
#             )
#             continue

#         try:
#             payload = _fast_loads(pj)
#         except Exception as e:
#             update_mapping_status(
#                 MAPPING_SCHEMA,
#                 "Map_Deposit",
#                 sid,
#                 "Failed",
#                 failure_reason=f"Bad JSON: {e}",
#                 increment_retry=True
#             )
#             logger.error(f"❌ Deposit {sid}: invalid JSON in batch: {e}")
#             continue

#         work.append((sid, payload))

#     if not work:
#         return

#     url, headers = get_qbo_auth()
#     batch_url = _derive_batch_url(url)

#     for i in range(0, len(work), batch_limit):
#         chunk = work[i:i + batch_limit]
#         batch_body = {
#             "BatchItemRequest": [
#                 {"bId": str(sid), "operation": "create", "Deposit": payload}
#                 for (sid, payload) in chunk
#             ]
#         }

#         try:
#             auto_refresh_token_if_needed()
#             resp =session.post(batch_url, headers=headers, json=batch_body, timeout=40)

#             if resp.status_code != 200:
#                 reason = (resp.text or f"HTTP {resp.status_code}")[:1000]
#                 for sid, _ in chunk:
#                     update_mapping_status(
#                         MAPPING_SCHEMA,
#                         "Map_Deposit",
#                         sid,
#                         "Failed",
#                         failure_reason=reason,
#                         increment_retry=True
#                     )
#                 logger.error(f"❌ Batch POST failed ({len(chunk)} items): {reason}")
#                 continue

#             rj = resp.json()
#             items = rj.get("BatchItemResponse", []) or []
#             seen = set()

#             for item in items:
#                 bid = item.get("bId")
#                 seen.add(bid)

#                 dep = item.get("Deposit")
#                 if dep and "Id" in dep:
#                     qid = dep["Id"]
#                     update_mapping_status(
#                         MAPPING_SCHEMA,
#                         "Map_Deposit",
#                         bid,
#                         "Success",
#                         target_id=qid
#                     )
#                     logger.info(f"✅ Deposit {bid} → QBO {qid}")
#                     continue

#                 fault = item.get("Fault") or {}
#                 errs = fault.get("Error") or []
#                 if errs:
#                     msg = errs[0].get("Message") or ""
#                     det = errs[0].get("Detail") or ""
#                     reason = (msg + " | " + det).strip()[:1000]
#                 else:
#                     reason = "Unknown batch failure"

#                 update_mapping_status(
#                     MAPPING_SCHEMA,
#                     "Map_Deposit",
#                     bid,
#                     "Failed",
#                     failure_reason=reason,
#                     increment_retry=True
#                 )
#                 logger.error(f"❌ Failed Deposit {bid}: {reason}")

#             # Any chunk items missing response
#             for sid, _ in chunk:
#                 if str(sid) not in seen:
#                     update_mapping_status(
#                         MAPPING_SCHEMA,
#                         "Map_Deposit",
#                         sid,
#                         "Failed",
#                         failure_reason="No response for bId",
#                         increment_retry=True
#                     )
#                     logger.error(f"❌ No response for Deposit {sid}")
#         except Exception as e:
#             reason = f"Batch exception: {e}"
#             for sid, _ in chunk:
#                 update_mapping_status(
#                     MAPPING_SCHEMA,
#                     "Map_Deposit",
#                     sid,
#                     "Failed",
#                     failure_reason=reason,
#                     increment_retry=True
#                 )
#             logger.exception(f"❌ Exception during batch POST ({len(chunk)} items)")


# def post_deposits_from_mapping(select_batch_size: int = 300, post_batch_limit: int = 20):
#     """
#     Read Map_Deposit and post all Ready/Failed rows to QBO.
#     """
#     rows = sql.fetch_table("Map_Deposit", MAPPING_SCHEMA)
#     if rows.empty:
#         logger.info("⚠️ Map_Deposit is empty – nothing to post.")
#         return

#     eligible = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
#     if eligible.empty:
#         logger.info("⚠️ No eligible Deposits to post (Ready/Failed).")
#         return

#     total = len(eligible)
#     logger.info(f"📤 Posting {total} Deposit(s) via QBO Batch API (limit {post_batch_limit}/call)...")

#     for i in range(0, total, select_batch_size):
#         slice_df = eligible.iloc[i:i + select_batch_size]
#         post_deposits_batch(slice_df, batch_limit=post_batch_limit)
#         done = min(i + select_batch_size, total)
#         logger.info(f"⏱️ {done}/{total} processed ({done * 100 // total}%)")

#     # Final single-record fallback for any remaining Failed with null Target_Id
#     failed_df = sql.fetch_table("Map_Deposit", MAPPING_SCHEMA)
#     failed_records = failed_df[(failed_df["Porter_Status"] == "Failed") & (failed_df["Target_Id"].isnull())]
#     if not failed_records.empty:
#         logger.info(f"🔁 Reprocessing {len(failed_records)} failed Deposits (single-record fallback)...")
#         for _, row in failed_records.iterrows():
#             post_deposit(row)

#     logger.info("🏁 Deposit posting completed.")


# # -------------------------------------------------------------------
# # ENTRYPOINT: FIRST GENERATE PAYLOAD, THEN POST
# # -------------------------------------------------------------------
# def migrate_deposits(DEPOSIT_DATE_FROM=None, DEPOSIT_DATE_TO=None):
#     """
#     Full flow:
#     1) Generate payloads (no mapping) into porter_entities_mapping.Map_Deposit
#     2) Post to QBO using those payloads
#     """
#     print("\n🚀 Starting Deposit Migration (direct restore)\n" + "=" * 50)
#     generate_all_deposit_payloads(DEPOSIT_DATE_FROM, DEPOSIT_DATE_TO)
#     post_deposits_from_mapping()
#     print("\n🏁 Deposit Migration completed.")


# if __name__ == "__main__":
#     # Example: migrate all deposits
#     migrate_deposits()

#     # Or with date filters:
#     # migrate_deposits(DEPOSIT_DATE_FROM="2024-01-01", DEPOSIT_DATE_TO="2025-12-31")
