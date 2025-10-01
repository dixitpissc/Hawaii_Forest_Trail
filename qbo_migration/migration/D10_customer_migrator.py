# """
# Sequence : 10
# Module: customer_migrator.py
# Author: Dixit Prajapati
# Created: 2025-09-17
# Description: Handles migration of customer records from source system to QBO,
#              including parent-child hierarchy processing, retry logic, and status tracking.
# Production : Ready
# Phase : 02 - Multi User
# """

import os, re, json, requests, pandas as pd
from dotenv import load_dotenv
try:
    import orjson
    def dumps_fast(obj): return orjson.dumps(obj, option=orjson.OPT_INDENT_2).decode("utf-8")
    def loads_fast(s):   return orjson.loads(s)
except Exception:
    def dumps_fast(obj): return json.dumps(obj, ensure_ascii=False, indent=2)
    def loads_fast(s):   return json.loads(s)
from storage.sqlserver import sql
from config.mapping.customer_mapping import CUSTOMER_COLUMN_MAPPING as column_mapping
from utils.token_refresher import auto_refresh_token_if_needed,get_qbo_context_migration
from utils.retry_handler import initialize_mapping_table, get_retryable_subset
from utils.log_timer import global_logger as logger, ProgressTimer
# keep update_mapping_status import for posting path only
from utils.mapping_updater import update_mapping_status

# ────────────────────────────────────────────────────────────────────
# Bootstrap & Config
# ────────────────────────────────────────────────────────────────────
auto_refresh_token_if_needed()
load_dotenv()

ctx = get_qbo_context_migration()

access_token = ctx['ACCESS_TOKEN']
realm_id = ctx["REALM_ID"]
environment  = os.getenv("QBO_ENVIRONMENT", "sandbox")
base_url     = "https://sandbox-quickbooks.api.intuit.com" if environment == "sandbox" else "https://quickbooks.api.intuit.com"
query_url    = f"{base_url}/v3/company/{realm_id}/query"
post_url     = f"{base_url}/v3/company/{realm_id}/customer"
headers      = {"Authorization": f"Bearer {access_token}", "Accept": "application/json", "Content-Type": "application/json"}

# DB config
source_schema  = os.getenv("SOURCE_SCHEMA", "dbo")
mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
source_table   = "Customer"
mapping_table  = "Map_Customer"

# Performance toggles
batch_size = int(os.getenv("CUSTOMER_BATCH_SIZE", "1000"))
max_retries = 3
ENABLE_CUSTOMER_EXISTENCE_SCAN = os.getenv("ENABLE_CUSTOMER_EXISTENCE_SCAN", "false").lower() == "true"

# HTTP session
session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(pool_connections=40, pool_maxsize=100, max_retries=3)
session.mount("https://", _adapter)
session.mount("http://", _adapter)

# ────────────────────────────────────────────────────────────────────
# Helpers (same logic, trimmed)
# ────────────────────────────────────────────────────────────────────

def _try_exact_lookup_customer_id(name: str) -> str | None:
    """Fast path: exact match DisplayName lookup; falls back to None if not found or parser fails."""
    if not name:
        return None
    safe = name.replace("'", "''")
    q = f"SELECT Id FROM Customer WHERE DisplayName = '{safe}'"
    # Use the same qbo_query wrapper to auto-fallback across content-types
    r, ok, _ = qbo_query(query_url, q, access_token)
    if not ok:
        return None
    items = r.json().get("QueryResponse", {}).get("Customer", [])
    if items:
        return items[0].get("Id")
    return None

def _norm_name(s: str) -> str:
    if s is None: return ""
    return (str(s).replace("\u2019", "'").replace("\u2018", "'").replace("\u00A0", " ").strip())

def _normalize_query(q: str) -> str:
    q = (q or "").replace("\u00A0", " ").replace("\r", " ").replace("\n", " ")
    return re.sub(r"\s+", " ", q).strip().rstrip(";")

def qbo_query(query_url: str, query: str, access_token: str, timeout: int = 30):
    q = _normalize_query(query)
    h1 = {"Authorization": f"Bearer {access_token}", "Accept":"application/json", "Content-Type":"text/plain"}
    h2 = {"Authorization": f"Bearer {access_token}", "Accept":"application/json", "Content-Type":"application/text"}
    r = session.post(query_url, headers=h1, data=q.encode("utf-8"), timeout=timeout)
    if r.status_code == 200: return r, True, None

    err_kind = None
    try:
        fault = r.json().get("Fault", {}).get("Error", [{}])[0]
        if r.status_code == 400 and ("QueryParserError" in str(fault.get("Message","")) or str(fault.get("code","")) == "4000"):
            err_kind = "parser"
    except Exception:
        pass

    if err_kind == "parser":
        r2 = session.post(query_url, headers=h2, data=q.encode("utf-8"), timeout=timeout)
        if r2.status_code == 200: return r2, True, None
        r3 = session.get(query_url, headers={"Authorization": f"Bearer {access_token}", "Accept":"application/json"}, params={"query": q}, timeout=timeout)
        return r3, (r3.status_code==200), "parser"

    return r, False, None

def _parse_qbo_error(resp):
    try:
        data = resp.json() if hasattr(resp,"json") else {}
        err = (data.get("Fault", {}) or {}).get("Error", []) or []
        if err:
            e0 = err[0] or {}
            code = str(e0.get("code")) if e0.get("code") is not None else None
            return code, e0.get("Message"), e0.get("Detail")
    except Exception:
        pass
    try:
        return None, None, resp.text[:300]
    except Exception:
        return None, None, None

def _best_name_from_payload(payload: dict) -> str:
    for k in ("DisplayName", "CompanyName", "PrintOnCheckName"):
        v = payload.get(k)
        if isinstance(v, str) and v.strip(): return _norm_name(v)
    return ""

# ────────────────────────────────────────────────────────────────────
# Optional: CustomerType sync (unchanged behavior)
# ────────────────────────────────────────────────────────────────────
def sync_qbo_customertype_to_map():
    all_types, start_pos, page_size = [], 1, 100
    try:
        while True:
            q = f"SELECT Id, Name FROM CustomerType STARTPOSITION {start_pos} MAXRESULTS {page_size}"
            r = session.post(query_url, headers={"Authorization": f"Bearer {access_token}","Accept":"application/json","Content-Type":"application/text"}, data=q)
            if r.status_code != 200: break
            page = r.json().get("QueryResponse", {}).get("CustomerType", [])
            if not page: break
            all_types.extend(page); start_pos += page_size
        if not all_types: return
        df = pd.DataFrame(all_types)[["Id","Name"]].rename(columns={"Id":"Target_Id"})
        df["Normalized_Name"] = df["Name"].astype(str).str.strip().str.lower().str.replace("\u00a0"," ", regex=False)
        df["Porter_Status"] = "Fetched"
        sql.ensure_schema_exists(mapping_schema)
        table = "Map_CustomerType"
        if not sql.table_exists(table, mapping_schema):
            sql.run_query(f"""
                CREATE TABLE [{mapping_schema}].[{table}](
                    Target_Id NVARCHAR(100), Name NVARCHAR(255),
                    Normalized_Name NVARCHAR(255), Porter_Status VARCHAR(50)
                )
            """)
        else:
            sql.run_query(f"DELETE FROM [{mapping_schema}].[{table}]")
        sql.insert_dataframe(df, table, mapping_schema)
    except Exception:
        logger.exception("CustomerType sync failed")

def ensure_additional_columns():
    alter_cols = [
        ("Payload_JSON", "NVARCHAR(MAX)"),
        ("Porter_Status", "VARCHAR(50)"),
        ("Failure_Reason", "NVARCHAR(1000)"),
        ("Target_Id", "VARCHAR(100)"),
        ("Retry_Count", "INT"),
        ("Source_CustomerType_Name", "NVARCHAR(255)"),
        ("Normalized_CustomerType_Name", "NVARCHAR(255)"),
        ("Target_CustomerType_Id", "VARCHAR(100)"),
        ("Target_Term_Id", "VARCHAR(100)"),
    ]
    for col, col_type in alter_cols:
        sql.run_query(f"""
            IF COL_LENGTH('{mapping_schema}.{mapping_table}', '{col}') IS NULL
            BEGIN
                ALTER TABLE [{mapping_schema}].[{mapping_table}] ADD {col} {col_type}
            END
        """)

def enrich_source_customertype_name():
    try:
        chk = sql.fetch_dataframe(f"""
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA='{source_schema}' AND TABLE_NAME='CustomerType'
        """)
        if chk.empty: return
        sql.run_query(f"""
            UPDATE M
            SET M.Source_CustomerType_Name = CT.Name,
                M.Normalized_CustomerType_Name = LOWER(LTRIM(RTRIM(REPLACE(CT.Name, CHAR(160),' '))))
            FROM [{mapping_schema}].[{mapping_table}] M
            INNER JOIN [{source_schema}].[{source_table}] S ON M.Source_Id = S.Id
            LEFT JOIN [{source_schema}].[CustomerType] CT ON S.[CustomerTypeRef.value] = CT.Id
            WHERE M.Source_CustomerType_Name IS NULL AND S.[CustomerTypeRef.value] IS NOT NULL
        """)
    except Exception:
        logger.exception("CustomerType name enrich failed")

def map_customertype_ids_to_mapping_table():
    try:
        chk = sql.fetch_dataframe(f"""
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA='{mapping_schema}' AND TABLE_NAME='Map_CustomerType'
        """)
        if chk.empty: return
        sql.run_query(f"""
            UPDATE MC
            SET MC.Target_CustomerType_Id = MCT.Target_Id
            FROM [{mapping_schema}].[{mapping_table}] MC
            INNER JOIN [{mapping_schema}].[Map_CustomerType] MCT
                ON MC.Normalized_CustomerType_Name = MCT.Normalized_Name
            WHERE MC.Target_CustomerType_Id IS NULL
              AND MC.Source_CustomerType_Name IS NOT NULL
        """)
    except Exception:
        logger.exception("CustomerType id map failed")

# ────────────────────────────────────────────────────────────────────
# Preloaded maps (PaymentMethod & Term) – O(1) lookups
# ────────────────────────────────────────────────────────────────────
_paymentmethod_map, _term_map = {}, {}

def preload_paymentmethod_map():
    global _paymentmethod_map
    try:
        df = sql.fetch_dataframe(f"SELECT Source_Id, Target_Id FROM {mapping_schema}.Map_PaymentMethod WHERE Target_Id IS NOT NULL")
        _paymentmethod_map = {str(r.Source_Id): str(r.Target_Id) for _, r in df.iterrows()}
        logger.info(f"🗺️ PaymentMethod map: {len(_paymentmethod_map)}")
    except Exception as e:
        _paymentmethod_map = {}
        logger.warning(f"PaymentMethod preload failed: {e}")

def preload_term_map():
    global _term_map
    try:
        df = sql.fetch_dataframe(f"SELECT Source_Id, Target_Id FROM {mapping_schema}.Map_Term WHERE Target_Id IS NOT NULL")
        _term_map = {str(r.Source_Id): str(r.Target_Id) for _, r in df.iterrows()}
        logger.info(f"🗺️ Term map: {len(_term_map)}")
    except Exception as e:
        _term_map = {}
        logger.warning(f"Term preload failed: {e}")

# def backfill_target_term_ids_into_map_customer():
#     try:
#         sql.run_query(f"""
#             UPDATE MC
#             SET MC.Target_Term_Id = MT.Target_Id
#             FROM [{mapping_schema}].[{mapping_table}] MC
#             LEFT JOIN [{mapping_schema}].[Map_Term] MT
#               ON TRY_CONVERT(VARCHAR(100), MC.[SalesTermRef.value]) = TRY_CONVERT(VARCHAR(100), MT.Source_Id)
#             WHERE MC.Target_Term_Id IS NULL
#               AND MC.[SalesTermRef.value] IS NOT NULL
#         """)
#     except Exception as e:
#         logger.warning(f"Backfill Target_Term_Id failed: {e}")

def backfill_target_term_ids_into_map_customer():
    try:
        # --- helpers (local) ---
        def _table_exists(schema: str, table: str) -> bool:
            df = sql.fetch_table_with_params(
                "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                (schema, table)
            )
            return not df.empty

        def _column_exists(schema: str, table: str, column: str) -> bool:
            df = sql.fetch_table_with_params(
                "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
                (schema, table, column)
            )
            return not df.empty

        # --- prechecks ---
        if not _table_exists(mapping_schema, mapping_table):
            logger.info(f"⏩ Skip: [{mapping_schema}].[{mapping_table}] does not exist.")
            return

        if not _column_exists(mapping_schema, mapping_table, "SalesTermRef.value"):
            logger.info("⏩ Skip: column [SalesTermRef.value] not found in "
                        f"[{mapping_schema}].[{mapping_table}].")
            return

        if not _column_exists(mapping_schema, mapping_table, "Target_Term_Id"):
            logger.info("⏩ Skip: column [Target_Term_Id] not found in "
                        f"[{mapping_schema}].[{mapping_table}].")
            return

        if not _table_exists(mapping_schema, "Map_Term"):
            logger.info(f"⏩ Skip: [{mapping_schema}].[Map_Term] does not exist.")
            return

        if not _column_exists(mapping_schema, "Map_Term", "Target_Id"):
            logger.info(f"⏩ Skip: column [Target_Id] not found in "
                        f"[{mapping_schema}].[Map_Term].")
            return

        # --- do the backfill ---
        rows_before = sql.fetch_table_with_params(
            f"""
            SELECT COUNT(1) AS cnt
            FROM [{mapping_schema}].[{mapping_table}]
            WHERE Target_Term_Id IS NULL AND [SalesTermRef.value] IS NOT NULL
            """,
            tuple()
        )
        to_update = int(rows_before.iloc[0]["cnt"]) if not rows_before.empty else 0

        if to_update == 0:
            logger.info("✅ No rows need backfill for Target_Term_Id.")
            return

        sql.run_query(f"""
            UPDATE MC
            SET MC.Target_Term_Id = MT.Target_Id
            FROM [{mapping_schema}].[{mapping_table}] MC
            LEFT JOIN [{mapping_schema}].[Map_Term] MT
              ON TRY_CONVERT(VARCHAR(100), MC.[SalesTermRef.value]) = TRY_CONVERT(VARCHAR(100), MT.Source_Id)
            WHERE MC.Target_Term_Id IS NULL
              AND MC.[SalesTermRef.value] IS NOT NULL
        """)

        rows_after = sql.fetch_table_with_params(
            f"""
            SELECT COUNT(1) AS cnt
            FROM [{mapping_schema}].[{mapping_table}]
            WHERE Target_Term_Id IS NULL AND [SalesTermRef.value] IS NOT NULL
            """,
            tuple()
        )
        still_null = int(rows_after.iloc[0]["cnt"]) if not rows_after.empty else 0
        updated = to_update - still_null

        logger.info(f"✅ Backfill Target_Term_Id completed. Updated: {updated}, Still null: {still_null}")

    except Exception as e:
        logger.warning(f"Back fill Target_Term_Id failed: {e}")




# ────────────────────────────────────────────────────────────────────
# Payload builder (vectorized-friendly)
# ────────────────────────────────────────────────────────────────────
# Build once: source columns we need from mapping table
_needed_src_cols = set(["Source_Id","DisplayName","FullyQualifiedName","CompanyName",
                        "PaymentMethodRef.value","SalesTermRef.value","Target_CustomerType_Id","Target_Term_Id"])
_needed_src_cols.update(list(column_mapping.keys()))

def _name_from_row(row_dict):
    return row_dict.get("DisplayName") or row_dict.get("FullyQualifiedName") or row_dict.get("CompanyName")

def build_payload_from_row(row_dict: dict) -> dict:
    """Fast payload builder that uses dicts & preloaded maps only."""
    payload = {}
    for src_col, qbo_col in column_mapping.items():
        v = row_dict.get(src_col)
        if v is None: continue
        if isinstance(v, str) and not v.strip(): continue
        if "." in qbo_col:
            p, c = qbo_col.split(".")
            payload.setdefault(p, {})[c] = v
        else:
            payload[qbo_col] = v

    # PaymentMethodRef via map
    pm_src = row_dict.get("PaymentMethodRef.value")
    if pm_src is not None:
        mapped_pm = _paymentmethod_map.get(str(pm_src))
        if mapped_pm:
            payload["PaymentMethodRef"] = {"value": mapped_pm}

    # CustomerTypeRef via mapped id
    t_ct = row_dict.get("Target_CustomerType_Id")
    if t_ct: payload["CustomerTypeRef"] = {"value": str(t_ct)}

    # SalesTermRef via mapped id (Target_Term_Id first, else map by sales term source id)
    t_term = row_dict.get("Target_Term_Id")
    if not t_term:
        st_src = row_dict.get("SalesTermRef.value")
        if st_src is not None:
            t_term = _term_map.get(str(st_src))
    if t_term:
        payload["SalesTermRef"] = {"value": str(t_term)}

    payload["Active"] = True
    # Ensure name present (post step also relies on this for dup handling)
    name = _name_from_row(row_dict)
    if name and "DisplayName" not in payload:
        payload["DisplayName"] = name
    return payload

# ────────────────────────────────────────────────────────────────────
# FAST: Bulk payload staging (no existence scan)
# ────────────────────────────────────────────────────────────────────
def stage_payloads_bulk():
    """
    Build payloads for rows with NULL Payload_JSON and write them back in set-based batches.
    We SKIP QBO existence scans here for speed (duplicates handled on post with code=6240).
    """
    # Pull only columns we need (fallback to SELECT * if necessary)
    df = sql.fetch_dataframe(f"""
        SELECT *
        FROM [{mapping_schema}].[{mapping_table}]
        WHERE Payload_JSON IS NULL
    """)
    if df.empty:
        logger.info("✅ No payloads to generate.")
        return

    # Filter to needed columns to lower memory and speed to_dict()
    cols = [c for c in df.columns if c in _needed_src_cols]
    df = df[cols].copy()

    logger.info(f"🧱 Staging payloads for {len(df)} rows (batched {batch_size})...")
    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start+batch_size]
        updates = []
        for row in batch.itertuples(index=False, name=None):
            rd = dict(zip(batch.columns, row))
            try:
                source_id = rd["Source_Id"]
                name = _name_from_row(rd)
                if not name:
                    updates.append((source_id, "Skipped", None, "Missing name"))
                    continue

                payload = build_payload_from_row(rd)
                if not payload:
                    updates.append((source_id, "Skipped", None, "Empty payload"))
                    continue

                # FAST: ready to post; duplicates handled on post if any
                updates.append((source_id, "Ready", dumps_fast(payload), None))
            except Exception as e:
                updates.append((rd.get("Source_Id"), "Failed", None, f"payload_error: {e}"))

        # Bulk write: stage to temp & update mapping table in one statement
        _bulk_update_payloads(updates)

def _bulk_update_payloads(updates):
    """
    updates: list of tuples (Source_Id, Porter_Status, Payload_JSON, Failure_Reason)
    Writes to a small staging table then set-based UPDATE join into Map_Customer.
    """
    if not updates: return
    dfu = pd.DataFrame(updates, columns=["Source_Id","Porter_Status","Payload_JSON","Failure_Reason"])

    staging = "_tmp_customer_payloads"
    # Create staging table (real table to avoid session scope issues with #temp)
    sql.run_query(f"""
        IF OBJECT_ID('{mapping_schema}.{staging}', 'U') IS NOT NULL DROP TABLE [{mapping_schema}].[{staging}];
        CREATE TABLE [{mapping_schema}].[{staging}](
            Source_Id NVARCHAR(100) NOT NULL,
            Porter_Status VARCHAR(50) NULL,
            Payload_JSON NVARCHAR(MAX) NULL,
            Failure_Reason NVARCHAR(1000) NULL
        );
    """)
    sql.insert_dataframe(dfu, staging, mapping_schema)

    # Single set-based update for all rows in this batch
    sql.run_query(f"""
        UPDATE MC
           SET MC.Porter_Status  = ISNULL(S.Porter_Status, MC.Porter_Status),
               MC.Payload_JSON   = CASE WHEN S.Payload_JSON IS NOT NULL THEN S.Payload_JSON ELSE MC.Payload_JSON END,
               MC.Failure_Reason = CASE WHEN S.Failure_Reason IS NOT NULL THEN S.Failure_Reason ELSE MC.Failure_Reason END
        FROM [{mapping_schema}].[{mapping_table}] MC
        INNER JOIN [{mapping_schema}].[{staging}] S
                ON S.Source_Id = MC.Source_Id;
        DROP TABLE [{mapping_schema}].[{staging}];
    """)

# ────────────────────────────────────────────────────────────────────
# Posting (per-record; existence only on duplicate error)
# ────────────────────────────────────────────────────────────────────
def post_staged_customers():
    df = sql.fetch_dataframe(f"""
        SELECT Source_Id, Payload_JSON
        FROM [{mapping_schema}].[{mapping_table}]
        WHERE Porter_Status IN ('Ready','Failed') AND Payload_JSON IS NOT NULL
    """)
    if df.empty:
        logger.info("✅ No customers to post.")
        return

    logger.info(f"📤 Posting {len(df)} customer(s) to QBO...")
    timer = ProgressTimer(len(df))
    for row in df.itertuples(index=False):
        source_id, payload_json = row
        try:
            payload = loads_fast(payload_json) if isinstance(payload_json, str) else payload_json
            r = session.post(post_url, headers=headers, json=payload)

            if r.status_code == 200:
                target_id = r.json()["Customer"]["Id"]
                update_mapping_status(mapping_schema, mapping_table, source_id, "Success", target_id=target_id)
                timer.update(); continue

            code, msg, det = _parse_qbo_error(r)

            # Duplicate name handling (only now → huge perf savings)
            # Duplicate name handling — always resolve and store Target_Id
            if str(code) == "6240" or (msg and "Duplicate Name" in msg):
                existing_id = _recover_customer_id_by_name(payload)  # always try now
                if existing_id:
                    update_mapping_status(mapping_schema, mapping_table, source_id,
                                        "Exists", target_id=existing_id, payload=payload)
                else:
                    # Fallback: still mark Exists to avoid re-post loops, but note missing id
                    update_mapping_status(mapping_schema, mapping_table, source_id,
                                        "Exists", payload=payload,
                                        failure_reason="Duplicate name; could not resolve existing Id")
                timer.update(); continue

            # Other errors → Failed
            update_mapping_status(mapping_schema, mapping_table, source_id, "Failed",
                                  failure_reason=f"code={code} | msg={msg} | detail={det}", increment_retry=True)
        except Exception as e:
            update_mapping_status(mapping_schema, mapping_table, source_id, "Failed", failure_reason=str(e), increment_retry=True)
        timer.update()

def _recover_customer_id_by_name(payload: dict) -> str | None:
    """
    Resolve existing QBO Customer Id by name for duplicate handling.
    Tries exact WHERE match first (fast), then falls back to the robust page-scan.
    """
    name = _best_name_from_payload(payload)
    if not name:
        return None

    # 1) Fast exact lookup
    cid = _try_exact_lookup_customer_id(name)
    if cid:
        return cid

    # 2) Robust page-scan (handles apostrophes / NBSP, etc.)
    target = _norm_name(name)
    start_pos, page_size = 1, 1000
    while True:
        q = f"SELECT Id, DisplayName, Active FROM Customer ORDERBY DisplayName STARTPOSITION {start_pos} MAXRESULTS {page_size}"
        r, ok, _ = qbo_query(query_url, q, access_token)
        if not ok:
            return None
        items = r.json().get("QueryResponse", {}).get("Customer", [])
        if not items:
            return None
        for it in items:
            if _norm_name(it.get("DisplayName")) == target:
                return it.get("Id")
        start_pos += page_size

# ────────────────────────────────────────────────────────────────────
# Diagnostics
# ────────────────────────────────────────────────────────────────────
def log_unmapped_customer_types():
    df = sql.fetch_dataframe(f"""
        SELECT DISTINCT Source_CustomerType_Name
        FROM [{mapping_schema}].[{mapping_table}]
        WHERE Source_CustomerType_Name IS NOT NULL AND Target_CustomerType_Id IS NULL
    """)
    if not df.empty:
        logger.warning("⚠️ Unmapped CustomerType names:")
        for n in df["Source_CustomerType_Name"].dropna().head(10):
            logger.warning(f"   - {n!r}")

def log_unmapped_terms():
    pass
    # df = sql.fetch_dataframe(f"""
    #     SELECT TOP 10 Source_Id, [SalesTermRef.value]
    #     FROM [{mapping_schema}].[{mapping_table}]
    #     WHERE [SalesTermRef.value] IS NOT NULL
    #       AND (Target_Term_Id IS NULL OR LTRIM(RTRIM(Target_Term_Id)) = '')
    # """)
    # if not df.empty:
    #     logger.warning("⚠️ Unmapped Terms (sample):")
    #     for _, r in df.iterrows():
    #         logger.warning(f"   - {r['Source_Id']} → {r['SalesTermRef.value']}")

# ────────────────────────────────────────────────────────────────────
# Orchestration
# ────────────────────────────────────────────────────────────────────
def migrate_customers():
    logger.info("🚀 Starting Customer Migration (fast mode)")
    df = sql.fetch_table(source_table, source_schema)
    if df.empty:
        logger.warning("⚠️ No Customer records found.")
        return

    sql.ensure_schema_exists(mapping_schema)
    initialize_mapping_table(df, mapping_table, mapping_schema)
    ensure_additional_columns()

    # Optional: customer types
    sync_qbo_customertype_to_map()
    enrich_source_customertype_name()
    map_customertype_ids_to_mapping_table()
    log_unmapped_customer_types()

    # Terms
    preload_term_map()
    backfill_target_term_ids_into_map_customer()
    log_unmapped_terms()

    # PaymentMethod map
    preload_paymentmethod_map()

    # FAST: stage payloads in bulk (no existence scans)
    stage_payloads_bulk()

    # Post
    post_staged_customers()

    # Retry once more for transient failures
    retry_df, _ = get_retryable_subset(source_table, source_schema, mapping_table, mapping_schema, max_retries)
    if not retry_df.empty:
        logger.info("🔁 Retrying failed customer posts...")
        post_staged_customers()

    logger.info("🏁 Customer migration completed")



# import os, json, requests, pandas as pd, re, urllib.parse as _url
# from datetime import datetime
# from dotenv import load_dotenv
# from functools import lru_cache

# from storage.sqlserver import sql
# from config.mapping.customer_mapping import CUSTOMER_COLUMN_MAPPING as column_mapping
# from utils.token_refresher import auto_refresh_token_if_needed
# from utils.retry_handler import initialize_mapping_table, get_retryable_subset
# from utils.log_timer import global_logger as logger, ProgressTimer
# from utils.mapping_updater import update_mapping_status

# # === Load env and refresh token ===
# auto_refresh_token_if_needed()
# load_dotenv()

# # === QBO Auth Config ===
# access_token = os.getenv("QBO_ACCESS_TOKEN")
# realm_id = os.getenv("QBO_REALM_ID")
# environment = os.getenv("QBO_ENVIRONMENT", "sandbox")
# base_url = "https://sandbox-quickbooks.api.intuit.com" if environment == "sandbox" else "https://quickbooks.api.intuit.com"
# query_url = f"{base_url}/v3/company/{realm_id}/query"
# post_url = f"{base_url}/v3/company/{realm_id}/customer"
# headers = {
#     "Authorization": f"Bearer {access_token}",
#     "Accept": "application/json",
#     "Content-Type": "application/json"
# }

# # === DB Config ===
# source_table = "Customer"
# source_schema = os.getenv("SOURCE_SCHEMA", "dbo")
# mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
# mapping_table = "Map_Customer"
# batch_size = 500
# max_retries = 3

# # === FAST HTTP SESSION (connection pooling) ===
# session = requests.Session()
# _adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=50, max_retries=3)
# session.mount("https://", _adapter)
# session.mount("http://", _adapter)

# # =========================
# # Helpers (unchanged logic)
# # =========================
# def _norm_name(s: str) -> str:
#     if s is None:
#         return ""
#     return (str(s)
#             .replace("\u2019", "'").replace("\u2018", "'")
#             .replace("\u00A0", " ")
#             .strip())

# def _normalize_query(q: str) -> str:
#     q = (q or "").replace("\u00A0", " ").replace("\r", " ").replace("\n", " ")
#     q = re.sub(r"\s+", " ", q).strip().rstrip(";")
#     return q

# def qbo_query(query_url: str, query: str, access_token: str, timeout: int = 30):
#     q = _normalize_query(query)
#     headers_text_plain = {
#         "Authorization": f"Bearer {access_token}",
#         "Accept": "application/json",
#         "Content-Type": "text/plain",
#     }
#     headers_app_text = {
#         "Authorization": f"Bearer {access_token}",
#         "Accept": "application/json",
#         "Content-Type": "application/text",
#     }

#     r = session.post(query_url, headers=headers_text_plain, data=q.encode("utf-8"), timeout=timeout)
#     if r.status_code == 200:
#         return r, True, None

#     err_kind = None
#     try:
#         fault = r.json().get("Fault", {}).get("Error", [{}])[0]
#         msg   = str(fault.get("Message", ""))
#         code  = str(fault.get("code", ""))
#         if r.status_code == 400 and ("QueryParserError" in msg or code in {"4000"}):
#             err_kind = "parser"
#     except Exception:
#         pass

#     if err_kind == "parser":
#         r2 = session.post(query_url, headers=headers_app_text, data=q.encode("utf-8"), timeout=timeout)
#         if r2.status_code == 200:
#             return r2, True, None
#         r3 = session.get(
#             query_url,
#             headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
#             params={"query": q},
#             timeout=timeout
#         )
#         return r3, (r3.status_code == 200), "parser"

#     return r, False, None

# def _parse_qbo_error(resp):
#     try:
#         data = resp.json() if hasattr(resp, "json") else {}
#         err = (data.get("Fault", {}) or {}).get("Error", []) or []
#         if err:
#             e0 = err[0] or {}
#             code = str(e0.get("code")) if e0.get("code") is not None else None
#             msg  = e0.get("Message")
#             det  = e0.get("Detail")
#             return code, msg, det
#     except Exception:
#         pass
#     try:
#         raw = resp.text[:300]
#     except Exception:
#         raw = None
#     return None, None, raw

# def _scan_customer_by_display_name(name: str, query_url: str, access_token: str,
#                                    active: bool | None = None, page_size: int = 1000):
#     target = _norm_name(name)
#     start_pos = 1
#     cols = "Id, DisplayName" + (", Active" if active is not None else "")
#     while True:
#         q = f"SELECT {cols} FROM Customer ORDERBY DisplayName STARTPOSITION {start_pos} MAXRESULTS {page_size}"
#         r, ok, _ = qbo_query(query_url, q, access_token)
#         if not ok:
#             return None
#         items = r.json().get("QueryResponse", {}).get("Customer", [])
#         if not items:
#             return None
#         for it in items:
#             disp = _norm_name(it.get("DisplayName"))
#             if disp == target and (active is None or bool(it.get("Active")) == active):
#                 return it.get("Id")
#         start_pos += page_size

# @lru_cache(maxsize=100_000)
# def _cached_existing_customer_id(name_norm: str) -> str | None:
#     cid = _scan_customer_by_display_name(name_norm, query_url, access_token, active=True)
#     if cid: return cid
#     cid = _scan_customer_by_display_name(name_norm, query_url, access_token, active=False)
#     if cid: return cid
#     if len(name_norm) > 100:
#         short = name_norm[:100]
#         cid = _scan_customer_by_display_name(short, query_url, access_token, active=True)
#         if cid: return cid
#         cid = _scan_customer_by_display_name(short, query_url, access_token, active=False)
#         if cid: return cid
#     return None

# def get_existing_qbo_customer_id_safe(name: str, query_url: str, access_token: str):
#     if not name:
#         return None
#     return _cached_existing_customer_id(_norm_name(name))

# def _best_name_from_payload(payload: dict) -> str:
#     for k in ("DisplayName", "CompanyName", "PrintOnCheckName"):
#         v = payload.get(k)
#         if isinstance(v, str) and v.strip():
#             return _norm_name(v)
#     return ""

# # =========================
# # CustomerType sync / mapping
# # =========================
# def sync_qbo_customertype_to_map():
#     headers_qbo = {
#         "Authorization": f"Bearer {access_token}",
#         "Accept": "application/json",
#         "Content-Type": "application/text"
#     }

#     all_types = []
#     start_pos = 1
#     page_size = 100

#     try:
#         while True:
#             query = f"SELECT Id, Name FROM CustomerType STARTPOSITION {start_pos} MAXRESULTS {page_size}"
#             response = session.post(query_url, headers=headers_qbo, data=query)
#             logger.info(f"📥 QBO response status: {response.status_code} at position {start_pos}")

#             if response.status_code != 200:
#                 logger.error(f"❌ Failed to fetch QBO CustomerTypes: {response.text[:500]}")
#                 break

#             page = response.json().get("QueryResponse", {}).get("CustomerType", [])
#             if not page:
#                 break

#             all_types.extend(page)
#             start_pos += page_size

#         if not all_types:
#             logger.warning("⚠️ No CustomerType records found in QBO.")
#             return

#         df = pd.DataFrame(all_types)[["Id", "Name"]].rename(columns={"Id": "Target_Id"})
#         df["Normalized_Name"] = df["Name"].astype(str).str.strip().str.lower().str.replace("\u00a0", " ", regex=False)
#         df["Porter_Status"] = "Fetched"

#         sql.ensure_schema_exists(mapping_schema)
#         table = "Map_CustomerType"
#         if not sql.table_exists(table, mapping_schema):
#             sql.run_query(f"""
#                 CREATE TABLE [{mapping_schema}].[{table}] (
#                     Target_Id NVARCHAR(100),
#                     Name NVARCHAR(255),
#                     Normalized_Name NVARCHAR(255),
#                     Porter_Status VARCHAR(50)
#                 )
#             """)
#         else:
#             sql.run_query(f"DELETE FROM [{mapping_schema}].[{table}]")

#         sql.insert_dataframe(df, table, mapping_schema)
#         logger.info(f"✅ Inserted {len(df)} CustomerTypes into {mapping_schema}.{table}")

#     except Exception:
#         logger.exception("❌ Exception fetching CustomerType from QBO")

# def ensure_additional_columns():
#     """
#     Adds necessary tracking and mapping columns to the Map_Customer table
#     if they do not already exist.
#     """
#     alter_cols = [
#         ("Payload_JSON", "NVARCHAR(MAX)"),
#         ("Porter_Status", "VARCHAR(50)"),
#         ("Failure_Reason", "NVARCHAR(1000)"),
#         ("Target_Id", "VARCHAR(100)"),
#         ("Retry_Count", "INT"),
#         ("Source_CustomerType_Name", "NVARCHAR(255)"),
#         ("Normalized_CustomerType_Name", "NVARCHAR(255)"),
#         ("Target_CustomerType_Id", "VARCHAR(100)"),
#         # NEW: Term mapping stash
#         ("Target_Term_Id", "VARCHAR(100)")
#     ]
#     for col, col_type in alter_cols:
#         sql.run_query(f"""
#             IF COL_LENGTH('{mapping_schema}.{mapping_table}', '{col}') IS NULL
#             BEGIN
#                 ALTER TABLE [{mapping_schema}].[{mapping_table}]
#                 ADD {col} {col_type}
#             END
#         """)

# def customer_exists(display_name, *_):
#     return get_existing_qbo_customer_id_safe(display_name, query_url, access_token)

# def enrich_source_customertype_name():
#     table_check_query = f"""
#         SELECT 1 FROM INFORMATION_SCHEMA.TABLES
#         WHERE TABLE_SCHEMA = '{source_schema}' AND TABLE_NAME = 'CustomerType'
#     """
#     try:
#         df_check = sql.fetch_dataframe(table_check_query)
#         if df_check.empty:
#             logger.warning(f"⚠️ Skipping CustomerType enrichment — table '{source_schema}.CustomerType' not found.")
#             return
#     except Exception:
#         logger.exception("❌ Failed to check existence of CustomerType table")
#         return

#     query = f"""
#         UPDATE M
#         SET 
#             M.Source_CustomerType_Name = CT.Name,
#             M.Normalized_CustomerType_Name = LOWER(LTRIM(RTRIM(REPLACE(CT.Name, CHAR(160), ' '))))
#         FROM [{mapping_schema}].[{mapping_table}] M
#         INNER JOIN [{source_schema}].[{source_table}] S ON M.Source_Id = S.Id
#         LEFT JOIN [{source_schema}].[CustomerType] CT ON S.[CustomerTypeRef.value] = CT.Id
#         WHERE M.Source_CustomerType_Name IS NULL AND S.[CustomerTypeRef.value] IS NOT NULL
#     """
#     try:
#         sql.run_query(query)
#     except Exception:
#         logger.exception("❌ Failed to enrich customer type names")

# def map_customertype_ids_to_mapping_table():
#     try:
#         table_check_query = f"""
#             SELECT 1 FROM INFORMATION_SCHEMA.TABLES
#             WHERE TABLE_SCHEMA = '{mapping_schema}' AND TABLE_NAME = 'Map_CustomerType'
#         """
#         df_check = sql.fetch_dataframe(table_check_query)
#         if df_check.empty:
#             logger.warning(f"⚠️ Skipping CustomerType mapping — table '{mapping_schema}.Map_CustomerType' not found.")
#             return
#     except Exception:
#         logger.exception("❌ Failed to check existence of Map_CustomerType table")
#         return

#     query = f"""
#         UPDATE MC
#         SET MC.Target_CustomerType_Id = MCT.Target_Id
#         FROM [{mapping_schema}].[{mapping_table}] MC
#         INNER JOIN [{mapping_schema}].[Map_CustomerType] MCT
#             ON MC.Normalized_CustomerType_Name = MCT.Normalized_Name
#         WHERE MC.Target_CustomerType_Id IS NULL
#           AND MC.Source_CustomerType_Name IS NOT NULL
#     """
#     try:
#         sql.run_query(query)
#         logger.info("✅ Target_CustomerType_Id populated using normalized name match.")
#     except Exception:
#         logger.exception("❌ Failed to update Target_CustomerType_Id from Map_CustomerType")

# def get_existing_qbo_customer_id(name):
#     safe_name = name.replace("'", "''")

#     query_customer = f"SELECT Id FROM Customer WHERE DisplayName = '{safe_name}'"
#     response = session.post(query_url, headers=headers, data=query_customer)
#     if response.status_code == 200:
#         customers = response.json().get("QueryResponse", {}).get("Customer", [])
#         if customers:
#             return customers[0]["Id"]

#     query_vendor = f"SELECT Id FROM Vendor WHERE DisplayName = '{safe_name}'"
#     response = session.post(query_url, headers=headers, data=query_vendor)
#     if response.status_code == 200:
#         vendors = response.json().get("QueryResponse", {}).get("Vendor", [])
#         if vendors:
#             vendor_id = vendors[0]["Id"]
#             logger.warning(f"⚠️ Name '{name}' already exists as a Vendor (Id={vendor_id}). Marking Customer as Failed.")
#             return None

#     query_employee = f"SELECT Id FROM Employee WHERE DisplayName = '{safe_name}'"
#     response = session.post(query_url, headers=headers, data=query_employee)
#     if response.status_code == 200:
#         employees = response.json().get("QueryResponse", {}).get("Employee", [])
#         if employees:
#             employee_id = employees[0]["Id"]
#             logger.warning(f"⚠️ Name '{name}' already exists as an Employee (Id={employee_id}). Marking Customer as Failed.")
#             return None

#     return None

# # =========================
# # Preload maps (perf-only)
# # =========================
# _paymentmethod_map = {}
# _term_map = {}  # NEW

# def preload_paymentmethod_map():
#     global _paymentmethod_map
#     try:
#         df_pm = sql.fetch_dataframe(f"SELECT Source_Id, Target_Id FROM {mapping_schema}.Map_PaymentMethod")
#         _paymentmethod_map = {
#             str(r["Source_Id"]): str(r["Target_Id"])
#             for _, r in df_pm.iterrows()
#             if pd.notna(r["Target_Id"])
#         }
#         logger.info(f"🗺️ Preloaded PaymentMethod map: {len(_paymentmethod_map)} entries")
#     except Exception as e:
#         _paymentmethod_map = {}
#         logger.warning(f"⚠️ Failed to preload PaymentMethod map: {e}")

# def preload_term_map():
#     """
#     Preload Source_Id -> Target_Id for Terms from porter_entities_mapping.Map_Term
#     """
#     global _term_map
#     try:
#         df_tm = sql.fetch_dataframe(f"SELECT Source_Id, Target_Id FROM {mapping_schema}.Map_Term")
#         _term_map = {
#             str(r["Source_Id"]): str(r["Target_Id"])
#             for _, r in df_tm.iterrows()
#             if pd.notna(r["Target_Id"])
#         }
#         logger.info(f"🗺️ Preloaded Term map: {len(_term_map)} entries")
#     except Exception as e:
#         _term_map = {}
#         logger.warning(f"⚠️ Failed to preload Term map: {e}")

# def backfill_target_term_ids_into_map_customer():
#     """
#     One-time enrichment to write Target_Term_Id into Map_Customer by joining Map_Term on SalesTermRef.value.
#     Keeps your Map_* table self-sufficient for downstream review/debug.
#     """
#     try:
#         sql.run_query(f"""
#             IF COL_LENGTH('{mapping_schema}.{mapping_table}', 'Target_Term_Id') IS NULL
#             BEGIN
#                 ALTER TABLE [{mapping_schema}].[{mapping_table}] ADD Target_Term_Id VARCHAR(100)
#             END
#         """)
#         sql.run_query(f"""
#             UPDATE MC
#             SET MC.Target_Term_Id = MT.Target_Id
#             FROM [{mapping_schema}].[{mapping_table}] MC
#             LEFT JOIN [{mapping_schema}].[Map_Term] MT
#               ON TRY_CONVERT(VARCHAR(100), MC.[SalesTermRef.value]) = TRY_CONVERT(VARCHAR(100), MT.Source_Id)
#             WHERE MC.Target_Term_Id IS NULL
#               AND MC.[SalesTermRef.value] IS NOT NULL
#         """)
#         logger.info("✅ Backfilled Target_Term_Id in Map_Customer from Map_Term.")
#     except Exception as e:
#         logger.warning(f"⚠️ Failed to backfill Target_Term_Id: {e}")

# def log_unmapped_terms():
#     """
#     Show a small sample of Customer rows with SalesTermRef.value present but no Target_Term_Id found.
#     """
#     try:
#         df = sql.fetch_dataframe(f"""
#             SELECT TOP 10 Source_Id, [SalesTermRef.value]
#             FROM [{mapping_schema}].[{mapping_table}]
#             WHERE [SalesTermRef.value] IS NOT NULL
#               AND (Target_Term_Id IS NULL OR LTRIM(RTRIM(Target_Term_Id)) = '')
#         """)
#         if not df.empty:
#             logger.warning("⚠️ Unmapped Terms (showing up to 10 Source_Id → SalesTermRef.value):")
#             for _, r in df.iterrows():
#                 logger.warning(f"   - {r['Source_Id']} → {r['SalesTermRef.value']}")
#     except Exception as e:
#         logger.warning(f"⚠️ Could not list unmapped terms: {e}")

# # =========================
# # Payload builder (now maps SalesTermRef)
# # =========================
# def build_payload(row):
#     """
#     Builds a valid QBO Customer JSON payload from a single record in Map_Customer.
#     """
#     payload = {}
#     for src_col, qbo_col in column_mapping.items():
#         value = row.get(src_col)
#         if pd.notna(value) and str(value).strip():
#             if "." in qbo_col:
#                 parent, child = qbo_col.split(".")
#                 payload.setdefault(parent, {})[child] = value
#             else:
#                 payload[qbo_col] = value

#     # PaymentMethodRef via preloaded map
#     src_payment_method_id = row.get("PaymentMethodRef.value")
#     if pd.notna(src_payment_method_id):
#         mapped = _paymentmethod_map.get(str(src_payment_method_id))
#         if mapped:
#             payload["PaymentMethodRef"] = {"value": mapped}

#     # CustomerTypeRef only if mapped
#     target_type_id = row.get("Target_CustomerType_Id")
#     if target_type_id and str(target_type_id).strip():
#         payload["CustomerTypeRef"] = {"value": str(target_type_id)}

#     # NEW: SalesTermRef via either pre-backfilled column or direct map lookup
#     target_term_id = row.get("Target_Term_Id")
#     if not target_term_id:
#         # Fallback: map by the raw source SalesTermRef.value using preloaded dict
#         src_term = row.get("SalesTermRef.value")
#         if pd.notna(src_term):
#             target_term_id = _term_map.get(str(src_term))
#     if target_term_id and str(target_term_id).strip():
#         payload["SalesTermRef"] = {"value": str(target_term_id)}

#     payload["Active"] = True
#     return payload

# # =========================
# # Batch payload generation
# # =========================
# def enrich_mapping_in_batches():
#     df = sql.fetch_dataframe(f"""
#         SELECT * FROM [{mapping_schema}].[{mapping_table}]
#         WHERE Payload_JSON IS NULL 
#     """)
#     if df.empty:
#         logger.info("✅ No payloads to generate.")
#         return

#     for start in range(0, len(df), batch_size):
#         batch = df.iloc[start:start + batch_size]
#         timer = ProgressTimer(len(batch))
#         for row in batch.itertuples(index=False, name=None):
#             row_dict = dict(zip(batch.columns, row))
#             try:
#                 source_id = row_dict["Source_Id"]
#                 payload = build_payload(pd.Series(row_dict))
#                 name = row_dict.get("DisplayName") or row_dict.get("FullyQualifiedName") or row_dict.get("CompanyName")

#                 if not name:
#                     update_mapping_status(mapping_schema, mapping_table, source_id,
#                                           "Skipped", failure_reason="Missing name")
#                     timer.update(); continue

#                 if not payload:
#                     update_mapping_status(mapping_schema, mapping_table, source_id,
#                                           "Skipped", failure_reason="Empty payload")
#                     timer.update(); continue

#                 if "DisplayName" not in payload:
#                     payload["DisplayName"] = name

#                 existing_id = get_existing_qbo_customer_id_safe(name, query_url, access_token)
#                 if existing_id:
#                     update_mapping_status(mapping_schema, mapping_table, source_id,
#                                           "Exists", target_id=existing_id, payload=payload)
#                 else:
#                     update_mapping_status(mapping_schema, mapping_table, source_id,
#                                           "Ready", payload=payload)
#             except Exception:
#                 logger.exception(f"❌ Payload generation failed for {row_dict.get('Source_Id')}")
#             finally:
#                 timer.update()

# # =========================
# # Posting (per-record)
# # =========================
# def post_staged_customers():
#     df = sql.fetch_dataframe(f"""
#         SELECT * FROM [{mapping_schema}].[{mapping_table}]
#         WHERE Porter_Status IN ('Ready', 'Failed') AND Payload_JSON IS NOT NULL
#     """)
#     if df.empty:
#         logger.info("✅ No customers to post.")
#         return

#     logger.info(f"📤 Posting {len(df)} customer(s) to QBO...")
#     timer = ProgressTimer(len(df))

#     for row in df.itertuples(index=False, name=None):
#         row_dict = dict(zip(df.columns, row))
#         source_id = row_dict["Source_Id"]
#         try:
#             payload = json.loads(row_dict["Payload_JSON"]) if isinstance(row_dict["Payload_JSON"], str) else row_dict["Payload_JSON"]
#             response = session.post(post_url, headers=headers, json=payload)

#             if response.status_code == 200:
#                 target_id = response.json()["Customer"]["Id"]
#                 logger.info(f"✅ Migrated: {payload.get('DisplayName')} → {target_id}")
#                 update_mapping_status(mapping_schema, mapping_table, source_id,
#                                       "Success", target_id=target_id)
#                 timer.update()
#                 continue

#             code, msg, det = _parse_qbo_error(response)
#             reason_full = f"code={code} | msg={msg} | detail={det}"

#             if str(code) == "6240" or (msg and "Duplicate Name" in msg):
#                 name = _best_name_from_payload(payload)
#                 existing_id = get_existing_qbo_customer_id_safe(name, query_url, access_token)
#                 if existing_id:
#                     logger.info(f"♻️ Duplicate detected. Recovered existing QBO Customer Id: {name!r} → {existing_id}")
#                     update_mapping_status(mapping_schema, mapping_table, source_id,
#                                           "Exists", target_id=existing_id, payload=payload)
#                     timer.update()
#                     continue
#                 else:
#                     logger.warning(f"⚠️ Duplicate name {name!r} but could not recover existing Customer Id.")
#                     update_mapping_status(mapping_schema, mapping_table, source_id,
#                                           "Failed", failure_reason="Duplicate name (6240), ID not found via scan",
#                                           increment_retry=False)
#                     timer.update()
#                     continue

#             update_mapping_status(mapping_schema, mapping_table, source_id,
#                                   "Failed", failure_reason=reason_full, increment_retry=True)

#         except Exception as e:
#             update_mapping_status(mapping_schema, mapping_table, source_id,
#                                   "Failed", failure_reason=str(e), increment_retry=True)

#         timer.update()

# def log_unmapped_customer_types():
#     query = f"""
#         SELECT DISTINCT Source_CustomerType_Name
#         FROM [{mapping_schema}].[{mapping_table}]
#         WHERE Source_CustomerType_Name IS NOT NULL
#           AND Target_CustomerType_Id IS NULL
#     """
#     df = sql.fetch_dataframe(query)
#     if not df.empty:
#         logger.warning(f"⚠️ Unmapped CustomerType names ({len(df)}):")
#         for name in df["Source_CustomerType_Name"].dropna().head(10):
#             logger.warning(f"   - '{name}'")

# # === Main Migration Orchestration
# def migrate_customers():
#     logger.info("🚀 Starting Customer Migration")
#     df = sql.fetch_table(source_table, source_schema)
#     if df.empty:
#         logger.warning("⚠️ No Customer records found.")
#         return

#     sql.ensure_schema_exists(mapping_schema)
#     initialize_mapping_table(df, mapping_table, mapping_schema)
#     ensure_additional_columns()

#     # Optional: customer types (unchanged logic)
#     sync_qbo_customertype_to_map()
#     enrich_source_customertype_name()
#     map_customertype_ids_to_mapping_table()
#     log_unmapped_customer_types()

#     # NEW: preload terms and backfill Target_Term_Id into Map_Customer
#     preload_term_map()
#     backfill_target_term_ids_into_map_customer()
#     log_unmapped_terms()

#     # Perf: preload PaymentMethod map once
#     preload_paymentmethod_map()

#     # Build payloads & stage
#     enrich_mapping_in_batches()

#     # Post one-by-one
#     post_staged_customers()

#     # Retry loop
#     retry_df, _ = get_retryable_subset(source_table, source_schema, mapping_table, mapping_schema, max_retries)
#     if not retry_df.empty:
#         logger.info("🔁 Retrying failed customer posts...")
#         post_staged_customers()

#     logger.info("🏁 Customer migration completed")



###############################################OldSchool#############################################
# """
# Sequence : 10
# Module: customer_migrator.py
# Author: Dixit Prajapati
# Created: 2025-07-22
# Description: Handles migration of customer records from source system to QBO,
#              including parent-child hierarchy processing, retry logic, and status tracking.
# Production : Ready
# Phase : 01
# """

# import os, json, requests, pandas as pd
# from datetime import datetime
# from dotenv import load_dotenv
# from storage.sqlserver import sql
# from config.mapping.customer_mapping import CUSTOMER_COLUMN_MAPPING as column_mapping
# from utils.token_refresher import auto_refresh_token_if_needed
# from utils.retry_handler import initialize_mapping_table, get_retryable_subset
# from utils.log_timer import global_logger as logger, ProgressTimer
# from utils.mapping_updater import update_mapping_status
# import re, urllib.parse as _url, requests
# import re, requests

# # === Load env and refresh token ===
# auto_refresh_token_if_needed()
# load_dotenv()

# # === QBO Auth Config ===
# access_token = os.getenv("QBO_ACCESS_TOKEN")
# realm_id = os.getenv("QBO_REALM_ID")
# environment = os.getenv("QBO_ENVIRONMENT", "sandbox")
# base_url = "https://sandbox-quickbooks.api.intuit.com" if environment == "sandbox" else "https://quickbooks.api.intuit.com"
# query_url = f"{base_url}/v3/company/{realm_id}/query"
# post_url = f"{base_url}/v3/company/{realm_id}/customer"
# headers = {
#     "Authorization": f"Bearer {access_token}",
#     "Accept": "application/json",
#     "Content-Type": "application/json"
# }

# # === DB Config ===
# source_table = "Customer"
# source_schema = os.getenv("SOURCE_SCHEMA", "dbo")
# mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
# mapping_table = "Map_Customer"
# batch_size = 500
# max_retries = 3


# def _norm_name(s: str) -> str:
#     if s is None:
#         return ""
#     return (str(s)
#             .replace("\u2019", "'").replace("\u2018", "'")  # curly → straight
#             .replace("\u00A0", " ")                         # NBSP → space
#             .strip())

# def _normalize_query(q: str) -> str:
#     q = (q or "").replace("\u00A0", " ").replace("\r", " ").replace("\n", " ")
#     q = re.sub(r"\s+", " ", q).strip().rstrip(";")
#     return q

# def qbo_query(query_url: str, query: str, access_token: str, timeout: int = 30):
#     """POST text/plain → POST application/text → GET fallback."""
#     q = _normalize_query(query)
#     headers_text_plain = {
#         "Authorization": f"Bearer {access_token}",
#         "Accept": "application/json",
#         "Content-Type": "text/plain",
#     }
#     headers_app_text = {
#         "Authorization": f"Bearer {access_token}",
#         "Accept": "application/json",
#         "Content-Type": "application/text",
#     }

#     r = requests.post(query_url, headers=headers_text_plain, data=q.encode("utf-8"), timeout=timeout)
#     if r.status_code == 200:
#         return r, True, None

#     # detect parser error
#     err_kind = None
#     try:
#         fault = r.json().get("Fault", {}).get("Error", [{}])[0]
#         msg   = str(fault.get("Message", ""))
#         code  = str(fault.get("code", ""))
#         if r.status_code == 400 and ("QueryParserError" in msg or code in {"4000"}):
#             err_kind = "parser"
#     except Exception:
#         pass

#     if err_kind == "parser":
#         r2 = requests.post(query_url, headers=headers_app_text, data=q.encode("utf-8"), timeout=timeout)
#         if r2.status_code == 200:
#             return r2, True, None
#         r3 = requests.get(query_url,
#                           headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
#                           params={"query": q}, timeout=timeout)
#         return r3, (r3.status_code == 200), "parser"

#     return r, False, None

# def _parse_qbo_error(resp):
#     """
#     Returns (code:str|None, message:str|None, detail:str|None)
#     Never raises; falls back to (None, None, resp.text[:300]).
#     """
#     try:
#         data = resp.json() if hasattr(resp, "json") else {}
#         err = (data.get("Fault", {}) or {}).get("Error", []) or []
#         if err:
#             e0 = err[0] or {}
#             code = str(e0.get("code")) if e0.get("code") is not None else None
#             msg  = e0.get("Message")
#             det  = e0.get("Detail")
#             return code, msg, det
#     except Exception:
#         pass
#     # Fallback to raw text
#     try:
#         raw = resp.text[:300]
#     except Exception:
#         raw = None
#     return None, None, raw

# def _scan_customer_by_display_name(name: str, query_url: str, access_token: str,
#                                    active: bool | None = None, page_size: int = 1000):
#     """
#     Literal-free lookup: page through Customers ordered by DisplayName and compare
#     client-side after normalization. Works even when name contains apostrophes.
#     """
#     target = _norm_name(name)
#     start_pos = 1
#     cols = "Id, DisplayName" + (", Active" if active is not None else "")
#     while True:
#         q = f"SELECT {cols} FROM Customer ORDERBY DisplayName STARTPOSITION {start_pos} MAXRESULTS {page_size}"
#         r, ok, _ = qbo_query(query_url, q, access_token)
#         if not ok:
#             return None
#         items = r.json().get("QueryResponse", {}).get("Customer", [])
#         if not items:
#             return None
#         for it in items:
#             disp = _norm_name(it.get("DisplayName"))
#             if disp == target and (active is None or bool(it.get("Active")) == active):
#                 return it.get("Id")
#         start_pos += page_size

# def get_existing_qbo_customer_id_safe(name: str, query_url: str, access_token: str):
#     """
#     Robust resolver for existing Customer Id by DisplayName:
#     tries literal-free scans for Active true/false, then a truncated variant
#     (QBO caps DisplayName ~100 chars). Returns Id or None.
#     """
#     if not name:
#         return None
#     name = _norm_name(name)
#     # Try exact (Active true/false)
#     cid = _scan_customer_by_display_name(name, query_url, access_token, active=True)
#     if cid: return cid
#     cid = _scan_customer_by_display_name(name, query_url, access_token, active=False)
#     if cid: return cid
#     # Try truncated form (server may have clipped to 100 chars)
#     if len(name) > 100:
#         short = name[:100]
#         cid = _scan_customer_by_display_name(short, query_url, access_token, active=True)
#         if cid: return cid
#         cid = _scan_customer_by_display_name(short, query_url, access_token, active=False)
#         if cid: return cid
#     return None

# def _best_name_from_payload(payload: dict) -> str:
#     for k in ("DisplayName", "CompanyName", "PrintOnCheckName"):
#         v = payload.get(k)
#         if isinstance(v, str) and v.strip():
#             return _norm_name(v)
#     return ""

# def qbo_query(query_url: str, query: str, access_token: str, timeout: int = 30):
#     """
#     POST text/plain → POST application/text → GET fallback.
#     Always returns a 3-tuple: (response, ok_bool, err_kind_or_None)
#     err_kind is 'parser' if we detected a QueryParserError, else None.
#     """
#     q = _normalize_query(query)
#     headers_text_plain = {
#         "Authorization": f"Bearer {access_token}",
#         "Accept": "application/json",
#         "Content-Type": "text/plain",
#     }
#     headers_app_text = {
#         "Authorization": f"Bearer {access_token}",
#         "Accept": "application/json",
#         "Content-Type": "application/text",
#     }

#     r = requests.post(query_url, headers=headers_text_plain, data=q.encode("utf-8"), timeout=timeout)
#     if r.status_code == 200:
#         return r, True, None

#     # Detect parser error
#     err_kind = None
#     try:
#         fault = r.json().get("Fault", {}).get("Error", [{}])[0]
#         msg   = str(fault.get("Message", ""))
#         code  = str(fault.get("code", ""))
#         if r.status_code == 400 and ("QueryParserError" in msg or code in {"4000"}):
#             err_kind = "parser"
#     except Exception:
#         pass

#     if err_kind == "parser":
#         r2 = requests.post(query_url, headers=headers_app_text, data=q.encode("utf-8"), timeout=timeout)
#         if r2.status_code == 200:
#             return r2, True, None
#         r3 = requests.get(
#             query_url,
#             headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
#             params={"query": q},
#             timeout=timeout
#         )
#         return r3, (r3.status_code == 200), "parser"

#     # Non-parser failure
#     return r, False, None

# def sync_qbo_customertype_to_map():
#     """
#     Fetches all CustomerType records from QBO (with pagination) and stores them into 
#     the porter_entities_mapping.Map_CustomerType table.
#     """
#     headers_qbo = {
#         "Authorization": f"Bearer {access_token}",
#         "Accept": "application/json",
#         "Content-Type": "application/text"
#     }

#     all_types = []
#     start_pos = 1
#     page_size = 100  # QBO max per query

#     try:
#         while True:
#             query = f"SELECT Id, Name FROM CustomerType STARTPOSITION {start_pos} MAXRESULTS {page_size}"
#             response = requests.post(query_url, headers=headers_qbo, data=query)
#             logger.info(f"📥 QBO response status: {response.status_code} at position {start_pos}")

#             if response.status_code != 200:
#                 logger.error(f"❌ Failed to fetch QBO CustomerTypes: {response.text[:500]}")
#                 break

#             page = response.json().get("QueryResponse", {}).get("CustomerType", [])
#             if not page:
#                 break

#             all_types.extend(page)
#             start_pos += page_size  # move to next page

#         if not all_types:
#             logger.warning("⚠️ No CustomerType records found in QBO.")
#             return

#         df = pd.DataFrame(all_types)[["Id", "Name"]].rename(columns={"Id": "Target_Id"})
#         df["Normalized_Name"] = df["Name"].str.strip().str.lower().str.replace("\u00a0", " ")
#         df["Porter_Status"] = "Fetched"

#         sql.ensure_schema_exists(mapping_schema)
#         table = "Map_CustomerType"
#         if not sql.table_exists(table, mapping_schema):
#             sql.run_query(f"""
#                 CREATE TABLE [{mapping_schema}].[{table}] (
#                     Target_Id NVARCHAR(100),
#                     Name NVARCHAR(255),
#                     Normalized_Name NVARCHAR(255),
#                     Porter_Status VARCHAR(50)
#                 )
#             """)
#         else:
#             sql.run_query(f"DELETE FROM [{mapping_schema}].[{table}]")

#         sql.insert_dataframe(df, table, mapping_schema)
#         logger.info(f"✅ Inserted {len(df)} CustomerTypes into {mapping_schema}.{table}")

#     except Exception as e:
#         logger.exception("❌ Exception fetching CustomerType from QBO")

# # === Step 2: Enrich Mapping Table
# def ensure_additional_columns():
#     """
#     Adds necessary tracking and mapping columns to the Map_Customer table
#     if they do not already exist. These include:
#     - JSON payload storage
#     - Retry tracking
#     - Migration status
#     - CustomerType mapping helpers

#     Returns:
#         None
#     """
#     alter_cols = [
#         ("Payload_JSON", "NVARCHAR(MAX)"),
#         ("Porter_Status", "VARCHAR(50)"),
#         ("Failure_Reason", "NVARCHAR(1000)"),
#         ("Target_Id", "VARCHAR(100)"),
#         ("Retry_Count", "INT"),
#         ("Source_CustomerType_Name", "NVARCHAR(255)"),
#         ("Normalized_CustomerType_Name", "NVARCHAR(255)"),
#         ("Target_CustomerType_Id", "VARCHAR(100)")
#     ]
#     for col, col_type in alter_cols:
#         sql.run_query(f"""
#             IF COL_LENGTH('{mapping_schema}.{mapping_table}', '{col}') IS NULL
#             BEGIN
#                 ALTER TABLE [{mapping_schema}].[{mapping_table}]
#                 ADD {col} {col_type}
#             END
#         """)

# def customer_exists(display_name, *_):
#     return get_existing_qbo_customer_id_safe(display_name, query_url, access_token)

# def enrich_source_customertype_name():
#     """
#     Enriches the Map_Customer table by joining the source CustomerType table
#     to fetch the name of the customer type using CustomerTypeRef.value.
#     - Skips enrichment if the CustomerType table does not exist.
#     """
#     # ✅ Check if table exists
#     table_check_query = f"""
#         SELECT 1 FROM INFORMATION_SCHEMA.TABLES
#         WHERE TABLE_SCHEMA = '{source_schema}' AND TABLE_NAME = 'CustomerType'
#     """
#     try:
#         df_check = sql.fetch_dataframe(table_check_query)
#         if df_check.empty:
#             logger.warning(f"⚠️ Skipping CustomerType enrichment — table '{source_schema}.CustomerType' not found.")
#             return
#     except Exception as e:
#         logger.exception("❌ Failed to check existence of CustomerType table")
#         return

#     # ✅ Run the enrichment only if table exists
#     query = f"""
#         UPDATE M
#         SET 
#             M.Source_CustomerType_Name = CT.Name,
#             M.Normalized_CustomerType_Name = LOWER(LTRIM(RTRIM(REPLACE(CT.Name, CHAR(160), ' '))))
#         FROM [{mapping_schema}].[{mapping_table}] M
#         INNER JOIN [{source_schema}].[{source_table}] S ON M.Source_Id = S.Id
#         LEFT JOIN [{source_schema}].[CustomerType] CT ON S.[CustomerTypeRef.value] = CT.Id
#         WHERE M.Source_CustomerType_Name IS NULL AND S.[CustomerTypeRef.value] IS NOT NULL
#     """
#     try:
#         sql.run_query(query)
#     except Exception as e:
#         logger.exception("❌ Failed to enrich customer type names")

# def map_customertype_ids_to_mapping_table():
#     """
#     Updates Map_Customer table by assigning Target_CustomerType_Id based on
#     normalized name match between Source_CustomerType_Name and the QBO
#     Map_CustomerType table.

#     Skips if Map_CustomerType table does not exist.
#     """
#     # ✅ Check if Map_CustomerType exists
#     try:
#         table_check_query = f"""
#             SELECT 1 FROM INFORMATION_SCHEMA.TABLES
#             WHERE TABLE_SCHEMA = '{mapping_schema}' AND TABLE_NAME = 'Map_CustomerType'
#         """
#         df_check = sql.fetch_dataframe(table_check_query)
#         if df_check.empty:
#             logger.warning(f"⚠️ Skipping CustomerType mapping — table '{mapping_schema}.Map_CustomerType' not found.")
#             return
#     except Exception as e:
#         logger.exception("❌ Failed to check existence of Map_CustomerType table")
#         return

#     # ✅ Run mapping only if table exists
#     query = f"""
#         UPDATE MC
#         SET MC.Target_CustomerType_Id = MCT.Target_Id
#         FROM [{mapping_schema}].[{mapping_table}] MC
#         INNER JOIN [{mapping_schema}].[Map_CustomerType] MCT
#             ON MC.Normalized_CustomerType_Name = MCT.Normalized_Name
#         WHERE MC.Target_CustomerType_Id IS NULL
#           AND MC.Source_CustomerType_Name IS NOT NULL
#     """
#     try:
#         sql.run_query(query)
#         logger.info("✅ Target_CustomerType_Id populated using normalized name match.")
#     except Exception as e:
#         logger.exception("❌ Failed to update Target_CustomerType_Id from Map_CustomerType")

# def get_existing_qbo_customer_id(name):
#     """
#     Checks if a customer with the given name already exists in QBO.
#     Args:
#         name (str): DisplayName or CompanyName of the customer.

#     Returns:
#         str or None:
#           - If found in Customer → returns the QBO Customer Id.
#           - If found only in Vendor/Employee → logs a warning and returns None
#             so the migration marks the Customer as Failed gracefully.
#     """
#     safe_name = name.replace("'", "''")

#     # 🔍 First check in Customer
#     query_customer = f"SELECT Id FROM Customer WHERE DisplayName = '{safe_name}'"
#     response = requests.post(query_url, headers=headers, data=query_customer)
#     if response.status_code == 200:
#         customers = response.json().get("QueryResponse", {}).get("Customer", [])
#         if customers:
#             return customers[0]["Id"]

#     # 🔍 Next check in Vendor
#     query_vendor = f"SELECT Id FROM Vendor WHERE DisplayName = '{safe_name}'"
#     response = requests.post(query_url, headers=headers, data=query_vendor)
#     if response.status_code == 200:
#         vendors = response.json().get("QueryResponse", {}).get("Vendor", [])
#         if vendors:
#             vendor_id = vendors[0]["Id"]
#             logger.warning(f"⚠️ Name '{name}' already exists as a Vendor (Id={vendor_id}). Marking Customer as Failed.")
#             return None

#     # 🔍 Finally check in Employee
#     query_employee = f"SELECT Id FROM Employee WHERE DisplayName = '{safe_name}'"
#     response = requests.post(query_url, headers=headers, data=query_employee)
#     if response.status_code == 200:
#         employees = response.json().get("QueryResponse", {}).get("Employee", [])
#         if employees:
#             employee_id = employees[0]["Id"]
#             logger.warning(f"⚠️ Name '{name}' already exists as an Employee (Id={employee_id}). Marking Customer as Failed.")
#             return None

#     return None

# def build_payload(row):
#     """
#     Builds a valid QBO Customer JSON payload from a single record in the 
#     Map_Customer table.

#     - Maps all supported fields using CUSTOMER_COLUMN_MAPPING.
#     - Conditionally adds:
#         - PaymentMethodRef from Map_PaymentMethod
#         - CustomerTypeRef if Target_CustomerType_Id is present

#     Args:
#         row (pandas.Series): A single record from Map_Customer.

#     Returns:
#         dict: JSON-compatible payload for QBO Customer.
#     """
#     payload = {}
#     for src_col, qbo_col in column_mapping.items():
#         value = row.get(src_col)
#         if pd.notna(value) and str(value).strip():
#             if "." in qbo_col:
#                 parent, child = qbo_col.split(".")
#                 payload.setdefault(parent, {})[child] = value
#             else:
#                 payload[qbo_col] = value

#     # Map PaymentMethodRef
#     src_payment_method_id = row.get("PaymentMethodRef.value")
#     if pd.notna(src_payment_method_id):
#         mapped = sql.fetch_scalar(f"""
#             SELECT Target_Id FROM {mapping_schema}.Map_PaymentMethod WHERE Source_Id = ?
#         """, (src_payment_method_id,))
#         if mapped:
#             payload["PaymentMethodRef"] = {"value": mapped}

#     # Add CustomerTypeRef
# # Add CustomerTypeRef only if mapped
#     target_type_id = row.get("Target_CustomerType_Id")
#     if target_type_id and str(target_type_id).strip():
#         payload["CustomerTypeRef"] = {"value": str(target_type_id)}

#     payload["Active"] = True  # ✅ Always mark customer as active
#     return payload

# def enrich_mapping_in_batches():
#     df = sql.fetch_dataframe(f"""
#         SELECT * FROM [{mapping_schema}].[{mapping_table}]
#         WHERE Payload_JSON IS NULL 
#     """)
#     if df.empty:
#         logger.info("✅ No payloads to generate.")
#         return

#     for start in range(0, len(df), batch_size):
#         batch = df.iloc[start:start + batch_size]
#         timer = ProgressTimer(len(batch))
#         for _, row in batch.iterrows():
#             try:
#                 source_id = row["Source_Id"]
#                 payload = build_payload(row)
#                 name = row.get("DisplayName") or row.get("FullyQualifiedName") or row.get("CompanyName")

#                 if not name:
#                     update_mapping_status(mapping_schema, mapping_table, source_id,
#                                           "Skipped", failure_reason="Missing name")
#                     timer.update(); continue

#                 if not payload:
#                     update_mapping_status(mapping_schema, mapping_table, source_id,
#                                           "Skipped", failure_reason="Empty payload")
#                     timer.update(); continue

#                 if "DisplayName" not in payload:
#                     payload["DisplayName"] = name

#                 existing_id = get_existing_qbo_customer_id_safe(name, query_url, access_token)
#                 if existing_id:
#                     update_mapping_status(mapping_schema, mapping_table, source_id,
#                                           "Exists", target_id=existing_id, payload=payload)
#                 else:
#                     update_mapping_status(mapping_schema, mapping_table, source_id,
#                                           "Ready", payload=payload)
#             except Exception:
#                 logger.exception(f"❌ Payload generation failed for {row.get('Source_Id')}")
#             finally:
#                 timer.update()

# session = requests.Session()

# def post_staged_customers():
#     """
#     Posts customers where Porter_Status in ('Ready','Failed') and Payload_JSON is present.
#     On 6240 (Duplicate Name Exists), recovers the existing Customer Id via literal-free scan
#     and marks the row as 'Exists' with Target_Id.
#     """
#     df = sql.fetch_dataframe(f"""
#         SELECT * FROM [{mapping_schema}].[{mapping_table}]
#         WHERE Porter_Status IN ('Ready', 'Failed') AND Payload_JSON IS NOT NULL
#     """)
#     if df.empty:
#         logger.info("✅ No customers to post.")
#         return

#     logger.info(f"📤 Posting {len(df)} customer(s) to QBO...")
#     timer = ProgressTimer(len(df))

#     for _, row in df.iterrows():
#         source_id = row["Source_Id"]
#         try:
#             payload = json.loads(row["Payload_JSON"])
#             response = session.post(post_url, headers=headers, json=payload)

#             if response.status_code == 200:
#                 target_id = response.json()["Customer"]["Id"]
#                 logger.info(f"✅ Migrated: {payload.get('DisplayName')} → {target_id}")
#                 update_mapping_status(mapping_schema, mapping_table, source_id,
#                                       "Success", target_id=target_id)
#                 timer.update()
#                 continue

#             # --- Non-200: parse safely and branch ---
#             code, msg, det = _parse_qbo_error(response)
#             reason_full = f"code={code} | msg={msg} | detail={det}"

#             # Handle duplicate name (6240) from QBO
#             if str(code) == "6240" or (msg and "Duplicate Name" in msg):
#                 # best name from payload; normalize inside helper
#                 name = _best_name_from_payload(payload)
#                 existing_id = get_existing_qbo_customer_id_safe(name, query_url, access_token)

#                 if existing_id:
#                     logger.info(f"♻️ Duplicate detected. Recovered existing QBO Customer Id: {name!r} → {existing_id}")
#                     update_mapping_status(mapping_schema, mapping_table, source_id,
#                                           "Exists", target_id=existing_id, payload=payload)
#                     timer.update()
#                     continue
#                 else:
#                     logger.warning(f"⚠️ Duplicate name {name!r} but could not recover existing Customer Id.")
#                     update_mapping_status(mapping_schema, mapping_table, source_id,
#                                           "Failed", failure_reason="Duplicate name (6240), ID not found via scan",
#                                           increment_retry=False)
#                     timer.update()
#                     continue

#             # Other faults → record full reason and increment retry
#             update_mapping_status(mapping_schema, mapping_table, source_id,
#                                   "Failed", failure_reason=reason_full, increment_retry=True)

#         except Exception as e:
#             # Catch-all safety net: record full exception
#             update_mapping_status(mapping_schema, mapping_table, source_id,
#                                   "Failed", failure_reason=str(e), increment_retry=True)

#         timer.update()

# def log_unmapped_customer_types():
#     """
#     Logs all distinct Source_CustomerType_Name values in Map_Customer
#     that have not been mapped to Target_CustomerType_Id.
#     Useful for debugging unmapped or misspelled types.
#     Returns:
#         None
#     """
#     query = f"""
#         SELECT DISTINCT Source_CustomerType_Name
#         FROM [{mapping_schema}].[{mapping_table}]
#         WHERE Source_CustomerType_Name IS NOT NULL
#           AND Target_CustomerType_Id IS NULL
#     """
#     df = sql.fetch_dataframe(query)
#     if not df.empty:
#         logger.warning(f"⚠️ Unmapped CustomerType names ({len(df)}):")
#         for name in df["Source_CustomerType_Name"].dropna().head(10):
#             logger.warning(f"   - '{name}'")

# # === Main Migration Orchestration
# def migrate_customers():
#     """
#     Main orchestration function for Customer migration to QBO.

#     - Ensures schema and table exist.
#     - Initializes mapping table from source.
#     - Loads QBO CustomerTypes and maps them locally.
#     - Generates JSON payloads for each record.
#     - Posts customers to QBO one-by-one with retries.

#     Returns:
#         None
#     """
#     logger.info("🚀 Starting Customer Migration")
#     df = sql.fetch_table(source_table, source_schema)
#     if df.empty:
#         logger.warning("⚠️ No Customer records found.")
#         return

#     sql.ensure_schema_exists(mapping_schema)
#     initialize_mapping_table(df, mapping_table, mapping_schema)
#     ensure_additional_columns()

#     sync_qbo_customertype_to_map()
#     enrich_source_customertype_name()
#     map_customertype_ids_to_mapping_table()
#     log_unmapped_customer_types()
#     enrich_mapping_in_batches()
#     post_staged_customers()

#     retry_df, _ = get_retryable_subset(source_table, source_schema, mapping_table, mapping_schema, max_retries)
#     if not retry_df.empty:
#         logger.info("🔁 Retrying failed customer posts...")
#         post_staged_customers()

#     logger.info("🏁 Customer migration completed")
