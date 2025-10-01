
# """
# Sequence : 11
# Module: employee_migrator.py
# Author: Dixit Prajapati
# Created: 2025-09-17
# Description: Handles migration of employee records from source system to QBO,
#              including parent-child hierarchy processing, retry logic, and status tracking.
# Production : Ready
# Phase : 02 - Multi User
# """

import os, json, requests, pandas as pd
from dotenv import load_dotenv
try:
    import orjson
    def dumps_fast(obj): return orjson.dumps(obj, option=orjson.OPT_INDENT_2).decode("utf-8")
    def loads_fast(s):   return orjson.loads(s)
except Exception:
    def dumps_fast(obj): return json.dumps(obj, ensure_ascii=False, indent=2)
    def loads_fast(s):   return json.loads(s)
from storage.sqlserver import sql
from config.mapping.employee_mapping import EMPLOYEE_COLUMN_MAPPING as column_mapping
from utils.token_refresher import auto_refresh_token_if_needed, get_qbo_context_migration
from utils.retry_handler import initialize_mapping_table, get_retryable_subset
from utils.log_timer import global_logger as logger, ProgressTimer
from utils.mapping_updater import update_mapping_status

# ───────────────────────────────────────────────
# Bootstrap
# ───────────────────────────────────────────────
auto_refresh_token_if_needed()
load_dotenv()

ctx = get_qbo_context_migration()
access_token = ctx["ACCESS_TOKEN"]
realm_id     = ctx["REALM_ID"]
environment  = os.getenv("QBO_ENVIRONMENT", "sandbox")
base_url     = "https://sandbox-quickbooks.api.intuit.com" if environment == "sandbox" else "https://quickbooks.api.intuit.com"

query_url = f"{base_url}/v3/company/{realm_id}/query"
post_url  = f"{base_url}/v3/company/{realm_id}/employee"
headers   = {"Authorization": f"Bearer {access_token}", "Accept": "application/json", "Content-Type": "application/json"}

# DB config
source_schema  = os.getenv("SOURCE_SCHEMA", "dbo")
mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
source_table   = "Employee"
mapping_table  = "Map_Employee"
batch_size     = int(os.getenv("EMPLOYEE_BATCH_SIZE", "1000"))
max_retries    = 3

# HTTP session
session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(pool_connections=40, pool_maxsize=100, max_retries=3)
session.mount("https://", _adapter)
session.mount("http://", _adapter)

# ───────────────────────────────────────────────
# Helpers
# ───────────────────────────────────────────────
def ensure_additional_columns():
    alter_cols = [
        ("Payload_JSON", "NVARCHAR(MAX)"),
        ("Porter_Status", "VARCHAR(50)"),
        ("Failure_Reason", "NVARCHAR(1000)"),
        ("Target_Id", "VARCHAR(100)"),
        ("Retry_Count", "INT"),
    ]
    for col, col_type in alter_cols:
        sql.run_query(f"""
            IF COL_LENGTH('{mapping_schema}.{mapping_table}', '{col}') IS NULL
            BEGIN
                ALTER TABLE [{mapping_schema}].[{mapping_table}] ADD {col} {col_type}
            END
        """)

def _norm_name(s: str) -> str:
    if s is None: return ""
    return (str(s).replace("\u2019", "'").replace("\u2018", "'").replace("\u00A0", " ").strip())

def _best_name_from_payload(payload: dict) -> str:
    for k in ("DisplayName", "GivenName", "FamilyName"):
        v = payload.get(k)
        if isinstance(v, str) and v.strip():
            return _norm_name(v)
    return ""

def _try_exact_lookup_employee_id(name: str) -> str | None:
    if not name: return None
    safe = name.replace("'", "''")
    q = f"SELECT Id FROM Employee WHERE DisplayName = '{safe}'"
    r = session.post(query_url, headers={"Authorization": f"Bearer {access_token}", "Accept":"application/json","Content-Type":"application/text"}, data=q)
    if r.status_code == 200:
        items = r.json().get("QueryResponse", {}).get("Employee", [])
        if items: return items[0].get("Id")
    return None

def _recover_employee_id_by_name(payload: dict) -> str | None:
    """Resolve existing QBO Employee Id by DisplayName (duplicate handling)."""
    name = _best_name_from_payload(payload)
    if not name: return None

    # 1. Try exact lookup
    cid = _try_exact_lookup_employee_id(name)
    if cid: return cid

    # 2. Page scan fallback
    target = _norm_name(name)
    start_pos, page_size = 1, 1000
    while True:
        q = f"SELECT Id, DisplayName FROM Employee ORDERBY DisplayName STARTPOSITION {start_pos} MAXRESULTS {page_size}"
        r = session.post(query_url, headers=headers, data=q)
        if r.status_code != 200: return None
        items = r.json().get("QueryResponse", {}).get("Employee", [])
        if not items: return None
        for it in items:
            if _norm_name(it.get("DisplayName")) == target:
                return it.get("Id")
        start_pos += page_size

def build_payload_from_row(row_dict: dict) -> dict:
    """Fast payload builder with nested object support (Address, Email, Phone)."""
    payload = {}
    for src_col, qbo_col in column_mapping.items():
        v = row_dict.get(src_col)
        if v is None or (isinstance(v, str) and not v.strip()):
            continue

        if "." in qbo_col:
            parent, child = qbo_col.split(".", 1)
            payload.setdefault(parent, {})[child] = v
        else:
            payload[qbo_col] = v

    payload["Active"] = True
    if "DisplayName" not in payload:
        payload["DisplayName"] = (
            row_dict.get("DisplayName") or row_dict.get("GivenName") or row_dict.get("FamilyName")
        )
    return payload

# ───────────────────────────────────────────────
# Payload staging
# ───────────────────────────────────────────────
def stage_payloads_bulk():
    df = sql.fetch_dataframe(f"""
        SELECT *
        FROM [{mapping_schema}].[{mapping_table}]
        WHERE Payload_JSON IS NULL
    """)
    if df.empty:
        logger.info("✅ No employee payloads to stage.")
        return

    cols = [c for c in df.columns if c in column_mapping.keys() or c in ("Source_Id","DisplayName","GivenName","FamilyName")]
    df = df[cols].copy()

    logger.info(f"🧱 Staging payloads for {len(df)} employees (batched {batch_size})...")
    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start+batch_size]
        updates = []
        for row in batch.itertuples(index=False, name=None):
            rd = dict(zip(batch.columns, row))
            try:
                source_id = rd["Source_Id"]
                payload = build_payload_from_row(rd)
                if not payload:
                    updates.append((source_id, "Skipped", None, "Empty payload")); continue
                updates.append((source_id, "Ready", dumps_fast(payload), None))
            except Exception as e:
                updates.append((rd.get("Source_Id"), "Failed", None, f"payload_error: {e}"))
        _bulk_update_payloads(updates)

def _bulk_update_payloads(updates):
    if not updates: return
    dfu = pd.DataFrame(updates, columns=["Source_Id","Porter_Status","Payload_JSON","Failure_Reason"])
    staging = "_tmp_employee_payloads"
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
    sql.run_query(f"""
        UPDATE ME
           SET ME.Porter_Status  = ISNULL(S.Porter_Status, ME.Porter_Status),
               ME.Payload_JSON   = CASE WHEN S.Payload_JSON IS NOT NULL THEN S.Payload_JSON ELSE ME.Payload_JSON END,
               ME.Failure_Reason = CASE WHEN S.Failure_Reason IS NOT NULL THEN S.Failure_Reason ELSE ME.Failure_Reason END
        FROM [{mapping_schema}].[{mapping_table}] ME
        INNER JOIN [{mapping_schema}].[{staging}] S
                ON S.Source_Id = ME.Source_Id;
        DROP TABLE [{mapping_schema}].[{staging}];
    """)

# ───────────────────────────────────────────────
# Posting
# ───────────────────────────────────────────────
def post_staged_employees():
    df = sql.fetch_dataframe(f"""
        SELECT Source_Id, Payload_JSON
        FROM [{mapping_schema}].[{mapping_table}]
        WHERE Porter_Status IN ('Ready','Failed') AND Payload_JSON IS NOT NULL
    """)
    if df.empty:
        logger.info("✅ No employees to post.")
        return

    logger.info(f"📤 Posting {len(df)} employee(s) to QBO...")
    timer = ProgressTimer(len(df))
    for row in df.itertuples(index=False):
        sid, payload_json = row
        try:
            payload = loads_fast(payload_json)
            r = session.post(post_url, headers=headers, json=payload)
            if r.status_code == 200:
                qid = r.json()["Employee"]["Id"]
                update_mapping_status(mapping_schema, mapping_table, sid, "Success", target_id=qid)
                timer.update(); continue

            data = r.json() if hasattr(r,"json") else {}
            err = (data.get("Fault", {}) or {}).get("Error", []) or []
            code = str(err[0].get("code")) if err else None
            msg  = err[0].get("Message") if err else None

            # Duplicate employee handling
            if str(code) == "6240" or (msg and "Duplicate Name" in msg):
                existing_id = _recover_employee_id_by_name(payload)
                if existing_id:
                    update_mapping_status(mapping_schema, mapping_table, sid,
                                          "Exists", target_id=existing_id, payload=payload)
                else:
                    update_mapping_status(mapping_schema, mapping_table, sid,
                                          "Exists", payload=payload,
                                          failure_reason="Duplicate name; could not resolve existing Id")
                timer.update(); continue

            reason = r.text[:300]
            update_mapping_status(mapping_schema, mapping_table, sid, "Failed", failure_reason=reason, increment_retry=True)
        except Exception as e:
            update_mapping_status(mapping_schema, mapping_table, sid, "Failed", failure_reason=str(e), increment_retry=True)
        timer.update()

# ───────────────────────────────────────────────
# Orchestrator
# ───────────────────────────────────────────────
def migrate_employees():
    logger.info("🚀 Starting Employee Migration (fast mode)")
    df = sql.fetch_table(source_table, source_schema)
    if df.empty:
        logger.warning("⚠️ No Employee records found.")
        return

    sql.ensure_schema_exists(mapping_schema)
    initialize_mapping_table(df, mapping_table, mapping_schema)
    ensure_additional_columns()

    # Stage & post
    stage_payloads_bulk()
    post_staged_employees()

    retry_df, _ = get_retryable_subset(source_table, source_schema, mapping_table, mapping_schema, max_retries)
    if not retry_df.empty:
        logger.info("🔁 Retrying failed employees...")
        post_staged_employees()

    logger.info("🏁 Employee migration completed")

