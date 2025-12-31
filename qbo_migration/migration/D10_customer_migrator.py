# """
# Sequence : 10
# Module: customer_migrator.py
# Author: Dixit Prajapati
# Created: 2025-10-09
# Description: Handles migration of customer records from source system to QBO,
#              including parent-child hierarchy processing, retry logic, and status tracking.
# Production : Ready
# Phase : 02 - Multi User + Parent-Child + Job
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
    """Exact match by DisplayName; includes inactive (deleted) customers."""
    if not name:
        return None
    safe = name.replace("'", "''")
    q = f"SELECT Id FROM Customer WHERE DisplayName = '{safe}' AND Active IN (true,false)"
    r, ok, _ = qbo_query(query_url, q, access_token)
    if not ok:
        return None
    items = r.json().get("QueryResponse", {}).get("Customer", [])
    if items:
        return items[0].get("Id")
    return None


def _try_exact_lookup_customer_id_by_fqn(fqn: str) -> str | None:
    """Exact match by FullyQualifiedName; includes inactive (deleted) customers."""
    if not fqn:
        return None
    safe = fqn.replace("'", "''")
    q = f"SELECT Id FROM Customer WHERE FullyQualifiedName = '{safe}' AND Active IN (true,false)"
    r, ok, _ = qbo_query(query_url, q, access_token)
    if not ok:
        return None
    items = r.json().get("QueryResponse", {}).get("Customer", [])
    if items:
        return items[0].get("Id")
    return None



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


_DELETED_PAT = re.compile(r"\s*\(deleted\)\s*\d*$", re.IGNORECASE)

def _is_deleted_style(name: str | None) -> bool:
    if not name:
        return False
    return bool(_DELETED_PAT.search(name.strip()))

def _strip_deleted_suffix(name: str) -> str:
    """Remove ' (deleted)' or ' (deleted) N' suffix to get the base name."""
    if not name:
        return ""
    return _DELETED_PAT.sub("", name).strip()

def _recover_deleted_style_id_by_displayname(display_name: str) -> str | None:
    """
    Resolve an Id for DisplayName that contains '(deleted)'.
    Strategy:
      1) Exact match (includes inactive).
      2) If not found, search LIKE '<base> (deleted%' and choose the closest (prefer exact text if present),
         else fall back to the first result.
    """
    exact = _try_exact_lookup_customer_id(display_name)
    if exact:
        return exact

    base = _strip_deleted_suffix(display_name)
    if not base:
        return None

    # Pull a page of candidates; include inactive; search by LIKE to catch numbered variants.
    # We’ll scan pages until none remain or we find a best match.
    start_pos, page_size = 1, 1000
    like_safe = base.replace("'", "''")
    while True:
        q = f"""
            SELECT Id, DisplayName, FullyQualifiedName, Active
            FROM Customer
            WHERE DisplayName LIKE '{like_safe} (deleted%'
              AND Active IN (true,false)
            ORDERBY DisplayName
            STARTPOSITION {start_pos}
            MAXRESULTS {page_size}
        """
        r, ok, _ = qbo_query(query_url, _normalize_query(q), access_token)
        if not ok:
            return None
        items = r.json().get("QueryResponse", {}).get("Customer", [])
        if not items:
            return None

        # Prefer exact textual match (case-insensitive); otherwise take first candidate.
        target_norm = _norm_name(display_name)
        for it in items:
            if _norm_name(it.get("DisplayName")) == target_norm:
                return it.get("Id")

        # If no exact textual match in this page, take the first item from first page as a fallback.
        # (But only if this is the first page; otherwise keep paging to look for an exact match.)
        if start_pos == 1 and items:
            return items[0].get("Id")

        start_pos += page_size


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
        ("Target_Parent_Id", "VARCHAR(100)"),
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

def backfill_target_parent_ids_into_map_customer():
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

        if not _column_exists(mapping_schema, mapping_table, "ParentRef.value"):
            logger.info("⏩ Skip: column [ParentRef.value] not found in "
                        f"[{mapping_schema}].[{mapping_table}].")
            return

        if not _column_exists(mapping_schema, mapping_table, "Target_Parent_Id"):
            logger.info("⏩ Skip: column [Target_Parent_Id] not found in "
                        f"[{mapping_schema}].[{mapping_table}].")
            return

        # --- do the backfill ---
        rows_before = sql.fetch_table_with_params(
            f"""
            SELECT COUNT(1) AS cnt
            FROM [{mapping_schema}].[{mapping_table}]
            WHERE Target_Parent_Id IS NULL AND [ParentRef.value] IS NOT NULL
            """,
            tuple()
        )
        to_update = int(rows_before.iloc[0]["cnt"]) if not rows_before.empty else 0

        if to_update == 0:
            logger.info("✅ No rows need backfill for Target_Parent_Id.")
            return

        # Update Target_Parent_Id by looking up the Target_Id from the same mapping table
        # where the Source_Id matches the ParentRef.value
        sql.run_query(f"""
            UPDATE MC
            SET MC.Target_Parent_Id = MP.Target_Id
            FROM [{mapping_schema}].[{mapping_table}] MC
            LEFT JOIN [{mapping_schema}].[{mapping_table}] MP
              ON TRY_CONVERT(VARCHAR(100), MC.[ParentRef.value]) = TRY_CONVERT(VARCHAR(100), MP.Source_Id)
            WHERE MC.Target_Parent_Id IS NULL
              AND MC.[ParentRef.value] IS NOT NULL
              AND MP.Target_Id IS NOT NULL
        """)

        rows_after = sql.fetch_table_with_params(
            f"""
            SELECT COUNT(1) AS cnt
            FROM [{mapping_schema}].[{mapping_table}]
            WHERE Target_Parent_Id IS NULL AND [ParentRef.value] IS NOT NULL
            """,
            tuple()
        )
        still_null = int(rows_after.iloc[0]["cnt"]) if not rows_after.empty else 0
        updated = to_update - still_null

        logger.info(f"✅ Backfill Target_Parent_Id completed. Updated: {updated}, Still null: {still_null}")

    except Exception as e:
        logger.warning(f"Backfill Target_Parent_Id failed: {e}")

# ────────────────────────────────────────────────────────────────────
# Payload builder (vectorized-friendly)
# ────────────────────────────────────────────────────────────────────
# Build once: source columns we need from mapping table
_needed_src_cols = set(["Source_Id","DisplayName","FullyQualifiedName","CompanyName",
                        "PaymentMethodRef.value","SalesTermRef.value","Target_CustomerType_Id","Target_Term_Id","Target_Parent_Id","Level"])
_needed_src_cols.update(list(column_mapping.keys()))

def _name_from_row(row_dict):
    return row_dict.get("DisplayName") or row_dict.get("FullyQualifiedName") or row_dict.get("CompanyName")

def build_payload_from_row(row_dict: dict) -> dict:
    """Fast payload builder that uses dicts & preloaded maps only."""
    payload = {}
    for src_col, qbo_col in column_mapping.items():
        # Skip ParentRef.value - we'll handle it separately with Target_Parent_Id
        if src_col == "ParentRef.value":
            continue
            
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

    # ParentRef via mapped Target_Parent_Id
    t_parent = row_dict.get("Target_Parent_Id")
    if t_parent:
        payload["ParentRef"] = {"value": str(t_parent)}

    payload["Active"] = True
    # Ensure name present (post step also relies on this for dup handling)
    name = _name_from_row(row_dict)
    if name and "DisplayName" not in payload:
        payload["DisplayName"] = name
    return payload

# ────────────────────────────────────────────────────────────────────
# FAST: Bulk payload staging (no existence scan)
# ────────────────────────────────────────────────────────────────────
def stage_payloads_bulk(level=None, phase=None):
    """
    Build payloads for rows with NULL Payload_JSON and write them back in set-based batches.
    We SKIP QBO existence scans here for speed (duplicates handled on post with code=6240).
    
    Args:
        level: Specific level to process (e.g., None, 1, 2, etc.)
        phase: Legacy parameter - 'parent' to process only parent customers (ParentRef.value IS NULL)
               'child' to process only child customers (ParentRef.value IS NOT NULL)
               None to process all customers
    """
    # Build WHERE clause based on level or phase
    # Support sentinel level='ALL' to process all rows (no Level filter)
    where_clause = "WHERE Payload_JSON IS NULL"

    if level == 'ALL':
        # No additional filter; process all rows
        pass
    elif level is None:
        where_clause += " AND ([Level] IS NULL)"
    elif level is not None:
        # Handle numeric levels (convert to string for SQL comparison)
        where_clause += f" AND [Level] = '{level}'"
    elif phase == 'parent':
        where_clause += " AND ([ParentRef.value] IS NULL OR LTRIM(RTRIM([ParentRef.value])) = '')"
    elif phase == 'child':
        where_clause += " AND [ParentRef.value] IS NOT NULL AND LTRIM(RTRIM([ParentRef.value])) != ''"
    
    # Pull only columns we need (fallback to SELECT * if necessary)
    df = sql.fetch_dataframe(f"""
        SELECT *
        FROM [{mapping_schema}].[{mapping_table}]
        {where_clause}
    """)
    if df.empty:
        level_msg = f" (Level {level} customers)" if level is not None else ""
        phase_msg = f" ({phase} customers)" if phase and not level_msg else ""
        logger.info(f"✅ No payloads to generate{level_msg}{phase_msg}.")
        return

    # Filter to needed columns to lower memory and speed to_dict()
    cols = [c for c in df.columns if c in _needed_src_cols]
    df = df[cols].copy()

    level_msg = f" for Level {level} customers" if level is not None else ""
    phase_msg = f" for {phase} customers" if phase and not level_msg else ""
    logger.info(f"🧱 Staging payloads for {len(df)} rows{level_msg}{phase_msg} (batched {batch_size})...")
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


def post_staged_customers(level=None, phase=None):
    """
    Post staged customer payloads to QBO.

    Args:
        level: Specific level to process (e.g., None, 1, 2, etc.)
        phase: 'parent' to process only parent customers (ParentRef.value IS NULL)
               'child'  to process only child customers (ParentRef.value IS NOT NULL)
               None     to process all customers
    """
    # Build WHERE clause based on level or phase
    # Support sentinel level='ALL' to process all rows (no Level filter)
    where_clause = "WHERE Porter_Status IN ('Ready','Failed') AND Payload_JSON IS NOT NULL"

    if level == 'ALL':
        pass
    elif level is None:
        where_clause += " AND ([Level] IS NULL)"
    elif level is not None:
        where_clause += f" AND [Level] = '{level}'"
    elif phase == 'parent':
        where_clause += " AND ([ParentRef.value] IS NULL OR LTRIM(RTRIM([ParentRef.value])) = '')"
    elif phase == 'child':
        where_clause += " AND [ParentRef.value] IS NOT NULL AND LTRIM(RTRIM([ParentRef.value])) != ''"

    # NOTE: include FullyQualifiedName so we can resolve dupes deterministically
    df = sql.fetch_dataframe(f"""
        SELECT Source_Id, Payload_JSON, FullyQualifiedName
        FROM [{mapping_schema}].[{mapping_table}]
        {where_clause}
    """)
    if df.empty:
        level_msg = f" (Level {level} customers)" if level is not None else ""
        phase_msg = f" ({phase} customers)" if phase and not level_msg else ""
        logger.info(f"✅ No customers to post{level_msg}{phase_msg}.")
        return

    level_msg = f" Level {level}" if level is not None else ""
    phase_msg = f" {phase}" if phase and not level_msg else ""
    logger.info(f"📤 Posting {len(df)}{level_msg}{phase_msg} customer(s) to QBO...")
    timer = ProgressTimer(len(df))

    for row in df.itertuples(index=False):
        source_id, payload_json, fqn_hint = row
        try:
            payload = loads_fast(payload_json) if isinstance(payload_json, str) else payload_json
            r = session.post(post_url, headers=headers, json=payload)

            if r.status_code == 200:
                target_id = r.json()["Customer"]["Id"]
                update_mapping_status(
                    mapping_schema, mapping_table, source_id,
                    "Success", target_id=target_id
                )
                timer.update()
                continue

            code, msg, det = _parse_qbo_error(r)

            # 1) Duplicate name → resolve using FQN-first (existing logic)
            if str(code) == "6240" or (msg and "Duplicate Name" in msg):
                existing_id = _recover_customer_id(payload, fqn_hint=fqn_hint)
                if existing_id:
                    update_mapping_status(
                        mapping_schema, mapping_table, source_id,
                        "Exists", target_id=existing_id, payload=payload
                    )
                else:
                    update_mapping_status(
                        mapping_schema, mapping_table, source_id,
                        "Exists", payload=payload,
                        failure_reason="Duplicate name; could not resolve Id via FQN"
                    )
                timer.update()
                continue

            # 2) Project name conflict (business validation): treat like duplicate,
            #    but if we cannot resolve an existing id, mark as Failed (and retry).
            project_conflict_msg = (msg or "").lower()
            if str(code) == "6000" or ("project" in project_conflict_msg and "already" in project_conflict_msg and "name" in project_conflict_msg):
                logger.info(f"ℹ️ Project-name conflict for Source_Id={source_id}; attempting to resolve existing Id.")
                existing_id = _recover_customer_id(payload, fqn_hint=fqn_hint)
                if existing_id:
                    update_mapping_status(
                        mapping_schema, mapping_table, source_id,
                        "Exists", target_id=existing_id, payload=payload
                    )
                    logger.info(f"✅ Resolved project-name conflict for Source_Id={source_id} -> Target_Id={existing_id}")
                else:
                    # If we couldn't resolve the target id, mark as Failed and allow retry.
                    failure_text = f"code={code} | msg={msg} | detail={det} (project conflict; could not resolve Id)"
                    update_mapping_status(
                        mapping_schema, mapping_table, source_id,
                        "Failed",
                        failure_reason=failure_text,
                        increment_retry=True
                    )
                    logger.warning(f"⚠️ Project-name conflict for Source_Id={source_id} and Id resolution failed. Marked as Failed for retry.")
                timer.update()
                continue

            # 3) Other errors → Failed (unchanged)
            update_mapping_status(
                mapping_schema, mapping_table, source_id,
                "Failed",
                failure_reason=f"code={code} | msg={msg} | detail={det}",
                increment_retry=True
            )
        except Exception as e:
            update_mapping_status(
                mapping_schema, mapping_table, source_id,
                "Failed", failure_reason=str(e), increment_retry=True
            )
        timer.update()


def _recover_customer_id(payload: dict, fqn_hint: str | None = None) -> str | None:
    """
    Resolve existing QBO Customer Id, prioritizing FullyQualifiedName (FQN).
    Special-case: if DisplayName looks like '(... deleted ...)', use a deleted-name resolver.
    Order (non-deleted):
      1) fqn_hint (from mapping)
      2) reconstruct FQN via ParentRef + DisplayName
      3) top-level FQN == DisplayName
      4) exact DisplayName
      5) paged scan (FQN, then DisplayName)
    """
    display_name = payload.get("DisplayName")

    # 🔸 Special-case branch for deleted-style names
    if _is_deleted_style(display_name):
        cid = _recover_deleted_style_id_by_displayname(display_name)
        if cid:
            return cid
        # If we still failed, continue with generic logic (it may still recover via FQN paths)

    # 1) FQN hint (from mapping table)
    if fqn_hint:
        cid = _try_exact_lookup_customer_id_by_fqn(_norm_name(fqn_hint))
        if cid:
            return cid

    parent_ref = (payload.get("ParentRef") or {}).get("value")

    # 2) Child: reconstruct FQN = "<parent_fqn>:<DisplayName>"
    if parent_ref and display_name:
        q = f"SELECT FullyQualifiedName FROM Customer WHERE Id = '{parent_ref}'"
        r, ok, _ = qbo_query(query_url, q, access_token)
        if ok:
            items = r.json().get("QueryResponse", {}).get("Customer", [])
            if items:
                parent_fqn = items[0].get("FullyQualifiedName")
                if parent_fqn:
                    candidate_fqn = f"{parent_fqn}:{display_name}"
                    cid = _try_exact_lookup_customer_id_by_fqn(candidate_fqn)
                    if cid:
                        return cid

    # 3) Top-level: FQN equals DisplayName
    if not parent_ref and display_name:
        cid = _try_exact_lookup_customer_id_by_fqn(display_name)
        if cid:
            return cid

    # 4) Exact DisplayName (includes inactive)
    name = _best_name_from_payload(payload)
    if name:
        cid = _try_exact_lookup_customer_id(name)
        if cid:
            return cid

    # 5) Paged scan; prefer FQN match (if we have a hint), else DisplayName
    start_pos, page_size = 1, 1000
    target_fqn = _norm_name(fqn_hint or "")
    target_name = _norm_name(name or "")
    while True:
        q = f"""
            SELECT Id, FullyQualifiedName, DisplayName
            FROM Customer
            ORDERBY FullyQualifiedName
            STARTPOSITION {start_pos}
            MAXRESULTS {page_size}
        """
        r, ok, _ = qbo_query(query_url, _normalize_query(q), access_token)
        if not ok:
            return None
        items = r.json().get("QueryResponse", {}).get("Customer", [])
        if not items:
            return None
        for it in items:
            fqn_val = _norm_name(it.get("FullyQualifiedName"))
            dn_val  = _norm_name(it.get("DisplayName"))
            if target_fqn and fqn_val == target_fqn:
                return it.get("Id")
            if not target_fqn and target_name and dn_val == target_name:
                return it.get("Id")
        start_pos += page_size

# ────────────────────────────────────────────────────────────────────
# Level-based hierarchy management
# ────────────────────────────────────────────────────────────────────
def get_customer_levels():
    """Get all distinct levels from the mapping table, sorted for proper hierarchy processing."""
    try:
        df = sql.fetch_dataframe(f"""
            SELECT DISTINCT [Level]
            FROM [{mapping_schema}].[{mapping_table}]
            WHERE [Level] IS NOT NULL
        """)
        
        # Keep original string values but sort them properly
        original_levels = []
        if not df.empty:
            original_levels = [str(level) for level in df['Level'].tolist() if level is not None]
        
        # Sort levels numerically by converting to float for comparison, but keep original strings
        def sort_key(level_str):
            try:
                return float(level_str)
            except (ValueError, TypeError):
                return 999  # Put non-numeric values at the end
        
        original_levels.sort(key=sort_key)
        
        # Check if there are records with NULL level (top-level parents)
        null_check = sql.fetch_dataframe(f"""
            SELECT COUNT(1) AS cnt
            FROM [{mapping_schema}].[{mapping_table}]
            WHERE [Level] IS NULL
        """)
        has_null_level = int(null_check.iloc[0]['cnt']) > 0 if not null_check.empty else False
        
        # Build final levels list - keep original string format for querying
        levels = []
        if has_null_level:
            levels.append(None)  # None represents NULL level
        levels.extend(original_levels)
            
        logger.info(f"📊 Customer hierarchy levels found: {levels}")
        
        # Log level distribution for better visibility
        log_level_distribution(levels)
        return levels
    except Exception as e:
        logger.warning(f"Failed to get customer levels: {e}")
        return [None]  # Fallback to just NULL level

def log_level_distribution(levels):
    """Log the count of customers at each level for better visibility."""
    try:
        for level in levels:
            if level is None:
                count_df = sql.fetch_dataframe(f"""
                    SELECT COUNT(1) AS cnt
                    FROM [{mapping_schema}].[{mapping_table}]
                    WHERE [Level] IS NULL
                """)
                count = int(count_df.iloc[0]['cnt']) if not count_df.empty else 0
                logger.info(f"  📈 Level NULL: {count} customers (top-level parents)")
            else:
                count_df = sql.fetch_dataframe(f"""
                    SELECT COUNT(1) AS cnt
                    FROM [{mapping_schema}].[{mapping_table}]
                    WHERE [Level] = '{level}'
                """)
                count = int(count_df.iloc[0]['cnt']) if not count_df.empty else 0
                logger.info(f"  📈 Level {level}: {count} customers")
    except Exception as e:
        logger.warning(f"Failed to log level distribution: {e}")

def migrate_customers_by_level():
    """
    Migrate customers level by level to maintain proper hierarchy.
    Process NULL level first, then 1, 2, 3, etc.
    Each level completion triggers parent reference alignment for next level.
    """
    logger.info("🏗️ Starting Level-Based Customer Migration")
    
    # If the mapping table doesn't have a 'Level' column, bypass level-based
    # migration and perform a flat (all-records) staging/posting flow.
    try:
        col_chk = sql.fetch_table_with_params(
            "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
            (mapping_schema, mapping_table, 'Level')
        )
        has_level_col = not col_chk.empty
    except Exception:
        has_level_col = False

    if not has_level_col:
        logger.info("ℹ️ 'Level' column not present — running flat (non-level) customer migration")
        # Stage all payloads (no Level filtering)
        stage_payloads_bulk(level='ALL')
        # Post all staged customers
        post_staged_customers(level='ALL')

        # Retry failed customers once (as original logic does per-level)
        retry_df, _ = get_retryable_subset(source_table, source_schema, mapping_table, mapping_schema, max_retries)
        if not retry_df.empty:
            logger.info("🔁 Retrying failed customer posts (flat mode)...")
            post_staged_customers(level='ALL')

        # Backfill parent ids once to align any children
        backfill_target_parent_ids_into_map_customer()
        logger.info("✅ Flat customer migration completed (no Level column)")
        return

    # Get all levels in the correct order
    levels = get_customer_levels()
    
    if not levels:
        logger.warning("⚠️ No customer levels found.")
        return
    
    for i, level in enumerate(levels):
        level_display = "NULL" if level is None else str(level)
        logger.info(f"🔥 Processing Level {level_display} customers ({i+1}/{len(levels)})")
        
        # Stage payloads for this level
        stage_payloads_bulk(level=level)
        
        # Post customers for this level
        post_staged_customers(level=level)
        
        # Retry failed customers for this level
        retry_df, _ = get_retryable_subset(source_table, source_schema, mapping_table, mapping_schema, max_retries)
        if not retry_df.empty:
            logger.info(f"🔁 Retrying failed Level {level_display} customer posts...")
            post_staged_customers(level=level)
        
        logger.info(f"✅ Level {level_display} migration completed")
        
        # After each level completion, align parent references for the next level
        # This ensures that customers at the next level can reference the correct QBO Target_Id
        if i < len(levels) - 1:  # Not the last level
            next_level = levels[i + 1]
            next_level_display = "NULL" if next_level is None else str(next_level)
            logger.info(f"🔗 Aligning parent references for upcoming Level {next_level_display} customers")
            backfill_target_parent_ids_into_map_customer()
    
    logger.info("🏁 Level-based customer migration completed with proper hierarchy alignment")

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

    # ========================================================================
    # DYNAMIC LEVEL-BASED MIGRATION
    # Process customers by hierarchy level: NULL -> 1 -> 2 -> 3 -> etc.
    # ========================================================================
    migrate_customers_by_level()

    logger.info("🏁 Customer migration completed (dynamic level-based hierarchy)")
