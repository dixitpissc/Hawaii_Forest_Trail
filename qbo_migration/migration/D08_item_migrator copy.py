"""
Sequence : 08
Module: item_migrator.py
Author: Dixit Prajapati
Created: 2025-09-17
Description: Handles migration of item records from source system to QBO,
             including parent-child hierarchy processing, retry logic, and status tracking.
Production : Ready
Phase : 02 - Multi User
"""

import os
import json
import time
import re
import requests
import pandas as pd
from functools import lru_cache
from dotenv import load_dotenv
from storage.sqlserver import sql
from config.mapping.item_mapping import (
    ITEM_COLUMN_MAPPING as column_mapping,
    ITEM_READONLY_FIELDS,
    ITEM_POST_ALLOWLIST,)
from utils.token_refresher import auto_refresh_token_if_needed,get_qbo_context_migration
from utils.retry_handler import initialize_mapping_table
from utils.log_timer import global_logger as logger, ProgressTimer

# ========= Tunables =========
CHECK_EXISTS_BEFORE_POST = False   # Avoid slow per-row QBO queries by default
HTTP_TIMEOUT_SECS = 30
LOG_EVERY = 100                    # progress log cadence during enrichment
QBO_MINOR_VERSION = "75"
# ============================

# === Auto-refresh QBO token if needed ===
auto_refresh_token_if_needed()
load_dotenv()

# === QBO Auth Config ===
ctx = get_qbo_context_migration()
access_token = ctx['ACCESS_TOKEN']
realm_id = ctx["REALM_ID"]
environment = os.getenv("QBO_ENVIRONMENT", "sandbox")

base_url  = "https://sandbox-quickbooks.api.intuit.com" if environment == "sandbox" else "https://quickbooks.api.intuit.com"
query_url = f"{base_url}/v3/company/{realm_id}/query"
post_url  = f"{base_url}/v3/company/{realm_id}/item"

# JSON headers (creates/updates)
headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# TEXT headers (for SQL /query)
QUERY_HEADERS = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/text",   # IMPORTANT for QBO SQL
}

# === DB Info ===
source_table   = "Item"
source_schema  = os.getenv("SOURCE_SCHEMA", "dbo")
mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
mapping_table  = "Map_Item"
max_retries    = 3

logger.info(f"🌍 QBO Environment: {environment.upper()} | Realm ID: {realm_id}")

# === Map_Account: Source_Id -> Target_Id ===
account_mapping_table = "Map_Account"
df_account_map = sql.fetch_table(account_mapping_table, mapping_schema)
if not df_account_map.empty and {"Source_Id","Target_Id"}.issubset(df_account_map.columns):
    # normalize as string keys to avoid type mismatches
    account_id_map = {str(s): str(t) for s, t in zip(df_account_map["Source_Id"], df_account_map["Target_Id"])}
else:
    logger.warning(f"⚠️ [{mapping_schema}].[{account_mapping_table}] missing/invalid. Account mapping will be empty.")
    account_id_map = {}

# === Map_ItemCategory: Source_Id -> Target_Id (for ParentRef) ===
category_mapping_table = "Map_ItemCategory"
_df_cat_map = sql.fetch_table(category_mapping_table, mapping_schema)
if not _df_cat_map.empty and {"Source_Id","Target_Id"}.issubset(_df_cat_map.columns):
    category_id_map = {str(s): str(t) for s, t in zip(_df_cat_map["Source_Id"], _df_cat_map["Target_Id"])}
else:
    logger.warning(f"⚠️ [{mapping_schema}].[{category_mapping_table}] missing/invalid. Category mapping will be empty.")
    category_id_map = {}

# ---------- util casters & cleaners ----------
_html_tag = re.compile(r"<[^>]+>")

def _to_bool(v):
    if isinstance(v, bool): return v
    if v is None: return None
    s = str(v).strip().lower()
    if s in {"true","1","yes","y"}: return True
    if s in {"false","0","no","n"}: return False
    return None

def _to_float(v):
    try:
        return float(v) if v is not None and str(v).strip() != "" else None
    except Exception:
        return None

def _clean_text(v):
    if v is None: return None
    return _html_tag.sub(" ", str(v)).strip()

# ---------- QBO query helpers (CORRECT: text body) ----------
def _qbo_sql(query: str):
    """
    POST the raw SQL string to /query with Content-Type: application/text.
    Returns QueryResponse dict, or {} on error.
    """
    try:
        resp = requests.post(
            query_url,
            headers=QUERY_HEADERS,
            params={"minorversion": QBO_MINOR_VERSION},
            data=query,  # raw string, not JSON
            timeout=HTTP_TIMEOUT_SECS,
        )
        if resp.status_code == 200:
            return resp.json().get("QueryResponse", {}) or {}
        logger.warning(f"QBO query failed {resp.status_code}: {resp.text[:300]}")
    except requests.RequestException as e:
        logger.warning(f"QBO query exception: {e}")
    return {}

@lru_cache(maxsize=2048)
def _get_account_info(acc_id: str):
    if not acc_id: return None
    q = f"SELECT Id, Name, Active, AccountType, AccountSubType, SyncToken FROM Account WHERE Id = '{str(acc_id)}'"
    rows = _qbo_sql(q).get("Account") or []
    return rows[0] if rows else None

@lru_cache(maxsize=2048)
def _get_item_info(item_id: str):
    if not item_id: return None
    q = f"SELECT Id, Name, Active, Type, SyncToken FROM Item WHERE Id = '{str(item_id)}'"
    rows = _qbo_sql(q).get("Item") or []
    return rows[0] if rows else None

def _ensure_account_active(acc_id: str) -> tuple[bool, str | None]:
    info = _get_account_info(acc_id)
    if not info:
        return False, f"Referenced Account {acc_id} not found"
    if info.get("Active") is True:
        return True, None
    payload = {"Id": info["Id"], "SyncToken": info["SyncToken"], "sparse": True, "Active": True}
    r = requests.post(
        f"{base_url}/v3/company/{realm_id}/account",
        headers=headers,
        params={"minorversion": QBO_MINOR_VERSION},
        json=payload,
        timeout=HTTP_TIMEOUT_SECS,
    )
    return (r.status_code == 200, None if r.status_code == 200 else f"Could not reactivate Account {acc_id}: {r.text[:200]}")

def _ensure_category_active(item_id: str) -> tuple[bool, str | None]:
    info = _get_item_info(item_id)
    if not info:
        return False, f"Referenced Parent Item {item_id} not found"
    if info.get("Active") is True:
        return True, None
    payload = {"Id": info["Id"], "SyncToken": info["SyncToken"], "sparse": True, "Active": True}
    r = requests.post(
        f"{base_url}/v3/company/{realm_id}/item",
        headers=headers,
        params={"minorversion": QBO_MINOR_VERSION},
        json=payload,
        timeout=HTTP_TIMEOUT_SECS,
    )
    return (r.status_code == 200, None if r.status_code == 200 else f"Could not reactivate Parent Item {item_id}: {r.text[:200]}")

# --- Inventory account-type validation
def _validate_inventory_account_types(inc_id: str | None, exp_id: str | None, ast_id: str | None) -> list[str]:
    """
    Returns list of problems, empty if OK.
    Rules:
      - IncomeAccountRef: AccountType in {"Income", "Other Income"}
      - ExpenseAccountRef: AccountType == "Cost of Goods Sold"
      - AssetAccountRef: AccountType == "Other Current Asset" AND AccountSubType in {"Inventory","InventoryAsset"}
    """
    problems = []

    if inc_id:
        inc = _get_account_info(inc_id)
        if not inc:
            problems.append(f"IncomeAccountRef {inc_id} not found")
        else:
            if (inc.get("AccountType") or "").lower().replace(" ", "") not in {"income", "otherincome"}:
                problems.append(f"IncomeAccountRef {inc_id} must be Income/Other Income (got {inc.get('AccountType')})")

    if exp_id:
        exp = _get_account_info(exp_id)
        if not exp:
            problems.append(f"ExpenseAccountRef {exp_id} not found")
        else:
            if (exp.get("AccountType") or "") != "Cost of Goods Sold":
                problems.append(f"ExpenseAccountRef {exp_id} must be Cost of Goods Sold (got {exp.get('AccountType')})")

    if ast_id:
        ast = _get_account_info(ast_id)
        if not ast:
            problems.append(f"AssetAccountRef {ast_id} not found")
        else:
            at = ast.get("AccountType") or ""
            st = (ast.get("AccountSubType") or "").lower()
            if at != "Other Current Asset" or st not in {"inventory", "inventoryasset"}:
                problems.append(
                    f"AssetAccountRef {ast_id} must be Other Current Asset with Inventory/InventoryAsset subtype "
                    f"(got {at}/{ast.get('AccountSubType')})"
                )

    return problems

def _normalize_inventory_edges(p: dict):
    """Trim edge values that can trigger odd BVEs."""
    if (p.get("Type") or "").upper() == "INVENTORY":
        rp = p.get("ReorderPoint")
        try:
            rpnum = int(rp) if rp is not None else None
        except Exception:
            rpnum = None
        if rpnum == 0:
            p.pop("ReorderPoint", None)


def generate_payload(row: pd.Series, target_parent_id: str | None):
    """
    Build QBO-safe Item payload using allowlist, correct types, and value-only refs.

    Skip rules implemented here:
      - INVENTORY: skip only if ALL of Income/Expense/Asset accounts are absent.
      - SERVICE:   never require accounts; if present, include; if not, proceed without them.
      - NONINVENTORY/others: keep current behavior (require at least Income or Expense).

    Returns: (payload, income_id, expense_id, asset_id, missing_required_list)
    """
    payload: dict = {}
    type_upper = (str(row.get("Type") or "").strip().upper())

    target_income: str | None = None
    target_expense: str | None = None
    target_asset: str | None = None

    def is_blank_val(v):
        try:
            return v is None or str(v).strip() == "" or str(v).strip().lower() == "null"
        except Exception:
            return True

    for src_col, qbo_col in column_mapping.items():
        # skip readonly + anything not on allowlist
        if qbo_col in ITEM_READONLY_FIELDS:
            continue
        if qbo_col not in ITEM_POST_ALLOWLIST:
            continue

        val = row.get(src_col)
        if is_blank_val(val):
            continue

        # map account source ids -> Target_Id (but only accept non-None mapping)
        if qbo_col == "IncomeAccountRef.value":
            mapped = account_id_map.get(str(val)) if val is not None else None
            if mapped is not None:
                val = mapped
                target_income = mapped
            else:
                # ensure we don't set a None value into payload
                val = None
        elif qbo_col == "ExpenseAccountRef.value":
            mapped = account_id_map.get(str(val)) if val is not None else None
            if mapped is not None:
                val = mapped
                target_expense = mapped
            else:
                val = None
        elif qbo_col == "AssetAccountRef.value":
            mapped = account_id_map.get(str(val)) if val is not None else None
            if mapped is not None:
                val = mapped
                target_asset = mapped
            else:
                val = None

        # If value is None after mapping, skip adding to payload
        if val is None:
            continue

        # cast/scalar cleanup
        if qbo_col in {"Active", "Taxable", "SalesTaxIncluded", "PurchaseTaxIncluded", "SubItem"}:
            val = _to_bool(val)
            if val is None:
                continue
        elif qbo_col in {"UnitPrice", "PurchaseCost"}:
            val = _to_float(val)
            if val is None:
                continue
        elif qbo_col in {"QtyOnHand", "ReorderPoint"}:
            # numeric expectation — if invalid, skip
            if val is None:
                continue

        # basic text fields (strip HTML)
        if qbo_col in {"Description", "PurchaseDesc", "Name"}:
            val = _clean_text(val)
            if is_blank_val(val):
                continue

        # place (nest refs value-only)
        if "." in qbo_col:
            parent, child = qbo_col.split(".", 1)
            if child == "value":
                payload.setdefault(parent, {})[child] = str(val)
        else:
            payload[qbo_col] = val

    # ParentRef + SubItem
    if target_parent_id:
        payload["ParentRef"] = {"value": str(target_parent_id)}
        payload["SubItem"] = True
    else:
        if "SubItem" not in payload:
            payload["SubItem"] = False

    if "Active" not in payload:
        payload["Active"] = True

    # ── Per-type account requirements ──────────────────────────────
    has_any_account = any(x is not None for x in (target_income, target_expense, target_asset))

    missing_required: list = []
    if type_upper == "INVENTORY":
        # Skip only if ALL three are missing
        if not has_any_account:
            missing_required.append("AnyAccountRef(Income/Expense/Asset)")
    elif type_upper == "SERVICE":
        # No account required; if present they’re included, otherwise omit & proceed
        # Ensure inventory-only fields do not slip in
        for k in ("AssetAccountRef", "ReorderPoint"):
            payload.pop(k, None)
    elif type_upper == "NONINVENTORY":
        # Keep previous behavior: require at least Income OR Expense
        if not (target_income is not None or target_expense is not None):
            missing_required.append("IncomeOrExpenseAccountRef")
        for k in ("AssetAccountRef", "ReorderPoint"):
            payload.pop(k, None)
    elif type_upper == "CATEGORY":
        # keep only minimal fields for categories
        keep = {"Name", "SubItem", "ParentRef", "Active", "Type"}
        for k in list(payload.keys()):
            if k not in keep:
                payload.pop(k, None)
    else:
        # unknown type → require at least one of the three to proceed
        if not has_any_account:
            missing_required.append("AnyAccountRef(Income/Expense/Asset)")

    # remove empty dicts / empty strings (but keep booleans and numeric 0)
    for k in list(payload.keys()):
        v = payload.get(k)
        if v in (None, "", {}) and not isinstance(v, bool):
            payload.pop(k, None)

    return payload, target_income, target_expense, target_asset, missing_required


def _sanitize_staged_payload(p: dict) -> dict:
    # drop *.name, readonly/system, and cast numbers/bools
    def drop_keys(d, keys):
        for k in list(d.keys()):
            if k in keys: d.pop(k, None)

    # top-level drops
    drop_keys(p, {"FullyQualifiedName","domain","sparse","Id","SyncToken","MetaData","Level"})
    # ensure refs are value-only
    for ref in ("IncomeAccountRef","ExpenseAccountRef","AssetAccountRef","ParentRef"):
        if ref in p and isinstance(p[ref], dict):
            if p[ref].get("value") is not None:
                p[ref] = {"value": str(p[ref]["value"])}
            else:
                p.pop(ref, None)

    # booleans
    for b in ("Active","Taxable","SalesTaxIncluded","PurchaseTaxIncluded","SubItem"):
        if b in p: 
            v = _to_bool(p[b])
            if v is None: p.pop(b, None)
            else: p[b] = v

    # numerics
    for f in ("UnitPrice","PurchaseCost"):
        if f in p:
            v = _to_float(p[f])
            if v is None: p.pop(f, None)
            else: p[f] = v
    for i in ("QtyOnHand","ReorderPoint"):
        if i in p:
            v = p[i]
            if v is None: p.pop(i, None)
            else: p[i] = v

    # strip HTML in text
    for t in ("Name","Description","PurchaseDesc"):
        if t in p: p[t] = _clean_text(p[t])

    return p

# ---------- enrichment (set-based update) ----------
def get_existing_qbo_item_id(name: str):
    """Optionally check QBO for an existing Item by Name (disabled by default for speed)."""
    if not CHECK_EXISTS_BEFORE_POST:
        return None
    safe = str(name).replace("'", "''")
    q = f"SELECT Id FROM Item WHERE Name = '{safe}'"
    rows = _qbo_sql(q).get("Item") or []
    return rows[0]["Id"] if rows else None

def enrich_mapping_with_json(df: pd.DataFrame):
    """
    Build payloads for all rows, store to a staging table, then set-based UPDATE Map_Item.
    Always stores Payload_JSON. Marks 'Skipped' with reason if required accounts missing.
    """
    # Filter out records where Type contains "Category" as they are migrated separately
    original_count = len(df)
    df = df[df['Type'] != 'Category']
    filtered_count = original_count - len(df)
    if filtered_count > 0:
        logger.info(f"🔍 Filtered out {filtered_count} Category records from Item migration")
    
    n = len(df)
    logger.info(f"🧪 Enriching mapping with JSON for {n} item(s)...")
    rows = []
    t0 = time.time()

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        parent_source_id = row.get("ParentRef.value")
        target_parent_id = category_id_map.get(str(parent_source_id))

        payload, inc_id, exp_id, ast_id, missing = generate_payload(row, target_parent_id)
        existing_id = get_existing_qbo_item_id(row.get("Name"))
        payload_json = json.dumps(payload, indent=2)

        porter_status = None
        failure_reason = None
        if existing_id:
            porter_status = "Exists"
        elif missing:
            porter_status = "Skipped"
            failure_reason = f"Missing required AccountRef(s): {', '.join(missing)}"

        rows.append({
            "Source_Id": str(row["Id"]),
            "Payload_JSON": payload_json,
            "IncomeAccount_Target_Id": inc_id,
            "ExpenseAccount_Target_Id": exp_id,
            "AssetAccount_Target_Id":  ast_id,
            "Target_ParentRef_Id": target_parent_id,
            "Target_Id": existing_id,
            "Porter_Status": porter_status,
            "Failure_Reason": failure_reason
        })

        if i % LOG_EVERY == 0:
            logger.info(f"   …built {i}/{n} payloads ({(time.time()-t0):.1f}s)")

    stage = pd.DataFrame(rows)

    # Create stage table and bulk insert
    sql.run_query(f"""
        IF OBJECT_ID('{mapping_schema}.Item_Enriched_Stage','U') IS NOT NULL
            DROP TABLE [{mapping_schema}].[Item_Enriched_Stage];
        CREATE TABLE [{mapping_schema}].[Item_Enriched_Stage] (
            Source_Id NVARCHAR(100) NOT NULL PRIMARY KEY,
            Payload_JSON NVARCHAR(MAX) NULL,
            IncomeAccount_Target_Id NVARCHAR(50) NULL,
            ExpenseAccount_Target_Id NVARCHAR(50) NULL,
            AssetAccount_Target_Id  NVARCHAR(50) NULL,
            Target_ParentRef_Id NVARCHAR(50) NULL,
            Target_Id NVARCHAR(50) NULL,
            Porter_Status NVARCHAR(20) NULL,
            Failure_Reason NVARCHAR(MAX) NULL
        );
    """)
    sql.insert_dataframe(stage, "Item_Enriched_Stage", mapping_schema)

    # Single set-based UPDATE
    sql.run_query(f"""
        UPDATE m SET
            m.Payload_JSON             = s.Payload_JSON,
            m.IncomeAccount_Target_Id  = s.IncomeAccount_Target_Id,
            m.ExpenseAccount_Target_Id = s.ExpenseAccount_Target_Id,
            m.AssetAccount_Target_Id   = s.AssetAccount_Target_Id,
            m.Target_ParentRef_Id      = s.Target_ParentRef_Id,
            m.Target_Id                = COALESCE(m.Target_Id, s.Target_Id),
            m.Porter_Status            = COALESCE(m.Porter_Status, s.Porter_Status),
            m.Failure_Reason           = COALESCE(s.Failure_Reason, m.Failure_Reason)
        FROM [{mapping_schema}].[{mapping_table}] m
        JOIN [{mapping_schema}].[Item_Enriched_Stage] s
          ON CAST(m.Source_Id AS NVARCHAR(100)) = s.Source_Id;
        DROP TABLE [{mapping_schema}].[Item_Enriched_Stage];
    """)

    logger.info(f"✅ Enrichment complete in {(time.time()-t0):.1f}s")

session = requests.Session()

def post_staged_json():
    """
    Post staged item payloads to QBO.
    - Processes where Porter_Status is NULL or 'Failed'
    - Skips 'Skipped' and 'Exists'
    """
    df_mapping = sql.fetch_table(mapping_table, mapping_schema)

    # post only eligible rows with a payload
    mask = ((df_mapping["Porter_Status"].isna()) | (df_mapping["Porter_Status"] == "Failed")) & (df_mapping["Payload_JSON"].notna())
    df_post = df_mapping[mask]

    if df_post.empty:
        logger.info("✅ No new items to post.")
        return

    logger.info(f"📤 Posting {len(df_post)} item(s) to QBO…")
    timer = ProgressTimer(len(df_post))

    for _, row in df_post.iterrows():
        source_id = row["Source_Id"]
        try:
            payload = json.loads(row["Payload_JSON"])
            payload = _sanitize_staged_payload(payload)
            _normalize_inventory_edges(payload)

            # Ensure referenced entities are active (accounts + parent category)
            preflight_errors = []

            inc_id = (payload.get("IncomeAccountRef") or {}).get("value")
            exp_id = (payload.get("ExpenseAccountRef") or {}).get("value")
            ast_id = (payload.get("AssetAccountRef")  or {}).get("value")
            par_id = (payload.get("ParentRef") or {}).get("value")


            # Only preflight fail if BOTH income and expense accounts are missing (for non-inventory, non-category)
            type_upper = (payload.get("Type") or "").upper()
            # For inventory, keep original logic (all 3 required)
            if type_upper == "INVENTORY":
                for acc_id, label in ((inc_id,"IncomeAccountRef"), (exp_id,"ExpenseAccountRef"), (ast_id,"AssetAccountRef")):
                    if acc_id:
                        ok, err = _ensure_account_active(acc_id)
                        if not ok and err:
                            preflight_errors.append(f"{label}: {err}")
                if par_id:
                    ok, err = _ensure_category_active(par_id)
                    if not ok and err:
                        preflight_errors.append(f"ParentRef: {err}")
                type_errors = _validate_inventory_account_types(inc_id, exp_id, ast_id)
                preflight_errors.extend(type_errors)
            elif type_upper == "CATEGORY":
                # No account checks for category
                if par_id:
                    ok, err = _ensure_category_active(par_id)
                    if not ok and err:
                        preflight_errors.append(f"ParentRef: {err}")
            else:
                # For SERVICE, NONINVENTORY, or others: only fail if BOTH income and expense are missing
                if not (inc_id or exp_id):
                    preflight_errors.append("Missing both IncomeAccountRef and ExpenseAccountRef")
                else:
                    # If either present, ensure active
                    for acc_id, label in ((inc_id,"IncomeAccountRef"), (exp_id,"ExpenseAccountRef")):
                        if acc_id:
                            ok, err = _ensure_account_active(acc_id)
                            if not ok and err:
                                preflight_errors.append(f"{label}: {err}")
                if par_id:
                    ok, err = _ensure_category_active(par_id)
                    if not ok and err:
                        preflight_errors.append(f"ParentRef: {err}")

            # Bail early with precise reason
            if preflight_errors:
                reason = "; ".join(preflight_errors)[:250]
                logger.error(f"⛔ Preflight failed for {payload.get('Name')}: {reason}")
                sql.run_query(f"""
                    UPDATE [{mapping_schema}].[{mapping_table}]
                    SET Porter_Status = 'Failed',
                        Failure_Reason = ?,
                        Retry_Count = ISNULL(Retry_Count, 0) + 1
                    WHERE Source_Id = ?
                """, (reason, source_id))
                timer.update()
                continue

            resp = session.post(
                post_url,
                headers=headers,
                params={"minorversion": QBO_MINOR_VERSION},
                json=payload,
                timeout=HTTP_TIMEOUT_SECS,
            )

            if resp.status_code == 200:
                target_id = resp.json()["Item"]["Id"]
                sql.run_query(f"""
                    UPDATE [{mapping_schema}].[{mapping_table}]
                    SET Target_Id = ?, Porter_Status = 'Success', Failure_Reason = NULL
                    WHERE Source_Id = ?
                """, (target_id, source_id))
                logger.info(f"✅ {payload.get('Name')} → {target_id}")

            else:
                reason_text = resp.text
                try:
                    err = resp.json()["Fault"]["Error"][0]
                    if err.get("code") == "6240":  # Duplicate Name
                        detail = err.get("Detail","")
                        m = re.search(r"Id=(\d+)", detail)
                        if m:
                            existing_id = m.group(1)
                            sql.run_query(f"""
                                UPDATE [{mapping_schema}].[{mapping_table}]
                                SET Target_Id = ?, Porter_Status = 'Exists', Failure_Reason = NULL
                                WHERE Source_Id = ?
                            """, (existing_id, source_id))
                            logger.info(f"ℹ️ Duplicate: {payload.get('Name')} marked Exists ({existing_id})")
                            timer.update()
                            continue
                except Exception:
                    pass

                logger.error(f"❌ Failed: {payload.get('Name')} → {reason_text[:250]}")
                sql.run_query(f"""
                    UPDATE [{mapping_schema}].[{mapping_table}]
                    SET Porter_Status = 'Failed',
                        Failure_Reason = ?,
                        Retry_Count = ISNULL(Retry_Count, 0) + 1
                    WHERE Source_Id = ?
                """, (reason_text[:250], source_id))

        except Exception as e:
            logger.exception(f"❌ Exception posting item")
            sql.run_query(f"""
                UPDATE [{mapping_schema}].[{mapping_table}]
                SET Porter_Status = 'Failed',
                    Failure_Reason = ?,
                    Retry_Count = ISNULL(Retry_Count, 0) + 1
                WHERE Source_Id = ?
            """, (str(e), source_id))

        timer.update()

def migrate_items():
    """
    End-to-end:
    - Load source items
    - Ensure mapping table exists + columns
    - Generate & store Payload_JSON (always) via set-based update
    - Mark rows 'Skipped' when required account refs are missing
    - Post only eligible rows (NULL/Failed)
    """
    logger.info("\n🚀 Starting Item Migration\n" + "="*35)
    try:
        df = sql.fetch_table(source_table, source_schema)
        if df.empty:
            logger.warning("⚠️ No Item records found.")
            return

        sql.ensure_schema_exists(mapping_schema)
        initialize_mapping_table(df, mapping_table, mapping_schema)

        # Ensure extra columns exist
        for col, dtype in [
            ("Payload_JSON", "NVARCHAR(MAX)"),
            ("IncomeAccount_Target_Id", "NVARCHAR(50)"),
            ("ExpenseAccount_Target_Id", "NVARCHAR(50)"),
            ("AssetAccount_Target_Id",  "NVARCHAR(50)"),
            ("Target_ParentRef_Id", "NVARCHAR(50)"),
        ]:
            try:
                sql.run_query(f"ALTER TABLE [{mapping_schema}].[{mapping_table}] ADD {col} {dtype} NULL")
                logger.info(f"✅ Added column: {col}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    pass

        # Enrich & update in bulk
        enrich_mapping_with_json(df)

        # Post (skips 'Skipped'/'Exists')
        post_staged_json()

        logger.info("\n🏁 Item migration completed.")
    except Exception as e:
        logger.exception(f"❌ Migration failed: {e}")


# import os
# import json
# import requests
# import pandas as pd
# import re
# from dotenv import load_dotenv
# from storage.sqlserver import sql
# from config.mapping.item_mapping import ITEM_COLUMN_MAPPING as column_mapping
# from utils.token_refresher import auto_refresh_token_if_needed
# from utils.retry_handler import initialize_mapping_table, get_retryable_subset
# from utils.log_timer import global_logger as logger, ProgressTimer

# # === Auto-refresh QBO token if needed ===
# auto_refresh_token_if_needed()
# load_dotenv()

# # === QBO Auth Config ===
# access_token = os.getenv("QBO_ACCESS_TOKEN")
# realm_id = os.getenv("QBO_REALM_ID")
# environment = os.getenv("QBO_ENVIRONMENT", "sandbox")

# base_url = (
#     "https://sandbox-quickbooks.api.intuit.com"
#     if environment == "sandbox"
#     else "https://quickbooks.api.intuit.com"
# )

# query_url = f"{base_url}/v3/company/{realm_id}/query"
# post_url = f"{base_url}/v3/company/{realm_id}/item"

# headers = {
#     "Authorization": f"Bearer {access_token}",
#     "Accept": "application/json",
#     "Content-Type": "application/json"
# }

# # === DB Info ===
# source_table = "Item"
# source_schema = os.getenv("SOURCE_SCHEMA", "dbo")
# mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
# mapping_table = "Map_Item"
# max_retries = 3

# # === Load Account Mapping ===
# account_mapping_table = "Map_Account"
# # df_account_map = sql.fetch_table(account_mapping_table, mapping_schema)
# # account_id_map = dict(zip(df_account_map["Source_Id"], df_account_map["Target_Id"]))

# df_account_map = sql.fetch_table(account_mapping_table, mapping_schema)

# if not df_account_map.empty and "Source_Id" in df_account_map.columns and "Target_Id" in df_account_map.columns:
#     account_id_map = dict(zip(df_account_map["Source_Id"], df_account_map["Target_Id"]))
# else:
#     logger.warning(f"⚠️ Mapping table [{mapping_schema}].[{account_mapping_table}] missing or invalid. Skipping account mapping.")
#     account_id_map = {}

# # === Load Category Mapping ===
# # category_mapping_table = "Map_ItemCategory"
# # df_cat_map = sql.fetch_table(category_mapping_table, mapping_schema)
# # category_id_map = dict(zip(df_cat_map["Source_Id"], df_cat_map["Target_Id"]))
# # === Load Category Mapping (with fallback if missing) ===
# category_mapping_table = "Map_ItemCategory"
# df_cat_map = sql.fetch_table(category_mapping_table, mapping_schema)

# if not df_cat_map.empty and "Source_Id" in df_cat_map.columns and "Target_Id" in df_cat_map.columns:
#     category_id_map = dict(zip(df_cat_map["Source_Id"], df_cat_map["Target_Id"]))
# else:
#     logger.warning(f"⚠️ Mapping table [{mapping_schema}].[{category_mapping_table}] missing or invalid. Skipping category mapping.")
#     category_id_map = {}


# def get_existing_qbo_item_id(name):
#     """
#     Check if an item with the given name already exists in QBO.
#     Args:
#         name (str): The name of the item to look up.
#     Returns:
#         str or None: The QBO Item ID if found, otherwise None.
#     """
#     name = name.replace("'", "''")
#     query = f"SELECT Id FROM Item WHERE Name = '{name}'"
#     response = requests.post(query_url, headers=headers, data=json.dumps({"query": query}))
#     if response.status_code == 200:
#         items = response.json().get("QueryResponse", {}).get("Item", [])
#         if items:
#             return items[0]["Id"]
#     return None

# def generate_payload(row, target_parent_id=None):
#     """
#     Construct a QBO-compatible JSON payload for an item, using mapped fields and parent reference.
#     Args:
#         row (pd.Series): A single row from the source DataFrame representing an item.
#         target_parent_id (str, optional): The QBO ID of the parent category (if applicable).
#     Returns:
#         tuple: (payload_dict, income_account_id, expense_account_id) or (None, None, None) if invalid.
#     """
#     payload = {}
#     target_income_account = None
#     target_expense_account = None

#     for src_col, qbo_col in column_mapping.items():
#         value = row.get(src_col)
#         if pd.isna(value) or str(value).strip() == "":
#             continue

#         if src_col == "IncomeAccountRef.value":
#             if value in account_id_map:
#                 target_income_account = account_id_map[value]
#                 value = target_income_account
#             else:
#                 return None, None, None

#         elif src_col == "ExpenseAccountRef.value":
#             if value in account_id_map:
#                 target_expense_account = account_id_map[value]
#                 value = target_expense_account
#             else:
#                 return None, None, None


#         if "." in qbo_col:
#             parent, child = qbo_col.split(".", 1)
#             payload.setdefault(parent, {})[child] = value
#         else:
#             payload[qbo_col] = value

#     # ✅ Inject ParentRef + SubItem if parent is mapped
#     if target_parent_id:
#         payload["ParentRef"] = {"value": target_parent_id}
#         payload["SubItem"] = True
#     else:
#         payload["SubItem"] = False

#     payload["Active"] = True  # ✅ Always mark item as active

#     return payload, target_income_account, target_expense_account

# def enrich_mapping_with_json(df):
#     """
#     Generate and store JSON payloads for all items into the mapping table.
#     - Applies field mappings.
#     - Translates account and parent category references.
#     - Detects existing items in QBO to mark as 'Exists'.
#     Args:
#         df (pd.DataFrame): Source DataFrame containing items to migrate.
#     """
#     enriched = []
#     for _, row in df.iterrows():
#         parent_source_id = row.get("ParentRef.value")
#         target_parent_id = category_id_map.get(parent_source_id)

#         payload, income_id, expense_id = generate_payload(row, target_parent_id=target_parent_id)
#         if payload is None:
#             logger.warning(f"⚠️ Skipped due to missing account mapping: {row['Name']}")
#             continue

#         existing_id = get_existing_qbo_item_id(row["Name"])
#         payload_json = json.dumps(payload)

#         enriched.append({
#             "Source_Id": row["Id"],
#             "Payload_JSON": payload_json,
#             "IncomeAccount_Target_Id": income_id,
#             "ExpenseAccount_Target_Id": expense_id,
#             "Target_ParentRef_Id": target_parent_id,
#             "Target_Id": existing_id if existing_id else None,
#             "Porter_Status": "Exists" if existing_id else None
#         })

#     df_enriched = pd.DataFrame(enriched)
#     for _, r in df_enriched.iterrows():
#         sql.run_query(f"""
#             UPDATE [{mapping_schema}].[{mapping_table}]
#             SET Payload_JSON = ?, IncomeAccount_Target_Id = ?, ExpenseAccount_Target_Id = ?,
#                 Target_ParentRef_Id = ?, Target_Id = ISNULL(Target_Id, ?), Porter_Status = ISNULL(Porter_Status, ?)
#             WHERE Source_Id = ?
#         """, (
#             r.Payload_JSON,
#             r.IncomeAccount_Target_Id,
#             r.ExpenseAccount_Target_Id,
#             r.Target_ParentRef_Id,
#             r.Target_Id,
#             r.Porter_Status,
#             r.Source_Id
#         ))

# session = requests.Session()

# def post_staged_json():
#     """
#     Post staged item payloads (from mapping table) to QBO one-by-one.
#     - Processes items where status is NULL or 'Failed'.
#     - Posts the payload to QBO.
#     - Handles duplicate name detection and updates mapping accordingly.
#     - Logs progress and failure reasons.
#     """
#     df_mapping = sql.fetch_table(mapping_table, mapping_schema)
#     df_post = df_mapping[(df_mapping["Porter_Status"].isna()) | (df_mapping["Porter_Status"] == "Failed")]
#     if df_post.empty:
#         logger.info("✅ No new items to post.")
#         return

#     timer = ProgressTimer(len(df_post))
#     for _, row in df_post.iterrows():
#         source_id = row["Source_Id"]
#         try:
#             payload = json.loads(row["Payload_JSON"])
#             response = session.post(post_url, headers=headers, json=payload)
#             if response.status_code == 200:
#                 target_id = response.json()["Item"]["Id"]
#                 logger.info(f"✅ Migrated: {payload.get('Name')} → {target_id}")
#                 sql.run_query(f"""
#                     UPDATE [{mapping_schema}].[{mapping_table}]
#                     SET Target_Id = ?, Porter_Status = 'Success', Failure_Reason = NULL
#                     WHERE Source_Id = ?
#                 """, (target_id, source_id))
#             else:
#                 reason_text = response.text

#                 try:
#                     error_obj = response.json()["Fault"]["Error"][0]
#                     if error_obj.get("code") == "6240":  # Duplicate Name
#                         detail = error_obj.get("Detail", "")
#                         match = re.search(r"Id=(\d+)", detail)
#                         if match:
#                             existing_id = match.group(1)
#                             sql.run_query(f"""
#                                 UPDATE [{mapping_schema}].[{mapping_table}]
#                                 SET Target_Id = ?, Porter_Status = 'Exists', Failure_Reason = NULL
#                                 WHERE Source_Id = ?
#                             """, (existing_id, source_id))
#                             logger.info(f"ℹ️ Duplicate Name: Marked as Exists with ID {existing_id}")
#                             timer.update()
#                             continue
#                     else:
#                         logger.error(f"❌ Failed to post {payload.get('Name')}: {reason_text[:250]}")

#                 except Exception as e:
#                     logger.warning(f"⚠️ Could not parse duplicate error response: {e}")

#                 sql.run_query(f"""
#                     UPDATE [{mapping_schema}].[{mapping_table}]
#                     SET Porter_Status = 'Failed', Failure_Reason = ?, Retry_Count = ISNULL(Retry_Count, 0) + 1
#                     WHERE Source_Id = ?
#                 """, (reason_text[:250], source_id))

#         except Exception as e:
#             logger.exception(f"❌ Exception posting item: {e}")
#             sql.run_query(f"""
#                 UPDATE [{mapping_schema}].[{mapping_table}]
#                 SET Porter_Status = 'Failed', Failure_Reason = ?, Retry_Count = ISNULL(Retry_Count, 0) + 1
#                 WHERE Source_Id = ?
#             """, (str(e), source_id))
#         timer.update()

# def migrate_items():
#     """
#     Orchestrates the full item migration process.
#     - Loads source item records.
#     - Ensures mapping table exists and is initialized.
#     - Adds necessary columns if missing.
#     - Generates payloads and stores them.
#     - Posts each item to QBO.
#     - Logs summary status.
#     """
#     logger.info("\n🚀 Starting Item Migration\n" + "=" * 35)
#     try:
#         df = sql.fetch_table(source_table, source_schema)
#         if df.empty:
#             logger.warning("⚠️ No Item records found.")
#             return

#         sql.ensure_schema_exists(mapping_schema)
#         initialize_mapping_table(df, mapping_table, mapping_schema)

#         for col in ["Payload_JSON", "IncomeAccount_Target_Id", "ExpenseAccount_Target_Id", "Target_ParentRef_Id"]:
#             try:
#                 sql.run_query(f"ALTER TABLE [{mapping_schema}].[{mapping_table}] ADD {col} NVARCHAR(MAX) NULL")
#                 logger.info(f"✅ Added column: {col}")
#             except Exception as e:
#                 if "already exists" in str(e):
#                     logger.debug(f"ℹ️ Column already exists: {col}")


#         enrich_mapping_with_json(df)
#         post_staged_json()
#         logger.info("\n🏁 Item migration completed.")

#     except Exception as e:
#         logger.exception(f"❌ Migration failed: {e}")

























#====================================old school==================================================
# """
# Sequence : 08
# Module: item_migrator.py
# Author: Dixit Prajapati
# Created: 2025-07-22
# Description: Handles migration of item records from source system to QBO,
#              including parent-child hierarchy processing, retry logic, and status tracking.
# Production : Ready
# Phase : 01
# """

# import os
# import json
# import requests
# import pandas as pd
# import re
# from dotenv import load_dotenv
# from storage.sqlserver import sql
# from config.mapping.item_mapping import ITEM_COLUMN_MAPPING as column_mapping
# from utils.token_refresher import auto_refresh_token_if_needed
# from utils.retry_handler import initialize_mapping_table, get_retryable_subset
# from utils.log_timer import global_logger as logger, ProgressTimer

# # === Auto-refresh QBO token if needed ===
# auto_refresh_token_if_needed()
# load_dotenv()

# # === QBO Auth Config ===
# access_token = os.getenv("QBO_ACCESS_TOKEN")
# realm_id = os.getenv("QBO_REALM_ID")
# environment = os.getenv("QBO_ENVIRONMENT", "sandbox")

# base_url = (
#     "https://sandbox-quickbooks.api.intuit.com"
#     if environment == "sandbox"
#     else "https://quickbooks.api.intuit.com"
# )

# query_url = f"{base_url}/v3/company/{realm_id}/query"
# post_url = f"{base_url}/v3/company/{realm_id}/item"

# headers = {
#     "Authorization": f"Bearer {access_token}",
#     "Accept": "application/json",
#     "Content-Type": "application/json"
# }

# # === DB Info ===
# source_table = "Item"
# source_schema = os.getenv("SOURCE_SCHEMA", "dbo")
# mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
# mapping_table = "Map_Item"
# max_retries = 3

# # === Load Account Mapping ===
# account_mapping_table = "Map_Account"
# # df_account_map = sql.fetch_table(account_mapping_table, mapping_schema)
# # account_id_map = dict(zip(df_account_map["Source_Id"], df_account_map["Target_Id"]))

# df_account_map = sql.fetch_table(account_mapping_table, mapping_schema)

# if not df_account_map.empty and "Source_Id" in df_account_map.columns and "Target_Id" in df_account_map.columns:
#     account_id_map = dict(zip(df_account_map["Source_Id"], df_account_map["Target_Id"]))
# else:
#     logger.warning(f"⚠️ Mapping table [{mapping_schema}].[{account_mapping_table}] missing or invalid. Skipping account mapping.")
#     account_id_map = {}

# # === Load Category Mapping ===
# # category_mapping_table = "Map_ItemCategory"
# # df_cat_map = sql.fetch_table(category_mapping_table, mapping_schema)
# # category_id_map = dict(zip(df_cat_map["Source_Id"], df_cat_map["Target_Id"]))
# # === Load Category Mapping (with fallback if missing) ===
# category_mapping_table = "Map_ItemCategory"
# df_cat_map = sql.fetch_table(category_mapping_table, mapping_schema)

# if not df_cat_map.empty and "Source_Id" in df_cat_map.columns and "Target_Id" in df_cat_map.columns:
#     category_id_map = dict(zip(df_cat_map["Source_Id"], df_cat_map["Target_Id"]))
# else:
#     logger.warning(f"⚠️ Mapping table [{mapping_schema}].[{category_mapping_table}] missing or invalid. Skipping category mapping.")
#     category_id_map = {}


# def get_existing_qbo_item_id(name):
#     """
#     Check if an item with the given name already exists in QBO.
#     Args:
#         name (str): The name of the item to look up.
#     Returns:
#         str or None: The QBO Item ID if found, otherwise None.
#     """
#     name = name.replace("'", "''")
#     query = f"SELECT Id FROM Item WHERE Name = '{name}'"
#     response = requests.post(query_url, headers=headers, data=json.dumps({"query": query}))
#     if response.status_code == 200:
#         items = response.json().get("QueryResponse", {}).get("Item", [])
#         if items:
#             return items[0]["Id"]
#     return None

# def generate_payload(row, target_parent_id=None):
#     """
#     Construct a QBO-compatible JSON payload for an item, using mapped fields and parent reference.
#     Args:
#         row (pd.Series): A single row from the source DataFrame representing an item.
#         target_parent_id (str, optional): The QBO ID of the parent category (if applicable).
#     Returns:
#         tuple: (payload_dict, income_account_id, expense_account_id) or (None, None, None) if invalid.
#     """
#     payload = {}
#     target_income_account = None
#     target_expense_account = None

#     for src_col, qbo_col in column_mapping.items():
#         value = row.get(src_col)
#         if pd.isna(value) or str(value).strip() == "":
#             continue

#         if src_col == "IncomeAccountRef.value":
#             if value in account_id_map:
#                 target_income_account = account_id_map[value]
#                 value = target_income_account
#             else:
#                 return None, None, None

#         elif src_col == "ExpenseAccountRef.value":
#             if value in account_id_map:
#                 target_expense_account = account_id_map[value]
#                 value = target_expense_account
#             else:
#                 return None, None, None


#         if "." in qbo_col:
#             parent, child = qbo_col.split(".", 1)
#             payload.setdefault(parent, {})[child] = value
#         else:
#             payload[qbo_col] = value

#     # ✅ Inject ParentRef + SubItem if parent is mapped
#     if target_parent_id:
#         payload["ParentRef"] = {"value": target_parent_id}
#         payload["SubItem"] = True
#     else:
#         payload["SubItem"] = False

#     payload["Active"] = True  # ✅ Always mark item as active

#     return payload, target_income_account, target_expense_account

# def enrich_mapping_with_json(df):
#     """
#     Generate and store JSON payloads for all items into the mapping table.
#     - Applies field mappings.
#     - Translates account and parent category references.
#     - Detects existing items in QBO to mark as 'Exists'.
#     Args:
#         df (pd.DataFrame): Source DataFrame containing items to migrate.
#     """
#     enriched = []
#     for _, row in df.iterrows():
#         parent_source_id = row.get("ParentRef.value")
#         target_parent_id = category_id_map.get(parent_source_id)

#         payload, income_id, expense_id = generate_payload(row, target_parent_id=target_parent_id)
#         if payload is None:
#             logger.warning(f"⚠️ Skipped due to missing account mapping: {row['Name']}")
#             continue

#         existing_id = get_existing_qbo_item_id(row["Name"])
#         payload_json = json.dumps(payload)

#         enriched.append({
#             "Source_Id": row["Id"],
#             "Payload_JSON": payload_json,
#             "IncomeAccount_Target_Id": income_id,
#             "ExpenseAccount_Target_Id": expense_id,
#             "Target_ParentRef_Id": target_parent_id,
#             "Target_Id": existing_id if existing_id else None,
#             "Porter_Status": "Exists" if existing_id else None
#         })

#     df_enriched = pd.DataFrame(enriched)
#     for _, r in df_enriched.iterrows():
#         sql.run_query(f"""
#             UPDATE [{mapping_schema}].[{mapping_table}]
#             SET Payload_JSON = ?, IncomeAccount_Target_Id = ?, ExpenseAccount_Target_Id = ?,
#                 Target_ParentRef_Id = ?, Target_Id = ISNULL(Target_Id, ?), Porter_Status = ISNULL(Porter_Status, ?)
#             WHERE Source_Id = ?
#         """, (
#             r.Payload_JSON,
#             r.IncomeAccount_Target_Id,
#             r.ExpenseAccount_Target_Id,
#             r.Target_ParentRef_Id,
#             r.Target_Id,
#             r.Porter_Status,
#             r.Source_Id
#         ))

# session = requests.Session()

# def post_staged_json():
#     """
#     Post staged item payloads (from mapping table) to QBO one-by-one.
#     - Processes items where status is NULL or 'Failed'.
#     - Posts the payload to QBO.
#     - Handles duplicate name detection and updates mapping accordingly.
#     - Logs progress and failure reasons.
#     """
#     df_mapping = sql.fetch_table(mapping_table, mapping_schema)
#     df_post = df_mapping[(df_mapping["Porter_Status"].isna()) | (df_mapping["Porter_Status"] == "Failed")]
#     if df_post.empty:
#         logger.info("✅ No new items to post.")
#         return

#     timer = ProgressTimer(len(df_post))
#     for _, row in df_post.iterrows():
#         source_id = row["Source_Id"]
#         try:
#             payload = json.loads(row["Payload_JSON"])
#             response = session.post(post_url, headers=headers, json=payload)
#             if response.status_code == 200:
#                 target_id = response.json()["Item"]["Id"]
#                 logger.info(f"✅ Migrated: {payload.get('Name')} → {target_id}")
#                 sql.run_query(f"""
#                     UPDATE [{mapping_schema}].[{mapping_table}]
#                     SET Target_Id = ?, Porter_Status = 'Success', Failure_Reason = NULL
#                     WHERE Source_Id = ?
#                 """, (target_id, source_id))
#             else:
#                 reason_text = response.text
#                 logger.error(f"❌ Failed to post {payload.get('Name')}: {reason_text[:250]}")

#                 try:
#                     error_obj = response.json()["Fault"]["Error"][0]
#                     if error_obj.get("code") == "6240":  # Duplicate Name
#                         detail = error_obj.get("Detail", "")
#                         match = re.search(r"Id=(\d+)", detail)
#                         if match:
#                             existing_id = match.group(1)
#                             sql.run_query(f"""
#                                 UPDATE [{mapping_schema}].[{mapping_table}]
#                                 SET Target_Id = ?, Porter_Status = 'Exists', Failure_Reason = NULL
#                                 WHERE Source_Id = ?
#                             """, (existing_id, source_id))
#                             logger.info(f"ℹ️ Duplicate Name: Marked as Exists with ID {existing_id}")
#                             timer.update()
#                             continue
#                 except Exception as e:
#                     logger.warning(f"⚠️ Could not parse duplicate error response: {e}")

#                 sql.run_query(f"""
#                     UPDATE [{mapping_schema}].[{mapping_table}]
#                     SET Porter_Status = 'Failed', Failure_Reason = ?, Retry_Count = ISNULL(Retry_Count, 0) + 1
#                     WHERE Source_Id = ?
#                 """, (reason_text[:250], source_id))

#         except Exception as e:
#             logger.exception(f"❌ Exception posting item: {e}")
#             sql.run_query(f"""
#                 UPDATE [{mapping_schema}].[{mapping_table}]
#                 SET Porter_Status = 'Failed', Failure_Reason = ?, Retry_Count = ISNULL(Retry_Count, 0) + 1
#                 WHERE Source_Id = ?
#             """, (str(e), source_id))
#         timer.update()

# def migrate_items():
#     """
#     Orchestrates the full item migration process.
#     - Loads source item records.
#     - Ensures mapping table exists and is initialized.
#     - Adds necessary columns if missing.
#     - Generates payloads and stores them.
#     - Posts each item to QBO.
#     - Logs summary status.
#     """
#     logger.info("\n🚀 Starting Item Migration\n" + "=" * 35)
#     try:
#         df = sql.fetch_table(source_table, source_schema)
#         if df.empty:
#             logger.warning("⚠️ No Item records found.")
#             return

#         sql.ensure_schema_exists(mapping_schema)
#         initialize_mapping_table(df, mapping_table, mapping_schema)

#         for col in ["Payload_JSON", "IncomeAccount_Target_Id", "ExpenseAccount_Target_Id", "Target_ParentRef_Id"]:
#             try:
#                 sql.run_query(f"ALTER TABLE [{mapping_schema}].[{mapping_table}] ADD {col} NVARCHAR(MAX) NULL")
#                 logger.info(f"✅ Added column: {col}")
#             except Exception as e:
#                 if "already exists" in str(e):
#                     logger.debug(f"ℹ️ Column already exists: {col}")


#         enrich_mapping_with_json(df)
#         post_staged_json()
#         logger.info("\n🏁 Item migration completed.")

#     except Exception as e:
#         logger.exception(f"❌ Migration failed: {e}")


