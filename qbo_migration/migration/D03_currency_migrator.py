"""
Sequence : 03
Module: Currency_Migrator.py
Author: Dixit Prajapati
Created: 2025-07-16
Description: Handles migration of Currency records from source system to QuickBooks Online (QBO),
             including retry logic, and status tracking.
Production : Ready
Phase : 01
"""

import os
import json
import pandas as pd
import requests
from storage.sqlserver import sql
from config.mapping.currency_mapping import CURRENCY_COLUMN_MAPPING as column_mapping
from utils.token_refresher import auto_refresh_token_if_needed
from utils.token_refresher import get_qbo_context_migration, MINOR_VERSION
from utils.mapping_updater import update_mapping_status
from utils.logger_builder import build_logger
from utils.log_timer import ProgressTimer

logger = build_logger("currency_migration")

def get_qbo_auth_currency():
    """
    DB-backed auth for CompanyCurrency:
      - returns post/query URLs (with minorversion)
      - returns correct headers for POST (JSON) and QUERY (text)
    """
    ctx = get_qbo_context_migration()
    base = ctx["BASE_URL"]
    realm_id = ctx["REALM_ID"]

    post_url  = f"{base}/v3/company/{realm_id}/companycurrency?minorversion={MINOR_VERSION}"
    query_url = ctx["QUERY_URL"]  # already includes ?minorversion=...

    headers_post = {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    headers_query = ctx["HEADERS"]  # Content-Type: application/text
    return post_url, query_url, headers_post, headers_query



# DB config
source_schema  = os.getenv("SOURCE_SCHEMA", "dbo")
source_table   = "CompanyCurrency"
mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
mapping_table  = "Map_Currency"

MAX_RETRIES = 3
session = requests.Session()


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def is_multicurrency_enabled() -> bool:
    """Check Multi-Currency via Preferences; fallback to CompanyCurrency count > 1."""
    # fresh, DB-backed context
    ctx = get_qbo_context_migration()
    base = ctx["BASE_URL"]
    realm_id = ctx["REALM_ID"]
    headers_post  = {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    headers_query = ctx["HEADERS"]
    query_url     = ctx["QUERY_URL"]
    preferences_url = f"{base}/v3/company/{realm_id}/preferences?minorversion={MINOR_VERSION}"

    try:
        r = session.get(preferences_url, headers=headers_post, timeout=60)
        if r.status_code == 200:
            prefs = r.json().get("Preferences", {})
            enabled = (
                prefs.get("CompanyPrefs", {})
                    .get("MultiCurrencyPrefs", {})
                    .get("MultiCurrencyEnabled")
            )
            if enabled is not None:
                logger.info(f"ℹ️ Multi-Currency Enabled? → {enabled}")
                return bool(enabled)

        # Fallback: POST /query with raw SQL
        q = "select * from CompanyCurrency startposition 1 maxresults 5"
        qr = session.post(query_url, headers=headers_query, data=q, timeout=60)
        if qr.status_code in (401, 403):
            # refresh once and retry
            _, query_url, _, headers_query = get_qbo_auth_currency()
            qr = session.post(query_url, headers=headers_query, data=q, timeout=60)

        if qr.status_code == 200:
            rows = qr.json().get("QueryResponse", {}).get("CompanyCurrency", []) or []
            inferred = len(rows) > 1
            logger.warning("⚠️ MultiCurrencyPrefs missing — inferred via CompanyCurrency query.")
            logger.info(f"ℹ️ Multi-Currency Inferred from count → {inferred}")
            return inferred

        logger.warning(f"⚠️ Preference/fallback checks failed: pref={r.status_code}, fallback={qr.status_code}")
        return False
    except Exception as e:
        logger.exception(f"❌ Multicurrency check error: {e}")
        return False


def qbo_lookup_currency_id_by_code(code: str) -> str | None:
    """Returns QBO CompanyCurrency.Id for given ISO Code."""
    if not code:
        return None
    q = f"select * from CompanyCurrency where Code = '{code}'"
    try:
        # resp = session.get(query_url, headers=headers_query, params={"query": q})
        _, query_url, _, headers_query = get_qbo_auth_currency()
        resp = session.post(query_url, headers=headers_query, data=q, timeout=60)
        if resp.status_code in (401, 403):
            _, query_url, _, headers_query = get_qbo_auth_currency()
            resp = session.post(query_url, headers=headers_query, data=q, timeout=60)

        if resp.status_code == 200:
            rows = resp.json().get("QueryResponse", {}).get("CompanyCurrency", []) or []
            if rows:
                return rows[0].get("Id")
        else:
            logger.warning(f"⚠️ Currency lookup failed for {code}: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        logger.exception(f"❌ Lookup error for currency {code}: {e}")
    return None


def build_currency_payload(src_row: pd.Series) -> dict:
    """Build CompanyCurrency payload."""
    payload: dict = {}
    for src_col, qbo_col in column_mapping.items():
        val = src_row.get(src_col)
        if pd.isna(val):
            continue
        if qbo_col == "Active":
            payload[qbo_col] = str(val).strip().lower() in ("true", "1", "yes")
        else:
            payload[qbo_col] = str(val).strip()
    return payload


# ──────────────────────────────────────────────────────────────
# Phase 1 — Ensure mapping & generate payloads
# ──────────────────────────────────────────────────────────────
def ensure_mapping_table_and_generate_payloads() -> int:
    df_src = sql.fetch_table(source_table, source_schema)
    if df_src.empty:
        logger.warning("⚠️ No currency records in source.")
        return 0

    sql.ensure_schema_exists(mapping_schema)

    if not sql.table_exists(mapping_table, mapping_schema):
        df_init = df_src.copy()
        df_init.rename(columns={"Id": "Source_Id"}, inplace=True)
        df_init["Target_Id"]      = None
        df_init["Porter_Status"]  = None
        df_init["Failure_Reason"] = None
        df_init["Retry_Count"]    = 0
        df_init["Payload_JSON"]   = None
        sql.insert_dataframe(df_init, mapping_table, mapping_schema)
        logger.info(f"✅ Created & populated [{mapping_schema}].[{mapping_table}] with {len(df_init)} row(s).")

    df_map = sql.fetch_table(mapping_table, mapping_schema)

    # Upsert any new source rows
    new_src = df_src[~df_src["Id"].isin(df_map["Source_Id"])]
    if not new_src.empty:
        to_add = new_src.copy()
        to_add.rename(columns={"Id": "Source_Id"}, inplace=True)
        to_add["Target_Id"]      = None
        to_add["Porter_Status"]  = None
        to_add["Failure_Reason"] = None
        to_add["Retry_Count"]    = 0
        to_add["Payload_JSON"]   = None
        sql.insert_dataframe(to_add, mapping_table, mapping_schema)
        logger.info(f"➕ Added {len(to_add)} new Source_Id(s).")

    # Refresh mapping
    df_map = sql.fetch_table(mapping_table, mapping_schema)

    timer = ProgressTimer(total_records=len(df_src), logger=logger)
    for _, srow in df_src.iterrows():
        source_id = srow["Id"]
        code      = str(srow.get("Code", "")).strip().upper()
        payload   = build_currency_payload(srow)  # dict

        # ⚠️ IMPORTANT: pass dict to update_mapping_status
        # It will json.dumps(..., indent=2) internally → no double-encoding.

        row_map = df_map[df_map["Source_Id"] == source_id]
        if not row_map.empty and pd.notna(row_map.iloc[0].get("Target_Id")) and \
           (row_map.iloc[0].get("Porter_Status") in ("Exists", "Success")):
            # If payload missing, set it explicitly with pretty JSON (no double dump)
            if not row_map.iloc[0].get("Payload_JSON"):
                pretty = json.dumps(payload, indent=2)
                sql.run_query(
                    f"UPDATE [{mapping_schema}].[{mapping_table}] SET Payload_JSON = ? WHERE Source_Id = ?",
                    (pretty, source_id),
                )
            timer.update()
            continue

        existing_id = qbo_lookup_currency_id_by_code(code)
        if not existing_id and code == "USD":
            existing_id = "1"  # common default, still safe to override if lookup works later

        if existing_id:
            update_mapping_status(
                mapping_schema, mapping_table, source_id,
                status="Exists", target_id=str(existing_id),
                payload=payload   # ← pass dict, not dumped string
            )
        else:
            update_mapping_status(
                mapping_schema, mapping_table, source_id,
                status="Ready",
                payload=payload   # ← pass dict, not dumped string
            )

        timer.update()

    timer.stop()
    logger.info("🧱 Phase 1 complete: payloads generated & mapping prepared.")
    return len(df_src)


# ──────────────────────────────────────────────────────────────
# Phase 2 — Post from mapping table
# ──────────────────────────────────────────────────────────────
def post_currencies(retry_only: bool = False):
    if retry_only:
        df = sql.fetch_dataframe(f"""
            SELECT * FROM [{mapping_schema}].[{mapping_table}]
            WHERE Porter_Status = 'Failed' AND Payload_JSON IS NOT NULL
        """)
    else:
        df = sql.fetch_dataframe(f"""
            SELECT * FROM [{mapping_schema}].[{mapping_table}]
            WHERE Porter_Status IN ('Ready','Failed') AND Payload_JSON IS NOT NULL
        """)

    if df.empty:
        logger.info("✅ Nothing to post.")
        return

    logger.info(f"📤 Posting {len(df)} currencies...")
    timer = ProgressTimer(total_records=len(df), logger=logger)

    for _, row in df.iterrows():
        source_id    = row["Source_Id"]
        porter_stat  = (row.get("Porter_Status") or "").strip()
        payload_json = row.get("Payload_JSON") or "{}"
        try:
            payload = json.loads(payload_json)  # dict for requests
        except Exception:
            # If somehow stored as double-encoded, normalize once
            try:
                payload = json.loads(json.loads(payload_json))
            except Exception:
                payload = {}

        code = (payload.get("Code") or "").strip().upper()

        # Skip already completed
        if porter_stat in ("Exists", "Success") and row.get("Target_Id"):
            timer.update()
            continue

        # Defensive re-check: if it now exists in QBO, mark Exists
        existing_id = qbo_lookup_currency_id_by_code(code) if code else None
        if existing_id:
            update_mapping_status(
                mapping_schema, mapping_table, source_id,
                status="Exists", target_id=str(existing_id),
                payload=payload   # ← pass dict
            )
            timer.update()
            continue

        # Post
        try:
            # resp = session.post(post_url, headers=headers_post, json=payload)
            post_url_local, _, headers_post_local, _ = get_qbo_auth_currency()
            auto_refresh_token_if_needed()
            resp = session.post(post_url_local, headers=headers_post_local, json=payload, timeout=60)
            if resp.status_code in (401, 403):
                post_url_local, _, headers_post_local, _ = get_qbo_auth_currency()
                resp = session.post(post_url_local, headers=headers_post_local, json=payload, timeout=60)

            if resp.status_code == 200:
                result = resp.json().get("CompanyCurrency", {})
                new_id = result.get("Id")
                update_mapping_status(
                    mapping_schema, mapping_table, source_id,
                    status="Success", target_id=str(new_id) if new_id else None,
                    payload=payload   # ← pass dict
                )
                logger.info(f"✅ Migrated: {code or source_id} → {new_id}")
            else:
                reason = resp.text
                update_mapping_status(
                    mapping_schema, mapping_table, source_id,
                    status="Failed", failure_reason=reason,
                    payload=payload,              # ← pass dict
                    increment_retry=True
                )
                logger.error(f"❌ Failed: {code or source_id} | {resp.status_code} | {reason[:240]}")
        except Exception as e:
            update_mapping_status(
                mapping_schema, mapping_table, source_id,
                status="Failed", failure_reason=str(e),
                payload=payload,                 # ← pass dict
                increment_retry=True
            )
            logger.exception(f"❌ Exception posting {code or source_id}: {e}")

        timer.update()

    timer.stop()
    logger.info("🏁 Phase 2 complete.")


# ──────────────────────────────────────────────────────────────
# Entrypoint
# ──────────────────────────────────────────────────────────────
def migrate_currencies(retry_only: bool = False):
    processed = ensure_mapping_table_and_generate_payloads()
    if processed == 0:
        return

    allow_post_flag = os.getenv("ENABLE_QBO_CURRENCY_POSTING", "false").lower() == "true"
    mc_enabled = is_multicurrency_enabled()
    logger.info(f"🔧 Post flags → ENABLE_QBO_CURRENCY_POSTING={allow_post_flag} | MultiCurrencyEnabled={mc_enabled}")

    # ✅ Proceed if EITHER the env flag is true OR multicurrency is enabled
    if not (allow_post_flag or mc_enabled):
        logger.warning("🚫 Skipping Phase 2 (posting disabled and multi-currency not enabled).")
        return
    auto_refresh_token_if_needed()
    post_currencies(retry_only=retry_only)
