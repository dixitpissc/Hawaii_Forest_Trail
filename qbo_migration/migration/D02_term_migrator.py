"""
Sequence : 02
Module: Term_Migrator.py
Author: Dixit Prajapati
Created: 2025-09-18
Description: Handles migration of Term records from source system to QuickBooks Online (QBO),
             including retry logic, and status tracking.
Production : Ready
Phase : 02 - Multi User
"""


import os
import requests
import pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from config.mapping.term_mapping import TERM_COLUMN_MAPPING as column_mapping
from utils.token_refresher import auto_refresh_token_if_needed, get_qbo_context_migration
from utils.logger_builder import build_logger
from utils.log_timer import ProgressTimer
import json,re,math
from utils.payload_cleaner import deep_clean

# === Load environment and refresh token ===
load_dotenv()
auto_refresh_token_if_needed()

# === Logging ===
logger = build_logger("term_migration")

ctx = get_qbo_context_migration()
base = ctx["BASE_URL"]

# === QBO Configuration ===
access_token = ctx['ACCESS_TOKEN']
realm_id = ctx["REALM_ID"]
environment = os.getenv("QBO_ENVIRONMENT", "sandbox")

base_url = (
    "https://sandbox-quickbooks.api.intuit.com"
    if environment == "sandbox"
    else "https://quickbooks.api.intuit.com"
)

query_url = f"{base_url}/v3/company/{realm_id}/query"
post_url = f"{base_url}/v3/company/{realm_id}/term"

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# === DB Info ===
source_table = "Term"
source_schema = os.getenv("SOURCE_SCHEMA", "dbo")
mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
mapping_table = f"Map_{source_table}"
max_retries = 3


# ---------- Helpers (types, inference) ----------
def _to_bool(v):
    if isinstance(v, bool): return v
    if v is None: return None
    s = str(v).strip().lower()
    if s in {"true","1","yes","y"}: return True
    if s in {"false","0","no","n"}: return False
    return None

def _to_int(v):
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)): return None
        return int(float(v))
    except Exception:
        return None

def _to_float(v):
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)): return None
        return float(v)
    except Exception:
        return None

_percent_in_name = re.compile(r'^\s*(\d+(?:\.\d+)?)\s*%\s*\d+', re.I)

def _safe_str(v):
    return "" if v is None else str(v)

def _normalize_type(raw_type, row):
    """Return 'STANDARD' or 'DATEDRIVEN'. Auto-detect if missing/ambiguous."""
    t = _safe_str(raw_type).strip().upper()
    if t.startswith("STANDARD"): 
        return "STANDARD"
    if t.startswith("DATE") or t == "DATEDRIVEN":
        return "DATEDRIVEN"
    # Auto-detect by presence of date-driven fields
    has_date_fields = any(
        _to_int(row.get(k)) is not None 
        for k in ("DayOfMonthDue","DueNextMonthDays","DiscountDayOfMonth")
    )
    return "DATEDRIVEN" if has_date_fields else "STANDARD"

# ---------- Payload builder ----------
def build_term_payload(row: pd.Series) -> dict:
    """
    Build a QBO-safe Term payload for both STANDARD and DATEDRIVEN terms.
    STANDARD:
      - Required: Name, Type='STANDARD', DueDays
      - Optional (paired): DiscountDays + DiscountPercent
    DATEDRIVEN:
      - Required: Name, Type='DATEDRIVEN', and at least one of DayOfMonthDue or DueNextMonthDays
      - Optional (paired): DiscountDayOfMonth + DiscountPercent
    Also infers DiscountPercent from names like '1% 10 Net 30' when missing.
    """
    name = _safe_str(row.get("Name")).strip()
    active = _to_bool(row.get("Active"))

    # Source fields (both styles; only what applies to the chosen Type will be used)
    due_days              = _to_int(row.get("DueDays"))
    discount_days         = _to_int(row.get("DiscountDays"))
    discount_percent      = _to_float(row.get("DiscountPercent"))

    day_of_month_due      = _to_int(row.get("DayOfMonthDue"))
    due_next_month_days   = _to_int(row.get("DueNextMonthDays"))
    discount_day_of_month = _to_int(row.get("DiscountDayOfMonth"))

    # Infer discount percent from name if not provided (e.g., "1% 10 Net 30")
    if (discount_percent is None) and name:
        m = _percent_in_name.match(name)
        if m:
            discount_percent = _to_float(m.group(1))

    # Determine type (explicit or inferred)
    term_type = _normalize_type(row.get("Type"), row)

    payload = {"Name": name, "Type": term_type}
    if active is not None:
        payload["Active"] = active

    if term_type == "STANDARD":
        # Required DueDays
        if due_days is not None:
            payload["DueDays"] = due_days
        # Include discount only if both present
        if discount_days is not None and discount_percent is not None:
            payload["DiscountDays"] = discount_days
            payload["DiscountPercent"] = discount_percent
        # If only one of the discount pair is present, drop both to avoid 2010
        else:
            # ensure neither lingering invalid partner is included
            payload.pop("DiscountDays", None)
            payload.pop("DiscountPercent", None)

        # Remove any date-driven fields if they snuck in
        for k in ("DayOfMonthDue","DueNextMonthDays","DiscountDayOfMonth"):
            payload.pop(k, None)

    else:  # DATEDRIVEN
        # At least one of DayOfMonthDue or DueNextMonthDays must be present
        has_due_anchor = False
        if day_of_month_due is not None:
            payload["DayOfMonthDue"] = day_of_month_due
            has_due_anchor = True
        if due_next_month_days is not None:
            payload["DueNextMonthDays"] = due_next_month_days
            has_due_anchor = True

        # Discount pair for date-driven
        if discount_day_of_month is not None and discount_percent is not None:
            payload["DiscountDayOfMonth"] = discount_day_of_month
            payload["DiscountPercent"] = discount_percent
        else:
            payload.pop("DiscountDayOfMonth", None)
            payload.pop("DiscountPercent", None)

        # Remove standard-only fields if present
        for k in ("DueDays","DiscountDays"):
            payload.pop(k, None)

        # If no due anchor fields, we will still send and let API validate,
        # but it's safer to log so you can fix your source data:
        if not has_due_anchor:
            logger.warning(f"⚠️ DATEDRIVEN term '{name}' missing DayOfMonthDue/DueNextMonthDays — QBO may reject.")

    # Always strip falsy/empty strings that can cause 2010
    for k in list(payload.keys()):
        if payload[k] == "" or payload[k] is None:
            payload.pop(k)

    return deep_clean(payload)

# ---------- QBO query ----------
def get_existing_qbo_term_id(name):
    """Check if a Term with the given name already exists in QBO."""
    # escape single quotes for QBO SQL
    safe_name = _safe_str(name).replace("'", "''")
    query = f"SELECT Id, Name FROM Term WHERE Name = '{safe_name}'"
    try:
        response = requests.get(
            url=query_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
                # QBO queries prefer text/plain
                "Content-Type": "text/plain"
            },
            params={"query": query}
        )

        if response.status_code == 200:
            terms = response.json().get("QueryResponse", {}).get("Term", [])
            if terms:
                return terms[0].get("Id")
        else:
            logger.warning(f"❌ Failed to query term '{name}': {response.status_code} - {response.text}")

    except Exception as e:
        logger.exception(f"❌ Exception querying QBO term: {name} → {e}")

    return None

session = requests.Session()

# ---------- Migration core ----------
def migrate(df_subset, df_mapping, remigrate_failed=False):
    for _, row in df_subset.iterrows():
        source_id = row["Id"]
        term_name = row["Name"]
        existing = df_mapping.query(f"Source_Id == '{source_id}'", engine="python")

        if not existing.empty and pd.notnull(existing.iloc[0].get("Target_Id")):
            if not remigrate_failed:
                logger.info(f"✅ Already migrated: {term_name}")
                continue
            elif existing.iloc[0].get("Porter_Status") == "Failed":
                retry_count = int(existing.iloc[0].get("Retry_Count", 0))
                if retry_count >= max_retries:
                    logger.warning(f"⛔ Skipping {term_name} — Retry limit reached")
                    continue
                logger.info(f"🔁 Retrying: {term_name} (Attempt {retry_count + 1})")
            else:
                continue

        payload = build_term_payload(row)
        payload_json = json.dumps(payload,indent=2)

        try:
            # Pre-check for duplicate by name
            existing_id = get_existing_qbo_term_id(term_name)
            if existing_id:
                logger.info(f"🔄 Exists in QBO: {term_name} → {existing_id}")
                sql.run_query(f"""
                    UPDATE [{mapping_schema}].[{mapping_table}]
                    SET Target_Id = ?, Porter_Status = 'Exists', Failure_Reason = NULL, Payload_JSON = ?
                    WHERE Source_Id = ?
                """, (existing_id, payload_json, source_id))
                continue

            logger.info(f"📤 Posting: {term_name}")
            response = session.post(post_url, headers=headers, json=payload)

            if response.status_code == 200:
                result = response.json()["Term"]
                target_id = result["Id"]
                logger.info(f"✅ Migrated: {term_name} → {target_id}")
                sql.run_query(f"""
                    UPDATE [{mapping_schema}].[{mapping_table}]
                    SET Target_Id = ?, Porter_Status = 'Success', Failure_Reason = NULL, Payload_JSON = ?
                    WHERE Source_Id = ?
                """, (target_id, payload_json, source_id))
            else:
                error_msg = response.text[:500]

                # Duplicate fallback (some tenants return 6240/duplicate text)
                dup_markers = ("Duplicate Name Exists Error", "already exists")
                if any(m in error_msg for m in dup_markers):
                    logger.warning(f"⚠️ Duplicate found: {term_name}, trying to fetch ID")
                    existing_id = get_existing_qbo_term_id(term_name)
                    if existing_id:
                        logger.info(f"✅ Retrieved duplicate ID for {term_name}: {existing_id}")
                        sql.run_query(f"""
                            UPDATE [{mapping_schema}].[{mapping_table}]
                            SET Target_Id = ?, Porter_Status = 'Exists', Failure_Reason = NULL, Payload_JSON = ?
                            WHERE Source_Id = ?
                        """, (existing_id, payload_json, source_id))
                        continue

                logger.error(f"❌ Failed to post {term_name}: {error_msg}")
                sql.run_query(f"""
                    UPDATE [{mapping_schema}].[{mapping_table}]
                    SET Porter_Status = 'Failed', Failure_Reason = ?, Retry_Count = ISNULL(Retry_Count, 0) + 1, Payload_JSON = ?
                    WHERE Source_Id = ?
                """, (error_msg, payload_json, source_id))

        except Exception as e:
            logger.exception(f"❌ Exception during post for {term_name}")
            sql.run_query(f"""
                UPDATE [{mapping_schema}].[{mapping_table}]
                SET Porter_Status = 'Failed', Failure_Reason = ?, Retry_Count = ISNULL(Retry_Count, 0) + 1, Payload_JSON = ?
                WHERE Source_Id = ?
            """, (str(e), payload_json, source_id))

def retry_failed_terms():
    logger.info("\n🔁 Retrying Failed Terms\n" + "=" * 30)
    df_mapping = sql.fetch_table(mapping_table, mapping_schema)
    if df_mapping.empty:
        logger.info("ℹ️ No mapping table yet. Nothing to retry.")
        return

    df_mapping["Retry_Count"] = pd.to_numeric(df_mapping["Retry_Count"], errors="coerce").fillna(0).astype(int)

    failed_ids = df_mapping[
        (df_mapping["Porter_Status"] == "Failed") & (df_mapping["Retry_Count"] < max_retries)
    ]["Source_Id"].tolist()

    if not failed_ids:
        logger.info("✅ No terms to retry.")
        return

    df_all = sql.fetch_table(source_table, source_schema)
    if df_all.empty:
        logger.info("⚠️ Source Term table empty; nothing to retry.")
        return

    df_remigrate = df_all[df_all["Id"].isin(failed_ids)]
    df_mapping = sql.fetch_table(mapping_table, mapping_schema)
    migrate(df_remigrate, df_mapping, remigrate_failed=True)

def migrate_terms():
    logger.info("\n🚀 Starting Term Migration\n" + "=" * 35)
    df = sql.fetch_table(source_table, source_schema)
    if df.empty:
        logger.warning("⚠️ No Term records found.")
        return

    sql.ensure_schema_exists(mapping_schema)

    if not sql.table_exists(mapping_table, mapping_schema):
        df_mapping = df.copy()
        df_mapping.rename(columns={"Id": "Source_Id"}, inplace=True)
        df_mapping["Target_Id"] = None
        df_mapping["Porter_Status"] = None
        df_mapping["Failure_Reason"] = None
        df_mapping["Retry_Count"] = 0
        df_mapping["Payload_JSON"] = None
        sql.insert_dataframe(df_mapping, mapping_table, mapping_schema)

    df_mapping = sql.fetch_table(mapping_table, mapping_schema)
    timer = ProgressTimer(total_records=len(df), logger=logger)

    migrate(df, df_mapping, remigrate_failed=False)
    retry_failed_terms()

    timer.stop()
    logger.info("\n🏁 Term migration completed.")


