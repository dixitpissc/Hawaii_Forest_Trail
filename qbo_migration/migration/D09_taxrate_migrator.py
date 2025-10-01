"""
Sequence : 09
Module: taxrate_migrator.py
Author: Dixit Prajapati
Created: 2025-09-18
Description: Handles migration of taxrate records from source system to QBO
Production : Ready
Phase : 02 - Multi User
"""

import os, json, requests, pandas as pd
from dotenv import load_dotenv
from storage.sqlserver import sql
from storage.sqlserver.sql import (
    ensure_schema_exists,
    insert_invoice_map_dataframe,
    table_exists,
    sanitize_column_name,
    fetch_value_dict,
)
from utils.token_refresher import auto_refresh_token_if_needed,get_qbo_context_migration
from utils.log_timer import global_logger as logger, ProgressTimer
from config.mapping.taxrate_mapping import TAXRATE_COLUMN_MAPPING as column_mapping
from urllib.parse import urlencode

load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA  = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
MINOR_VERSION  = os.getenv("QBO_MINOR_VERSION", "75")
FORCE_TAXSERVICE = os.getenv("USE_TAXSERVICE_FOR_TAXRATE", "").lower() in ("1","true","yes","y")

ensure_schema_exists(MAPPING_SCHEMA)

_session = requests.Session()

ctx = get_qbo_context_migration()
access_token = ctx['ACCESS_TOKEN']
realm_id = ctx["REALM_ID"]


def _qbo_query(sql_text: str):
    """
    Executes a QBO SQL query via /query and returns the JSON.
    Uses minorversion if provided. Safe for small lookups.
    """
    url = f"{_base_url()}/v3/company/{_realm()}/query"
    params = {"query": sql_text}
    if MINOR_VERSION:
        params["minorversion"] = MINOR_VERSION
    return _session.get(f"{url}?{urlencode(params)}", headers=_headers(), timeout=60)

def find_existing_taxrate_id_by_name(name: str) -> str | None:
    """
    Looks up TaxRate Id by Name using the /query endpoint.
    Returns Id (str) if found else None.
    """
    if not name:
        return None
    safe_name = name.replace("'", "''")  # escape single quotes for QBO SQL
    q = f"SELECT Id FROM TaxRate WHERE Name = '{safe_name}'"
    resp = _qbo_query(q)
    if resp.status_code != 200:
        logger.warning(f"⚠️ TaxRate lookup failed ({resp.status_code}): {resp.text[:300]}")
        return None
    data = resp.json()
    rows = (data.get("QueryResponse") or {}).get("TaxRate", [])
    if rows and isinstance(rows, list):
        return str(rows[0].get("Id")) if rows[0].get("Id") is not None else None
    return None

def _bool(val):
    if pd.isna(val): return None
    if isinstance(val, bool): return val
    s = str(val).strip().lower()
    if s in ("1","true","t","yes","y"): return True
    if s in ("0","false","f","no","n"):  return False
    return None

def _clean_nulls(df: pd.DataFrame) -> pd.DataFrame:
    # pandas >=2 recommends .map on DataFrame
    return df.map(lambda x: None if isinstance(x, str) and x.strip() == "" else x)

def _num(val):
    try:
        s = str(val).strip()
        if s == "" or s.lower() == "none" or s.lower() == "null":
            return None
        return float(s)
    except Exception:
        return None

def _coalesce_rate(row: pd.Series) -> float:
    """
    Prefer RateValue; if missing, try EffectiveTaxRate[0].RateValue then [1].RateValue.
    Fallback to 0.0 (valid for 'No Tax' style rates).
    """
    candidates = [
        _src("RateValue"),
        _src("EffectiveTaxRate[0].RateValue"),
        _src("EffectiveTaxRate[1].RateValue"),
    ]
    for col in candidates:
        v = row.get(col)
        n = _num(v)
        if n is not None:
            return n
    return 0.0

def _src(key: str) -> str:
    return sanitize_column_name(column_mapping[key])

def _base_url():
    env = os.getenv("QBO_ENVIRONMENT", "sandbox")
    return "https://sandbox-quickbooks.api.intuit.com" if env == "sandbox" else "https://quickbooks.api.intuit.com"

def _realm(): return realm_id

def _headers():
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

def _with_mv(url: str) -> str:
    return f"{url}?minorversion={MINOR_VERSION}" if MINOR_VERSION else url

# ── AST detection (Preferences.TaxPrefs.PartnerTaxEnabled) ─────────────────────
def is_ast_enabled() -> bool:
    # If caller forced behavior, skip detection.
    if FORCE_TAXSERVICE:
        return True
    try:
        url = _with_mv(f"{_base_url()}/v3/company/{_realm()}/preferences")
        resp = _session.get(url, headers=_headers(), timeout=60)
        if resp.status_code != 200:
            logger.warning(f"⚠️ Preferences check failed ({resp.status_code}). Assuming AST disabled.")
            return False
        prefs = resp.json().get("Preferences", {})
        taxprefs = prefs.get("TaxPrefs", {})
        # PartnerTaxEnabled → indicates AST exposure for API
        val = taxprefs.get("PartnerTaxEnabled")
        return bool(val)
    except Exception as e:
        logger.warning(f"⚠️ Could not determine AST state: {e}. Assuming AST disabled.")
        return False

# ── Mapping table init & enrichment ────────────────────────────────────────────
def ensure_mapping_table():
    logger.info("🧭 Initializing Map_TaxRate from source...")
    df = sql.fetch_table("TaxRate", SOURCE_SCHEMA)
    if df.empty:
        logger.warning("⚠️ No rows found in source [TaxRate]. Nothing to initialize.")
        return

    df = _clean_nulls(df.copy())
    df["Source_Id"] = df["Id"] if "Id" in df.columns else range(1, len(df)+1)

    for col in ("Target_Id","Porter_Status","Retry_Count","Failure_Reason","Payload_JSON","Mapped_AgencyRef"):
        if col not in df.columns: df[col] = None

    df["Porter_Status"]  = "Ready"
    df["Retry_Count"]    = 0
    df["Failure_Reason"] = None
    df["Payload_JSON"]   = None

    if table_exists("Map_TaxRate", MAPPING_SCHEMA):
        logger.info("🧹 Clearing existing rows in Map_TaxRate...")
        sql.run_query(f"DELETE FROM [{MAPPING_SCHEMA}].[Map_TaxRate]")

    insert_invoice_map_dataframe(df, "Map_TaxRate", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df)} rows into {MAPPING_SCHEMA}.Map_TaxRate")

    # Enrich Mapped_AgencyRef from Map_TaxAgency(Target_Id) using source AgencyRef.value
    logger.info("🔗 Enriching Mapped_AgencyRef from Map_TaxAgency.Target_Id...")
    agency_map = fetch_value_dict(
        f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_TaxAgency]",
        key_col="Source_Id",
        value_col="Target_Id"
    )
    map_df = sql.fetch_table("Map_TaxRate", MAPPING_SCHEMA)
    agency_src_col = _src("AgencyRef.value")
    updates = 0
    for _, r in map_df.iterrows():
        sid = r["Source_Id"]
        src_agency = r.get(agency_src_col)
        tgt = agency_map.get(src_agency) if src_agency is not None else None
        if tgt:
            sql.update_multiple_columns(
                table="Map_TaxRate",
                source_id=str(sid),
                column_value_map={"Mapped_AgencyRef": str(tgt)},
                schema=MAPPING_SCHEMA
            )
            updates += 1
    logger.info(f"🔁 Enriched Mapped_AgencyRef for {updates} rows.")

# ── Payload builder for /taxrate (non-AST) ─────────────────────────────────────
def build_payload(row: pd.Series) -> (dict, list):
    missing = []

    # Name: synthesize if absent
    name_val = row.get(_src("Name"))
    if not name_val or str(name_val).strip() == "":
        name_val = f"Rate-{row.get('Source_Id')}"
    name = str(name_val).strip()

    # Rate: coalesce (never missing)
    rate_val = _coalesce_rate(row)

    # Agency required (must be mapped)
    agency_ref = row.get("Mapped_AgencyRef")
    if not agency_ref or str(agency_ref).strip() == "":
        missing.append("AgencyRef")

    if missing:
        return {}, missing

    payload = {
        "Name": name,
        "RateValue": rate_val,
        "AgencyRef": {"value": str(agency_ref)}
    }

    active = _bool(row.get(_src("Active")))
    if active is not None:
        payload["Active"] = active

    desc = row.get(_src("Description"))
    if desc and str(desc).strip():
        payload["Description"] = str(desc).strip()

    return payload, []

# ── TaxService payload (AST / fallback) ────────────────────────────────────────
def build_taxservice_payload(row: pd.Series) -> (dict, list):
    missing = []

    # Name: synthesize if absent
    name_val = row.get(_src("Name"))
    if not name_val or str(name_val).strip() == "":
        name_val = f"Rate-{row.get('Source_Id')}"
    name = str(name_val).strip()

    # Rate: coalesce (never missing)
    rate_val = _coalesce_rate(row)

    # Agency required
    agency_ref = row.get("Mapped_AgencyRef")
    if not agency_ref or str(agency_ref).strip() == "":
        missing.append("AgencyRef")

    if missing:
        return {}, missing

    taxcode_name = f"porter-rate-{name}"[:100]
    payload = {
        "TaxCode": taxcode_name,
        "TaxRateDetails": [{
            "TaxRateName": name,
            "RateValue": rate_val,
            "TaxAgencyId": str(agency_ref),
            "TaxApplicableOn": "Sales"
        }]
    }
    return payload, []


# ── Batch payload generation ───────────────────────────────────────────────────
def generate_taxrate_payloads_in_batches(batch_size=500):
    logger.info("🧱 Generating TaxRate JSON payloads...")
    while True:
        df = sql.fetch_table_with_params(
            f"""
            SELECT TOP {batch_size} *
            FROM [{MAPPING_SCHEMA}].[Map_TaxRate]
            WHERE Porter_Status = 'Ready'
              AND (Payload_JSON IS NULL OR Payload_JSON = '')
            """,
            ()
        )
        if df.empty:
            logger.info("✅ All TaxRate payloads generated.")
            break

        for _, row in df.iterrows():
            sid = row["Source_Id"]
            payload, missing = build_payload(row)

            # Only hard-fail if AgencyRef missing (we cannot post without it)
            if missing:
                reason = f"Missing required fields: {', '.join(missing)}"
                logger.warning(f"⚠️ Skipping Source_Id={sid} — {reason}")
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_TaxRate]
                    SET Porter_Status='Failed', Failure_Reason=?
                    WHERE Source_Id=?
                """, (reason, sid))
                continue

            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_TaxRate]
                SET Payload_JSON=?, Failure_Reason=NULL
                WHERE Source_Id=?
            """, (json.dumps(payload, indent=2), sid))

        logger.info(f"🧩 Generated payloads for {len(df)} TaxRate rows in this batch.")

# ── Posting (with AST-aware fallback to TaxService) ────────────────────────────
def post_via_taxrate(payload: dict):
    url = _with_mv(f"{_base_url()}/v3/company/{_realm()}/taxrate")
    return _session.post(url, headers=_headers(), json=payload, timeout=60)

def post_via_taxservice(payload: dict):
    url = _with_mv(f"{_base_url()}/v3/company/{_realm()}/taxservice/taxcode")
    return _session.post(url, headers=_headers(), json=payload, timeout=60)

def post_taxrate(row: pd.Series, ast: bool):
    sid = row["Source_Id"]
    if row.get("Porter_Status") == "Success":
        return
    if int(row.get("Retry_Count") or 0) >= 5:
        logger.warning(f"⏭️ Skipping Source_Id={sid} — exceeded retry limit.")
        return

    payload = json.loads(row["Payload_JSON"]) if row.get("Payload_JSON") else None
    if not payload:
        logger.warning(f"⏭️ Skipping Source_Id={sid} — missing Payload_JSON.")
        return

    auto_refresh_token_if_needed()

    # Decide approach
    use_taxservice = ast or FORCE_TAXSERVICE

    try:
        if use_taxservice:
            # Build TaxService payload per row
            ts_payload, missing = build_taxservice_payload(row)
            if missing:
                reason = f"Missing required fields: {', '.join(missing)}"
                raise ValueError(reason)
            resp = post_via_taxservice(ts_payload)
        else:
            resp = post_via_taxrate(payload)

        if resp.status_code == 200:
            data = resp.json()
            # Extract created TaxRate Id
            qid = None
            if use_taxservice:
                # TaxService returns TaxCode + TaxRateDetails with TaxRateId
                details = (data or {}).get("TaxRateDetails") or (data.get("TaxService", {}).get("TaxRateDetails") if isinstance(data, dict) else [])
                if isinstance(details, list) and details:
                    qid = str(details[0].get("TaxRateId") or "")
            else:
                qid = (data or {}).get("TaxRate", {}).get("Id")

            if qid:
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_TaxRate]
                    SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL
                    WHERE Source_Id=?
                """, (qid, sid))
                logger.info(f"✅ TaxRate Source_Id={sid} → QBO Id={qid} ({'TaxService' if use_taxservice else 'TaxRate'})")
            else:
                # Successful HTTP but no ID? mark failed with hint
                reason = f"Success without Id (endpoint={'TaxService' if use_taxservice else 'TaxRate'}). Raw: {str(data)[:300]}"
                raise RuntimeError(reason)
        else:
            text = resp.text[:600]

            # Duplicate Name? try to retrieve existing Id and mark as Exists
            if "Duplicate Name Exists Error" in text or '"code":"6240"' in text:
                # choose the name we tried to create
                tried_name = payload.get("Name")
                # for TaxService path, payload is different – pull from builder again if needed
                if use_taxservice and not tried_name:
                    # rebuild the name used in TaxService builder
                    name_val = row.get(_src("Name")) or f"Rate-{row.get('Source_Id')}"
                    tried_name = str(name_val).strip()

                existing_id = find_existing_taxrate_id_by_name(tried_name)
                if existing_id:
                    sql.run_query(f"""
                        UPDATE [{MAPPING_SCHEMA}].[Map_TaxRate]
                        SET Target_Id=?, Porter_Status='Exists', Failure_Reason=NULL
                        WHERE Source_Id=?
                    """, (existing_id, sid))
                    logger.info(f"✅ TaxRate Source_Id={sid} already exists → QBO Id={existing_id} (marked Exists)")
                    return
                # If we couldn’t find it, fall through to normal error handling
                logger.warning(f"⚠️ Duplicate reported but lookup failed for name '{tried_name}'. Raw: {text}")

            # If we tried /taxrate and received Unsupported Operation, retry with TaxService
            if (not use_taxservice) and ("Unsupported Operation" in text or resp.status_code in (400, 500)):
                logger.warning(f"↩️ Fallback: /taxrate unsupported. Retrying via TaxService for Source_Id={sid}.")
                ts_payload, missing = build_taxservice_payload(row)
                if missing:
                    reason = f"Missing required fields: {', '.join(missing)}"
                    raise ValueError(reason)
                resp2 = post_via_taxservice(ts_payload)
                if resp2.status_code == 200:
                    data = resp2.json()
                    details = data.get("TaxRateDetails", [])
                    qid = str(details[0].get("TaxRateId")) if details else None
                    if qid:
                        sql.run_query(f"""
                            UPDATE [{MAPPING_SCHEMA}].[Map_TaxRate]
                            SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL
                            WHERE Source_Id=?
                        """, (qid, sid))
                        logger.info(f"✅ TaxRate Source_Id={sid} → QBO Id={qid} (TaxService fallback)")
                        return

                    text2 = resp2.text[:600]
                    # Try duplicate handling on fallback too
                    if "Duplicate Name Exists Error" in text2 or '"code":"6240"' in text2:
                        name_val = row.get(_src("Name")) or f"Rate-{row.get('Source_Id')}"
                        tried_name = str(name_val).strip()
                        existing_id = find_existing_taxrate_id_by_name(tried_name)
                        if existing_id:
                            sql.run_query(f"""
                                UPDATE [{MAPPING_SCHEMA}].[Map_TaxRate]
                                SET Target_Id=?, Porter_Status='Exists', Failure_Reason=NULL
                                WHERE Source_Id=?
                            """, (existing_id, sid))
                            logger.info(f"✅ TaxRate Source_Id={sid} already exists → QBO Id={existing_id} (marked Exists)")
                            return
                        raise RuntimeError(f"TaxService success without Id. Raw: {text2}")
                else:
                    text2 = resp2.text[:600]
                    # Duplicate handling on fallback failure
                    if "Duplicate Name Exists Error" in text2 or '"code":"6240"' in text2:
                        name_val = row.get(_src("Name")) or f"Rate-{row.get('Source_Id')}"
                        tried_name = str(name_val).strip()
                        existing_id = find_existing_taxrate_id_by_name(tried_name)
                        if existing_id:
                            sql.run_query(f"""
                                UPDATE [{MAPPING_SCHEMA}].[Map_TaxRate]
                                SET Target_Id=?, Porter_Status='Exists', Failure_Reason=NULL
                                WHERE Source_Id=?
                            """, (existing_id, sid))
                            logger.info(f"✅ TaxRate Source_Id={sid} already exists → QBO Id={existing_id} (marked Exists)")
                            return
                    raise RuntimeError(f"TaxService failed: {text2}")

            # Normal failure path
            raise RuntimeError(text)

    except Exception as e:
        reason = str(e)[:600]
        logger.error(f"❌ TaxRate Source_Id={sid} failed: {reason}")
        sql.run_query(f"""
            UPDATE [{MAPPING_SCHEMA}].[Map_TaxRate]
            SET Porter_Status='Failed',
                Retry_Count=ISNULL(Retry_Count,0)+1,
                Failure_Reason=?
            WHERE Source_Id=?
        """, (reason, sid))

# ── Orchestration ──────────────────────────────────────────────────────────────
def migrate_taxrates():
    print("\n🚀 Starting TaxRate Migration\n" + "=" * 33)
    ensure_mapping_table()
    generate_taxrate_payloads_in_batches(batch_size=500)

    rows = sql.fetch_table("Map_TaxRate", MAPPING_SCHEMA)
    eligible = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("ℹ️ No eligible TaxRate rows to post.")
        return

    ast = is_ast_enabled()
    logger.info(f"🧭 AST detected: {ast or FORCE_TAXSERVICE} (forced={FORCE_TAXSERVICE})")

    pt = ProgressTimer(len(eligible))
    for _, r in eligible.iterrows():
        post_taxrate(r, ast)
        pt.update()

    print("\n🏁 TaxRate migration completed.")

def resume_or_post_taxrates():
    print("\n🔁 Resuming TaxRate Migration (conditional)\n" + "=" * 46)

    if not table_exists("Map_TaxRate", MAPPING_SCHEMA):
        logger.warning("❌ Map_TaxRate not found. Running full migration.")
        migrate_taxrates()
        return

    mapped_df = sql.fetch_table("Map_TaxRate", MAPPING_SCHEMA)
    src_df    = sql.fetch_table("TaxRate", SOURCE_SCHEMA)

    if len(mapped_df) != len(src_df):
        logger.warning(f"❌ Row count mismatch (Map={len(mapped_df)}, Source={len(src_df)}). Running full migration.")
        migrate_taxrates()
        return

    if mapped_df["Payload_JSON"].isnull().any():
        logger.warning("❌ Some Map_TaxRate rows missing Payload_JSON. Running full migration.")
        migrate_taxrates()
        return

    eligible = mapped_df[mapped_df["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("ℹ️ No eligible TaxRate rows to post.")
        return

    ast = is_ast_enabled()
    logger.info(f"🧭 AST detected: {ast or FORCE_TAXSERVICE} (forced={FORCE_TAXSERVICE})")

    pt = ProgressTimer(len(eligible))
    for _, r in eligible.iterrows():
        post_taxrate(r, ast)
        pt.update()

    print("\n🏁 TaxRate posting completed.")
