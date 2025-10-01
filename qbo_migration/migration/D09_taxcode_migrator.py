"""
Sequence : 09
Module: Taxcode_migrator.py
Author: Dixit Prajapati
Created: 2025-09-18
Description: Handles migration of taxcode records from source system to QBO
Production : Ready
Phase : 02 - Multi User
"""

import os, json, requests, pandas as pd
from urllib.parse import urlencode
from dotenv import load_dotenv
from storage.sqlserver import sql
from storage.sqlserver.sql import (
    ensure_schema_exists,
    insert_invoice_map_dataframe,
    table_exists,
    sanitize_column_name,
    fetch_value_dict,
)
from utils.token_refresher import auto_refresh_token_if_needed, get_qbo_context_migration
from utils.log_timer import global_logger as logger, ProgressTimer
from config.mapping.taxcode_mapping import TAXCODE_COLUMN_MAPPING as column_mapping

# ── Bootstrap ─────────────────────────────────────────────────
load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA  = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
MINOR_VERSION  = os.getenv("QBO_MINOR_VERSION", "75")
FORCE_TAXSERVICE = True  # Prefer TaxService for TaxCode creation consistently

ensure_schema_exists(MAPPING_SCHEMA)

_session = requests.Session()

ctx = get_qbo_context_migration()
access_token = ctx['ACCESS_TOKEN']
realm_id = ctx["REALM_ID"]


def _resolve_col(df: pd.DataFrame, primary_key: str, alternates: list[str]) -> str | None:
    """
    Find the first existing column among sanitized primary_key and alternates.
    Returns the existing column name (already in df), or None.
    """
    candidates = [sanitize_column_name(primary_key)] + [sanitize_column_name(a) for a in alternates]
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _get_str(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s != "" and s.lower() != "null" else None


# ── Helpers ───────────────────────────────────────────────────
def _bool(val):
    if pd.isna(val): return None
    if isinstance(val, bool): return val
    s = str(val).strip().lower()
    if s in ("1","true","t","yes","y"): return True
    if s in ("0","false","f","no","n"):  return False
    return None

def _clean_nulls(df: pd.DataFrame) -> pd.DataFrame:
    # pandas>=2: use .map on DataFrame
    return df.map(lambda x: None if isinstance(x, str) and x.strip() == "" else x)

def _src(key: str) -> str:
    # after insert_invoice_map_dataframe, column names are sanitized (.] -> _)
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

def _qbo_query(sql_text: str):
    url = f"{_base_url()}/v3/company/{_realm()}/query"
    params = {"query": sql_text}
    if MINOR_VERSION:
        params["minorversion"] = MINOR_VERSION
    return _session.get(f"{url}?{urlencode(params)}", headers=_headers(), timeout=60)

def find_existing_taxcode_id_by_name(name: str) -> str | None:
    if not name:
        return None
    safe = name.replace("'", "''")
    resp = _qbo_query(f"SELECT Id FROM TaxCode WHERE Name = '{safe}'")
    if resp.status_code != 200:
        logger.warning(f"⚠️ TaxCode lookup failed ({resp.status_code}): {resp.text[:300]}")
        return None
    data = resp.json()
    rows = (data.get("QueryResponse") or {}).get("TaxCode", [])
    if rows and isinstance(rows, list):
        return str(rows[0].get("Id")) if rows[0].get("Id") is not None else None
    return None

# ── Ensure Map_TaxCode and enrich ref mappings ────────────────
def ensure_mapping_table():
    logger.info("🧭 Initializing Map_TaxCode from source...")
    df = sql.fetch_table("TaxCode", SOURCE_SCHEMA)
    if df.empty:
        logger.warning("⚠️ No rows found in source [TaxCode]. Nothing to initialize.")
        return

    df = _clean_nulls(df.copy())
    df["Source_Id"] = df["Id"] if "Id" in df.columns else range(1, len(df)+1)

    # migration cols + mapped refs
    extra_cols = [
        "Target_Id","Porter_Status","Retry_Count","Failure_Reason","Payload_JSON",
        # mapped rate refs
        "Mapped_SalesRate0","Mapped_SalesRate1",
        "Mapped_PurchaseRate0","Mapped_PurchaseRate1",
    ]
    for c in extra_cols:
        if c not in df.columns: df[c] = None

    df["Porter_Status"]  = "Ready"
    df["Retry_Count"]    = 0
    df["Failure_Reason"] = None
    df["Payload_JSON"]   = None

    if table_exists("Map_TaxCode", MAPPING_SCHEMA):
        logger.info("🧹 Clearing existing rows in Map_TaxCode...")
        sql.run_query(f"DELETE FROM [{MAPPING_SCHEMA}].[Map_TaxCode]")

    insert_invoice_map_dataframe(df, "Map_TaxCode", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df)} rows into {MAPPING_SCHEMA}.Map_TaxCode")

    # Build rate map: Source_Id -> Target_Id from Map_TaxRate
    rate_map = fetch_value_dict(
        f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_TaxRate]",
        key_col="Source_Id",
        value_col="Target_Id"
    )

       # Read back Map_TaxCode (sanitized column names are in DB now)
    map_df = sql.fetch_table("Map_TaxCode", MAPPING_SCHEMA)

    # Build rate maps: by Source_Id and by Name (fallback)
    rate_id_map = fetch_value_dict(
        f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_TaxRate]",
        key_col="Source_Id",
        value_col="Target_Id"
    )
    rate_name_map = fetch_value_dict(
        f"SELECT Name, Target_Id FROM [{MAPPING_SCHEMA}].[Map_TaxRate]",
        key_col="Name",
        value_col="Target_Id"
    )

    # Resolve column names robustly (handle bracket variants)
    s0_val_col = _resolve_col(
        map_df,
        column_mapping["Sales[0].TaxRateRef.value"],
        ["SalesTaxRateList.TaxRateDetail[0].TaxRateRef.value",  # single bracket
         "SalesTaxRateList_TaxRateDetail_0__TaxRateRef_value"]  # already sanitized form
    )
    s1_val_col = _resolve_col(
        map_df,
        column_mapping["Sales[1].TaxRateRef.value"],
        ["SalesTaxRateList.TaxRateDetail[1].TaxRateRef.value",
         "SalesTaxRateList_TaxRateDetail_1__TaxRateRef_value"]
    )
    p0_val_col = _resolve_col(
        map_df,
        column_mapping["Purchase[0].TaxRateRef.value"],
        ["PurchaseTaxRateList.TaxRateDetail[0].TaxRateRef.value",
         "PurchaseTaxRateList_TaxRateDetail_0__TaxRateRef_value"]
    )
    p1_val_col = _resolve_col(
        map_df,
        column_mapping["Purchase[1].TaxRateRef.value"],
        ["PurchaseTaxRateList.TaxRateDetail[1].TaxRateRef.value",
         "PurchaseTaxRateList_TaxRateDetail_1__TaxRateRef_value"]
    )

    s0_name_col = _resolve_col(
        map_df,
        column_mapping["Sales[0].TaxRateRef.name"],
        ["SalesTaxRateList.TaxRateDetail[0].TaxRateRef.name",
         "SalesTaxRateList_TaxRateDetail_0__TaxRateRef_name"]
    )
    s1_name_col = _resolve_col(
        map_df,
        column_mapping["Sales[1].TaxRateRef.name"],
        ["SalesTaxRateList.TaxRateDetail[1].TaxRateRef.name",
         "SalesTaxRateList_TaxRateDetail_1__TaxRateRef_name"]
    )
    p0_name_col = _resolve_col(
        map_df,
        column_mapping["Purchase[0].TaxRateRef.name"],
        ["PurchaseTaxRateList.TaxRateDetail[0].TaxRateRef.name",
         "PurchaseTaxRateList_TaxRateDetail_0__TaxRateRef_name"]
    )
    p1_name_col = _resolve_col(
        map_df,
        column_mapping["Purchase[1].TaxRateRef.name"],
        ["PurchaseTaxRateList.TaxRateDetail[1].TaxRateRef.name",
         "PurchaseTaxRateList_TaxRateDetail_1__TaxRateRef_name"]
    )

    # Nothing to do if none of the columns exist
    if not any([s0_val_col, s1_val_col, p0_val_col, p1_val_col, s0_name_col, s1_name_col, p0_name_col, p1_name_col]):
        logger.warning("⚠️ No Sales/Purchase TaxRateRef columns found after sanitation.")
        return

    # Map and persist
    updated_rows = 0
    id_hits = name_hits = 0

    for _, r in map_df.iterrows():
        sid = r["Source_Id"]

        def map_rate(val_col, name_col):
            # Prefer mapping by Source_Id; fallback by Name
            src_val = _get_str(r.get(val_col)) if val_col else None
            if src_val and src_val in rate_id_map:
                return str(rate_id_map[src_val]), "id"
            src_name = _get_str(r.get(name_col)) if name_col else None
            if src_name and src_name in rate_name_map:
                return str(rate_name_map[src_name]), "name"
            return None, None

        s0, t0 = map_rate(s0_val_col, s0_name_col)
        s1, t1 = map_rate(s1_val_col, s1_name_col)
        p0, u0 = map_rate(p0_val_col, p0_name_col)
        p1, u1 = map_rate(p1_val_col, p1_name_col)

        colmap = {}
        if s0: colmap["Mapped_SalesRate0"]    = s0
        if s1: colmap["Mapped_SalesRate1"]    = s1
        if p0: colmap["Mapped_PurchaseRate0"] = p0
        if p1: colmap["Mapped_PurchaseRate1"] = p1

        # count hits
        for tag in (t0, t1, u0, u1):
            if tag == "id":   id_hits += 1
            if tag == "name": name_hits += 1

        if colmap:
            sql.update_multiple_columns(
                table="Map_TaxCode",
                source_id=str(sid),
                column_value_map=colmap,
                schema=MAPPING_SCHEMA
            )
            updated_rows += 1

    logger.info(f"🔗 Enriched mapped rate refs for {updated_rows} rows. (by Id: {id_hits}, by Name: {name_hits})")


# ── Builders ──────────────────────────────────────────────────
def _clean_name(n, sid):  # synthesize if needed
    if n and str(n).strip():
        return str(n).strip()
    return f"Code-{sid}"

def build_taxcode_payload_non_ast(row: pd.Series) -> (dict, list):
    """
    Build /taxcode entity payload (non-AST tenants).
    Requires at least one of Sales/Purchase rate details.
    """
    missing = []
    name = _clean_name(row.get(_src("Name")), row.get("Source_Id"))

    desc = row.get(_src("Description"))
    active = _bool(row.get(_src("Active")))
    taxable = _bool(row.get(_src("Taxable")))
    taxgroup = _bool(row.get(_src("TaxGroup")))

    # Rate detail assembly using mapped ids (from Map_TaxRate.Target_Id)
    sales_details = []
    for i in (0,1):
        mapped = row.get(f"Mapped_SalesRate{i}")
        if mapped:
            ttype = row.get(_src(f"Sales[{i}].TaxTypeApplicable")) or "TaxOnAmount"
            order = row.get(_src(f"Sales[{i}].TaxOrder"))
            try:
                order = int(order) if order is not None and str(order).strip() != "" else 0
            except Exception:
                order = 0
            sales_details.append({
                "TaxTypeApplicable": str(ttype),
                "TaxRateRef": {"value": str(mapped)},
                "TaxOrder": order
            })

    purchase_details = []
    for i in (0,1):
        mapped = row.get(f"Mapped_PurchaseRate{i}")
        if mapped:
            ttype = row.get(_src(f"Purchase[{i}].TaxTypeApplicable")) or "TaxOnAmount"
            order = row.get(_src(f"Purchase[{i}].TaxOrder"))
            try:
                order = int(order) if order is not None and str(order).strip() != "" else 0
            except Exception:
                order = 0
            purchase_details.append({
                "TaxTypeApplicable": str(ttype),
                "TaxRateRef": {"value": str(mapped)},
                "TaxOrder": order
            })

    if not sales_details and not purchase_details:
        missing.append("AnyRateDetail(Sales or Purchase)")

    payload = {
        "Name": name,
        "SalesTaxRateList": {"TaxRateDetail": sales_details},
        "PurchaseTaxRateList": {"TaxRateDetail": purchase_details},
    }
    if desc and str(desc).strip(): payload["Description"] = str(desc).strip()
    if active is not None:  payload["Active"] = active
    if taxable is not None: payload["Taxable"] = taxable
    if taxgroup is not None: payload["TaxGroup"] = taxgroup

    return payload, missing

def build_taxservice_payload(row: pd.Series) -> (dict, list):
    """
    Build TaxService payload (AST/hybrid; also default here).
    Requires at least one rate detail. Use Mapped_* (TaxRateId) and set TaxApplicableOn.
    """
    missing = []
    name = _clean_name(row.get(_src("Name")), row.get("Source_Id"))

    details = []
    for i in (0,1):
        mapped = row.get(f"Mapped_SalesRate{i}")
        if mapped:
            details.append({
                "TaxRateName": f"{name}-S{i}",
                "TaxRateId": str(mapped),
                "TaxApplicableOn": "Sales"
            })
    for i in (0,1):
        mapped = row.get(f"Mapped_PurchaseRate{i}")
        if mapped:
            details.append({
                "TaxRateName": f"{name}-P{i}",
                "TaxRateId": str(mapped),
                "TaxApplicableOn": "Purchase"
            })

    if not details:
        missing.append("AnyRateDetail(Sales or Purchase)")

    payload = {
        "TaxCode": name,
        "TaxRateDetails": details
    }
    return payload, missing

# ── Posting endpoints ─────────────────────────────────────────
def post_via_taxcode(payload: dict):
    url = _with_mv(f"{_base_url()}/v3/company/{_realm()}/taxcode")
    return _session.post(url, headers=_headers(), json=payload, timeout=60)

def post_via_taxservice(payload: dict):
    url = _with_mv(f"{_base_url()}/v3/company/{_realm()}/taxservice/taxcode")
    return _session.post(url, headers=_headers(), json=payload, timeout=60)

# ── Payload generation (store JSON) ───────────────────────────
def generate_taxcode_payloads_in_batches(batch_size=400):
    logger.info("🧱 Generating TaxCode JSON payloads...")
    while True:
        df = sql.fetch_table_with_params(
            f"""
            SELECT TOP {batch_size} *
            FROM [{MAPPING_SCHEMA}].[Map_TaxCode]
            WHERE Porter_Status = 'Ready'
              AND (Payload_JSON IS NULL OR Payload_JSON = '')
            """,
            ()
        )
        if df.empty:
            logger.info("✅ All TaxCode payloads generated.")
            break

        for _, row in df.iterrows():
            sid = row["Source_Id"]

            # Build for non-AST (entity); we store JSON of the entity shape
            payload, missing = build_taxcode_payload_non_ast(row)
            if missing:
                reason = f"Missing required fields: {', '.join(missing)}"
                logger.warning(f"⚠️ Skipping Source_Id={sid} — {reason}")
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_TaxCode]
                    SET Porter_Status='Failed', Failure_Reason=?
                    WHERE Source_Id=?
                """, (reason, sid))
                continue

            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_TaxCode]
                SET Payload_JSON=?, Failure_Reason=NULL
                WHERE Source_Id=?
            """, (json.dumps(payload, indent=2), sid))

        logger.info(f"🧩 Generated payloads for {len(df)} TaxCode rows in this batch.")

# ── Posting (prefer TaxService; handle duplicates 6240) ───────
def migrate_post_taxcode_row(row: pd.Series):
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

    # Always try TaxService first for reliability
    ts_payload, missing_ts = build_taxservice_payload(row)
    if missing_ts:
        reason = f"Missing required fields: {', '.join(missing_ts)}"
        logger.error(f"❌ TaxCode Source_Id={sid} cannot post (TaxService): {reason}")
        sql.run_query(f"""
            UPDATE [{MAPPING_SCHEMA}].[Map_TaxCode]
            SET Porter_Status='Failed', Failure_Reason=?, Retry_Count=ISNULL(Retry_Count,0)+1
            WHERE Source_Id=?
        """, (reason, sid))
        return

    resp = post_via_taxservice(ts_payload)
    if resp.status_code == 200:
        data = resp.json()
        # For TaxService response, look for created TaxCode Id
        # Many tenants return TaxCode with Id; sometimes nested
        tc = data.get("TaxCode") or data.get("TaxService", {}).get("TaxCode")
        qid = str(tc.get("Id")) if isinstance(tc, dict) and tc.get("Id") is not None else None
        if not qid:
            # Fallback: query by name
            qid = find_existing_taxcode_id_by_name(ts_payload["TaxCode"])
        if qid:
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_TaxCode]
                SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL
                WHERE Source_Id=?
            """, (qid, sid))
            logger.info(f"✅ TaxCode Source_Id={sid} → QBO Id={qid} (TaxService)")
            return
        # Unexpected success-without-id
        text = str(data)[:400]
        logger.error(f"❌ Success without Id for Source_Id={sid}. Raw: {text}")
        sql.run_query(f"""
            UPDATE [{MAPPING_SCHEMA}].[Map_TaxCode]
            SET Porter_Status='Failed', Failure_Reason=?, Retry_Count=ISNULL(Retry_Count,0)+1
            WHERE Source_Id=?
        """, (f"Success(no Id). Raw: {text}", sid))
        return

    # Non-200 → check duplicates (6240)
    text = resp.text[:600]
    if "Duplicate Name Exists Error" in text or '"code":"6240"' in text:
        tried_name = ts_payload["TaxCode"]
        existing_id = find_existing_taxcode_id_by_name(tried_name)
        if existing_id:
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_TaxCode]
                SET Target_Id=?, Porter_Status='Exists', Failure_Reason=NULL
                WHERE Source_Id=?
            """, (existing_id, sid))
            logger.info(f"✅ TaxCode Source_Id={sid} already exists → QBO Id={existing_id} (marked Exists)")
            return

    # If TaxService failed for other reasons, try /taxcode entity as a fallback (non-AST tenants)
    resp2 = post_via_taxcode(payload)
    if resp2.status_code == 200:
        data = resp2.json()
        qid = (data or {}).get("TaxCode", {}).get("Id")
        if qid:
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_TaxCode]
                SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL
                WHERE Source_Id=?
            """, (qid, sid))
            logger.info(f"✅ TaxCode Source_Id={sid} → QBO Id={qid} (/taxcode fallback)")
            return
        text2 = str(data)[:400]
        logger.error(f"❌ Success(/taxcode) without Id for Source_Id={sid}. Raw: {text2}")
        sql.run_query(f"""
            UPDATE [{MAPPING_SCHEMA}].[Map_TaxCode]
            SET Porter_Status='Failed', Failure_Reason=?, Retry_Count=ISNULL(Retry_Count,0)+1
            WHERE Source_Id=?
        """, (f"Success(no Id) /taxcode. Raw: {text2}", sid))
        return

    # Fallback non-200 → duplicate check again
    text2 = resp2.text[:600]
    if "Duplicate Name Exists Error" in text2 or '"code":"6240"' in text2:
        tried_name = payload.get("Name")
        existing_id = find_existing_taxcode_id_by_name(tried_name)
        if existing_id:
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_TaxCode]
                SET Target_Id=?, Porter_Status='Exists', Failure_Reason=NULL
                WHERE Source_Id=?
            """, (existing_id, sid))
            logger.info(f"✅ TaxCode Source_Id={sid} already exists → QBO Id={existing_id} (marked Exists)")
            return

    # Final failure
    logger.error(f"❌ TaxCode Source_Id={sid} failed.\nTaxService: {text}\n/taxcode: {text2}")
    sql.run_query(f"""
        UPDATE [{MAPPING_SCHEMA}].[Map_TaxCode]
        SET Porter_Status='Failed', Failure_Reason=?, Retry_Count=ISNULL(Retry_Count,0)+1
        WHERE Source_Id=?
    """, (f"TaxService: {text} | /taxcode: {text2}", sid))

# ── Orchestration ─────────────────────────────────────────────
def migrate_taxcodes():
    print("\n🚀 Starting TaxCode Migration\n" + "=" * 33)
    ensure_mapping_table()
    generate_taxcode_payloads_in_batches(batch_size=400)

    rows = sql.fetch_table("Map_TaxCode", MAPPING_SCHEMA)
    eligible = rows[rows["Porter_Status"].isin(["Ready","Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("ℹ️ No eligible TaxCode rows to post.")
        return

    pt = ProgressTimer(len(eligible))
    for _, r in eligible.iterrows():
        migrate_post_taxcode_row(r)
        pt.update()

    print("\n🏁 TaxCode migration completed.")

def resume_or_post_taxcodes():
    print("\n🔁 Resuming TaxCode Migration (conditional)\n" + "=" * 46)

    if not table_exists("Map_TaxCode", MAPPING_SCHEMA):
        logger.warning("❌ Map_TaxCode not found. Running full migration.")
        migrate_taxcodes()
        return

    mapped_df = sql.fetch_table("Map_TaxCode", MAPPING_SCHEMA)
    src_df    = sql.fetch_table("TaxCode", SOURCE_SCHEMA)

    if len(mapped_df) != len(src_df):
        logger.warning(f"❌ Row count mismatch (Map={len(mapped_df)}, Source={len(src_df)}). Running full migration.")
        migrate_taxcodes()
        return

    if mapped_df["Payload_JSON"].isnull().any():
        logger.warning("❌ Some Map_TaxCode rows missing Payload_JSON. Running full migration.")
        migrate_taxcodes()
        return

    eligible = mapped_df[mapped_df["Porter_Status"].isin(["Ready","Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("ℹ️ No eligible TaxCode rows to post.")
        return

    pt = ProgressTimer(len(eligible))
    for _, r in eligible.iterrows():
        migrate_post_taxcode_row(r)
        pt.update()

    print("\n🏁 TaxCode posting completed.")
