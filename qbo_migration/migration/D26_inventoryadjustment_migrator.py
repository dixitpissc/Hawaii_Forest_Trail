"""
Sequence : 25
Author: Dixit Prajapati
Created: 2025-09-02
Description: Handles migration of InventoryAdjustment records from source system to QBO.
Production : Ready
Development : Require when necessary
Phase : 01
"""

import os, json, requests
import pandas as pd
from functools import lru_cache
from dotenv import load_dotenv

from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed, _should_refresh_token, refresh_qbo_token
from utils.log_timer import global_logger as logger, ProgressTimer
from config.mapping.inventoryadjustment_mapping import (
    INVENTORYADJUSTMENT_HEADER_MAPPING,
    ITEM_ADJUSTMENT_LINE_MAPPING,
)

# ──────────────────────────────────────────────────────────────
# Bootstrap
# ──────────────────────────────────────────────────────────────
load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA  = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = "porter_entities_mapping"
QBO_MINOR_VERSION = os.getenv("QBO_MINOR_VERSION", "75")

session = requests.Session()

# ──────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────
@lru_cache(maxsize=200_000)
def _map_target_id(map_table: str, source_id):
    s = None if source_id is None else str(source_id).strip()
    if not s:
        return None
    return sql.fetch_single_value(
        f"SELECT Target_Id FROM [{MAPPING_SCHEMA}].[Map_{map_table}] WHERE Source_Id = ?",
        (s,)
    )

def _float_safe(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return float(default)

def _get_qbo_endpoint():
    env = os.getenv("QBO_ENVIRONMENT", "sandbox")
    base = "https://sandbox-quickbooks.api.intuit.com" if env == "sandbox" else "https://quickbooks.api.intuit.com"
    realm = os.getenv("QBO_REALM_ID")
    url = f"{base}/v3/company/{realm}/inventoryadjustment?minorversion={QBO_MINOR_VERSION}"
    headers = {
        "Authorization": f"Bearer {os.getenv('QBO_ACCESS_TOKEN')}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    return url, headers

# ──────────────────────────────────────────────────────────────
# 1) Ensure mapping table
# ──────────────────────────────────────────────────────────────
def ensure_mapping_table():
    """
    Replicate InventoryAdjustment → porter_entities_mapping.Map_InventoryAdjustment
    and enrich header-level mappings:
      - Mapped_AdjustAccountRef (from Map_Account)
      - Mapped_DepartmentRef   (from Map_Department)
    """
    logger.info("🔧 Ensuring Map_InventoryAdjustment from SOURCE.InventoryAdjustment ...")

    # Optional date filter support (same style as your other migrators)
    date_from = os.getenv("INVENTORYADJUSTMENT_DATE_FROM")
    date_to   = os.getenv("INVENTORYADJUSTMENT_DATE_TO")

    query = f"""
        SELECT *
        FROM [{SOURCE_SCHEMA}].[InventoryAdjustment]
        WHERE (TxnDate >= ? OR ? IS NULL)
          AND (TxnDate <= ? OR ? IS NULL)
    """
    df = sql.fetch_table_with_params(query, (date_from, date_from, date_to, date_to))

    if df.empty:
        logger.info("⚠️ No InventoryAdjustment records found for the specified date range.")
        return

    # Standard porter columns
    df["Source_Id"]     = df["Id"]
    df["Target_Id"]     = None
    df["Porter_Status"] = "Ready"
    df["Retry_Count"]   = 0
    df["Failure_Reason"]= None

    # Placeholder columns (created if absent)
    for col in ["Payload_JSON", "Mapped_AdjustAccountRef", "Mapped_DepartmentRef"]:
        if col not in df.columns:
            df[col] = None

    # Enrich mappings
    df["Mapped_AdjustAccountRef"] = df[INVENTORYADJUSTMENT_HEADER_MAPPING["AdjustAccountRef.value"]].map(
        lambda x: _map_target_id("Account", x) if pd.notna(x) else None
    )
    df["Mapped_DepartmentRef"] = df.get(INVENTORYADJUSTMENT_HEADER_MAPPING["DepartmentRef.value"], pd.Series([None]*len(df))).map(
        lambda x: _map_target_id("Department", x) if pd.notna(x) else None
    )

    # Reset map table
    if sql.table_exists("Map_InventoryAdjustment", MAPPING_SCHEMA):
        sql.run_query(f"DELETE FROM [{MAPPING_SCHEMA}].[Map_InventoryAdjustment]")

    sql.insert_invoice_map_dataframe(df, "Map_InventoryAdjustment", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df)} rows into {MAPPING_SCHEMA}.Map_InventoryAdjustment")

# ──────────────────────────────────────────────────────────────
# 2) Duplicate DocNumber strategy (optional but consistent)
# ──────────────────────────────────────────────────────────────
def apply_duplicate_docnumber_strategy():
    logger.info("🔍 Detecting duplicate DocNumbers for InventoryAdjustment...")
    sql.run_query(f"""
        IF COL_LENGTH('{MAPPING_SCHEMA}.Map_InventoryAdjustment', 'Duplicate_Docnumber') IS NULL
        BEGIN
          ALTER TABLE [{MAPPING_SCHEMA}].[Map_InventoryAdjustment]
          ADD Duplicate_Docnumber NVARCHAR(30);
        END
    """)

    df = sql.fetch_table("Map_InventoryAdjustment", MAPPING_SCHEMA)
    if df.empty or "DocNumber" not in df.columns:
        logger.info("ℹ️ No DocNumber column or no data; skipping duplicate strategy.")
        return

    df = df[df["DocNumber"].notna() & (df["DocNumber"].astype(str).str.strip().str.lower() != "null") & (df["DocNumber"].astype(str).str.strip() != "")]
    if df.empty:
        logger.info("ℹ️ No non-empty DocNumber values to process.")
        return

    df["DocNumber"] = df["DocNumber"].astype(str)
    duplicates = df["DocNumber"].value_counts()
    dup_keys = duplicates[duplicates > 1].index.tolist()

    for docnum in dup_keys:
        rows = df[df["DocNumber"] == docnum]
        for i, (_, row) in enumerate(rows.iterrows(), start=1):
            sid = row["Source_Id"]
            new_docnum = f"{docnum}-{i:02d}" if len(docnum) <= 18 else f"{i:02d}{docnum[2:]}"[:21]
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_InventoryAdjustment]
                SET Duplicate_Docnumber = ?
                WHERE Source_Id = ?
            """, (new_docnum, sid))
            logger.warning(f"⚠️ Duplicate DocNumber '{docnum}' → '{new_docnum}'")

    uniques = df[~df["DocNumber"].isin(dup_keys)]
    for _, row in uniques.iterrows():
        sql.run_query(f"""
            UPDATE [{MAPPING_SCHEMA}].[Map_InventoryAdjustment]
            SET Duplicate_Docnumber = ?
            WHERE Source_Id = ?
        """, (row["DocNumber"], row["Source_Id"]))

    logger.info("✅ Duplicate docnumber strategy applied.")

# ──────────────────────────────────────────────────────────────
# 3) Lines fetcher
# ──────────────────────────────────────────────────────────────
def get_lines(parent_id):
    return sql.fetch_table_with_params(
        f"SELECT * FROM [{SOURCE_SCHEMA}].[InventoryAdjustment_Line] WHERE Parent_Id = ?",
        (parent_id,)
    )

# ──────────────────────────────────────────────────────────────
# 4) Payload builder
# ──────────────────────────────────────────────────────────────
def build_payload(row, lines_df):
    """
    Build QBO InventoryAdjustment JSON.
    Required: AdjustAccountRef + at least one line with ItemAdjustmentLineDetail{ ItemRef, QtyDiff }.
    DepartmentRef is optional (header).
    """
    payload = {
        "DocNumber": row.get("Duplicate_Docnumber") or row.get(INVENTORYADJUSTMENT_HEADER_MAPPING["DocNumber"]),
        "TxnDate": row.get(INVENTORYADJUSTMENT_HEADER_MAPPING["TxnDate"]),
        "domain": "QBO",
        "AdjustAccountRef": {"value": str(row.get("Mapped_AdjustAccountRef"))} if row.get("Mapped_AdjustAccountRef") else None,
        "DepartmentRef": {"value": str(row.get("Mapped_DepartmentRef"))} if row.get("Mapped_DepartmentRef") else None,
        "Line": [],
    }

    # Build line array
    for _, ln in lines_df.iterrows():
        detail_type = str(ln.get(ITEM_ADJUSTMENT_LINE_MAPPING["DetailType"]) or "").strip()
        if detail_type != "ItemAdjustmentLineDetail":
            # Only ItemAdjustmentLineDetail is valid for InventoryAdjustment
            continue

        item_src = ln.get(ITEM_ADJUSTMENT_LINE_MAPPING["ItemAdjustmentLineDetail.ItemRef.value"])
        item_id  = _map_target_id("Item", item_src)
        qtydiff  = ln.get(ITEM_ADJUSTMENT_LINE_MAPPING["ItemAdjustmentLineDetail.QtyDiff"])

        # Require item and QtyDiff for posting
        if not item_id:
            continue
        if pd.isna(qtydiff) or str(qtydiff).strip() == "":
            continue

        line_obj = {
            "DetailType": "ItemAdjustmentLineDetail",
            "ItemAdjustmentLineDetail": {
                "ItemRef": {"value": str(item_id)},
                "QtyDiff": _float_safe(qtydiff, 0.0),
            }
        }
        payload["Line"].append(line_obj)

    # Prune Nones
    payload = {k: v for k, v in payload.items() if v is not None}
    return payload

# ──────────────────────────────────────────────────────────────
# 5) Batch payload generator
# ──────────────────────────────────────────────────────────────
def generate_payloads_in_batches(batch_size=500):
    logger.info("🧩 Generating InventoryAdjustment payloads...")
    while True:
        df = sql.fetch_table_with_params(
            f"""
            SELECT TOP {batch_size} *
            FROM [{MAPPING_SCHEMA}].[Map_InventoryAdjustment]
            WHERE Porter_Status = 'Ready'
              AND (Payload_JSON IS NULL OR Payload_JSON = '')
            """, ()
        )
        if df.empty:
            logger.info("✅ All InventoryAdjustment payloads generated.")
            break

        for _, row in df.iterrows():
            sid = row["Source_Id"]

            # Validate header mapping requirements
            if not row.get("Mapped_AdjustAccountRef"):
                logger.warning(f"⚠️ Skipping {sid} — Missing AdjustAccountRef mapping.")
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_InventoryAdjustment]
                    SET Porter_Status='Failed', Failure_Reason='Missing Mapped_AdjustAccountRef'
                    WHERE Source_Id=?
                """, (sid,))
                continue

            lines = get_lines(sid)
            payload_obj = build_payload(row, lines)
            if not payload_obj.get("Line"):
                logger.warning(f"⚠️ Skipping {sid} — No valid ItemAdjustmentLineDetail lines.")
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_InventoryAdjustment]
                    SET Porter_Status='Failed', Failure_Reason='No valid lines'
                    WHERE Source_Id=?
                """, (sid,))
                continue

            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_InventoryAdjustment]
                SET Payload_JSON=?, Failure_Reason=NULL
                WHERE Source_Id=?
            """, (json.dumps(payload_obj, indent=2), sid))

        logger.info(f"✅ Generated payloads for {len(df)} InventoryAdjustments this batch.")

# ──────────────────────────────────────────────────────────────
# 6) Poster
# ──────────────────────────────────────────────────────────────
def _normalize_qbo_body(obj_or_str):
    return json.loads(obj_or_str) if isinstance(obj_or_str, str) else obj_or_str

def post_inventoryadjustment(row):
    sid = row["Source_Id"]
    if row.get("Porter_Status") == "Success":
        return
    if int(row.get("Retry_Count") or 0) >= 5:
        logger.warning(f"⚠️ Skipped {sid} — exceeded retry limit.")
        return
    if not row.get("Payload_JSON"):
        logger.warning(f"⚠️ Skipped {sid} — missing Payload_JSON.")
        return

    if _should_refresh_token():
        refresh_qbo_token()

    url, headers = _get_qbo_endpoint()
    payload = _normalize_qbo_body(row["Payload_JSON"])

    try:
        resp = session.post(url, headers=headers, json=payload)
        if resp.status_code == 200:
            qid = resp.json().get("InventoryAdjustment", {}).get("Id")
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_InventoryAdjustment]
                SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL
                WHERE Source_Id=?
            """, (qid, sid))
            logger.info(f"✅ Posted InventoryAdjustment {sid} → QBO {qid}")
        else:
            reason = resp.text[:500]
            logger.error(f"❌ Failed posting {sid}: {reason}")
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_InventoryAdjustment]
                SET Porter_Status='Failed', Retry_Count=ISNULL(Retry_Count,0)+1, Failure_Reason=?
                WHERE Source_Id=?
            """, (reason, sid))
    except Exception as e:
        logger.exception(f"❌ Exception posting {sid}: {e}")
        sql.run_query(f"""
            UPDATE [{MAPPING_SCHEMA}].[Map_InventoryAdjustment]
            SET Porter_Status='Failed', Retry_Count=ISNULL(Retry_Count,0)+1, Failure_Reason=?
            WHERE Source_Id=?
        """, (str(e), sid))

# ──────────────────────────────────────────────────────────────
# 7) Orchestrators
# ──────────────────────────────────────────────────────────────
def migrate_inventoryadjustments():
    print("\n🚀 Starting InventoryAdjustment Migration\n" + "=" * 46)
    ensure_mapping_table()
    apply_duplicate_docnumber_strategy()  # optional but consistent
    generate_payloads_in_batches()
    rows = sql.fetch_table("Map_InventoryAdjustment", MAPPING_SCHEMA)
    to_post = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if to_post.empty:
        logger.info("ℹ️ No InventoryAdjustments ready for posting.")
        print("\n🏁 InventoryAdjustment Migration Completed (no posts).\n")
        return

    pt = ProgressTimer(len(to_post))
    for _, row in to_post.iterrows():
        post_inventoryadjustment(row)
        pt.update()
    print("\n🏁 InventoryAdjustment Migration Completed.\n")

def resume_or_post_inventoryadjustments():
    """
    Idempotent entry that ensures mapping exists, payloads exist,
    then posts anything Ready/Failed.
    """
    print("\n🔁 resume_or_post_inventoryadjustments start")
    if not sql.table_exists("Map_InventoryAdjustment", MAPPING_SCHEMA):
        logger.info("📂 Map_InventoryAdjustment missing — building fresh.")
        ensure_mapping_table()
        apply_duplicate_docnumber_strategy()
        generate_payloads_in_batches()
    else:
        # Ensure any 'Ready' rows without payloads are generated
        generate_payloads_in_batches()

    rows = sql.fetch_table("Map_InventoryAdjustment", MAPPING_SCHEMA)
    to_post = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if to_post.empty:
        logger.info("ℹ️ Nothing to post.")
        return

    pt = ProgressTimer(len(to_post))
    for _, row in to_post.iterrows():
        post_inventoryadjustment(row)
        pt.update()
    print("🔁 resume_or_post_inventoryadjustments end\n")
