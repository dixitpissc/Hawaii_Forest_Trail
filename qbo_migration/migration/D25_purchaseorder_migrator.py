"""
Sequence : 25
Author: Dixit Prajapati
Created: 2026-01-02
Description: Handles migration of PurchaseOrder records from source system to QBO.
Production : Ready
Development : Require when necessary
Phase : 02 - Multi User
"""

import os
import json
import pandas as pd
import requests
from functools import lru_cache
from dotenv import load_dotenv
from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed, get_qbo_context_migration
from utils.log_timer import global_logger as logger #, ProgressTimer
from utils.apply_duplicate_docnumber import apply_duplicate_docnumber_strategy_dynamic
from config.mapping.purchaseorder_mapping import (
    PURCHASEORDER_HEADER_MAPPING,
    ITEM_BASED_EXPENSE_LINE_MAPPING,
    PROJECT_REF_MAPPING
)

load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = "porter_entities_mapping"
QBO_MINOR_VERSION = os.getenv("QBO_MINOR_VERSION", "75")

# NEW: Minimal mapping keys for AccountBasedExpenseLineDetail
ACCOUNT_BASED_EXPENSE_LINE_MAPPING = {
    "AccountBasedExpenseLineDetail.AccountRef.value": "AccountBasedExpenseLineDetail.AccountRef.value",
    "AccountBasedExpenseLineDetail.CustomerRef.value": "AccountBasedExpenseLineDetail.CustomerRef.value",
    # Optional extras if present in your line table (kept safe if missing)
    "AccountBasedExpenseLineDetail.TaxCodeRef.value": "AccountBasedExpenseLineDetail.TaxCodeRef.value",
    "AccountBasedExpenseLineDetail.BillableStatus": "AccountBasedExpenseLineDetail.BillableStatus",
}

# NEW: Cached mapper for Target_Id lookups (fast, safe)
@lru_cache(maxsize=200_000)
def _map_target_id(map_table: str, source_id):
    s = None if source_id is None else str(source_id).strip()
    if not s:
        return None
    return sql.fetch_single_value(
        f"SELECT Target_Id FROM [{MAPPING_SCHEMA}].[Map_{map_table}] WHERE Source_Id = ?",
        (s,)
    )


def ensure_mapping_table():
    """
    Prepare Map_PurchaseOrder base data and map master references.
    """
    date_from = os.getenv("PURCHASEORDER_DATE_FROM")
    date_to = os.getenv("PURCHASEORDER_DATE_TO")

    query = f"""
        SELECT * FROM [{SOURCE_SCHEMA}].[PurchaseOrder]
        WHERE (TxnDate >= ? OR ? IS NULL)
          AND (TxnDate <= ? OR ? IS NULL)
    """
    params = (date_from, date_from, date_to, date_to)
    df = sql.fetch_table_with_params(query, params)

    if df.empty:
        logger.info("⚠️ No PurchaseOrder records found for the specified date range.")
        return

    df["Source_Id"] = df["Id"]
    df["Target_Id"] = None
    df["Porter_Status"] = "Ready"
    df["Retry_Count"] = 0
    df["Failure_Reason"] = None
    for col in ["Payload_JSON", "Mapped_VendorRef", "Mapped_APAccountRef", "Mapped_CurrencyRef"]:
        if col not in df.columns:
            df[col] = None

    # Bulk mapping: fetch map tables once and build dicts to avoid per-row SQL round-trips
    # Vendor map
    if sql.table_exists("Map_Vendor", MAPPING_SCHEMA):
        vm = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Vendor]", tuple())
        vendor_dict = {str(s): t for s, t in zip(vm["Source_Id"], vm["Target_Id"]) }
    else:
        vendor_dict = {}

    df["Mapped_VendorRef"] = df[PURCHASEORDER_HEADER_MAPPING["VendorRef.value"]].map(
        lambda x: vendor_dict.get(str(x)) if pd.notna(x) else None
    )

    # Account (APAccountRef) map
    if sql.table_exists("Map_Account", MAPPING_SCHEMA):
        am = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Account]", tuple())
        account_dict = {str(s): t for s, t in zip(am["Source_Id"], am["Target_Id"]) }
    else:
        account_dict = {}

    df["Mapped_APAccountRef"] = df[PURCHASEORDER_HEADER_MAPPING["APAccountRef.value"]].map(
        lambda x: account_dict.get(str(x)) if pd.notna(x) else None
    )

    # Currency map (optional)
    if sql.table_exists("Map_Currency", MAPPING_SCHEMA):
        cm = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Currency]", tuple())
        currency_dict = {str(s): t for s, t in zip(cm["Source_Id"], cm["Target_Id"]) }
        df["Mapped_CurrencyRef"] = df[PURCHASEORDER_HEADER_MAPPING["CurrencyRef.value"]].map(
            lambda x: currency_dict.get(str(x)) if pd.notna(x) else None
        )
    else:
        logger.info(f"⚠️ Table '{MAPPING_SCHEMA}.Map_Currency' not found; skipping Currency mapping for PurchaseOrder.")
        df["Mapped_CurrencyRef"] = None

    # Build Target_Item_Refs by bulk-reading PurchaseOrder_Line and using Map_Item
    if sql.table_exists("Map_Item", MAPPING_SCHEMA):
        item_map = sql.fetch_table_with_params(f"SELECT Source_Id, Target_Id FROM [{MAPPING_SCHEMA}].[Map_Item]", tuple())
        item_dict = {str(s): t for s, t in zip(item_map["Source_Id"], item_map["Target_Id"]) }
    else:
        item_dict = {}

    po_lines = sql.fetch_table_with_params(f"SELECT * FROM [{SOURCE_SCHEMA}].[PurchaseOrder_Line]", tuple())
    if po_lines.empty:
        df["Target_Item_Refs"] = ""
    else:
        grouped = po_lines.groupby("Parent_Id")
        def _get_target_items(po_id):
            try:
                group = grouped.get_group(po_id)
            except KeyError:
                return ""
            items = []
            for _, ln in group.iterrows():
                if ln.get("DetailType") == "ItemBasedExpenseLineDetail":
                    item_source_id = ln.get(ITEM_BASED_EXPENSE_LINE_MAPPING["ItemBasedExpenseLineDetail.ItemRef.value"])
                    if pd.notna(item_source_id):
                        tgt = item_dict.get(str(item_source_id))
                        if tgt:
                            items.append(str(tgt))
            return ";".join(items)
        df["Target_Item_Refs"] = df["Id"].map(_get_target_items)

    # Clear existing map and insert fresh
    if sql.table_exists("Map_PurchaseOrder", MAPPING_SCHEMA):
        sql.run_query(f"DELETE FROM [{MAPPING_SCHEMA}].[Map_PurchaseOrder]")

    sql.insert_invoice_map_dataframe(df, "Map_PurchaseOrder", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df)} records into {MAPPING_SCHEMA}.Map_PurchaseOrder")


def apply_duplicate_docnumber_strategy():
    """Apply dynamic duplicate DocNumber handling like invoices/estimates."""
    apply_duplicate_docnumber_strategy_dynamic(
        target_table="Map_PurchaseOrder",
        schema=MAPPING_SCHEMA,
        docnumber_column="DocNumber",
        source_id_column="Source_Id",
        duplicate_column="Duplicate_Docnumber",
        check_against_tables=["Map_Estimate","Map_CreditMemo","Map_Bill","Map_Invoice","Map_VendorCredit","Map_JournalEntry","Map_Deposit","Map_Purchase","Map_Salesreceipt","Map_Refundreceipt","Map_Payment","Map_Purchase","Map_BillPayment"]
    )


def get_lines(purchaseorder_id):
    """
    Fetch line items for PurchaseOrder lines.
    """
    return sql.fetch_table_with_params(
        f"SELECT * FROM [{SOURCE_SCHEMA}].[PurchaseOrder_Line] WHERE Parent_Id = ?", (purchaseorder_id,)
    )


def _hdr_val(row, key):
    """Return header value from a row using the mapping key.

    Supports both raw source column names (e.g., 'ShipAddr.Line1') and
    sanitized map table column names (e.g., 'ShipAddr_Line1').
    """
    mv = PURCHASEORDER_HEADER_MAPPING.get(key)
    if mv is None:
        return None
    v = row.get(mv)
    if v is None:
        v = row.get(sql.sanitize_column_name(mv))
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    return v

def build_payload(row, lines):
    """
    Creates QBO PurchaseOrder JSON payload with master data references.
    """
    po = {
        "DocNumber": row.get("Duplicate_Docnumber") or _hdr_val(row, "DocNumber"),
        "domain": "QBO",
        "TxnDate": _hdr_val(row, "TxnDate"),
        "TotalAmt": float(_hdr_val(row, "TotalAmt") or 0.0),
        "EmailStatus": _hdr_val(row, "EmailStatus"),
        "POEmail": {"Address": _hdr_val(row, "POEmail.Address")} if _hdr_val(row, "POEmail.Address") else None,
        "APAccountRef": {"value": str(row.get("Mapped_APAccountRef"))} if row.get("Mapped_APAccountRef") else None,
        "CurrencyRef": {"value": str(row.get("Mapped_CurrencyRef"))} if row.get("Mapped_CurrencyRef") else None,
        "VendorRef": {"value": str(row.get("Mapped_VendorRef"))} if row.get("Mapped_VendorRef") else None,
        "POStatus": _hdr_val(row, "POStatus"),
        "PrivateNote": _hdr_val(row, "PrivateNote"),
        # "ExchangeRate": float(_hdr_val(row, "ExchangeRate") or 1.0),
        "ShipAddr": {},
        "VendorAddr": {},
        "Line": []
    }

    # ShipAddr mapping (try both original and sanitized column names)
    v = _hdr_val(row, "ShipAddr.Line1")
    if v:
        po["ShipAddr"]["Line1"] = v
    v = _hdr_val(row, "ShipAddr.Line2")
    if v:
        po["ShipAddr"]["Line2"] = v
    v = _hdr_val(row, "ShipAddr.Line3")
    if v:
        po["ShipAddr"]["Line3"] = v
    v = _hdr_val(row, "ShipAddr.Line4")
    if v:
        po["ShipAddr"]["Line4"] = v
    v = _hdr_val(row, "ShipAddr.Line5")
    if v:
        po["ShipAddr"]["Line5"] = v
    v = _hdr_val(row, "ShipAddr.City")
    if v:
        po["ShipAddr"]["City"] = v
    v = _hdr_val(row, "ShipAddr.Country")   
    if v:
        po["ShipAddr"]["Country"] = v
    v = _hdr_val(row, "ShipAddr.CountrySubDivisionCode")
    if v:
        po["ShipAddr"]["CountrySubDivisionCode"] = v
    v = _hdr_val(row, "ShipAddr.PostalCode")
    if v:
        po["ShipAddr"]["PostalCode"] = v

    # VendorAddr mapping
    v = _hdr_val(row, "VendorAddr.Line1")
    if v:
        po["VendorAddr"]["Line1"] = v
    v = _hdr_val(row, "VendorAddr.Line2")
    if v:
        po["VendorAddr"]["Line2"] = v
    v = _hdr_val(row, "VendorAddr.Line3")
    if v:
        po["VendorAddr"]["Line3"] = v
    v = _hdr_val(row, "VendorAddr.Line4")
    if v:
        po["VendorAddr"]["Line4"] = v
    v = _hdr_val(row, "VendorAddr.Line5")
    if v:
        po["VendorAddr"]["Line5"] = v
    v = _hdr_val(row, "VendorAddr.City")
    if v:
        po["VendorAddr"]["City"] = v
    v = _hdr_val(row, "VendorAddr.Country")
    if v:
        po["VendorAddr"]["Country"] = v
    v = _hdr_val(row, "VendorAddr.CountrySubDivisionCode")
    if v:
        po["VendorAddr"]["CountrySubDivisionCode"] = v
    v = _hdr_val(row, "VendorAddr.PostalCode")
    if v:
        po["VendorAddr"]["PostalCode"] = v

    # Clean empty dicts
    if not po["ShipAddr"]:
        po.pop("ShipAddr")
    if not po["VendorAddr"]:
        po.pop("VendorAddr")

    # Build lines
    for _, ln in lines.iterrows():
        detail_type = ln.get("DetailType")
        if not detail_type:
            continue

        line_obj = {
            "DetailType": detail_type,
            "Amount": float(ln.get("Amount") or 0),
            "Description": ln.get("Description"),
        }

        # Item-based lines (existing)
        if detail_type == "ItemBasedExpenseLineDetail":
            item_source_id = ln.get(ITEM_BASED_EXPENSE_LINE_MAPPING["ItemBasedExpenseLineDetail.ItemRef.value"])
            item_id = _map_target_id("Item", item_source_id)

            customer_source_id = ln.get(ITEM_BASED_EXPENSE_LINE_MAPPING.get("ItemBasedExpenseLineDetail.CustomerRef.value"))
            customer_id = _map_target_id("Customer", customer_source_id) if customer_source_id else None

            project_source_id = ln.get(PROJECT_REF_MAPPING.get("ProjectRef.value"))
            project_id = _map_target_id("Project", project_source_id) if project_source_id else None

            item_based_expense_detail = {
                "ItemRef": {"value": str(item_id)} if item_id else None,
                "Qty": float(ln.get(ITEM_BASED_EXPENSE_LINE_MAPPING.get("ItemBasedExpenseLineDetail.Qty"), 0)),
                "UnitPrice": float(ln.get(ITEM_BASED_EXPENSE_LINE_MAPPING.get("ItemBasedExpenseLineDetail.UnitPrice"), 0)),
                "TaxCodeRef": {"value": ln.get(ITEM_BASED_EXPENSE_LINE_MAPPING.get("ItemBasedExpenseLineDetail.TaxCodeRef.value"), "NON")},
                "BillableStatus": ln.get(ITEM_BASED_EXPENSE_LINE_MAPPING.get("ItemBasedExpenseLineDetail.BillableStatus")),
            }
            if customer_id:
                item_based_expense_detail["CustomerRef"] = {"value": str(customer_id)}
            if project_id:
                item_based_expense_detail["ProjectRef"] = {"value": str(project_id)}

            item_based_expense_detail = {k: v for k, v in item_based_expense_detail.items() if v is not None}
            if item_based_expense_detail:
                line_obj["ItemBasedExpenseLineDetail"] = item_based_expense_detail

        # NEW: Account-based lines
        elif detail_type == "AccountBasedExpenseLineDetail":
            # Map AccountRef (required for this detail type)
            acct_src = ln.get(ACCOUNT_BASED_EXPENSE_LINE_MAPPING["AccountBasedExpenseLineDetail.AccountRef.value"])
            acct_id = _map_target_id("Account", acct_src)

            cust_src = ln.get(ACCOUNT_BASED_EXPENSE_LINE_MAPPING.get("AccountBasedExpenseLineDetail.CustomerRef.value"))
            cust_id = _map_target_id("Customer", cust_src) if cust_src else None

            tax_src = ln.get(ACCOUNT_BASED_EXPENSE_LINE_MAPPING.get("AccountBasedExpenseLineDetail.TaxCodeRef.value"))
            billable = ln.get(ACCOUNT_BASED_EXPENSE_LINE_MAPPING.get("AccountBasedExpenseLineDetail.BillableStatus"))

            acct_detail = {
                "AccountRef": {"value": str(acct_id)} if acct_id else None,
                "BillableStatus": billable,
                "TaxCodeRef": {"value": tax_src} if tax_src else None,
            }
            if cust_id:
                acct_detail["CustomerRef"] = {"value": str(cust_id)}

            acct_detail = {k: v for k, v in acct_detail.items() if v is not None}
            if acct_detail:
                line_obj["AccountBasedExpenseLineDetail"] = acct_detail
            else:
                # If no AccountRef got mapped, drop the line (cannot post account-based without AccountRef)
                continue

        # Clean None values and append
        line_obj_clean = {k: v for k, v in line_obj.items() if v not in [None, ""]}
        if line_obj_clean:
            po["Line"].append(line_obj_clean)

    po_clean = {k: v for k, v in po.items() if v is not None}
    return po_clean


def get_qbo_auth():
    ctx = get_qbo_context_migration()
    base = ctx["BASE_URL"]
    realm = ctx["REALM_ID"]
    url = f"{base}/v3/company/{realm}/purchaseorder"
    headers = {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    return url, headers


session = requests.Session()


def generate_purchaseorder_payloads_in_batches(batch_size=500):
    logger.info("🔧 Generating JSON payloads for PurchaseOrders in batches...")
    while True:
        df = sql.fetch_table_with_params(
            f"""
            SELECT TOP {batch_size} *
            FROM [{MAPPING_SCHEMA}].[Map_PurchaseOrder]
            WHERE Porter_Status = 'Ready' AND (Payload_JSON IS NULL OR Payload_JSON = '')
            """, ()
        )
        if df.empty:
            logger.info("✅ All PurchaseOrder payloads generated.")
            break

        for _, row in df.iterrows():
            sid = row["Source_Id"]

            if not row.get("Mapped_VendorRef"):
                logger.warning(f"⚠️ Skipping PurchaseOrder {sid} - missing VendorRef mapping.")
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_PurchaseOrder]
                    SET Porter_Status = 'Failed', Failure_Reason = 'Missing Mapped_VendorRef'
                    WHERE Source_Id = ?
                """, (sid,))
                continue

            if not row.get("Mapped_APAccountRef"):
                logger.warning(f"⚠️ Skipping PurchaseOrder {sid} - missing APAccountRef mapping.")
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_PurchaseOrder]
                    SET Porter_Status = 'Failed', Failure_Reason = 'Missing Mapped_APAccountRef'
                    WHERE Source_Id = ?
                """, (sid,))
                continue

            lines = get_lines(sid)
            payload_obj = build_payload(row, lines)
            if not payload_obj.get("Line"):
                logger.warning(f"⚠️ Skipped PurchaseOrder {sid} - no valid line items.")
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_PurchaseOrder]
                    SET Porter_Status = 'Failed', Failure_Reason = 'No valid Line items'
                    WHERE Source_Id = ?
                """, (sid,))
                continue

            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_PurchaseOrder]
                SET Payload_JSON = ?, Failure_Reason = NULL
                WHERE Source_Id = ?
            """, (json.dumps(payload_obj, indent=2), sid))

        logger.info(f"✅ Generated payloads for {len(df)} PurchaseOrders this batch.")


def _normalize_qbo_body(obj_or_str):
    if isinstance(obj_or_str, str):
        body = json.loads(obj_or_str)
    else:
        body = obj_or_str
    if isinstance(body, dict) and "PurchaseOrder" in body and isinstance(body["PurchaseOrder"], dict):
        body = body["PurchaseOrder"]
    return body

# ============================ Shared Helpers ============================
# Fast JSON loader (uses orjson if available)
if "_fast_loads" not in globals():
    try:
        import orjson as _orjson
        def _fast_loads(s):
            if isinstance(s, dict):
                return s
            if isinstance(s, (bytes, bytearray)):
                return _orjson.loads(s)
            if isinstance(s, str):
                return _orjson.loads(s.encode("utf-8"))
            return s
    except Exception:
        import json as _json
        def _fast_loads(s):
            if isinstance(s, dict):
                return s
            if isinstance(s, (bytes, bytearray)):
                return _json.loads(s.decode("utf-8")) if isinstance(s, (bytes, bytearray)) else _json.loads(s)
            return s

# Convert entity endpoint → /batch endpoint
if "_derive_batch_url" not in globals():
    def _derive_batch_url(entity_url: str, entity_name: str) -> str:
        """
        entity_url: https://.../v3/company/<realm>/<Entity>?minorversion=XX
        entity_name: "Invoice" | "Bill" | "VendorCredit" | ...
        -> https://.../v3/company/<realm>/batch?minorversion=XX
        """
        qpos = entity_url.find("?")
        query = entity_url[qpos:] if qpos != -1 else ""
        path = entity_url[:qpos] if qpos != -1 else entity_url

        seg = f"/{entity_name}".lower()
        lower = path.lower()
        if lower.endswith(seg):
            path = path[: -len(seg)]
        return f"{path}/batch{query}"

# Ensure a shared HTTP session exists
try:
    session
except NameError:
    import requests
    session = requests.Session()


def executemany(query: str, param_list):
    if not param_list:
        return
    conn = sql.get_sqlserver_connection()
    try:
        cur = conn.cursor()
        try:
            cur.fast_executemany = True
        except Exception:
            pass
        cur.executemany(query, param_list)
        conn.commit()
    finally:
        conn.close()
# ========================================================================

def _post_batch_purchaseorders(
    eligible_batch,
    url,
    headers,
    timeout=40,
    post_batch_limit=10,
    max_manual_retries=1
):
    successes, failures = [], []

    if eligible_batch is None or eligible_batch.empty:
        return successes, failures

    work = []
    for _, r in eligible_batch.iterrows():
        sid = r["Source_Id"]

        if r.get("Porter_Status") == "Success":
            continue
        if int(r.get("Retry_Count") or 0) >= 5:
            continue

        pj = r.get("Payload_JSON")
        if not pj:
            failures.append(("Missing Payload_JSON", sid))
            continue

        try:
            payload = _fast_loads(pj)
        except Exception as e:
            failures.append((f"Invalid JSON: {e}", sid))
            continue

        work.append((sid, payload))

    if not work:
        return successes, failures

    batch_url = _derive_batch_url(url, "PurchaseOrder")

    idx = 0
    while idx < len(work):
        auto_refresh_token_if_needed()
        chunk = work[idx: idx + post_batch_limit]
        idx += post_batch_limit

        def _do_post(_headers):
            body = {
                "BatchItemRequest": [
                    {"bId": str(sid), "operation": "create", "PurchaseOrder": payload}
                    for sid, payload in chunk
                ]
            }
            return session.post(batch_url, headers=_headers, json=body, timeout=timeout)

        attempted_refresh = False
        for _ in range(max_manual_retries + 1):
            try:
                resp = _do_post(headers)
                sc = resp.status_code

                if sc == 200:
                    items = resp.json().get("BatchItemResponse", []) or []
                    seen = set()

                    for item in items:
                        bid = item.get("bId")
                        seen.add(bid)

                        po = item.get("PurchaseOrder")
                        if po and "Id" in po:
                            successes.append((po["Id"], bid))
                            logger.info(f"✅ PurchaseOrder {bid} → QBO {po['Id']}")
                        else:
                            fault = item.get("Fault", {})
                            err = fault.get("Error", [{}])[0]
                            reason = f"{err.get('Message','')} | {err.get('Detail','')}".strip()[:1000]
                            failures.append((reason, bid))
                            logger.error(f"❌ PurchaseOrder {bid} failed: {reason}")

                    for sid, _ in chunk:
                        if str(sid) not in seen:
                            failures.append(("No response for bId", sid))
                    break

                elif sc in (401, 403) and not attempted_refresh:
                    logger.warning("🔐 Token expired. Refreshing and retrying PurchaseOrder batch...")
                    auto_refresh_token_if_needed()
                    url, headers = get_qbo_auth()
                    batch_url = _derive_batch_url(url, "PurchaseOrder")
                    attempted_refresh = True
                    continue

                else:
                    reason = (resp.text or f"HTTP {sc}")[:1000]
                    for sid, _ in chunk:
                        failures.append((reason, sid))
                    break

            except Exception as e:
                for sid, _ in chunk:
                    failures.append((f"Batch exception: {e}", sid))
                break

    return successes, failures

def _apply_batch_updates_purchaseorder(successes, failures):
    if successes:
        executemany(
            f"""
            UPDATE [{MAPPING_SCHEMA}].[Map_PurchaseOrder]
            SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL
            WHERE Source_Id=?
            """,
            [(qid, sid) for qid, sid in successes]
        )

    if failures:
        executemany(
            f"""
            UPDATE [{MAPPING_SCHEMA}].[Map_PurchaseOrder]
            SET Porter_Status='Failed',
                Retry_Count = ISNULL(Retry_Count,0)+1,
                Failure_Reason=?
            WHERE Source_Id=?
            """,
            failures
        )

def post_purchaseorder_batch(rows, post_batch_limit=10, timeout=40):
    url, headers = get_qbo_auth()
    successes, failures = _post_batch_purchaseorders(
        rows,
        url,
        headers,
        timeout=timeout,
        post_batch_limit=post_batch_limit
    )
    _apply_batch_updates_purchaseorder(successes, failures)

def migrate_purchaseorders():
    print("\n🚀 Starting PurchaseOrder Migration (batch mode)\n" + "=" * 50)

    ensure_mapping_table()
    apply_duplicate_docnumber_strategy()
    generate_purchaseorder_payloads_in_batches(batch_size=500)

    mapped_df = sql.fetch_table("Map_PurchaseOrder", MAPPING_SCHEMA)
    eligible = mapped_df[mapped_df["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)

    if eligible.empty:
        logger.info("⚠️ No PurchaseOrders ready for posting.")
        return

    logger.info("🚚 Posting PurchaseOrders using QBO batch API...")

    select_batch_size = 100     # DB slice
    post_batch_limit  = 10      # QBO batch limit
    timeout           = 40

    for i in range(0, len(eligible), select_batch_size):
        batch = eligible.iloc[i:i + select_batch_size]
        post_purchaseorder_batch(batch, post_batch_limit, timeout)

        done = min(i + select_batch_size, len(eligible))
        logger.info(f"⏱️ {done}/{len(eligible)} processed ({done*100//len(eligible)}%)")

    print("\n🏁 PurchaseOrder Migration Completed.\n")

def resume_or_post_purchaseorders():
    """
    Enhanced resume/post logic for PurchaseOrder migration:
    1. Check Map_PurchaseOrder exists.
    2. Ensure Duplicate_Docnumber dedup if needed.
    3. Generate missing Payload_JSON only.
    4. Post only Ready/Failed via /batch.
    """
    print("\n🔁 Resuming PurchaseOrder Migration (enhanced mode)\n" + "=" * 50)

    # 1. Table existence check
    if not sql.table_exists("Map_PurchaseOrder", MAPPING_SCHEMA):
        logger.warning("❌ Map_PurchaseOrder table does not exist. Running full migration.")
        migrate_purchaseorders()
        return

    mapped_df = sql.fetch_table("Map_PurchaseOrder", MAPPING_SCHEMA)
    source_df = sql.fetch_table("PurchaseOrder", SOURCE_SCHEMA)

    # 2. Deduplicate DocNumber if needed
    if "Duplicate_Docnumber" not in mapped_df.columns or \
       mapped_df["Duplicate_Docnumber"].isnull().any() or (mapped_df["Duplicate_Docnumber"] == "").any():
        logger.info("🔍 Some Duplicate_Docnumber values missing. Running deduplication...")
        apply_duplicate_docnumber_strategy_dynamic(
            target_table="Map_PurchaseOrder",
            schema=MAPPING_SCHEMA,
            docnumber_column="DocNumber",
            source_id_column="Source_Id",
            duplicate_column="Duplicate_Docnumber",
            check_against_tables=[]
        )
        mapped_df = sql.fetch_table("Map_PurchaseOrder", MAPPING_SCHEMA)
    else:
        logger.info("✅ All Duplicate_Docnumber values present. Skipping deduplication.")

    # 3. Generate payloads only for missing
    payload_missing = mapped_df["Payload_JSON"].isnull() | (mapped_df["Payload_JSON"] == "")
    missing_count = int(payload_missing.sum())
    if missing_count > 0:
        logger.info(f"🔧 {missing_count} purchaseorders missing Payload_JSON. Generating for those...")
        generate_purchaseorder_payloads_in_batches(batch_size=500)
        mapped_df = sql.fetch_table("Map_PurchaseOrder", MAPPING_SCHEMA)
    else:
        logger.info("✅ All PurchaseOrders have Payload_JSON.")

    # 4. Eligible only
    eligible_rows = mapped_df[mapped_df["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible_rows.empty:
        logger.info("⚠️ No eligible PurchaseOrders to post.")
        return

    logger.info("✅ All validations passed. Posting eligible PurchaseOrders...")
    select_batch_size = 100
    post_batch_limit  = 10
    timeout           = 40

    for i in range(0, len(eligible_rows), select_batch_size):
        batch = eligible_rows.iloc[i:i+select_batch_size]
        post_purchaseorder_batch(batch, post_batch_limit=post_batch_limit, timeout=timeout)
        done = min(i + select_batch_size, len(eligible_rows))
        logger.info(f"⏱️ {done}/{len(eligible_rows)} processed ({done*100//len(eligible_rows)}%)")

    print("\n🏁 PurchaseOrder posting completed.")


def smrt_purchaseorder_migration():
    """
    Smart migration entrypoint:
    - If Map_PurchaseOrder table exists and row count matches PurchaseOrder table, resume/post PurchaseOrders.
    - If table missing or row count mismatch, perform full migration.
    """
    logger.info("🔎 Running smrt_purchaseorder_migration...")
    if sql.table_exists("Map_PurchaseOrder", MAPPING_SCHEMA):
        mapped_df = sql.fetch_table("Map_PurchaseOrder", MAPPING_SCHEMA)
        source_df = sql.fetch_table("PurchaseOrder", SOURCE_SCHEMA)
        if len(mapped_df) == len(source_df):
            logger.info("✅ Table exists and row count matches. Resuming/posting PurchaseOrders.")
            resume_or_post_purchaseorders()
            return
        else:
            logger.warning(f"❌ Row count mismatch: Map_PurchaseOrder={len(mapped_df)}, PurchaseOrder={len(source_df)}. Running full migration.")
    else:
        logger.warning("❌ Map_PurchaseOrder table does not exist. Running full migration.")
    migrate_purchaseorders()
