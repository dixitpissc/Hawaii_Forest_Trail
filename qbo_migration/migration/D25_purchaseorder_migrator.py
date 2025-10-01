"""
Sequence : 25
Author: Dixit Prajapati
Created: 2025-09-02
Description: Handles migration of PurchaseOrder records from source system to QBO.
Production : Ready
Development : Require when necessary
Phase : 01
"""
import os
import json
import pandas as pd
import requests
from functools import lru_cache
from dotenv import load_dotenv

from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed, _should_refresh_token, refresh_qbo_token
from utils.log_timer import global_logger as logger, ProgressTimer
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

    # Map VendorRef to Target_Id
    df["Mapped_VendorRef"] = df[PURCHASEORDER_HEADER_MAPPING["VendorRef.value"]].map(
        lambda x: _map_target_id("Vendor", x) if pd.notna(x) else None
    )

    # Map APAccountRef to Target_Id
    df["Mapped_APAccountRef"] = df[PURCHASEORDER_HEADER_MAPPING["APAccountRef.value"]].map(
        lambda x: _map_target_id("Account", x) if pd.notna(x) else None
    )

    # Map CurrencyRef to Target_Id
    df["Mapped_CurrencyRef"] = df[PURCHASEORDER_HEADER_MAPPING["CurrencyRef.value"]].map(
        lambda x: _map_target_id("Currency", x) if pd.notna(x) else None
    )

    # Line overview (unchanged logic — still item-focused, optional)
    def get_target_items(po_id):
        lines = sql.fetch_table_with_params(
            f"SELECT * FROM [{SOURCE_SCHEMA}].[PurchaseOrder_Line] WHERE Parent_Id = ?", (po_id,)
        )
        target_items = []
        for _, ln in lines.iterrows():
            if ln.get("DetailType") == "ItemBasedExpenseLineDetail":
                item_source_id = ln.get(ITEM_BASED_EXPENSE_LINE_MAPPING["ItemBasedExpenseLineDetail.ItemRef.value"])
                item_id = _map_target_id("Item", item_source_id)
                if item_id:
                    target_items.append(str(item_id))
        return ";".join(target_items)

    df["Target_Item_Refs"] = df["Id"].map(get_target_items)

    # Clear existing map and insert fresh
    if sql.table_exists("Map_PurchaseOrder", MAPPING_SCHEMA):
        sql.run_query(f"DELETE FROM [{MAPPING_SCHEMA}].[Map_PurchaseOrder]")

    sql.insert_invoice_map_dataframe(df, "Map_PurchaseOrder", MAPPING_SCHEMA)
    logger.info(f"✅ Inserted {len(df)} records into {MAPPING_SCHEMA}.Map_PurchaseOrder")


def apply_duplicate_docnumber_strategy():
    """
    Handle duplicate DocNumber in Map_PurchaseOrder.
    """
    logger.info("🔍 Detecting duplicate DocNumbers in Map_PurchaseOrder...")
    sql.run_query(f"""
        IF COL_LENGTH('{MAPPING_SCHEMA}.Map_PurchaseOrder', 'Duplicate_Docnumber') IS NULL
        BEGIN
          ALTER TABLE [{MAPPING_SCHEMA}].[Map_PurchaseOrder]
          ADD Duplicate_Docnumber NVARCHAR(30);
        END
    """)

    df = sql.fetch_table("Map_PurchaseOrder", MAPPING_SCHEMA)
    df = df[df["DocNumber"].notna() & (df["DocNumber"].astype(str).str.strip().str.lower() != "null") & (df["DocNumber"].astype(str).str.strip() != "")]
    df["DocNumber"] = df["DocNumber"].astype(str)

    duplicates = df["DocNumber"].value_counts()
    duplicates = duplicates[duplicates > 1].index.tolist()

    for docnum in duplicates:
        rows = df[df["DocNumber"] == docnum]
        for i, (_, row) in enumerate(rows.iterrows(), start=1):
            sid = row["Source_Id"]
            new_docnum = f"{docnum}-{i:02d}" if len(docnum) <= 18 else f"{i:02d}{docnum[2:]}"[:21]
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_PurchaseOrder]
                SET Duplicate_Docnumber = ?
                WHERE Source_Id = ?
            """, (new_docnum, sid))
            logger.warning(f"⚠️ Duplicate DocNumber '{docnum}' updated to '{new_docnum}'")

    uniques = df[~df["DocNumber"].isin(duplicates)]
    for _, row in uniques.iterrows():
        sid = row["Source_Id"]
        sql.run_query(f"""
            UPDATE [{MAPPING_SCHEMA}].[Map_PurchaseOrder]
            SET Duplicate_Docnumber = ?
            WHERE Source_Id = ?
        """, (row["DocNumber"], sid))

    logger.info("✅ Duplicate docnumber strategy applied.")


def get_lines(purchaseorder_id):
    """
    Fetch line items for PurchaseOrder lines.
    """
    return sql.fetch_table_with_params(
        f"SELECT * FROM [{SOURCE_SCHEMA}].[PurchaseOrder_Line] WHERE Parent_Id = ?", (purchaseorder_id,)
    )


def build_payload(row, lines):
    """
    Creates QBO PurchaseOrder JSON payload with master data references.
    """
    po = {
        "DocNumber": row.get("Duplicate_Docnumber") or row.get(PURCHASEORDER_HEADER_MAPPING["DocNumber"]),
        "domain": "QBO",
        "TxnDate": row.get(PURCHASEORDER_HEADER_MAPPING.get("TxnDate")),
        "TotalAmt": float(row.get(PURCHASEORDER_HEADER_MAPPING.get("TotalAmt")) or 0.0),
        "EmailStatus": row.get(PURCHASEORDER_HEADER_MAPPING.get("EmailStatus")),
        "POEmail": {"Address": row.get(PURCHASEORDER_HEADER_MAPPING.get("POEmail.Address"))} if row.get(PURCHASEORDER_HEADER_MAPPING.get("POEmail.Address")) else None,
        "APAccountRef": {"value": str(row.get("Mapped_APAccountRef"))} if row.get("Mapped_APAccountRef") else None,
        "CurrencyRef": {"value": str(row.get("Mapped_CurrencyRef"))} if row.get("Mapped_CurrencyRef") else None,
        "VendorRef": {"value": str(row.get("Mapped_VendorRef"))} if row.get("Mapped_VendorRef") else None,
        "ShipAddr": {},
        "VendorAddr": {},
        "Line": []
    }

    # ShipAddr mapping
    if row.get(PURCHASEORDER_HEADER_MAPPING.get("ShipAddr.Line1")):
        po["ShipAddr"]["Line1"] = row.get(PURCHASEORDER_HEADER_MAPPING.get("ShipAddr.Line1"))
    if row.get(PURCHASEORDER_HEADER_MAPPING.get("ShipAddr.Line2")):
        po["ShipAddr"]["Line2"] = row.get(PURCHASEORDER_HEADER_MAPPING.get("ShipAddr.Line2"))
    if row.get(PURCHASEORDER_HEADER_MAPPING.get("ShipAddr.Line3")):
        po["ShipAddr"]["Line3"] = row.get(PURCHASEORDER_HEADER_MAPPING.get("ShipAddr.Line3"))
    if row.get(PURCHASEORDER_HEADER_MAPPING.get("ShipAddr.Line4")):
        po["ShipAddr"]["Line4"] = row.get(PURCHASEORDER_HEADER_MAPPING.get("ShipAddr.Line4"))

    # VendorAddr mapping
    if row.get(PURCHASEORDER_HEADER_MAPPING.get("VendorAddr.Line1")):
        po["VendorAddr"]["Line1"] = row.get(PURCHASEORDER_HEADER_MAPPING.get("VendorAddr.Line1"))
    if row.get(PURCHASEORDER_HEADER_MAPPING.get("VendorAddr.Line2")):
        po["VendorAddr"]["Line2"] = row.get(PURCHASEORDER_HEADER_MAPPING.get("VendorAddr.Line2"))
    if row.get(PURCHASEORDER_HEADER_MAPPING.get("VendorAddr.Line3")):
        po["VendorAddr"]["Line3"] = row.get(PURCHASEORDER_HEADER_MAPPING.get("VendorAddr.Line3"))
    if row.get(PURCHASEORDER_HEADER_MAPPING.get("VendorAddr.Line4")):
        po["VendorAddr"]["Line4"] = row.get(PURCHASEORDER_HEADER_MAPPING.get("VendorAddr.Line4"))

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
    env = os.getenv("QBO_ENVIRONMENT", "sandbox")
    base = "https://sandbox-quickbooks.api.intuit.com" if env == "sandbox" else "https://quickbooks.api.intuit.com"
    realm = os.getenv("QBO_REALM_ID")
    url = f"{base}/v3/company/{realm}/purchaseorder?minorversion={QBO_MINOR_VERSION}"
    headers = {
        "Authorization": f"Bearer {os.getenv('QBO_ACCESS_TOKEN')}",
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


def post_purchaseorder(row):
    sid = row["Source_Id"]
    if row.get("Porter_Status") == "Success":
        return
    if int(row.get("Retry_Count") or 0) >= 5:
        logger.warning(f"⚠️ Skipped PurchaseOrder {sid} - exceeded retry limit")
        return
    if not row.get("Payload_JSON"):
        logger.warning(f"⚠️ Skipped PurchaseOrder {sid} - missing Payload_JSON")
        return

    if _should_refresh_token():
        refresh_qbo_token()

    url, headers = get_qbo_auth()
    payload = _normalize_qbo_body(row["Payload_JSON"])

    try:
        resp = session.post(url, headers=headers, json=payload)
        if resp.status_code == 200:
            qid = resp.json().get("PurchaseOrder", {}).get("Id")
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_PurchaseOrder]
                SET Target_Id = ?, Porter_Status = 'Success', Failure_Reason = NULL
                WHERE Source_Id = ?
            """, (qid, sid))
            logger.info(f"✅ Posted PurchaseOrder {sid} → QBO {qid}")
        else:
            reason = resp.text[:300]
            logger.error(f"❌ Failed to post PurchaseOrder {sid}: {reason}")
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_PurchaseOrder]
                SET Porter_Status = 'Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason = ?
                WHERE Source_Id = ?
            """, (reason, sid))
    except Exception as e:
        logger.exception(f"❌ Exception posting PurchaseOrder {sid}: {e}")
        sql.run_query(f"""
            UPDATE [{MAPPING_SCHEMA}].[Map_PurchaseOrder]
            SET Porter_Status = 'Failed', Retry_Count = ISNULL(Retry_Count,0)+1, Failure_Reason = ?
            WHERE Source_Id = ?
        """, (str(e), sid))


def migrate_purchaseorders():
    print("\n🚀 Starting PurchaseOrder Migration\n" + "=" * 40)
    ensure_mapping_table()
    apply_duplicate_docnumber_strategy()
    generate_purchaseorder_payloads_in_batches()
    rows = sql.fetch_table("Map_PurchaseOrder", MAPPING_SCHEMA)
    to_post = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if to_post.empty:
        logger.info("⚠️ No PurchaseOrders ready for posting.")
        return
    pt = ProgressTimer(len(to_post))
    for _, row in to_post.iterrows():
        post_purchaseorder(row)
        pt.update()
    print("\n🏁 PurchaseOrder Migration Completed.\n")
