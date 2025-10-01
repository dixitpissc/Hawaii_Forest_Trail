import os
import math
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from utils.token_refresher import auto_refresh_token_if_needed, refresh_qbo_token, _should_refresh_token
from utils.log_timer import global_logger as logger, ProgressTimer

# ── Config ─────────────────────────────────────────────
load_dotenv()
auto_refresh_token_if_needed()

realm_id = os.getenv("QBO_REALM_ID")
environment = os.getenv("QBO_ENVIRONMENT", "sandbox").lower()
base_url = "https://sandbox-quickbooks.api.intuit.com" if environment == "sandbox" else "https://quickbooks.api.intuit.com"

# Tune these safely. QBO batch limit is 25 ops/request.
QUERY_PAGE_SIZE = 1000      # max per SELECT page
BATCH_SIZE = 25             # QBO batch endpoint max
MAX_WORKERS = 6             # concurrent batch requests
MAX_RETRIES = 5             # per batch request
BACKOFF_BASE = 0.8          # exponential backoff base seconds

# Fill with the entities you want to delete
QBO_ENTITIES = [
    # Transaction Entities
    # "Invoice",
    # "Bill",
    # "CreditMemo",
    # "Payment",
    # "JournalEntry",
    # "Purchase",
    # "SalesReceipt",
    # "VendorCredit",
    # "RefundReceipt",
    # "TimeActivity",
    # "Transfer",
    # "Deposit",
    # "Estimate",
    # "PurchaseOrder",
    # # "StatementCharge",  # rarely used
    "BillPayment",
    # "VendorCredit",
    # ## Master Entities
    # "Customer",
    # "Vendor",
    # "Employee",
    # "Item",
    # "Account",
    # "Class",
    # "Department",         # maps to Location
    # "PaymentMethod",
    # "Term",
    # "TaxCode",
    # "TaxRate"
]


session = requests.Session()

def _headers_json():
    return {
        "Authorization": f"Bearer {os.getenv('QBO_ACCESS_TOKEN')}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def _headers_query():
    # QBO SQL parser behaves best with application/text
    return {
        "Authorization": f"Bearer {os.getenv('QBO_ACCESS_TOKEN')}",
        "Accept": "application/json",
        "Content-Type": "application/text",
    }

def _run_query(query: str):
    url = f"{base_url}/v3/company/{realm_id}/query"
    if _should_refresh_token():
        refresh_qbo_token()
    resp = session.post(url, headers=_headers_query(), data=query)
    # quick token retry
    if resp.status_code == 401:
        refresh_qbo_token()
        resp = session.post(url, headers=_headers_query(), data=query)
    return resp

# ── Query helpers ──────────────────────────────────────
def fetch_ids_and_tokens_all(entity: str, page_size: int = QUERY_PAGE_SIZE):
    """
    Paginates Id/SyncToken with a STARTPOSITION query.
    If QBO returns a 400 parser error, falls back to non-paged MAXRESULTS.
    """
    out, start_pos = [], 1
    while True:
        q = f"SELECT Id, SyncToken FROM {entity} STARTPOSITION {start_pos} MAXRESULTS {page_size}"
        resp = _run_query(q)

        # Fallback: some entities (or certain realms) 400 on STARTPOSITION
        if resp.status_code == 400:
            logger.warning(f"🔁 Falling back to simple query for {entity} (no STARTPOSITION)")
            q_simple = f"SELECT Id, SyncToken FROM {entity} MAXRESULTS {page_size}"
            resp = _run_query(q_simple)

        if resp.status_code != 200:
            logger.error(f"❌ Query failed for {entity} @ {start_pos}: {resp.status_code} - {resp.text}")
            break

        data = resp.json()
        rows = data.get("QueryResponse", {}).get(entity, [])
        if not rows:
            break

        out.extend((r["Id"], r["SyncToken"]) for r in rows)
        if len(rows) < page_size:
            break
        start_pos += page_size

    return out

# ── Batch delete (25 per request) ──────────────────────
def _build_batch_payload(entity: str, chunk):
    # QBO batch spec: each BatchItemRequest has the entity-object key
    # Example: {"operation":"delete", "BillPayment":{"Id":"123","SyncToken":"0"}}
    ops = []
    for idx, (entity_id, sync_token) in enumerate(chunk):
        ops.append({
            "bId": f"{entity}-{entity_id}-{idx}",
            "operation": "delete",
            entity: {  # <- entity name here, exact casing
                "Id": str(entity_id),
                "SyncToken": str(sync_token),
            }
        })
    return {"BatchItemRequest": ops}


def _post_batch_with_retry(entity: str, chunk):
    url = f"{base_url}/v3/company/{realm_id}/batch"
    for attempt in range(1, MAX_RETRIES + 1):
        if _should_refresh_token():
            refresh_qbo_token()

        payload = _build_batch_payload(entity, chunk)
        resp = session.post(url, headers=_headers_json(), json=payload)

        if resp.status_code == 401 and attempt < MAX_RETRIES:
            logger.warning(f"🔐 401 on batch; refreshing and retrying (attempt {attempt})...")
            refresh_qbo_token()
            continue

        if resp.status_code == 200:
            data = resp.json()
            items = data.get("BatchItemResponse", []) or []
            # Success = no Fault key; Failure = has Fault/Error
            ok = sum(1 for r in items if "Fault" not in r)
            fail = len(items) - ok
            if fail:
                logger.warning(f"⚠️ Batch for {entity}: {ok} ok, {fail} failed; sample={items[0:1]}")
            return ok, fail

        if resp.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
            sleep_s = BACKOFF_BASE * (2 ** (attempt - 1)) + (0.05 * attempt)
            logger.warning(f"⏳ {resp.status_code} on batch; retrying in {sleep_s:.2f}s (attempt {attempt})...")
            time.sleep(sleep_s)
            continue

        logger.error(f"❌ Batch request failed: {resp.status_code} - {resp.text}")
        return 0, len(chunk)

    logger.error("❌ Exhausted retries for a batch.")
    return 0, len(chunk)

def delete_all_for_entity(entity: str):
    """
    Collects all Id/SyncToken pairs, then deletes using:
      - concurrent batch requests (25 ops/request)
      - retries for throttling/errors
    """
    logger.info(f"📥 Scanning {entity}...")
    pairs = fetch_ids_and_tokens_all(entity)
    total = len(pairs)
    if total == 0:
        logger.info(f"✅ No records found for {entity}.")
        return

    logger.info(f"🧹 Deleting {total} {entity} record(s) via batched concurrency...")
    timer = ProgressTimer(total)

    # Chunk into batches of 25
    chunks = [pairs[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
    success = 0
    failed = 0

    # Submit batches concurrently
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_post_batch_with_retry, entity, chunk): len(chunk) for chunk in chunks}
        for fut in as_completed(futures):
            ok, fail = fut.result()
            success += ok
            failed += fail
            # Progress per batch
            timer.increment_by(ok + fail)

    logger.info(f"✅ Finished {entity}: deleted={success}, failed={failed}")

def main():
    logger.info("🚀 Starting QBO bulk delete (batched + concurrent)\n" + "=" * 60)
    for entity in QBO_ENTITIES:
        logger.info(f"\n📂 Processing: {entity}")
        delete_all_for_entity(entity)
    logger.info("\n🏁 All deletions complete.")
