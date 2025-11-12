import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

def url():
    env = os.getenv("QBO_ENVIRONMENT")
    realm_id = os.getenv("REALM_ID")
    if env == "sandbox":
        return f"https://sandbox-quickbooks.api.intuit.com/v3/company/{realm_id}"
    elif env == "production":
        return f"https://quickbooks.api.intuit.com/v3/company/{realm_id}"
    else:
        raise ValueError(f"Unknown QBO_ENVIRONMENT: {env!r}")

import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ---------------------------------------------------------------------
# ENV + logging
# ---------------------------------------------------------------------
ACCESS_TOKEN = os.getenv("QBO_ACCESS_TOKEN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
    handlers=[
        logging.FileHandler("quickbooks_delete_invoice_parallel.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_auth_headers():
    if not ACCESS_TOKEN:
        raise RuntimeError("QBO_ACCESS_TOKEN is not set in environment.")
    return {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Accept": "application/json",
    }


def get_delete_headers():
    h = get_auth_headers()
    h["Content-Type"] = "application/json"
    return h


def fetch_invoice_by_id(invoice_id: str):
    """
    Fetch an Invoice by Id to get SyncToken (required for delete).

    Returns:
        (invoice_id, invoice_dict_or_None)
    """
    headers = get_auth_headers()
    query_url = f"{url()}/invoice/{invoice_id}"

    try:
        resp = requests.get(query_url, headers=headers)
    except requests.RequestException as e:
        logger.error(f"⚠️ Error fetching Invoice {invoice_id}: {e}")
        return invoice_id, None

    if resp.status_code == 200:
        try:
            data = resp.json()
        except Exception as e:
            logger.error(f"⚠️ JSON parse error for Invoice {invoice_id}: {e} - {resp.text}")
            return invoice_id, None

        invoice = data.get("Invoice")
        if not invoice:
            logger.warning(f"⚠️ No 'Invoice' object for ID {invoice_id}: {data}")
            return invoice_id, None

        return invoice_id, invoice

    elif resp.status_code == 404:
        logger.warning(f"⚠️ Invoice ID {invoice_id} not found (404).")
        return invoice_id, None

    else:
        logger.error(f"❌ Failed to fetch Invoice {invoice_id}: {resp.status_code} - {resp.text}")
        return invoice_id, None


def delete_invoice(invoice_id: str, sync_token: str) -> bool:
    """
    Delete a specific Invoice using its Id and SyncToken.

    Returns:
        True if deleted, False otherwise.
    """
    headers = get_delete_headers()
    delete_url = f"{url()}/invoice?operation=delete"
    payload = {
        "Id": invoice_id,
        "SyncToken": sync_token,
    }

    try:
        resp = requests.post(delete_url, headers=headers, json=payload)
    except requests.RequestException as e:
        logger.error(f"⚠️ Error deleting Invoice ID {invoice_id}: {e}")
        return False

    if resp.status_code == 200:
        logger.info(f"✅ Deleted Invoice ID {invoice_id}")
        return True

    try:
        data = resp.json()
    except Exception:
        data = resp.text

    logger.warning(
        f"❌ Failed to delete Invoice ID {invoice_id}: "
        f"{resp.status_code} - {data}"
    )
    return False


def process_single_invoice(invoice_id: str):
    """
    Worker function: fetch Invoice (for SyncToken) and delete it.

    Returns:
        {
          "id": invoice_id,
          "deleted": True/False,
          "reason": str | None
        }
    """
    invoice_id_str = str(invoice_id).strip()
    if not invoice_id_str:
        return {"id": invoice_id, "deleted": False, "reason": "Empty ID"}

    iid, invoice = fetch_invoice_by_id(invoice_id_str)
    if not invoice:
        return {"id": iid, "deleted": False, "reason": "Not found or fetch failed"}

    sync_token = str(invoice.get("SyncToken", "0"))
    success = delete_invoice(invoice_id_str, sync_token)
    return {
        "id": invoice_id_str,
        "deleted": bool(success),
        "reason": None if success else "Delete failed",
    }


def process_invoices_parallel(invoice_ids, max_workers: int = 8):
    """
    Process Invoice deletions in parallel using a thread pool.

    Args:
        invoice_ids: iterable of Invoice IDs
        max_workers: number of parallel workers (8 by default)
    """
    invoice_ids = [str(iid).strip() for iid in invoice_ids if str(iid).strip()]
    total = len(invoice_ids)
    if total == 0:
        logger.warning("⚠️ No valid Invoice IDs provided to delete.")
        return

    logger.info(f"🚀 Starting parallel deletion for {total} Invoices "
                f"with max_workers={max_workers}...")

    deleted_count = 0
    skipped_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_id = {
            executor.submit(process_single_invoice, iid): iid
            for iid in invoice_ids
        }

        for future in tqdm(as_completed(future_to_id), total=total, desc="Deleting Invoices", unit="invoice"):
            result = None
            try:
                result = future.result()
            except Exception as e:
                iid = future_to_id[future]
                logger.error(f"❌ Unhandled error while processing Invoice {iid}: {e}")
                skipped_count += 1
                continue

            if not result:
                skipped_count += 1
                continue

            if result.get("deleted"):
                deleted_count += 1
            else:
                skipped_count += 1
                reason = result.get("reason")
                if reason:
                    logger.info(f"ℹ️ Invoice {result['id']} skipped: {reason}")

    logger.info(f"🏁 Deletion complete. Deleted: {deleted_count}, Skipped/Failed: {skipped_count}")


if __name__ == "__main__":
    # Replace this with actual Invoice IDs you want to delete
    invoice_ids_to_delete = [
        # Example:
        # "123",
        # "456",
    ]

    logger.info(f"Found {len(invoice_ids_to_delete)} Invoices to delete.")
    process_invoices_parallel(invoice_ids_to_delete, max_workers=8)
