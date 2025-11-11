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
        logging.FileHandler("quickbooks_delete_purchase_parallel.log", encoding="utf-8"),
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


def fetch_purchase_by_id(purchase_id: str):
    """
    Fetch a Purchase by Id to get SyncToken (required for delete).

    Returns:
        (purchase_id, purchase_dict_or_None)
    """
    headers = get_auth_headers()
    query_url = f"{url()}/purchase/{purchase_id}"

    try:
        resp = requests.get(query_url, headers=headers)
    except requests.RequestException as e:
        logger.error(f"⚠️ Error fetching Purchase {purchase_id}: {e}")
        return purchase_id, None

    if resp.status_code == 200:
        try:
            data = resp.json()
        except Exception as e:
            logger.error(f"⚠️ JSON parse error for Purchase {purchase_id}: {e} - {resp.text}")
            return purchase_id, None

        purchase = data.get("Purchase")
        if not purchase:
            logger.warning(f"⚠️ No 'Purchase' object for ID {purchase_id}: {data}")
            return purchase_id, None

        return purchase_id, purchase

    elif resp.status_code == 404:
        logger.warning(f"⚠️ Purchase ID {purchase_id} not found (404).")
        return purchase_id, None

    else:
        logger.error(f"❌ Failed to fetch Purchase {purchase_id}: {resp.status_code} - {resp.text}")
        return purchase_id, None


def delete_purchase(purchase_id: str, sync_token: str) -> bool:
    """
    Delete a specific Purchase using its Id and SyncToken.

    Returns:
        True if deleted, False otherwise.
    """
    headers = get_delete_headers()
    delete_url = f"{url()}/purchase?operation=delete"
    payload = {
        "Id": purchase_id,
        "SyncToken": sync_token,
    }

    try:
        resp = requests.post(delete_url, headers=headers, json=payload)
    except requests.RequestException as e:
        logger.error(f"⚠️ Error deleting Purchase ID {purchase_id}: {e}")
        return False

    if resp.status_code == 200:
        logger.info(f"✅ Deleted Purchase ID {purchase_id}")
        return True

    # Try to log Fault nicely if present
    try:
        data = resp.json()
    except Exception:
        data = resp.text

    logger.warning(
        f"❌ Failed to delete Purchase ID {purchase_id}: "
        f"{resp.status_code} - {data}"
    )
    return False


def process_single_purchase(purchase_id: str):
    """
    Worker function: fetch Purchase (for SyncToken) and delete it.

    Returns:
        {
          "id": purchase_id,
          "deleted": True/False,
          "reason": str | None
        }
    """
    purchase_id_str = str(purchase_id).strip()
    if not purchase_id_str:
        return {"id": purchase_id, "deleted": False, "reason": "Empty ID"}

    pid, purchase = fetch_purchase_by_id(purchase_id_str)
    if not purchase:
        return {"id": pid, "deleted": False, "reason": "Not found or fetch failed"}

    sync_token = str(purchase.get("SyncToken", "0"))
    success = delete_purchase(purchase_id_str, sync_token)
    return {
        "id": purchase_id_str,
        "deleted": bool(success),
        "reason": None if success else "Delete failed",
    }


def process_purchases_parallel(purchase_ids, max_workers: int = 8):
    """
    Process Purchase deletions in parallel using a thread pool.

    Args:
        purchase_ids: iterable of Purchase IDs
        max_workers: number of parallel workers (8 by default)
    """
    purchase_ids = [str(pid).strip() for pid in purchase_ids if str(pid).strip()]
    total = len(purchase_ids)
    if total == 0:
        logger.warning("⚠️ No valid Purchase IDs provided to delete.")
        return

    logger.info(f"🚀 Starting parallel deletion for {total} Purchases "
                f"with max_workers={max_workers}...")

    deleted_count = 0
    skipped_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_id = {
            executor.submit(process_single_purchase, pid): pid
            for pid in purchase_ids
        }

        for future in tqdm(as_completed(future_to_id), total=total, desc="Deleting Purchases", unit="purchase"):
            result = None
            try:
                result = future.result()
            except Exception as e:
                # This is a defensive catch; individual functions already log errors
                pid = future_to_id[future]
                logger.error(f"❌ Unhandled error while processing Purchase {pid}: {e}")
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
                    logger.info(f"ℹ️ Purchase {result['id']} skipped: {reason}")

    logger.info(f"🏁 Deletion complete. Deleted: {deleted_count}, Skipped/Failed: {skipped_count}")


if __name__ == "__main__":
    # Replace this with actual Purchase IDs you want to delete
    purchase_ids_to_delete = [

    ]

    logger.info(f"Found {len(purchase_ids_to_delete)} Purchases to delete.")
    process_purchases_parallel(purchase_ids_to_delete, max_workers=8)
