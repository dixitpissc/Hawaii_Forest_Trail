import os
import logging
import requests
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# === Load environment variables ===
load_dotenv()

ACCESS_TOKEN = os.getenv("QBO_ACCESS_TOKEN")
REALM_ID = os.getenv("REALM_ID")
ENVIRONMENT = os.getenv("QBO_ENVIRONMENT", "sandbox")

# === Setup logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# === Base URL builder ===
def base_url():
    return (
        f"https://sandbox-quickbooks.api.intuit.com/v3/company/{REALM_ID}"
        if ENVIRONMENT == "sandbox"
        else f"https://quickbooks.api.intuit.com/v3/company/{REALM_ID}"
    )

# === Fetch all customer payments ===
import time

def fetch_all_customer_payments():
    logger.info("📥 Fetching all customer payments...")

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/text"
    }

    start_position = 1
    max_results = 100
    all_payments = []

    while True:
        query = f"SELECT Id, SyncToken FROM Payment STARTPOSITION {start_position} MAXRESULTS {max_results}"
        url = f"{base_url()}/query"

        for attempt in range(3):  # Retry up to 3 times
            try:
                response = requests.post(url, headers=headers, data=query.encode("utf-8"))

                if response.status_code == 200:
                    payments = response.json().get("QueryResponse", {}).get("Payment", [])
                    if not payments:
                        break

                    all_payments.extend(payments)
                    logger.info(f"📄 Retrieved {len(payments)} payments (Total: {len(all_payments)})")

                    if len(payments) < max_results:
                        break
                    start_position += max_results
                    break  # break out of retry loop on success

                elif response.status_code >= 500:
                    logger.warning(f"🔁 Retry {attempt+1}/3: Server error {response.status_code}")
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"❌ Failed to fetch payments: {response.status_code} - {response.text}")
                    return []

            except requests.RequestException as e:
                logger.error(f"⚠️ Request error on attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)

        else:
            logger.error("❌ Max retries reached. Exiting fetch.")
            break

    logger.info(f"📦 Total customer payments fetched: {len(all_payments)}")
    return all_payments

# === Delete a single payment ===
def delete_payment(entry_id, sync_token, session):
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    url = f"{base_url()}/payment?operation=delete"
    payload = {"Id": entry_id, "SyncToken": sync_token}

    try:
        response = session.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            return f"✅ Deleted payment ID: {entry_id}"
        else:
            return f"❌ Failed payment ID {entry_id}: {response.status_code} - {response.text}"
    except requests.RequestException as e:
        return f"⚠️ Error deleting payment {entry_id}: {e}"

# === Main deletion routine ===
def delete_all_customer_payments():
    payments = fetch_all_customer_payments()
    if not payments:
        logger.warning("⚠️ No customer payments found.")
        return

    logger.info(f"🗑️ Starting deletion of {len(payments)} payments...")
    session = requests.Session()
    max_workers = 50  # Respect rate limits

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_payment = {
            executor.submit(delete_payment, p["Id"], p["SyncToken"], session): p for p in payments
        }

        with tqdm(total=len(payments), desc="Deleting Payments", unit="payment") as pbar:
            for future in as_completed(future_to_payment):
                result = future.result()
                logger.info(result)
                pbar.update(1)

    logger.info("✅ All customer payments deletion completed!")

# === Run on script execution ===
if __name__ == "__main__":
    delete_all_customer_payments()
