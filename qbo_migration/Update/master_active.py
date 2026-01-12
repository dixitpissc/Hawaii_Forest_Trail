import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import requests
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def base_url():
    if os.getenv("QBO_ENVIRONMENT") == 'sandbox':
        return f"https://sandbox-quickbooks.api.intuit.com/v3/company/{os.getenv('REALM_ID')}"
    elif os.getenv("QBO_ENVIRONMENT") == 'production':
        return f"https://quickbooks.api.intuit.com/v3/company/{os.getenv('REALM_ID')}"
def url():
    return base_url()

# Load environment variables
load_dotenv()

# QuickBooks API credentials
ACCESS_TOKEN = os.getenv("QBO_ACCESS_TOKEN")
REALM_ID = os.getenv("REALM_ID")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s",
    handlers=[
        logging.FileHandler("quickbooks_sync.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ----------------------------------------
# Helper: Headers for all API calls
# ----------------------------------------
def get_headers():
    return {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

# ----------------------------------------
# Fetch all entities
# ----------------------------------------
def fetch_entities(entity_type, active_status=None):
    logger.info(f"Fetching {entity_type} (Active={active_status}) from QuickBooks Online...")
    headers = get_headers()

    start_position = 1
    max_results = 100
    all_entities = []

    status_filter = f"WHERE Active = {str(active_status).lower()}" if active_status is not None else ""

    while True:
        query_url = (
            f"{url()}/query"
            f"?query=SELECT * FROM {entity_type} {status_filter} STARTPOSITION {start_position} MAXRESULTS {max_results}"
        )
        try:
            response = requests.get(query_url, headers=headers)
            if response.status_code == 200:
                entities = response.json().get("QueryResponse", {}).get(entity_type, [])
                all_entities.extend(entities)
                logger.info(f"Fetched {len(entities)} records. Total so far: {len(all_entities)}")

                if len(entities) < max_results:
                    break
                start_position += max_results
            else:
                logger.error(f"Failed to fetch {entity_type}: {response.status_code} - {response.text}")
                break
        except requests.RequestException as e:
            logger.error(f"Error fetching {entity_type}: {e}")
            break

    return all_entities

# ----------------------------------------
# Fetch full entity details (to get fresh SyncToken)
# ----------------------------------------
def fetch_full_entity(entity_type, entity_id, session):
    headers = get_headers()
    fetch_url = f"{url()}/{entity_type.lower()}/{entity_id}"
    response = session.get(fetch_url, headers=headers)
    if response.status_code == 200:
        return response.json().get(entity_type, {})
    else:
        logger.error(f"Failed to fetch full {entity_type} {entity_id}: {response.status_code} - {response.text}")
        return None

# ----------------------------------------
# Update entity with current SyncToken and all required fields
# ----------------------------------------
def update_entity_status(entity_type, entity_id, new_status, session):
    entity_type_lower = entity_type.lower()
    headers = get_headers()

    entity_data = fetch_full_entity(entity_type, entity_id, session)
    if not entity_data:
        return f"❌ Skipped {entity_type} {entity_id}: Failed to get full data."

    sync_token = entity_data.get("SyncToken", "0")
    entity_data["SyncToken"] = sync_token
    entity_data["Active"] = new_status

    # Preserve required name/display fields
    if entity_type_lower in ["vendor", "customer"]:
        if not entity_data.get("DisplayName"):
            return f"⚠️ Missing DisplayName for {entity_type} {entity_id}. Skipped."
    elif entity_type_lower in ["account", "item"]:
        if not entity_data.get("Name"):
            return f"⚠️ Missing Name for {entity_type} {entity_id}. Skipped."

    update_url = f"{url()}/{entity_type_lower}"
    update_response = session.post(update_url, headers=headers, json=entity_data)

    if update_response.status_code == 200:
        return f"✅ Updated {entity_type} {entity_id} to {'active' if new_status else 'inactive'}"
    else:
        return f"❌ Failed {entity_type} {entity_id}: {update_response.status_code} - {update_response.text}"

# ----------------------------------------
# Process entities with concurrency
# ----------------------------------------
def process_entities(entity_type, target_status, new_status):
    logger.info(f"Processing {entity_type} to set Active={new_status}...")
    entities = fetch_entities(entity_type, active_status=target_status)

    if not entities:
        logger.warning(f"No {entity_type} found with active={target_status}.")
        return

    session = requests.Session()
    max_workers = 5  # run with 5 concurrent threads

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_entity = {
            executor.submit(update_entity_status, entity_type, entity["Id"], new_status, session): entity
            for entity in entities
        }

        with tqdm(total=len(entities), desc=f"Updating {entity_type}", unit="entity") as pbar:
            for future in as_completed(future_to_entity):
                result = future.result()
                logger.info(result)
                pbar.update(1)

    logger.info(f"✅ Finished processing {entity_type}!")

# ----------------------------------------
# Main interactive loop
# ----------------------------------------
if __name__ == "__main__":
    while True:
        choice = input("Do you want to (1) Activate or (2) Deactivate entities? Enter 1 or 2 (or # to exit): ")
        if choice == "#":
            break

        new_status = True if choice == "1" else False

        print("Select entity types to process:")
        entity_options = {
            "1": "Account",
            # "2": "Vendor",
            # "3": "Customer",
            # "4": "Term",
            # "5": "Item",
            # "6": "Department",
            # "7": "Class"
        }
        for key, value in entity_options.items():
            print(f"{key}. {value}")

        selections = input("Enter numbers separated by commas (or # to exit): ")
        if selections == "#":
            break

        selected_entities = [entity_options[num.strip()] for num in selections.split(",") if num.strip() in entity_options]

        for entity in selected_entities:
            process_entities(entity, target_status=not new_status, new_status=new_status)