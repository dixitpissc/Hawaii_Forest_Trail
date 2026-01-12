#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import requests
import pandas as pd
import json
from dotenv import load_dotenv
from datetime import datetime
from utils.token_refresher import auto_refresh_token_if_needed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from utils.log_timer import ProgressTimer
import concurrent.futures
import threading
import argparse

# =============================================================================
# LOGGING SETUP
# =============================================================================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = f"activate_vendor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG_FILE_PATH = os.path.join(LOG_DIR, log_filename)

logger = logging.getLogger("activate_vendors")
logger.setLevel(logging.INFO)
logger.propagate = False

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
if logger.hasHandlers():
    logger.handlers.clear()

file_handler = logging.FileHandler(LOG_FILE_PATH, mode="w", encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# =============================================================================
# ENV + TOKEN
# =============================================================================
load_dotenv()
auto_refresh_token_if_needed()

access_token = os.getenv("QBO_ACCESS_TOKEN")
realm_id = os.getenv("QBO_REALM_ID")
environment = os.getenv("QBO_ENVIRONMENT", "sandbox")

base_url = (
    "https://sandbox-quickbooks.api.intuit.com"
    if environment == "sandbox"
    else "https://quickbooks.api.intuit.com"
)

query_url = f"{base_url}/v3/company/{realm_id}/query"
post_url = f"{base_url}/v3/company/{realm_id}/vendor?minorversion=75"

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

session = requests.Session()
# Configure retries for transient network errors (backoff and status codes)
retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET","POST"])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

max_retries = 3

CSV_FILE = "Vendor_update-ISSC-DEL156.csv"

# Lock to protect token refresh and header update across threads
token_lock = threading.Lock()

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def fetch_qbo_vendor_by_id(qbo_id):
    """Fetch latest SyncToken by Vendor Id using direct GET, fallback to query."""
    try:
        # Preferred: direct GET for the vendor
        get_url = f"{base_url}/v3/company/{realm_id}/vendor/{qbo_id}"
        resp = session.get(get_url, headers=headers, params={"minorversion": "75"}, timeout=30)
        if resp.status_code == 200:
            vendor = resp.json().get("Vendor")
            if vendor:
                return vendor.get("Id"), vendor.get("SyncToken"), vendor
        elif resp.status_code == 401:
            logger.warning("⚠️ Received 401 while GETting vendor — refreshing token and retrying GET")
            auto_refresh_token_if_needed()
            new_token = os.getenv("QBO_ACCESS_TOKEN")
            if new_token:
                headers["Authorization"] = f"Bearer {new_token}"
                resp = session.get(get_url, headers=headers, params={"minorversion": "75"}, timeout=30)
                if resp.status_code == 200:
                    vendor = resp.json().get("Vendor")
                    if vendor:
                        return vendor.get("Id"), vendor.get("SyncToken"), vendor
        else:
            logger.debug(f"GET vendor {qbo_id} returned HTTP {resp.status_code} - {resp.text[:1000]}")

        # Fallback to query endpoint if GET didn't succeed
        query = f"SELECT Id, SyncToken FROM Vendor WHERE Id = '{qbo_id}'"
        resp = session.get(query_url, headers=headers, params={"query": query, "minorversion": "75"}, timeout=30)
        if resp.status_code == 200:
            vendors = resp.json().get("QueryResponse", {}).get("Vendor", [])
            if vendors:
                return vendors[0]["Id"], vendors[0]["SyncToken"], vendors[0]
        else:
            logger.debug(f"Query for {qbo_id} returned HTTP {resp.status_code} - {resp.text[:1000]}")

    except Exception:
        logger.exception(f"❌ Error fetching SyncToken for Vendor Id={qbo_id}")

    return None, None, None


def generate_vendor_activate_payload(qbo_id, sync_token, vendor_obj=None):
    """Create payload to activate vendor. Include a display/name field if available to satisfy API requirements."""
    payload = {
        "Id": qbo_id,
        "SyncToken": sync_token,
        "sparse": True,
        "Active": False
    }
    if vendor_obj:
        # QuickBooks may require a display name or company/given name on updates
        name = (
            vendor_obj.get("DisplayName")
            or vendor_obj.get("CompanyName")
            or vendor_obj.get("GivenName")
            or vendor_obj.get("Name")
        )
        if name:
            # Many vendor updates accept DisplayName
            payload["DisplayName"] = name
    return payload

def parse_qbo_id(value):
    """Normalize Target_Id values from CSV to QBO Id strings."""
    if pd.isna(value):
        return None
    # handle floats like 433.0
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()

# =============================================================================
# MAIN UPDATE FUNCTION
# =============================================================================
def process_vendor(qbo_id):
    """Worker that activates a single vendor and returns (qbo_id, success, message)."""
    try:
        _, sync_token, vendor_obj = fetch_qbo_vendor_by_id(qbo_id)
        if not sync_token:
            return (qbo_id, False, "SyncToken not found")

        payload = generate_vendor_activate_payload(qbo_id, sync_token, vendor_obj)

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Attempt {attempt}/{max_retries} activating Vendor Id={qbo_id}")
                resp = session.post(post_url, headers=headers, json=payload, timeout=30)

                if resp.status_code in (200, 201):
                    return (qbo_id, True, "Activated")

                # Handle auth errors by refreshing token and retrying once (protected by lock)
                if resp.status_code == 401:
                    logger.warning("⚠️ Received 401 — refreshing token and retrying")
                    with token_lock:
                        auto_refresh_token_if_needed()
                        new_token = os.getenv("QBO_ACCESS_TOKEN")
                        if new_token:
                            headers["Authorization"] = f"Bearer {new_token}"
                    continue

                # Handle stale object error by refetching SyncToken & name
                if "Stale Object Error" in (resp.text or ""):
                    logger.warning(f"⚠️ Stale Object Error for {qbo_id}, refetching SyncToken and DisplayName")
                    _, new_sync, new_vendor = fetch_qbo_vendor_by_id(qbo_id)
                    if new_sync:
                        payload["SyncToken"] = new_sync
                        if not payload.get("DisplayName") and new_vendor:
                            name = (
                                new_vendor.get("DisplayName")
                                or new_vendor.get("CompanyName")
                                or new_vendor.get("GivenName")
                                or new_vendor.get("Name")
                            )
                            if name:
                                payload["DisplayName"] = name
                        continue

                return (qbo_id, False, f"HTTP {resp.status_code} - {resp.text[:500]}")

            except requests.exceptions.RequestException as e:
                logger.exception(f"❌ Request error on attempt {attempt} for {qbo_id}")
                if attempt == max_retries:
                    return (qbo_id, False, f"RequestException: {e}")
        return (qbo_id, False, "Max retries exceeded")
    except Exception as e:
        logger.exception(f"❌ Unexpected error processing Vendor Id={qbo_id}")
        return (qbo_id, False, f"Exception: {e}")


def activate_vendors_from_csv(workers=8):
    logger.info("🚀 Starting vendor activation using CSV...")

    if not os.path.exists(CSV_FILE):
        logger.error(f"❌ CSV file not found: {CSV_FILE}")
        return

    df = pd.read_csv(CSV_FILE)

    if df.empty:
        logger.warning("⚠️ CSV file is empty.")
        return

    if "Target_Id" not in df.columns:
        logger.error("❌ CSV must contain 'Target_Id' column.")
        return

    # prepare ids
    ids = [parse_qbo_id(v) for v in df["Target_Id"].drop_duplicates().tolist()]
    ids = [i for i in ids if i]

    timer = ProgressTimer(len(ids), logger=logger)

    # Use ThreadPoolExecutor to process up to `workers` vendors concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_id = {executor.submit(process_vendor, qid): qid for qid in ids}
        for future in concurrent.futures.as_completed(future_to_id):
            qid = future_to_id[future]
            try:
                qid, success, message = future.result()
                if success:
                    logger.info(f"✅ Vendor activated successfully: {qid}")
                else:
                    logger.error(f"❌ Failed activating {qid}: {message}")
            except Exception:
                logger.exception(f"❌ Worker raised exception for {qid}")
            finally:
                timer.update()

    timer.stop()
    logger.info("✅ Vendor activation process completed using CSV.")

# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    activate_vendors_from_csv()
