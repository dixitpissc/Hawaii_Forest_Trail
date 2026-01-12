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

# =============================================================================
# LOGGING SETUP
# =============================================================================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = f"activate_account_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG_FILE_PATH = os.path.join(LOG_DIR, log_filename)

logger = logging.getLogger("activate_accounts")
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
post_url = f"{base_url}/v3/company/{realm_id}/account?minorversion=75"

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

CSV_FILE = "Account_update.csv"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def fetch_qbo_account_by_id(qbo_id):
    """Fetch latest SyncToken by Account Id using direct GET, fallback to query."""
    try:
        # Preferred: direct GET for the account
        get_url = f"{base_url}/v3/company/{realm_id}/account/{qbo_id}"
        resp = session.get(get_url, headers=headers, params={"minorversion": "75"}, timeout=30)
        if resp.status_code == 200:
            acc = resp.json().get("Account")
            if acc:
                return acc.get("Id"), acc.get("SyncToken"), acc
        elif resp.status_code == 401:
            logger.warning("⚠️ Received 401 while GETting account — refreshing token and retrying GET")
            auto_refresh_token_if_needed()
            new_token = os.getenv("QBO_ACCESS_TOKEN")
            if new_token:
                headers["Authorization"] = f"Bearer {new_token}"
                resp = session.get(get_url, headers=headers, params={"minorversion": "75"}, timeout=30)
                if resp.status_code == 200:
                    acc = resp.json().get("Account")
                    if acc:
                        return acc.get("Id"), acc.get("SyncToken"), acc
        else:
            logger.debug(f"GET account {qbo_id} returned HTTP {resp.status_code} - {resp.text[:1000]}")

        # Fallback to query endpoint if GET didn't succeed
        query = f"SELECT Id, SyncToken FROM Account WHERE Id = '{qbo_id}'"
        resp = session.get(query_url, headers=headers, params={"query": query, "minorversion": "75"}, timeout=30)
        if resp.status_code == 200:
            accounts = resp.json().get("QueryResponse", {}).get("Account", [])
            if accounts:
                return accounts[0]["Id"], accounts[0]["SyncToken"], accounts[0]
        else:
            logger.debug(f"Query for {qbo_id} returned HTTP {resp.status_code} - {resp.text[:1000]}")

    except Exception:
        logger.exception(f"❌ Error fetching SyncToken for Account Id={qbo_id}")

    return None, None, None


def generate_activate_payload(qbo_id, sync_token, account_obj=None):
    """Create payload to activate account. Include Name if available to satisfy API requirements."""
    payload = {
        "Id": qbo_id,
        "SyncToken": sync_token,
        "sparse": True,
        "Active": False
    }
    if account_obj:
        # QuickBooks requires Name on some update calls; prefer the explicit Name field
        name = account_obj.get("Name") or account_obj.get("DisplayName")
        if name:
            payload["Name"] = name
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
def activate_accounts_from_csv():
    logger.info("🚀 Starting account activation using CSV...")

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

    df = df.drop_duplicates(subset=["Target_Id"]).copy()

    timer = ProgressTimer(len(df), logger=logger)

    for idx, row in df.iterrows():
        qbo_id = parse_qbo_id(row["Target_Id"])

        if not qbo_id:
            logger.warning("⏭️ Skipping row with empty Target_Id")
            timer.update()
            continue

        _, sync_token, account_obj = fetch_qbo_account_by_id(qbo_id)

        if not sync_token:
            logger.error(f"❌ SyncToken not found for Account Id={qbo_id}")
            timer.update()
            continue

        payload = generate_activate_payload(qbo_id, sync_token, account_obj)
        last_response = None

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Attempt {attempt}/{max_retries} activating Account Id={qbo_id}")
                resp = session.post(post_url, headers=headers, json=payload, timeout=30)
                last_response = resp.text[:2000]

                if resp.status_code in (200, 201):
                    logger.info(f"✅ Account activated successfully: {qbo_id}")
                    break

                # Handle auth errors by refreshing token and retrying once
                if resp.status_code == 401:
                    logger.warning("⚠️ Received 401 — refreshing token and retrying")
                    auto_refresh_token_if_needed()
                    new_token = os.getenv("QBO_ACCESS_TOKEN")
                    if new_token:
                        headers["Authorization"] = f"Bearer {new_token}"
                        continue

                # Handle stale object error by refetching SyncToken
                if "Stale Object Error" in (resp.text or ""):
                    logger.warning(f"⚠️ Stale Object Error for {qbo_id}, refetching SyncToken and Name")
                    _, new_sync, new_acc = fetch_qbo_account_by_id(qbo_id)
                    if new_sync:
                        payload["SyncToken"] = new_sync
                        # Ensure Name is present if available
                        if not payload.get("Name") and new_acc:
                            name = new_acc.get("Name") or new_acc.get("DisplayName")
                            if name:
                                payload["Name"] = name
                        continue

                logger.error(f"❌ Failed activating {qbo_id}: HTTP {resp.status_code} - {last_response}")
                break

            except requests.exceptions.RequestException:
                logger.exception(f"❌ Request error on attempt {attempt} for {qbo_id}")
                if attempt == max_retries:
                    logger.error(f"❌ Giving up on Account Id={qbo_id}")

        timer.update()

    timer.stop()
    logger.info("✅ Account activation process completed using CSV.")

# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    activate_accounts_from_csv()
