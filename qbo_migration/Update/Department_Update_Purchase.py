#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import base64
import json
import random
import threading
from typing import Dict, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
from dotenv import load_dotenv

from utils.token_refresher import auto_refresh_token_if_needed

# ---------------------- Bootstrap ----------------------
auto_refresh_token_if_needed()
load_dotenv()

# ---------------------- Config -------------------------
CSV_PATH = "Purchase_Department_Update.csv"
MINOR_VERSION = os.getenv("QBO_MINOR_VERSION", "65")
TIMEOUT_SEC = int(os.getenv("QBO_TIMEOUT_SEC", "45"))

MAX_RETRIES_429 = 6
INITIAL_BACKOFF_SEC = 1.5

CONCURRENCY = int(os.getenv("QBO_CONCURRENCY", "6"))
MAX_RPS = float(os.getenv("QBO_MAX_RPS", "8"))

# ---------------------- Logging ------------------------
import logging

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = f"department_update_purchase_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG_FILE_PATH = os.path.join(LOG_DIR, log_filename)

logger = logging.getLogger("department_update_purchase")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

file_handler = logging.FileHandler(LOG_FILE_PATH, mode="w", encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

purchase_log_entries = []
purchase_log_lock = threading.Lock()

def log_info(msg):
    ts = datetime.now().isoformat()
    logger.info(msg)
    with purchase_log_lock:
        purchase_log_entries.append({"time": ts, "level": "INFO", "message": msg})

def log_err(msg):
    ts = datetime.now().isoformat()
    logger.error(msg)
    with purchase_log_lock:
        purchase_log_entries.append({"time": ts, "level": "ERROR", "message": msg})

# ---------------------- OAuth --------------------------
def refresh_access_token(session, client_id, client_secret, refresh_token):
    url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    token = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    resp = session.post(url, headers=headers, data=data, timeout=TIMEOUT_SEC)
    if resp.status_code != 200:
        raise RuntimeError(resp.text)
    j = resp.json()
    return j["access_token"], j.get("refresh_token", refresh_token)

# ---------------------- QBO Helpers --------------------
def base_url(env):
    return (
        "https://sandbox-quickbooks.api.intuit.com"
        if env == "sandbox"
        else "https://quickbooks.api.intuit.com"
    )

def qbo_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

class Qbo429(Exception): pass
class Qbo401(Exception): pass

def handle_response(resp, expect=None):
    if resp.status_code == 429:
        raise Qbo429(resp.text)
    if resp.status_code == 401:
        raise Qbo401(resp.text)
    if resp.status_code >= 300:
        raise RuntimeError(resp.text)

    data = resp.json()
    if "Fault" in data:
        raise RuntimeError(json.dumps(data["Fault"]))

    return data[expect] if expect else data

# ---------------------- Rate Limiter -------------------
class GlobalLimiter:
    def __init__(self, rps):
        self.interval = 1.0 / rps
        self.lock = threading.Lock()
        self.next = time.perf_counter()

    def wait(self):
        with self.lock:
            now = time.perf_counter()
            if now < self.next:
                time.sleep(self.next - now)
            self.next = time.perf_counter() + self.interval * random.uniform(0.9, 1.1)

# ---------------------- Purchase API -------------------
def get_purchase(session, base, realm, purchase_id, access_token):
    url = f"{base}/v3/company/{realm}/purchase/{purchase_id}"
    r = session.get(
        url,
        headers=qbo_headers(access_token),
        params={"minorversion": MINOR_VERSION},
        timeout=TIMEOUT_SEC
    )
    return handle_response(r, "Purchase")

def update_purchase(session, base, realm, body, access_token):
    url = f"{base}/v3/company/{realm}/purchase"
    r = session.post(
        url,
        headers=qbo_headers(access_token),
        params={"operation": "update", "minorversion": MINOR_VERSION},
        data=json.dumps(body),
        timeout=TIMEOUT_SEC
    )
    return handle_response(r, "Purchase")

# ---------------------- Business Logic -----------------
def set_department_header_only(purchase: Dict[str, Any], dept_id: str) -> Dict[str, Any]:
    obj = json.loads(json.dumps(purchase))
    obj["sparse"] = False

    if not obj.get("Id") or obj.get("SyncToken") is None:
        raise ValueError("Purchase missing Id or SyncToken")

    obj["DepartmentRef"] = {"value": str(dept_id)}
    return obj

# ---------------------- Retry Wrapper ------------------
def with_retries(limiter, session, func, tokens, lock, *args):
    backoff = INITIAL_BACKOFF_SEC
    for _ in range(MAX_RETRIES_429 + 1):
        limiter.wait()

        with lock:
            auto_refresh_token_if_needed()
            tokens["access"] = os.getenv("QBO_ACCESS_TOKEN", tokens["access"])

        try:
            return func(session, *args, access_token=tokens["access"])
        except Qbo401:
            log_err("401 received – refreshing token and retrying")
        except Qbo429:
            time.sleep(backoff)
            backoff *= 2

    raise RuntimeError("Max retries exceeded")

# ---------------------- Worker -------------------------
def process_row(limiter, session, base, realm, tokens, lock, purchase_id, dept):
    try:
        purchase = with_retries(
            limiter, session, get_purchase,
            tokens, lock,
            base, realm, purchase_id
        )

        purchase = set_department_header_only(purchase, dept)

        updated = with_retries(
            limiter, session, update_purchase,
            tokens, lock,
            base, realm, purchase
        )

        return purchase_id, True, updated.get("SyncToken")
    except Exception as e:
        return purchase_id, False, str(e)

# ---------------------- Main ---------------------------
def main():
    realm = os.getenv("QBO_REALM_ID")
    env = os.getenv("QBO_ENVIRONMENT", "production")

    session = requests.Session()
    base = base_url(env)

    tokens = {
        "access": os.getenv("QBO_ACCESS_TOKEN", ""),
        "refresh": os.getenv("QBO_REFRESH_TOKEN", "")
    }

    limiter = GlobalLimiter(MAX_RPS)
    lock = threading.Lock()

    df = pd.read_csv(CSV_PATH, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.dropna(subset=["target_id", "departmentref"])
    df = df.drop_duplicates("target_id", keep="last")

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as exe:
        futures = [
            exe.submit(
                process_row,
                limiter, session, base, realm,
                tokens, lock,
                r.target_id, r.departmentref
            )
            for r in df.itertuples(index=False)
        ]

        for f in as_completed(futures):
            pid, ok, msg = f.result()
            if ok:
                log_info(f"✅ Purchase {pid} updated (SyncToken {msg})")
            else:
                log_err(f"❌ Purchase {pid} failed: {msg}")

    log_path = os.path.join(
        LOG_DIR, f"purchase_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(purchase_log_entries, f, indent=2)

    logger.info(f"✅ Purchase log written to {log_path}")

if __name__ == "__main__":
    main()
