import os
import sys
import time
import base64
import json
import random
import threading
from typing import Dict, Any

import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from utils.token_refresher import auto_refresh_token_if_needed

auto_refresh_token_if_needed()

# ---------------------- Load .env ----------------------
load_dotenv()

# ---------------------- Config -------------------------
CSV_PATH = "SalesReceipt_Department_Update.csv"
MINOR_VERSION = os.getenv("QBO_MINOR_VERSION", "65")
TIMEOUT_SEC = int(os.getenv("QBO_TIMEOUT_SEC", "45"))

MAX_RETRIES_429 = 6
INITIAL_BACKOFF_SEC = 1.5

CONCURRENCY = int(os.getenv("QBO_CONCURRENCY", "6"))
MAX_RPS = float(os.getenv("QBO_MAX_RPS", "8"))

# ---------------------- Logging ------------------------
import logging
from datetime import datetime

# Ensure logs directory exists and create a timestamped log file
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = f"department_update_salesreceipt_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG_FILE_PATH = os.path.join(LOG_DIR, log_filename)

logger = logging.getLogger("department_update_salesreceipt")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

file_handler = logging.FileHandler(LOG_FILE_PATH, mode="w", encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# In-memory structured logs to be written at the end as salesreceipt_log
salesreceipt_log_entries = []
salesreceipt_log_lock = threading.Lock()

# Wrapper functions that log to both logger and in-memory list
def log_info(msg):
    ts = datetime.now().isoformat()
    logger.info(msg)
    with salesreceipt_log_lock:
        salesreceipt_log_entries.append({"time": ts, "level": "INFO", "message": msg})

def log_warn(msg):
    ts = datetime.now().isoformat()
    logger.warning(msg)
    with salesreceipt_log_lock:
        salesreceipt_log_entries.append({"time": ts, "level": "WARN", "message": msg})

def log_err(msg):
    ts = datetime.now().isoformat()
    logger.error(msg)
    with salesreceipt_log_lock:
        salesreceipt_log_entries.append({"time": ts, "level": "ERROR", "message": msg})

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
    if resp.status_code == 429: raise Qbo429(resp.text)
    if resp.status_code == 401: raise Qbo401(resp.text)
    if resp.status_code >= 300: raise RuntimeError(resp.text)

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

# ---------------------- SalesReceipt Logic ---------------------
def get_salesreceipt(session, base, realm, sr_id, access_token):
    url = f"{base}/v3/company/{realm}/salesreceipt/{sr_id}"
    r = session.get(
        url,
        headers=qbo_headers(access_token),
        params={"minorversion": MINOR_VERSION},
        timeout=TIMEOUT_SEC
    )
    return handle_response(r, "SalesReceipt")

def update_salesreceipt(session, base, realm, body, access_token):
    url = f"{base}/v3/company/{realm}/salesreceipt"
    r = session.post(
        url,
        headers=qbo_headers(access_token),
        params={"operation": "update", "minorversion": MINOR_VERSION},
        data=json.dumps(body),
        timeout=TIMEOUT_SEC
    )
    return handle_response(r, "SalesReceipt")

# ✅ HEADER-LEVEL ONLY UPDATE
def set_department_header_only(salesreceipt: Dict[str, Any], dept_id: str) -> Dict[str, Any]:
    sr_obj = json.loads(json.dumps(salesreceipt))  # deep copy
    sr_obj["sparse"] = False

    if not sr_obj.get("Id") or sr_obj.get("SyncToken") is None:
        raise ValueError("SalesReceipt missing Id or SyncToken")

    sr_obj["DepartmentRef"] = {"value": str(dept_id)}

    return sr_obj

# ---------------------- Retry Wrapper ------------------
def with_retries(limiter, session, func, tokens, refresh_args, lock, *args):
    """Call `func` with retries, handle 401 by auto-refreshing token and continue.

    Uses `auto_refresh_token_if_needed()` to refresh the access token safely (and updates
    `tokens` dict from the environment). On Qbo429 it backs off and retries. If all
    retries are exhausted it raises a RuntimeError which will be handled per-row.
    """
    backoff = INITIAL_BACKOFF_SEC
    for _ in range(MAX_RETRIES_429 + 1):
        limiter.wait()

        # Try to auto-refresh token lazily before each attempt (no-op if not needed)
        try:
            with lock:
                auto_refresh_token_if_needed()
                tokens["access"] = os.getenv("QBO_ACCESS_TOKEN", tokens.get("access"))
                tokens["refresh"] = os.getenv("QBO_REFRESH_TOKEN", tokens.get("refresh"))
        except Exception:
            logger.exception("❌ auto_refresh_token_if_needed failed before request; proceeding with existing token")

        try:
            return func(session, *args, access_token=tokens["access"])
        except Qbo401:
            # On 401 attempt an explicit refresh and continue; do not raise immediately
            try:
                with lock:
                    auto_refresh_token_if_needed()
                    tokens["access"] = os.getenv("QBO_ACCESS_TOKEN", tokens.get("access"))
                    tokens["refresh"] = os.getenv("QBO_REFRESH_TOKEN", tokens.get("refresh"))
                logger.warning("⚠️ Received 401 — refreshed token and retrying")
            except Exception:
                logger.exception("❌ Token refresh failed while handling 401; will retry with existing token")
            # continue to next retry
        except Qbo429:
            logger.warning("⚠️ Received 429 rate limit — backing off and retrying")
            time.sleep(backoff)
            backoff *= 2
    # If we get here, all retries are exhausted
    raise RuntimeError("Max retries exceeded")

# ---------------------- Worker -------------------------
def process_row(limiter, session, base, realm, tokens, refresh_args, lock, salesreceipt_id, dept):
    try:
        sr = with_retries(
            limiter, session, get_salesreceipt,
            tokens, refresh_args, lock,
            base, realm, salesreceipt_id
        )

        sr = set_department_header_only(sr, dept)

        updated = with_retries(
            limiter, session, update_salesreceipt,
            tokens, refresh_args, lock,
            base, realm, sr
        )

        return salesreceipt_id, True, updated.get("SyncToken")
    except Exception as e:
        return salesreceipt_id, False, str(e)

# ---------------------- Main ---------------------------
def main():
    realm = os.getenv("QBO_REALM_ID")
    client_id = os.getenv("QBO_CLIENT_ID")
    client_secret = os.getenv("QBO_CLIENT_SECRET")
    refresh_token = os.getenv("QBO_REFRESH_TOKEN")
    access_token = os.getenv("QBO_ACCESS_TOKEN", "")
    env = os.getenv("QBO_ENVIRONMENT", "production")

    session = requests.Session()
    base = base_url(env)

    if not access_token:
        # Try lazy auto-refresh first (reads/refreshes from .env if needed)
        try:
            new_token = auto_refresh_token_if_needed()
            access_token = new_token or access_token
        except Exception:
            logger.exception("❌ auto_refresh_token_if_needed failed during startup; will attempt direct refresh")

        if not access_token:
            try:
                access_token, refresh_token = refresh_access_token(
                    session, client_id, client_secret, refresh_token
                )
            except Exception:
                logger.exception("❌ Direct refresh failed during startup; continuing with whatever token is available")

    tokens = {"access": access_token, "refresh": refresh_token}
    lock = threading.Lock()
    limiter = GlobalLimiter(MAX_RPS)

    df = pd.read_csv(CSV_PATH, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.dropna(subset=["target_id", "departmentref"])
    df = df.drop_duplicates("target_id", keep="last")

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as exe:
        futures = [
            exe.submit(
                process_row,
                limiter, session, base, realm,
                tokens, (client_id, client_secret), lock,
                r.target_id, r.departmentref
            )
            for r in df.itertuples(index=False)
        ]

        for f in as_completed(futures):
            salesreceipt_id, ok, msg = f.result()
            if ok:
                log_info(f"✅ SalesReceipt {salesreceipt_id} updated (New SyncToken {msg})")
                with salesreceipt_log_lock:
                    salesreceipt_log_entries.append({"time": datetime.now().isoformat(), "salesreceipt_id": salesreceipt_id, "ok": True, "message": f"New SyncToken {msg}"})
            else:
                log_err(f"❌ SalesReceipt {salesreceipt_id} failed: {msg}")
                with salesreceipt_log_lock:
                    salesreceipt_log_entries.append({"time": datetime.now().isoformat(), "salesreceipt_id": salesreceipt_id, "ok": False, "message": str(msg)})

    # Write structured salesreceipt_log JSON file with timestamped name
    try:
        salesreceipt_log_path = os.path.join(LOG_DIR, f"salesreceipt_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(salesreceipt_log_path, "w", encoding="utf-8") as fh:
            json.dump(salesreceipt_log_entries, fh, indent=2, ensure_ascii=False)
        logger.info(f"✅ salesreceipt_log written to {salesreceipt_log_path}")
    except Exception:
        logger.exception("❌ Failed to write salesreceipt_log")

if __name__ == "__main__":
    main()
