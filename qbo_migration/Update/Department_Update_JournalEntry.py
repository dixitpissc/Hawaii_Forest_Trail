#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fast updater: set DepartmentRef on all lines of JournalEntries from CSV.

CSV: Journal_Entry_departmentref.csv
Columns: Target_Id,DepartmentRef
"""

import os
import sys
import time
import base64
import json
import random
import threading
from typing import Dict, Any, Tuple, Optional

import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# ---------------------- Load .env ----------------------
load_dotenv()

# ---------------------- Config -------------------------
CSV_PATH = "JournalEntry_Department_Update.csv"
MINOR_VERSION = os.getenv("QBO_MINOR_VERSION", "65")
TIMEOUT_SEC = int(os.getenv("QBO_TIMEOUT_SEC", "45"))

MAX_RETRIES_429 = 6
INITIAL_BACKOFF_SEC = 1.5

CONCURRENCY = int(os.getenv("QBO_CONCURRENCY", "6"))
MAX_RPS = float(os.getenv("QBO_MAX_RPS", "8"))

# ---------------------- Logging ------------------------
def log_info(msg): print(f"[INFO] {msg}", flush=True)
def log_warn(msg): print(f"[WARN] {msg}", flush=True)
def log_err(msg): print(f"[ERROR] {msg}", file=sys.stderr, flush=True)

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
    return "https://sandbox-quickbooks.api.intuit.com" if env == "sandbox" else "https://quickbooks.api.intuit.com"

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

# ---------------------- JE Logic -----------------------
def get_journalentry(session, base, realm, je_id, access_token):
    url = f"{base}/v3/company/{realm}/journalentry/{je_id}"
    r = session.get(url, headers=qbo_headers(access_token),
                    params={"minorversion": MINOR_VERSION},
                    timeout=TIMEOUT_SEC)
    return handle_response(r, "JournalEntry")

def update_journalentry(session, base, realm, body, access_token):
    url = f"{base}/v3/company/{realm}/journalentry"
    r = session.post(url, headers=qbo_headers(access_token),
                     params={"operation": "update", "minorversion": MINOR_VERSION},
                     data=json.dumps(body),
                     timeout=TIMEOUT_SEC)
    return handle_response(r, "JournalEntry")

def set_department_on_all_lines(je: Dict[str, Any], dept_id: str) -> Dict[str, Any]:
    """
    Add DepartmentRef ONLY to lines where it is missing.
    Existing DepartmentRef values are preserved.
    """
    je_obj = json.loads(json.dumps(je))
    je_obj["sparse"] = False

    for ln in je_obj.get("Line", []):
        d = ln.get("JournalEntryLineDetail")
        if not isinstance(d, dict):
            continue

        # If DepartmentRef already exists, DO NOTHING
        if d.get("DepartmentRef") and d["DepartmentRef"].get("value"):
            continue

        # Add DepartmentRef only if missing
        d["DepartmentRef"] = {"value": str(dept_id)}

    return je_obj


# ---------------------- Retry Wrapper ------------------
def with_retries(limiter, session, func, tokens, refresh_args, lock, *args):
    backoff = INITIAL_BACKOFF_SEC
    for _ in range(MAX_RETRIES_429 + 1):
        limiter.wait()
        try:
            return func(session, *args, access_token=tokens["access"])
        except Qbo401:
            with lock:
                tokens["access"], tokens["refresh"] = refresh_access_token(
                    session, *refresh_args, tokens["refresh"]
                )
        except Qbo429:
            time.sleep(backoff)
            backoff *= 2
    raise RuntimeError("Max retries exceeded")

# ---------------------- Worker -------------------------
def process_row(limiter, session, base, realm, tokens, refresh_args, lock, je_id, dept):
    try:
        je = with_retries(limiter, session, get_journalentry,
                          tokens, refresh_args, lock, base, realm, je_id)

        edited = set_department_on_all_lines(je, dept)

        updated = with_retries(limiter, session, update_journalentry,
                               tokens, refresh_args, lock, base, realm, edited)

        return je_id, True, updated.get("SyncToken")
    except Exception as e:
        return je_id, False, str(e)

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
        access_token, refresh_token = refresh_access_token(
            session, client_id, client_secret, refresh_token
        )

    tokens = {"access": access_token, "refresh": refresh_token}
    lock = threading.Lock()
    limiter = GlobalLimiter(MAX_RPS)

    df = pd.read_csv(CSV_PATH, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]

    if "target_id" not in df or "departmentref" not in df:
        raise RuntimeError("CSV must contain Target_Id and DepartmentRef")

    df = df.dropna(subset=["target_id", "departmentref"])
    df = df.drop_duplicates("target_id", keep="last")

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as exe:
        futures = [
            exe.submit(process_row, limiter, session, base, realm,
                       tokens, (client_id, client_secret), lock,
                       r.target_id, r.departmentref)
            for r in df.itertuples(index=False)
        ]

        for f in as_completed(futures):
            je_id, ok, msg = f.result()
            if ok:
                log_info(f"✅ JE {je_id} updated (SyncToken {msg})")
            else:
                log_err(f"❌ JE {je_id} failed: {msg}")

if __name__ == "__main__":
    main()
