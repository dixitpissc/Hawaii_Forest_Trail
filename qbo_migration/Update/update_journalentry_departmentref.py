#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fast updater: set DepartmentRef on all lines of JournalEntries from CSV.

CSV: Journal_Entry_departmentref.csv
Columns: Target_Id,DepartmentRef

ENV required:
  QBO_ACCESS_TOKEN
  QBO_REFRESH_TOKEN
  QBO_REALM_ID
  QBO_CLIENT_ID
  QBO_CLIENT_SECRET
  QBO_ENVIRONMENT     (production | sandbox)
  REDIRECT_URI        (not used here, kept for completeness)

Optional tuning:
  QBO_CONCURRENCY     (default 6)  - number of parallel workers
  QBO_MAX_RPS         (default 8)  - global max requests per second across all workers
  QBO_TIMEOUT_SEC     (default 45) - per-request timeout
  QBO_MINOR_VERSION   (default 65) - Intuit minorversion param
"""

import os
import sys
import time
import base64
import json
import math
import random
import threading
from typing import Dict, Any, Tuple, Optional, Iterable

import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

# ---------------------- Load .env ----------------------
load_dotenv()

# ---------------------- Config -------------------------
CSV_PATH = "Journal_Entry_departmentref.csv"
MINOR_VERSION = os.getenv("QBO_MINOR_VERSION", "65")
TIMEOUT_SEC = int(os.getenv("QBO_TIMEOUT_SEC", "45"))

MAX_RETRIES_429 = 6
INITIAL_BACKOFF_SEC = 1.5

CONCURRENCY = int(os.getenv("QBO_CONCURRENCY", "6"))
MAX_RPS = float(os.getenv("QBO_MAX_RPS", "8"))  # global throttle across all threads

# ---------------------- Logging ------------------------
def log_info(msg: str):
    print(f"[INFO] {msg}", flush=True)

def log_warn(msg: str):
    print(f"[WARN] {msg}", flush=True)

def log_err(msg: str):
    print(f"[ERROR] {msg}", file=sys.stderr, flush=True)

# ---------------------- OAuth --------------------------
def _intuit_token_endpoint() -> str:
    return "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

def _auth_header_basic(client_id: str, client_secret: str) -> str:
    token = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"

def refresh_access_token(
    session: requests.Session,
    client_id: str,
    client_secret: str,
    refresh_token: str
) -> Tuple[str, str]:
    url = _intuit_token_endpoint()
    headers = {
        "Authorization": _auth_header_basic(client_id, client_secret),
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    resp = session.post(url, headers=headers, data=data, timeout=TIMEOUT_SEC)
    if resp.status_code != 200:
        raise RuntimeError(f"Refresh failed ({resp.status_code}): {resp.text}")
    payload = resp.json()
    access_token = payload.get("access_token")
    new_refresh_token = payload.get("refresh_token", refresh_token)
    if not access_token:
        raise RuntimeError(f"Refresh returned no access_token: {payload}")
    return access_token, new_refresh_token

# ---------------------- QBO HTTP -----------------------
def _base_url(environment: str) -> str:
    env = (environment or "production").strip().lower()
    if env == "sandbox":
        return "https://sandbox-quickbooks.api.intuit.com"
    return "https://quickbooks.api.intuit.com"

def _qbo_headers(access_token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        # requests adds Accept-Encoding automatically; keep-alive via session
    }

class QboRateLimitError(Exception):
    pass

class QboUnauthorizedError(Exception):
    pass

def _handle_qbo_response(resp: requests.Response, expect_key: Optional[str] = None) -> Dict[str, Any]:
    status = resp.status_code
    text = resp.text

    if status == 429:
        raise QboRateLimitError(f"429 Too Many Requests: {text}")
    if status == 401:
        raise QboUnauthorizedError(f"401 Unauthorized: {text}")
    if status < 200 or status >= 300:
        raise RuntimeError(f"HTTP {status}: {text}")

    try:
        payload = resp.json()
    except Exception:
        raise RuntimeError(f"Non-JSON response ({status}): {text}")

    if isinstance(payload, dict) and "Fault" in payload:
        raise RuntimeError(f"QBO Fault: {json.dumps(payload.get('Fault'), ensure_ascii=False)}")

    if expect_key:
        if expect_key not in payload:
            raise RuntimeError(f"Expected key '{expect_key}' not in response: {json.dumps(payload, ensure_ascii=False)}")
        return payload[expect_key]
    return payload

# ---------------------- Global rate limiter -------------
class GlobalRateLimiter:
    """
    Token-bucket-ish limiter: allows ~MAX_RPS across all threads.
    """
    def __init__(self, max_rps: float):
        self.max_rps = max_rps
        self.min_interval = 1.0 / max(0.001, max_rps)
        self._lock = threading.Lock()
        self._next_time = time.perf_counter()

    def wait(self):
        with self._lock:
            now = time.perf_counter()
            if now < self._next_time:
                sleep_for = self._next_time - now
                time.sleep(sleep_for)
                now = self._next_time
            # schedule next slot with small jitter (±10%)
            jitter = random.uniform(-0.1, 0.1) * self.min_interval
            self._next_time = now + self.min_interval + jitter

# ---------------------- Business logic -----------------
def get_journalentry(
    session: requests.Session,
    base_url: str,
    realm_id: str,
    target_id: str,
    access_token: str,  # <-- moved to the end so it's filled via keyword
) -> Dict[str, Any]:
    url = f"{base_url}/v3/company/{realm_id}/journalentry/{target_id}"
    params = {"minorversion": MINOR_VERSION}
    resp = session.get(url, headers=_qbo_headers(access_token), params=params, timeout=TIMEOUT_SEC)
    return _handle_qbo_response(resp, expect_key="JournalEntry")

def full_update_journalentry(
    session: requests.Session,
    base_url: str,
    realm_id: str,
    je_body: Dict[str, Any],
    access_token: str,  # <-- moved to the end so it's filled via keyword
) -> Dict[str, Any]:
    url = f"{base_url}/v3/company/{realm_id}/journalentry"
    params = {"operation": "update", "minorversion": MINOR_VERSION}
    resp = session.post(
        url,
        headers=_qbo_headers(access_token),
        params=params,
        data=json.dumps(je_body),
        timeout=TIMEOUT_SEC,
    )
    return _handle_qbo_response(resp, expect_key="JournalEntry")

def set_department_on_all_lines(je: Dict[str, Any], department_value: str) -> Dict[str, Any]:
    # Work on a deep copy (json roundtrip is fast and safe)
    je_obj = json.loads(json.dumps(je))
    je_obj["sparse"] = False
    if not je_obj.get("Id") or je_obj.get("SyncToken") is None:
        raise ValueError("JournalEntry missing Id or SyncToken; cannot update.")

    line_list = je_obj.get("Line", [])
    if isinstance(line_list, list):
        for ln in line_list:
            jeld = ln.get("JournalEntryLineDetail")
            if isinstance(jeld, dict):
                jeld["DepartmentRef"] = {"value": str(department_value)}
    return je_obj

# ---------------------- Retry wrapper (thread-safe) ----
def do_with_retries(
    limiter: GlobalRateLimiter,
    session: requests.Session,
    func,
    tokens: Dict[str, str],
    refresh_params: Tuple[str, str],
    token_lock: threading.Lock,
    *args, **kwargs
):
    backoff = INITIAL_BACKOFF_SEC
    for attempt in range(1, MAX_RETRIES_429 + 2):
        # global throttle
        limiter.wait()
        try:
            return func(session, *args, access_token=tokens["access_token"], **kwargs)
        except QboUnauthorizedError as e:
            # refresh exactly once at a time
            with token_lock:
                log_warn(f"401 for thread; refreshing token... ({e})")
                new_access, new_refresh = refresh_access_token(session, refresh_params[0], refresh_params[1], tokens["refresh_token"])
                tokens["access_token"] = new_access
                tokens["refresh_token"] = new_refresh
            # retry immediately
            continue
        except QboRateLimitError as e:
            if attempt > MAX_RETRIES_429:
                log_err(f"429 too many times; giving up. Last error: {e}")
                raise
            sleep_for = backoff * (1.0 + random.random() * 0.25)  # add jitter
            log_warn(f"429 rate limit. Backoff {sleep_for:.2f}s (attempt {attempt}/{MAX_RETRIES_429+1})")
            time.sleep(sleep_for)
            backoff *= 2.0
            continue

# ---------------------- Worker -------------------------
def process_one_row(
    limiter: GlobalRateLimiter,
    session: requests.Session,
    base_url: str,
    realm_id: str,
    tokens: Dict[str, str],
    refresh_params: Tuple[str, str],
    token_lock: threading.Lock,
    target_id: str,
    dept_val: str,
) -> Tuple[str, bool, str]:
    """
    Returns (target_id, True/False, message)
    """
    try:
        je = do_with_retries(limiter, session, get_journalentry, tokens, refresh_params, token_lock,
                             base_url, realm_id, target_id)
        edited = set_department_on_all_lines(je, dept_val)
        updated = do_with_retries(limiter, session, full_update_journalentry, tokens, refresh_params, token_lock,
                                  base_url, realm_id, edited)
        return target_id, True, f"Updated (SyncToken {updated.get('SyncToken')})"
    except Exception as e:
        return target_id, False, str(e)

# ---------------------- Main ---------------------------
def main():
    # ENV
    access_token = os.getenv("QBO_ACCESS_TOKEN", "").strip()
    refresh_token = os.getenv("QBO_REFRESH_TOKEN", "").strip()
    realm_id = os.getenv("QBO_REALM_ID", "").strip()
    client_id = os.getenv("QBO_CLIENT_ID", "").strip()
    client_secret = os.getenv("QBO_CLIENT_SECRET", "").strip()
    environment = os.getenv("QBO_ENVIRONMENT", "production").strip().lower()

    if not (refresh_token and realm_id and client_id and client_secret):
        log_err("Missing required env vars: QBO_REFRESH_TOKEN, QBO_REALM_ID, QBO_CLIENT_ID, QBO_CLIENT_SECRET")
        sys.exit(2)

    # Session with connection pooling
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=max(10, CONCURRENCY * 2),
        pool_maxsize=max(20, CONCURRENCY * 4),
        max_retries=0,  # we do our own retries
    )
    base_url = _base_url(environment)
    session.mount(base_url, adapter)
    session.mount("https://", adapter)

    # Prefetch access token if missing
    if not access_token:
        log_info("No QBO_ACCESS_TOKEN; refreshing via refresh token...")
        access_token, refresh_token = refresh_access_token(session, client_id, client_secret, refresh_token)

    # Shared tokens + lock (for refresh rotation)
    tokens = {"access_token": access_token, "refresh_token": refresh_token}
    token_lock = threading.Lock()
    refresh_params = (client_id, client_secret)

    # Rate limiter
    limiter = GlobalRateLimiter(MAX_RPS)

    # CSV
    if not os.path.exists(CSV_PATH):
        log_err(f"CSV not found: {CSV_PATH}")
        sys.exit(3)

    df = pd.read_csv(CSV_PATH, dtype=str)
    if df.empty:
        log_warn("CSV is empty. Nothing to do.")
        return

    df.columns = [c.strip() for c in df.columns]
    cols_lower = {c.lower(): c for c in df.columns}
    if "target_id" not in cols_lower or "departmentref" not in cols_lower:
        log_err("CSV must contain columns: Target_Id, DepartmentRef")
        sys.exit(4)

    target_col = cols_lower["target_id"]
    dept_col = cols_lower["departmentref"]

    # Basic cleaning + dedupe on Target_Id (keep last occurrence)
    df[target_col] = df[target_col].astype(str).str.strip()
    df[dept_col] = df[dept_col].astype(str).str.strip()
    df = df[(df[target_col] != "") & (df[dept_col] != "")]
    df = df.drop_duplicates(subset=[target_col], keep="last").reset_index(drop=True)

    total = len(df)
    log_info(f"Loaded {total} unique Target_Id rows from {CSV_PATH}")
    log_info(f"Concurrency={CONCURRENCY}, Max RPS={MAX_RPS}, Timeout={TIMEOUT_SEC}s, MinorVersion={MINOR_VERSION}")
    log_info(f"Environment: {environment} | Base URL: {base_url}")

    success = 0
    failures = 0
    skipped = 0

    # Parallel processing
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as exe:
        futures = []
        for r in df.itertuples(index=False):
            target_id = getattr(r, target_col)
            dept_val = getattr(r, dept_col)
            futures.append(
                exe.submit(
                    process_one_row,
                    limiter, session, base_url, realm_id,
                    tokens, refresh_params, token_lock,
                    target_id, dept_val
                )
            )

        # progress loop
        done = 0
        for fut in as_completed(futures):
            done += 1
            target_id, ok, msg = fut.result()
            if ok:
                success += 1
                log_info(f"[{done}/{total}] ✅ JE {target_id}: {msg}")
            else:
                failures += 1
                log_err(f"[{done}/{total}] ❌ JE {target_id}: {msg}")

    # Summary
    log_info("=== Summary ===")
    log_info(f"Total rows:   {total}")
    log_info(f"Success:      {success}")
    log_info(f"Skipped:      {skipped}")
    log_info(f"Failures:     {failures}")

    # Print rotated refresh token if changed
    if tokens["refresh_token"] != os.getenv("QBO_REFRESH_TOKEN", "").strip():
        log_warn("Refresh token was rotated by Intuit during this run. Persist the NEW refresh token securely:")
        print(tokens["refresh_token"])

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log_warn("Interrupted by user.")
        sys.exit(130)



# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# """
# Update JournalEntry line DepartmentRef values in QBO from a CSV mapping.

# CSV: Journal_Entry_departmentref.csv
# Columns: Target_Id,DepartmentRef    (e.g., 37988,975)

# ENV required:
#   QBO_ACCESS_TOKEN
#   QBO_REFRESH_TOKEN
#   QBO_REALM_ID
#   QBO_CLIENT_ID
#   QBO_CLIENT_SECRET
#   QBO_ENVIRONMENT   (production | sandbox)
#   REDIRECT_URI      (not used for refresh, but kept for completeness)

# Behavior:
#   - For each row: GET /journalentry/{Target_Id}
#   - Add/overwrite DepartmentRef on each line with JournalEntryLineDetail
#   - POST ?operation=update with Id, SyncToken, full Line array (sparse=false)
#   - Retries on 401 (after refresh) and 429 (exponential backoff)
# """

# import os
# import sys
# import time
# import base64
# import json
# from typing import Dict, Any, Tuple, Optional

# import requests
# import pandas as pd
# from dotenv import load_dotenv

# load_dotenv()
# # --- Config ------------------------------------------------------------------

# CSV_PATH = "Journal_Entry_departmentref.csv"
# MINOR_VERSION = "65"  # safe default; adjust if your QBO account needs a different minor version

# # Backoff / retry
# MAX_RETRIES_429 = 5
# INITIAL_BACKOFF_SEC = 2.0
# TIMEOUT_SEC = 60

# # --- Logging helpers ---------------------------------------------------------

# def log_info(msg: str):
#     print(f"[INFO] {msg}", flush=True)

# def log_warn(msg: str):
#     print(f"[WARN] {msg}", flush=True)

# def log_err(msg: str):
#     print(f"[ERROR] {msg}", file=sys.stderr, flush=True)

# # --- OAuth helpers -----------------------------------------------------------

# def _intuit_token_endpoint() -> str:
#     # Same endpoint for sandbox/production
#     return "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

# def _auth_header_basic(client_id: str, client_secret: str) -> str:
#     token = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
#     return f"Basic {token}"

# def refresh_access_token(
#     client_id: str,
#     client_secret: str,
#     refresh_token: str
# ) -> Tuple[str, str]:
#     """
#     Returns (access_token, new_refresh_token).
#     Intuit may rotate the refresh token. Persist it somewhere safe if needed.
#     """
#     url = _intuit_token_endpoint()
#     headers = {
#         "Authorization": _auth_header_basic(client_id, client_secret),
#         "Accept": "application/json",
#         "Content-Type": "application/x-www-form-urlencoded"
#     }
#     data = {
#         "grant_type": "refresh_token",
#         "refresh_token": refresh_token
#     }
#     resp = requests.post(url, headers=headers, data=data, timeout=TIMEOUT_SEC)
#     if resp.status_code != 200:
#         raise RuntimeError(f"Refresh failed ({resp.status_code}): {resp.text}")
#     payload = resp.json()
#     access_token = payload.get("access_token")
#     new_refresh_token = payload.get("refresh_token", refresh_token)
#     if not access_token:
#         raise RuntimeError(f"Refresh returned no access_token: {payload}")
#     return access_token, new_refresh_token

# # --- QBO helpers -------------------------------------------------------------

# def _base_url(environment: str) -> str:
#     env = (environment or "production").strip().lower()
#     if env == "sandbox":
#         return "https://sandbox-quickbooks.api.intuit.com"
#     # default to production
#     return "https://quickbooks.api.intuit.com"

# def _qbo_headers(access_token: str) -> Dict[str, str]:
#     return {
#         "Authorization": f"Bearer {access_token}",
#         "Accept": "application/json",
#         "Content-Type": "application/json"
#     }

# def get_journalentry(
#     base_url: str,
#     realm_id: str,
#     access_token: str,
#     target_id: str
# ) -> Dict[str, Any]:
#     url = f"{base_url}/v3/company/{realm_id}/journalentry/{target_id}"
#     params = {"minorversion": MINOR_VERSION}
#     resp = requests.get(url, headers=_qbo_headers(access_token), params=params, timeout=TIMEOUT_SEC)
#     return _handle_qbo_response(resp, expect_key="JournalEntry")

# def full_update_journalentry(
#     base_url: str,
#     realm_id: str,
#     access_token: str,
#     je_body: Dict[str, Any]
# ) -> Dict[str, Any]:
#     """
#     Performs Full Update (operation=update). Expects full object (sparse=false).
#     """
#     url = f"{base_url}/v3/company/{realm_id}/journalentry"
#     params = {"operation": "update", "minorversion": MINOR_VERSION}
#     resp = requests.post(url, headers=_qbo_headers(access_token), params=params, data=json.dumps(je_body), timeout=TIMEOUT_SEC)
#     return _handle_qbo_response(resp, expect_key="JournalEntry")

# def _handle_qbo_response(resp: requests.Response, expect_key: Optional[str] = None) -> Dict[str, Any]:
#     """
#     Handles QBO JSON response:
#       - OK: returns resp.json()[expect_key] if provided, else full JSON
#       - 429: raises a special exception to trigger backoff
#       - 401: raises for caller to handle refresh
#       - Fault: raises with detailed message
#     """
#     status = resp.status_code
#     text = resp.text

#     # Throttling
#     if status == 429:
#         raise QboRateLimitError(f"429 Too Many Requests: {text}")

#     # Unauthorized
#     if status == 401:
#         raise QboUnauthorizedError(f"401 Unauthorized: {text}")

#     # Other non-OK
#     if status < 200 or status >= 300:
#         raise RuntimeError(f"HTTP {status}: {text}")

#     try:
#         payload = resp.json()
#     except Exception:
#         raise RuntimeError(f"Non-JSON response ({status}): {text}")

#     # QBO Faults
#     if isinstance(payload, dict) and "Fault" in payload:
#         raise RuntimeError(f"QBO Fault: {json.dumps(payload.get('Fault'), ensure_ascii=False)}")

#     if expect_key:
#         # QBO returns {"JournalEntry":{...},"time":"..."} on success
#         if expect_key not in payload:
#             raise RuntimeError(f"Expected key '{expect_key}' not in response: {json.dumps(payload, ensure_ascii=False)}")
#         return payload[expect_key]

#     return payload

# class QboRateLimitError(Exception):
#     pass

# class QboUnauthorizedError(Exception):
#     pass

# # --- Business logic ----------------------------------------------------------

# def set_department_on_all_lines(je: Dict[str, Any], department_value: str) -> Dict[str, Any]:
#     """
#     Returns a deep-updated copy of JE with DepartmentRef set on each line that has JournalEntryLineDetail.
#     """
#     # Work on a copy
#     je_obj = json.loads(json.dumps(je))

#     # Ensure full-update intent
#     je_obj["sparse"] = False

#     # Sanity: Id and SyncToken must exist for update
#     if not je_obj.get("Id") or je_obj.get("SyncToken") is None:
#         raise ValueError("JournalEntry missing Id or SyncToken; cannot update.")

#     line_list = je_obj.get("Line", [])
#     if not isinstance(line_list, list):
#         return je_obj

#     for ln in line_list:
#         jeld = ln.get("JournalEntryLineDetail")
#         if jeld is not None and isinstance(jeld, dict):
#             jeld["DepartmentRef"] = {"value": str(department_value)}

#     return je_obj

# def do_with_retries(func, *args, **kwargs):
#     """
#     Wrap a QBO call with 401 refresh handling and 429 exponential backoff.
#     Expects kwargs to include tokens dict for mutation on refresh:
#       kwargs["_tokens"] = {"access_token": "...", "refresh_token": "...", ...}
#       kwargs["_refresh_params"] = (client_id, client_secret)
#     """
#     tokens = kwargs.pop("_tokens", None)
#     refresh_params = kwargs.pop("_refresh_params", None)
#     assert tokens and refresh_params, "do_with_retries requires _tokens and _refresh_params"

#     backoff = INITIAL_BACKOFF_SEC
#     for attempt in range(1, MAX_RETRIES_429 + 2):  # e.g., up to 6 tries (1 initial + 5 retries)
#         try:
#             return func(*args, **kwargs, access_token=tokens["access_token"])
#         except QboUnauthorizedError as e:
#             log_warn(f"401 encountered. Attempting token refresh... ({e})")
#             # refresh once
#             new_access, new_refresh = refresh_access_token(refresh_params[0], refresh_params[1], tokens["refresh_token"])
#             tokens["access_token"] = new_access
#             tokens["refresh_token"] = new_refresh
#             # retry immediately after refresh
#             continue
#         except QboRateLimitError as e:
#             if attempt > MAX_RETRIES_429:
#                 log_err(f"Hit 429 rate limit too many times. Giving up. Last error: {e}")
#                 raise
#             log_warn(f"429 rate limited. Backing off for {backoff:.1f}s (attempt {attempt}/{MAX_RETRIES_429+1})")
#             time.sleep(backoff)
#             backoff *= 2.0  # exponential
#             continue

# def main():
#     # --- Load ENV ---
#     access_token = os.getenv("QBO_ACCESS_TOKEN", "").strip()
#     refresh_token = os.getenv("QBO_REFRESH_TOKEN", "").strip()
#     realm_id = os.getenv("QBO_REALM_ID", "").strip()
#     client_id = os.getenv("QBO_CLIENT_ID", "").strip()
#     client_secret = os.getenv("QBO_CLIENT_SECRET", "").strip()
#     environment = os.getenv("QBO_ENVIRONMENT", "production").strip().lower()

#     if not (refresh_token and realm_id and client_id and client_secret):
#         log_err("Missing required env vars: QBO_REFRESH_TOKEN, QBO_REALM_ID, QBO_CLIENT_ID, QBO_CLIENT_SECRET")
#         sys.exit(2)

#     # If no access_token present, try refreshing first
#     if not access_token:
#         log_info("No QBO_ACCESS_TOKEN provided; refreshing using refresh token...")
#         access_token, refresh_token = refresh_access_token(client_id, client_secret, refresh_token)

#     base_url = _base_url(environment)
#     log_info(f"Environment: {environment} | Base URL: {base_url}")

#     # Prepare token bundle for retry wrapper
#     tokens = {"access_token": access_token, "refresh_token": refresh_token}
#     refresh_params = (client_id, client_secret)

#     # --- Read CSV ---
#     if not os.path.exists(CSV_PATH):
#         log_err(f"CSV not found: {CSV_PATH}")
#         sys.exit(3)

#     df = pd.read_csv(CSV_PATH, dtype=str).fillna("")
#     # Normalize columns
#     cols_lower = {c.lower(): c for c in df.columns}
#     if "target_id" not in cols_lower or "departmentref" not in cols_lower:
#         log_err("CSV must contain columns: Target_Id, DepartmentRef")
#         sys.exit(4)

#     target_col = cols_lower["target_id"]
#     dept_col = cols_lower["departmentref"]

#     total = len(df)
#     log_info(f"Loaded {total} rows from {CSV_PATH}")

#     success = 0
#     skipped = 0
#     failures = 0

#     for idx, row in df.iterrows():
#         target_id = (row.get(target_col) or "").strip()
#         dept_val = (row.get(dept_col) or "").strip()

#         if not target_id or not dept_val:
#             log_warn(f"[Row {idx+1}] Missing Target_Id or DepartmentRef. Skipping.")
#             skipped += 1
#             continue

#         log_info(f"[{idx+1}/{total}] Processing JE Id={target_id} → DepartmentRef={dept_val}")

#         try:
#             # GET current JE
#             je = do_with_retries(
#                 get_journalentry,
#                 _tokens=tokens,
#                 _refresh_params=refresh_params,
#                 base_url=base_url,
#                 realm_id=realm_id,
#                 target_id=target_id,
#             )

#             # Modify lines
#             edited = set_department_on_all_lines(je, dept_val)

#             # Full Update
#             updated = do_with_retries(
#                 full_update_journalentry,
#                 _tokens=tokens,
#                 _refresh_params=refresh_params,
#                 base_url=base_url,
#                 realm_id=realm_id,
#                 je_body=edited,
#             )

#             log_info(f"✅ Updated JE {updated.get('Id')} (SyncToken {updated.get('SyncToken')})")
#             success += 1

#         except Exception as e:
#             log_err(f"❌ Failed for JE {target_id}: {e}")
#             failures += 1

#     # Final summary
#     log_info("=== Summary ===")
#     log_info(f"Total rows:   {total}")
#     log_info(f"Success:      {success}")
#     log_info(f"Skipped:      {skipped}")
#     log_info(f"Failures:     {failures}")

#     # If the refresh token rotated, let the user know (so they can persist it)
#     if tokens["refresh_token"] != os.getenv("QBO_REFRESH_TOKEN", "").strip():
#         log_warn("Refresh token was rotated by Intuit during this run.")
#         log_warn("Please persist the NEW refresh token securely for future runs:")
#         print(tokens["refresh_token"])

# if __name__ == "__main__":
#     try:
#         main()
#     except KeyboardInterrupt:
#         log_warn("Interrupted by user.")
#         sys.exit(130)
