#!/usr/bin/env python3
"""
qbo_customer_rename_twophase.py

Two-phase customer rename:
  Phase A: Pre-check all rows; if NewName already exists in QBO -> skip row.
  Phase B: For remaining rows, find exact Old Name and rename to NewName.

CSV columns (header row required):
  Old Name,NewName   (flexible: accepts 'OldName' and 'New Name' variants)

Environment (required):
  QBO_CLIENT_ID=
  QBO_CLIENT_SECRET=
  QBO_ENVIRONMENT=production   # or 'sandbox'
  REDIRECT_URI=https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl
  QBO_ACCESS_TOKEN=
  QBO_REFRESH_TOKEN=
  QBO_REALM_ID=

Optional:
  QBO_MINOR_VERSION=75
  QBO_TIMEOUT_SECS=30
"""

import os
import sys
import time
import json
import base64
import logging
from typing import Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
import re

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s - %(message)s",
)
log = logging.getLogger("qbo_rename_2phase")

# Reduce logging verbosity
log.setLevel(logging.WARNING)

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()

QBO_CLIENT_ID       = os.getenv("QBO_CLIENT_ID") or ""
QBO_CLIENT_SECRET   = os.getenv("QBO_CLIENT_SECRET") or ""
QBO_ENVIRONMENT     = (os.getenv("QBO_ENVIRONMENT") or "production").lower()
REDIRECT_URI        = os.getenv("REDIRECT_URI") or ""
QBO_ACCESS_TOKEN    = os.getenv("QBO_ACCESS_TOKEN") or ""
QBO_REFRESH_TOKEN   = os.getenv("QBO_REFRESH_TOKEN") or ""
QBO_REALM_ID        = os.getenv("QBO_REALM_ID") or ""

MINOR_VERSION       = os.getenv("QBO_MINOR_VERSION") or "75"
TIMEOUT_SECS        = float(os.getenv("QBO_TIMEOUT_SECS") or "30")

if QBO_ENVIRONMENT not in {"production", "sandbox"}:
    log.warning("QBO_ENVIRONMENT should be 'production' or 'sandbox'; defaulting to 'production'.")
    QBO_ENVIRONMENT = "production"

if QBO_ENVIRONMENT == "production":
    QBO_BASE = "https://quickbooks.api.intuit.com"
else:
    QBO_BASE = "https://sandbox-quickbooks.api.intuit.com"

TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
SESSION = requests.Session()

def _sleep_backoff(attempt: int):
    time.sleep(min(10, 2 ** attempt))

# ──────────────────────────────────────────────────────────────────────────────
# OAuth helpers
# ──────────────────────────────────────────────────────────────────────────────
def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")

def refresh_access_token() -> Tuple[str, str]:
    headers = {
        "Authorization": _basic_auth_header(QBO_CLIENT_ID, QBO_CLIENT_SECRET),
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {"grant_type": "refresh_token", "refresh_token": _refresh_token_cache}
    resp = SESSION.post(TOKEN_URL, headers=headers, data=data, timeout=TIMEOUT_SECS)
    if resp.status_code != 200:
        raise RuntimeError(f"Token refresh failed {resp.status_code}: {resp.text}")
    payload = resp.json()
    at = payload.get("access_token")
    rt = payload.get("refresh_token", _refresh_token_cache)
    if not at:
        raise RuntimeError(f"Token refresh returned no access_token: {payload}")
    return at, rt

def auth_headers() -> dict:
    return {"Authorization": f"Bearer {get_access_token()}"}

_access_token_cache = QBO_ACCESS_TOKEN
_refresh_token_cache = QBO_REFRESH_TOKEN

def get_access_token() -> str:
    return _access_token_cache

def set_tokens(new_at: str, new_rt: Optional[str] = None):
    global _access_token_cache, _refresh_token_cache
    _access_token_cache = new_at
    if new_rt:
        _refresh_token_cache = new_rt

# ──────────────────────────────────────────────────────────────────────────────
# QBO API helpers
# ──────────────────────────────────────────────────────────────────────────────
def _escape_sql_literal(value: str) -> str:
    if value is None:
        return ""
    cleaned = ''.join(ch for ch in value if ch >= ' ')
    cleaned = cleaned.replace("\\", "\\\\")   # escape backslashes first
    cleaned = cleaned.replace("'", r"\'")     # QBO wants backslash before apostrophes
    return cleaned





def _normalize_name(value: str) -> str:
    """Normalize a display name for robust equality checks in Python.
    Lowercase, collapse whitespace and remove punctuation to compare "logical" equality.
    """
    if value is None:
        return ""
    # lowercase
    s = value.lower()
    # replace all non-alphanumeric characters with a single space
    s = re.sub(r"[^0-9a-z]+", " ", s)
    # collapse whitespace
    s = " ".join(s.split())
    return s

import urllib.parse as _url

def _qbo_query(sql: str) -> dict:
    """
    Execute a QBO SQL-like query and return JSON.
    Strategy:
      - Try POST with application/text (best for most realms)
      - If 400 QueryParserError, fall back to GET /query?query=...
      - Auto-refreshes token on 401 once
    """
    base = f"{QBO_BASE}/v3/company/{QBO_REALM_ID}/query?minorversion={MINOR_VERSION}"

    def _post(sql_text: str) -> requests.Response:
        headers = {
            **auth_headers(),
            "Accept": "application/json",
            # 'application/text' tends to be most compatible with QBO for SQL body
            "Content-Type": "application/text",
        }
        return SESSION.post(base, headers=headers, data=sql_text, timeout=TIMEOUT_SECS)

    def _get(sql_text: str) -> requests.Response:
        headers = {
            **auth_headers(),
            "Accept": "application/json",
        }
        # URL-encode the query in the 'query=' param
        q = _url.urlencode({"query": sql_text})
        return SESSION.get(f"{base}&{q}", headers=headers, timeout=TIMEOUT_SECS)

    last_resp = None
    for attempt in range(0, 2):  # 0: first try; 1: after optional token refresh
        try:
            log.info(f"QBO QUERY SQL => {sql}")
            # 1) Try POST
            resp = _post(sql)
            last_resp = resp

            # Token refresh once on 401
            if resp.status_code == 401 and attempt == 0:
                log.info("401 on query; refreshing token and retrying once...")
                at, rt = refresh_access_token()
                set_tokens(at, rt)
                continue

            # If 400 parser error, try GET fallback immediately
            if resp.status_code == 400 and "QueryParserError" in (resp.text or ""):
                log.warning("400 QueryParserError on POST — trying GET fallback...")
                resp = _get(sql)
                last_resp = resp
                if resp.status_code == 401 and attempt == 0:
                    log.info("401 on GET fallback; refreshing token and retrying once...")
                    at, rt = refresh_access_token()
                    set_tokens(at, rt)
                    # re-run loop to try POST again which will then fall back to GET if needed
                    continue

            # Handle transient
            if resp.status_code in (429, 500, 502, 503, 504):
                log.warning(f"Query transient error {resp.status_code}; backing off and retrying...")
                _sleep_backoff(1 + attempt)
                continue

            # Raise for non-2xx
            resp.raise_for_status()
            return resp.json()

        except requests.HTTPError as http_e:
            body = ""
            status = 'NA'
            try:
                status = http_e.response.status_code if http_e.response is not None else 'NA'
                body = (http_e.response.text or "")[:1000] if http_e.response is not None else ""
            except Exception:
                pass
            raise RuntimeError(f"_qbo_query HTTP {status}: {body} -- SQL: {sql}") from None
        except Exception as e:
            if attempt == 0:
                log.warning(f"_qbo_query exception ({e}); retrying...")
                _sleep_backoff(1)
                continue
            raise

    # Shouldn’t get here
    raise RuntimeError(f"Query failed after retries: {last_resp.status_code if last_resp else 'NA'} "
                       f"{(last_resp.text[:500] if last_resp and last_resp.text else '')}")


def _qbo_update_customer_sparse(customer_id: str, sync_token: str, new_display_name: Optional[str], new_company_name: Optional[str]) -> dict:
    url = f"{QBO_BASE}/v3/company/{QBO_REALM_ID}/customer?minorversion={MINOR_VERSION}"
    headers = {**auth_headers(), "Accept": "application/json", "Content-Type": "application/json"}
    payload = {"sparse": True, "Id": str(customer_id), "SyncToken": str(sync_token)}

    if new_display_name:
        payload["DisplayName"] = new_display_name
    if new_company_name:
        payload["CompanyName"] = new_company_name

    body = json.dumps(payload)
    for attempt in range(0, 3):
        resp = SESSION.post(url, headers=headers, data=body, timeout=TIMEOUT_SECS)
        if resp.status_code == 401 and attempt == 0:
            log.info("401 on update; refreshing token and retrying once...")
            at, rt = refresh_access_token()
            set_tokens(at, rt)
            headers["Authorization"] = f"Bearer {get_access_token()}"
            continue
        if resp.status_code in (429, 500, 502, 503, 504):
            log.warning(f"Update transient error {resp.status_code}; attempt {attempt+1}/3; backing off...")
            _sleep_backoff(attempt+1)
            continue
        if resp.status_code >= 300:
            raise requests.HTTPError(f"{resp.status_code}: {resp.text}", response=resp)
        return resp.json()
    raise RuntimeError(f"Update failed after retries: {resp.status_code} {resp.text[:500]}")

def _qbo_select_customer_eq(field: str, value: str) -> list[dict]:
    lit = _escape_sql_literal(value)
    sql = (
        "SELECT Id, SyncToken, CompanyName, FullyQualifiedName, Active "
        "FROM Customer "
        f"WHERE {field} = '{lit}' "
        "STARTPOSITION 1 MAXRESULTS 100"
    )
    data = _qbo_query(sql)
    return data.get("QueryResponse", {}).get("Customer", []) or []


def find_customer_by_exact_name(name: str, field: str) -> list[dict]:
    """
    Return customers whose specified field equals `name`.
    To avoid QBO QueryParser quirks, we issue two separate equality queries instead of using OR.
    """
    if not name:
        return []

    lit = _escape_sql_literal(name)
    norm_target = _normalize_name(name)

    def _run(sql: str) -> list[dict]:
        data = _qbo_query(sql)
        return data.get("QueryResponse", {}).get("Customer", []) or []

    # Query based on the dynamic field
    sql = (
        "SELECT Id, SyncToken, CompanyName, FullyQualifiedName, Active "
        "FROM Customer "
        f"WHERE {field} = '{lit}' "
        "STARTPOSITION 1 MAXRESULTS 100"
    )
    custs = _run(sql)

    # Normalize to ensure strict match
    matches = [
        c for c in custs
        if _normalize_name(c.get(field)) == norm_target
    ]
    return matches


# ──────────────────────────────────────────────────────────────────────────────
# Two-phase logic
# ──────────────────────────────────────────────────────────────────────────────
def phase_a_precheck_newnames(rows: list[dict], update_field: str) -> list[dict]:
    """
    For each row, check if NewName already exists in QBO.
    Return list of result dicts (some marked 'SKIP_NEWINAME_PRESENT').
    """
    results = []
    for idx, r in enumerate(rows, 1):
        old_name = r["OldName"]
        new_name = r["NewName"]
        if not new_name:
            results.append({
                "OldName": old_name, "NewName": new_name,
                "status": "SKIP_BAD_ROW", "details": "Blank NewName", "phase": "A", "customer_id": None
            })
            print(f"[Phase A] {old_name} -> {new_name}: SKIP_BAD_ROW (Blank NewName)")
            continue
        try:
            matches = find_customer_by_exact_name(new_name, update_field)
            if matches:
                cust_id = None
                same_customer = False
                try:
                    old_matches = find_customer_by_exact_name(old_name, update_field)
                    if old_matches and len(old_matches) == 1 and len(matches) == 1:
                        old_match = old_matches[0]
                        new_match = matches[0]
                        if str(old_match.get("Id")) == str(new_match.get("Id")):
                            same_customer = True
                            cust_id = str(new_match.get("Id"))
                except Exception:
                    pass

                if same_customer:
                    results.append({
                        "OldName": old_name, "NewName": new_name,
                        "status": "SKIP_ALREADY_SET",
                        "details": "Old and New resolve to the same customer.",
                        "phase": "A", "customer_id": cust_id
                    })
                    print(f"[Phase A] {old_name} -> {new_name}: SKIP_ALREADY_SET (Same customer)")
                else:
                    results.append({
                        "OldName": old_name, "NewName": new_name,
                        "status": "SKIP_NEWINAME_PRESENT",
                        "details": f"NewName already exists in QBO (matches={len(matches)}).",
                        "phase": "A", "customer_id": str(matches[0].get("Id")) if matches else None
                    })
                    print(f"[Phase A] {old_name} -> {new_name}: SKIP_NEWINAME_PRESENT (NewName exists)")
            else:
                results.append({
                    "OldName": old_name, "NewName": new_name,
                    "status": "OK_FOR_PHASE_B",
                    "details": "NewName not present; candidate for rename.",
                    "phase": "A", "customer_id": None
                })
                print(f"[Phase A] {old_name} -> {new_name}: OK_FOR_PHASE_B (Ready for rename)")
        except Exception as e:
            results.append({
                "OldName": old_name, "NewName": new_name,
                "status": "FAILED_A_EXCEPTION", "details": repr(e), "phase": "A", "customer_id": None
            })
            print(f"[Phase A] {old_name} -> {new_name}: FAILED_A_EXCEPTION ({repr(e)})")
    return results

def phase_b_rename_old_to_new(rows_a: list[dict], update_field: str) -> list[dict]:
    """
    For rows that passed Phase A (OK_FOR_PHASE_B), attempt the actual rename.
    """
    results_b = []
    candidates = [r for r in rows_a if r["status"] == "OK_FOR_PHASE_B"]
    total = len(candidates)
    for i, r in enumerate(candidates, 1):
        old_name = r["OldName"]
        new_name = r["NewName"]
        try:
            matches = find_customer_by_exact_name(old_name, update_field)
            if not matches:
                results_b.append({
                    "OldName": old_name, "NewName": new_name,
                    "status": "SKIP_NOT_FOUND_OLD",
                    "details": "No customer matched exact Old Name.", "phase": "B", "customer_id": None
                })
                print(f"[Phase B] {old_name} -> {new_name}: SKIP_NOT_FOUND_OLD (Old Name not found)")
                continue
            if len(matches) > 1:
                exact_matches = [
                    c for c in matches
                    if old_name in (c.get("DisplayName", ""), c.get("CompanyName", ""))
                ]
                if len(exact_matches) == 1:
                    matches = exact_matches
                else:
                    results_b.append({
                        "OldName": old_name, "NewName": new_name,
                        "status": "AMBIGUOUS_OLD",
                        "details": f"Multiple matches ({len(matches)}) for Old Name.", "phase": "B", "customer_id": None
                    })
                    print(f"[Phase B] {old_name} -> {new_name}: AMBIGUOUS_OLD (Multiple matches)")
                    continue

            cust = matches[0]
            cid = str(cust.get("Id"))
            sync = str(cust.get("SyncToken", "0"))
            current_field_value = cust.get(update_field, "")

            if old_name not in current_field_value:
                results_b.append({
                    "OldName": old_name, "NewName": new_name,
                    "status": "SKIP_ALREADY_SET",
                    "details": f"{update_field} does not contain Old Name.", "phase": "B", "customer_id": cid
                })
                print(f"[Phase B] {old_name} -> {new_name}: SKIP_ALREADY_SET (No update needed)")
                continue

            try:
                _ = _qbo_update_customer_sparse(
                    cid, sync,
                    new_display_name=new_name if update_field == "DisplayName" else None,
                    new_company_name=new_name if update_field == "CompanyName" else None
                )
                results_b.append({
                    "OldName": old_name, "NewName": new_name,
                    "status": "UPDATED",
                    "details": "Rename successful.", "phase": "B", "customer_id": cid
                })
                print(f"[Phase B] {old_name} -> {new_name}: UPDATED (Rename successful)")
            except requests.HTTPError as e:
                text = e.response.text if e.response is not None else str(e)
                if "Duplicate Name Exists Error" in text or "Duplicate name" in text:
                    status = "FAILED_DUPLICATE"
                else:
                    status = "FAILED_HTTP"
                results_b.append({
                    "OldName": old_name, "NewName": new_name,
                    "status": status, "details": text[:600], "phase": "B", "customer_id": cid
                })
                print(f"[Phase B] {old_name} -> {new_name}: {status} (HTTP Error)")
        except Exception as e:
            results_b.append({
                "OldName": old_name, "NewName": new_name,
                "status": "FAILED_B_EXCEPTION", "details": repr(e), "phase": "B", "customer_id": None
            })
            print(f"[Phase B] {old_name} -> {new_name}: FAILED_B_EXCEPTION ({repr(e)})")
    return results_b

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def validate_env():
    missing = []
    for k in [
        "QBO_CLIENT_ID", "QBO_CLIENT_SECRET", "QBO_ENVIRONMENT",
        "REDIRECT_URI", "QBO_ACCESS_TOKEN", "QBO_REFRESH_TOKEN", "QBO_REALM_ID"
    ]:
        if not os.getenv(k):
            missing.append(k)
    if missing:
        raise SystemExit(f"Missing required env vars: {', '.join(missing)}")

def _pick_col(df: pd.DataFrame, *names) -> Optional[str]:
    for n in names:
        if n in df.columns:
            return n
    return None

# Add a parameter to control whether to update DisplayName or CompanyName
def main(update_field: str = "DisplayName"):
    """
    Main function to control the update process.
    :param update_field: Field to update, either "DisplayName" or "CompanyName".
    """
    if update_field not in {"DisplayName", "CompanyName"}:
        raise ValueError("update_field must be either 'DisplayName' or 'CompanyName'")

    if len(sys.argv) < 2:
        print("Usage: python qbo_customer_rename_twophase.py <csv_path>")
        sys.exit(2)

    validate_env()

    csv_path = sys.argv[1]
    if not os.path.exists(csv_path):
        raise SystemExit(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path, dtype=str).fillna("")
    col_old = _pick_col(df, "Old Name", "OldName")
    col_new = _pick_col(df, "NewName", "New Name")
    if not col_old or not col_new:
        raise SystemExit("CSV must contain headers 'Old Name' and 'NewName' (case/space variants allowed).")

    rows = [{"OldName": (r.get(col_old) or "").strip(), "NewName": (r.get(col_new) or "").strip()}
            for _, r in df.iterrows()]

    # Phase A
    log.warning(f"Phase A: pre-check {len(rows)} row(s) for existing NewName...")
    results_a = phase_a_precheck_newnames(rows, update_field)

    # Phase B
    log.warning("Phase B: rename only rows cleared by Phase A...")
    results_b = phase_b_rename_old_to_new(results_a, update_field)

    # Merge results: include all Phase A rows; for those with Phase B outcome, prefer B result
    key = lambda d: (d["OldName"], d["NewName"])
    b_map = {key(r): r for r in results_b}
    final = []
    for ra in results_a:
        fb = b_map.get(key(ra))
        final.append(fb if fb is not None else ra)

    out_df = pd.DataFrame(final)
    out_path = os.path.splitext(csv_path)[0] + "_rename_results.csv"
    out_df.to_csv(out_path, index=False)

    # Summary
    updated = (out_df["status"] == "UPDATED").sum()
    skipped_new = (out_df["status"] == "SKIP_NEWINAME_PRESENT").sum()
    skipped_other = (out_df["status"].isin(["SKIP_BAD_ROW", "SKIP_NOT_FOUND_OLD", "SKIP_ALREADY_SET"])).sum()
    ambiguous = (out_df["status"] == "AMBIGUOUS_OLD").sum()
    failed = len(out_df) - updated - skipped_new - skipped_other - ambiguous

    log.warning("Summary:")
    log.warning(f"  UPDATED                 : {updated}")
    log.warning(f"  SKIP_NEWINAME_PRESENT   : {skipped_new}")
    log.warning(f"  SKIP_OTHER              : {skipped_other}")
    log.warning(f"  AMBIGUOUS_OLD           : {ambiguous}")
    log.warning(f"  FAILED                  : {failed}")
    log.warning(f"Wrote results CSV: {out_path}")


def _quick_self_test():
    """
    Quick 3-step health check: realm, token, query path.
    Raises with explicit error if anything's wrong.
    """
    log.info("🔎 Self-test: checking env & token/realm...")
    for k in ["QBO_CLIENT_ID","QBO_CLIENT_SECRET","QBO_REALM_ID","QBO_ACCESS_TOKEN","QBO_REFRESH_TOKEN"]:
        if not os.getenv(k):
            raise SystemExit(f"Missing env var: {k}")

    # Minimal query to prove tokens/realm are valid
    try:
        _ = _qbo_query("SELECT Id FROM CompanyInfo")  # cheap & universal
        log.info("✅ Query succeeded: tokens/realm look OK.")
    except Exception as e:
        raise SystemExit(f"❌ Self-test failed: {e}")


if __name__ == "__main__":
    set_tokens(QBO_ACCESS_TOKEN, QBO_REFRESH_TOKEN)
    # _quick_self_test()  # ← add this line temporarily
    main()

