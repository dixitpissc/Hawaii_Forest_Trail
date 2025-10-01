#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
QBO Company + Tax Mode Inspector
Inputs: AccessToken (OAuth2 bearer), RealmId (company id)
What it does:
  - Reads CompanyInfo
  - Reads Preferences (TaxPrefs) and determines if AST is enabled
  - Lists key tax flags (PartnerTaxEnabled, UsingSalesTax)
  - Counts TaxAgency / TaxRate / TaxCode via SELECT COUNT(*)
  - Prints guidance on what you should or should not see in the UI

Usage:
  - Put your creds in the variables at the bottom (ACCESS_TOKEN, REALM_ID)
  - Run: python qbo_tax_mode_check.py
"""

import json
import time
import sys
from typing import Any, Dict, Optional

import requests

BASE_HOST = "https://quickbooks.api.intuit.com"
MINOR_VERSION = "75"  # safe modern minorversion; adjust if you need a specific one
TIMEOUT = 30
RETRY_COUNT = 3
RETRY_BACKOFF = 1.5


class QBOClient:
    def __init__(self, access_token: str, realm_id: str):
        if not access_token or not realm_id:
            raise ValueError("AccessToken and RealmId are required.")

        self.access_token = access_token.strip()
        self.realm_id = realm_id.strip()
        self.session = requests.Session()
        self.base_url = f"{BASE_HOST}/v3/company/{self.realm_id}"

    @property
    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

    def _request(self, method: str, url: str, *, params=None, json_body=None) -> requests.Response:
        last_exc = None
        for attempt in range(1, RETRY_COUNT + 1):
            try:
                resp = self.session.request(
                    method=method.upper(),
                    url=url,
                    headers=self._headers,
                    params=params,
                    json=json_body,
                    timeout=TIMEOUT,
                )
                # Retry on 429/5xx
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"Transient HTTP {resp.status_code}", response=resp)
                return resp
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
                last_exc = e
                if attempt < RETRY_COUNT:
                    time.sleep(RETRY_BACKOFF ** attempt)
                else:
                    raise
        # Should not reach here
        raise last_exc  # type: ignore

    def query(self, sql: str) -> Dict[str, Any]:
        url = f"{self.base_url}/query"
        params = {"query": sql, "minorversion": MINOR_VERSION}
        resp = self._request("GET", url, params=params)
        self._raise_for_qbo_error(resp)
        return resp.json().get("QueryResponse", {}) or {}

    def read_company_info(self) -> Dict[str, Any]:
        # Using the query endpoint; you could also do /companyinfo/{realmId}
        data = self.query("select * from CompanyInfo")
        items = data.get("CompanyInfo", [])
        return items[0] if items else {}

    def read_preferences(self) -> Dict[str, Any]:
        data = self.query("select * from Preferences")
        items = data.get("Preferences", [])
        return items[0] if items else {}

    @staticmethod
    def _raise_for_qbo_error(resp: requests.Response) -> None:
        """
        Raise helpful errors for 401/403 and QBO error payloads.
        """
        if resp.status_code == 401:
            raise RuntimeError("401 Unauthorized: Access token expired/invalid or wrong realm.")
        if resp.status_code == 403:
            raise RuntimeError("403 Forbidden: App not authorized for this realm or missing scopes.")
        # Try to parse QBO fault
        try:
            payload = resp.json()
        except Exception:
            payload = None

        if resp.status_code >= 400:
            if isinstance(payload, dict) and "Fault" in payload:
                faults = payload.get("Fault", {}).get("Error", [])
                details = "; ".join([f"[{e.get('code')}] {e.get('Message')} :: {e.get('Detail')}" for e in faults])
                raise RuntimeError(f"QBO Error {resp.status_code}: {details or payload}")
            else:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:1000]}")

    # Convenience helpers -------------------------------------------------------

    def count_entities(self, entity_name: str) -> int:
        """
        Runs SELECT COUNT(*) FROM <Entity> and returns int count.
        """
        qr = self.query(f"select count(*) from {entity_name}")
        # QBO returns counts as e.g. {"totalCount": 5} or { "maxResults": ... } depending on entity
        # For COUNT(*), the count appears under 'totalCount' for most entities.
        count = qr.get("totalCount")
        if isinstance(count, int):
            return count
        # Fallback: some entities return a single row with meta; try reading any known counter
        # If not present, return 0
        return int(count or 0)

    def detect_ast_mode(self, prefs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Intuit convention:
          - If TaxPrefs.PartnerTaxEnabled is present (True/False): company is AST-enabled.
          - If PartnerTaxEnabled is missing: manual tax experience.
        """
        taxprefs = prefs.get("TaxPrefs", {}) or {}
        partner = taxprefs.get("PartnerTaxEnabled", None)
        using_tax = taxprefs.get("UsingSalesTax", None)
        return {
            "AST_Company": (partner is not None),
            "PartnerTaxEnabled": partner,   # True = AST set up; False = AST present but not completed
            "UsingSalesTax": using_tax,
        }


def pretty(obj: Any) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False)


def summarize(company: Dict[str, Any], prefs: Dict[str, Any], counts: Dict[str, int]) -> str:
    name = company.get("CompanyName") or company.get("LegalName") or "(unknown)"
    country = company.get("Country") or company.get("CountrySubDivisionCode") or "(unknown)"
    # Extract tax preferences safely
    taxprefs = prefs.get("TaxPrefs", {}) or {}
    partner = taxprefs.get("PartnerTaxEnabled", None)
    using_tax = taxprefs.get("UsingSalesTax", None)

    if partner is None:
        tax_mode = "Manual Tax (classic)"
        ui_note = ("You can see and manage custom Tax Agencies/Codes/Rates in the Taxes UI. "
                   "Migrated objects should be visible.")
    else:
        if partner is True:
            tax_mode = "AST (Automated Sales Tax) — configured"
        else:
            tax_mode = "AST (Automated Sales Tax) — present but not fully set up"
        ui_note = ("In AST companies, custom TaxAgency/TaxRate/TaxCode created via API generally do NOT appear in the UI. "
                   "They exist via API and can be referenced by Id.")

    lines = [
        "================ QBO COMPANY SUMMARY ================",
        f"CompanyName : {name}",
        f"Country     : {country}",
        "",
        "---------------- TAX PREFERENCES --------------------",
        f"UsingSalesTax       : {using_tax}",
        f"PartnerTaxEnabled   : {partner}  (presence implies AST company)",
        f"Detected Tax Mode   : {tax_mode}",
        "",
        "---------------- TAX OBJECT COUNTS ------------------",
        f"TaxAgency : {counts.get('TaxAgency', 0)}",
        f"TaxRate   : {counts.get('TaxRate', 0)}",
        f"TaxCode   : {counts.get('TaxCode', 0)}",
        "",
        "---------------- UI VISIBILITY NOTE -----------------",
        ui_note,
        "=====================================================",
    ]
    return "\n".join(lines)


def main(access_token: str, realm_id: str) -> None:
    client = QBOClient(access_token, realm_id)

    # 1) CompanyInfo
    company = client.read_company_info()

    # 2) Preferences (TaxPrefs)
    prefs = client.read_preferences()
    ast_info = client.detect_ast_mode(prefs)

    # 3) Quick counts for confirmation
    counts = {
        "TaxAgency": client.count_entities("TaxAgency"),
        "TaxRate": client.count_entities("TaxRate"),
        "TaxCode": client.count_entities("TaxCode"),
    }

    # 4) Print concise summary
    print(summarize(company, prefs, counts))

    # 5) (Optional) Dump raw payloads for debugging
    print("\nRaw CompanyInfo:\n", pretty(company))
    print("\nRaw Preferences:\n", pretty(prefs))
    print("\nAST Detection:\n", pretty(ast_info))


if __name__ == "__main__":
    # ======= PUT YOUR CREDENTIALS HERE =======
    ACCESS_TOKEN = "<YOUR_ACCESS_TOKEN>"
    REALM_ID = "<YOUR_REALM_ID>"
    # You may also read from env or arguments; keeping it simple per your ask
    # import os
    # ACCESS_TOKEN = os.environ.get("QBO_ACCESS_TOKEN")
    # REALM_ID = os.environ.get("QBO_REALM_ID")

    if ACCESS_TOKEN.startswith("<") or REALM_ID.startswith("<"):
        sys.stderr.write("Please set ACCESS_TOKEN and REALM_ID in the script.\n")
        sys.exit(1)

    try:
        main(ACCESS_TOKEN, REALM_ID)
    except Exception as e:
        sys.stderr.write(f"\nERROR: {e}\n")
        sys.exit(2)
