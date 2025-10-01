
# ies_deep_diag.py
import os, sys, json, requests
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

INTUIT_GRAPHQL_URL = "https://qb.api.intuit.com/graphql"
ACCOUNTING_BASE    = "https://quickbooks.api.intuit.com"


ACCESS  = ""
REFRESH = ""
REALM   = ""
CID     = ""
CSEC    = ""

missing = [k for k,v in {
    "QBO_ACCESS_TOKEN": ACCESS,
    "QBO_REFRESH_TOKEN": REFRESH,
    "QBO_REALM_ID": REALM,
    "QBO_CLIENT_ID": CID,
    "QBO_CLIENT_SECRET": CSEC,
}.items() if not v]
if missing:
    print("❌ Missing env:", ", ".join(missing)); sys.exit(2)

@dataclass
class Tokens:
    access_token: str
    refresh_token: str

tokens = Tokens(ACCESS, REFRESH)

def show_headers(h: dict):
    keep = [
        "date","content-type","x-request-id","x-intuit-tid","intuit_tid","x-intuit-response-source",
        "www-authenticate","content-length","server"
    ]
    return {k.lower(): h.get(k) for k in h.keys() if k.lower() in keep}

def check_accounting_companyinfo(tokens: Tokens, realm: str) -> int:
    url = f"{ACCOUNTING_BASE}/v3/company/{realm}/companyinfo/{realm}"
    hdrs = {"Authorization": f"Bearer {tokens.access_token}", "Accept": "application/json"}
    r = requests.get(url, headers=hdrs, timeout=60)
    print("🧪 Accounting API /companyinfo HTTP:", r.status_code)
    print("↪ Resp headers:", show_headers(r.headers))
    if r.status_code != 200:
        try: print("↪ Body:", json.dumps(r.json(), indent=2)[:1200])
        except: print("↪ Body (text):", r.text[:1000])
    return r.status_code

def probe_dimensions_graphql(tokens: Tokens, realm: str) -> int:
    tiny = {"query": "query { appFoundationsActiveCustomDimensionDefinitions(first:1){ edges { node { id label active }}}}"}
    hdrs = {
        "Authorization": f"Bearer {tokens.access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "intuit-realm-id": str(realm),
    }
    r = requests.post(INTUIT_GRAPHQL_URL, headers=hdrs, json=tiny, timeout=90)
    print("\n🧪 IES Dimensions GraphQL HTTP:", r.status_code)
    print("↪ Resp headers:", show_headers(r.headers))
    if r.status_code != 200:
        # print both JSON and text in case one is empty
        try: print("↪ Body JSON:", json.dumps(r.json(), indent=2)[:1500])
        except: print("↪ Body (text):", r.text[:1200])
    else:
        try: print("↪ Data:", json.dumps(r.json().get("data"), indent=2))
        except: print("↪ Body (text):", r.text[:1200])
    return r.status_code

def main():
    print(f"Realm: {REALM} (string), client: {CID[:6]}…")
    print("\nStep 1 — Verify token+realm via Accounting REST:")
    s1 = check_accounting_companyinfo(tokens, REALM)

    print("\nStep 2 — Probe IES Dimensions GraphQL:")
    s2 = probe_dimensions_graphql(tokens, REALM)

    print("\nConclusion:")
    if s1 == 200 and s2 == 403:
        print("• Token is valid for this company (Accounting API works), but Dimensions GraphQL is forbidden.")
        print("  → Almost certainly one of:")
        print("    - The access token was issued WITHOUT scope 'app-foundations.custom-dimensions.read'.")
        print("      Fix: Re-run OAuth consent including that scope (use prompt=consent to force re-grant).")
        print("    - Your app/client is NOT allow-listed for IES Dimensions GraphQL for this realm.")
        print("      Fix: Open Intuit Developer support ticket; include x-intuit-tid from the GraphQL response headers above.")
    elif s1 in (401, 403):
        print("• Accounting call failed, so this access token likely isn’t valid for this REALM (or is expired).")
        print("  → Re-run consent for THIS company and use the new access token, then retry.")
    elif s1 == 200 and s2 == 200:
        print("• Both services returned 200 — you are good to proceed with extraction.")
    else:
        print("• Unexpected combination. Use the headers (x-intuit-tid) printed above when contacting Intuit support.")

if __name__ == "__main__":
    main()
