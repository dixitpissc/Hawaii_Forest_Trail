import os
import requests
from dotenv import load_dotenv
from utils.token_refresher import auto_refresh_token_if_needed

# Load environment variables
auto_refresh_token_if_needed()
load_dotenv()
realm_id = os.getenv("QBO_REALM_ID")
access_token = os.getenv("QBO_ACCESS_TOKEN")
environment = os.getenv("QBO_ENVIRONMENT", "sandbox")
base_url = "https://sandbox-quickbooks.api.intuit.com" if environment == "sandbox" else "https://quickbooks.api.intuit.com"

# === Headers ===
headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# === Step 1: Fetch all Invoices ===
def fetch_all_invoices():
    query = "SELECT Id, SyncToken FROM Invoice"
    url = f"{base_url}/v3/company/{realm_id}/query?query={query}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json().get("QueryResponse", {}).get("Invoice", [])
    else:
        print(f"❌ Failed to fetch invoices: {response.text}")
        return []

# === Step 2: Delete an Invoice ===
def delete_invoice(invoice_id, sync_token):
    url = f"{base_url}/v3/company/{realm_id}/invoice?operation=delete"
    payload = {
        "Id": str(invoice_id),
        "SyncToken": str(sync_token)
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"✅ Deleted Invoice ID: {invoice_id}")
    else:
        print(f"❌ Failed to delete Invoice ID: {invoice_id} → {response.text}")

# === Step 3: Main Runner ===
def delete_all_invoices():
    invoices = fetch_all_invoices()
    print(f"🔎 Found {len(invoices)} invoices.")

    for inv in invoices:
        delete_invoice(inv["Id"], inv["SyncToken"])

if __name__ == "__main__":
    delete_all_invoices()
