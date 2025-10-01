import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
access_token = os.getenv("QBO_ACCESS_TOKEN")
realm_id = os.getenv("QBO_REALM_ID")
environment = os.getenv("QBO_ENVIRONMENT", "sandbox")

# Set base URL
base_url = (
    "https://sandbox-quickbooks.api.intuit.com"
    if environment == "sandbox"
    else "https://quickbooks.api.intuit.com"
)

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/text"
}

def fetch_all_bill_ids():
    """Fetch all Bill Ids and SyncTokens from QBO"""
    query_url = f"{base_url}/v3/company/{realm_id}/query"
    query = "SELECT Id, SyncToken FROM Bill MAXRESULTS 1000"
    response = requests.post(query_url, headers=headers, data=query)

    if response.status_code != 200:
        print(f"❌ Failed to fetch bills: {response.status_code} - {response.text}")
        return []

    bills = response.json().get("QueryResponse", {}).get("Bill", [])
    return [{"Id": b["Id"], "SyncToken": b["SyncToken"]} for b in bills]

def delete_qbo_bill(bill_id: str, sync_token: str):
    """Delete bill using its Id and SyncToken"""
    delete_url = f"{base_url}/v3/company/{realm_id}/bill?operation=delete"
    delete_headers = headers.copy()
    delete_headers["Content-Type"] = "application/json"

    payload = {
        "Id": bill_id,
        "SyncToken": sync_token
    }

    response = requests.post(delete_url, headers=delete_headers, json=payload)
    if response.status_code == 200:
        print(f"✅ Deleted Bill ID: {bill_id}")
    else:
        print(f"❌ Failed to delete Bill ID {bill_id}: {response.status_code} - {response.text}")

def delete_all_qbo_bills():
    print("📥 Fetching all Bills from QBO...")
    bills = fetch_all_bill_ids()
    if not bills:
        print("⚠️ No bills found.")
        return

    print(f"🔍 Found {len(bills)} bill(s) to delete.")
    for bill in bills:
        delete_qbo_bill(bill["Id"], bill["SyncToken"])

# Run the function
if __name__ == "__main__":
    delete_all_qbo_bills()
