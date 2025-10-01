import os
import requests
from dotenv import load_dotenv
from tabulate import tabulate

# === Load Environment ===
load_dotenv()

realm_id = os.getenv("QBO_REALM_ID")
access_token = os.getenv("QBO_ACCESS_TOKEN")
environment = os.getenv("QBO_ENVIRONMENT", "sandbox").lower()

base_url = (
    "https://sandbox-quickbooks.api.intuit.com"
    if environment == "sandbox"
    else "https://quickbooks.api.intuit.com"
)

HEADERS = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/text"
}

# === Master and Transaction Entities ===
MASTER_ENTITIES = [
    "Account","Class","Customer","Department","Employee","Item","PaymentMethod",
    "Term","Vendor","TaxCode","TaxRate","Currency",
    # "SalesRep",
    "Preferences","CompanyInfo","CustomFieldDefinition",
    # "Location",
    "Attachable"  # optional for file/receipt attachments
]


TRANSACTION_ENTITIES = [
    "Bill","BillPayment","CreditMemo","Deposit","Estimate",
    "Invoice","JournalEntry","Payment","Purchase","PurchaseOrder",
    "RefundReceipt","SalesReceipt","TimeActivity","Transfer",
    "VendorCredit","InventoryAdjustment",
    # "CreditCardCharge","CreditCardCredit","Check"
]

def query_count(entity):
    query_url = f"{base_url}/v3/company/{realm_id}/query?minorversion=65"
    query = f"SELECT COUNT(*) FROM {entity}"
    response = requests.post(query_url, headers=HEADERS, data=query)
    
    if response.status_code == 200:
        try:
            return response.json().get("QueryResponse", {}).get("totalCount", 0)
        except Exception as e:
            return f"⚠️ Error parsing {entity}"
    else:
        return f"❌ {response.status_code}"


def pre_evaluation_summary():
    print("🔍 QBO Pre-Evaluation Summary")
    print("=" * 50)

    master_summary = []
    transaction_summary = []

    print("📘 Evaluating Master Entities...")
    for entity in MASTER_ENTITIES:
        count = query_count(entity)
        master_summary.append([entity, count])

    print("📄 Evaluating Transaction Entities...")
    for entity in TRANSACTION_ENTITIES:
        count = query_count(entity)
        transaction_summary.append([entity, count])

    print("\n📊 Master Entity Counts")
    print(tabulate(master_summary, headers=["Entity", "Record Count"], tablefmt="pretty"))

    print("\n📊 Transaction Entity Counts")
    print(tabulate(transaction_summary, headers=["Entity", "Record Count"], tablefmt="pretty"))


if __name__ == "__main__":
    pre_evaluation_summary()
