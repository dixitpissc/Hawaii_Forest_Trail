import os
import json
import requests
from dotenv import load_dotenv
from utils.token_refresher import auto_refresh_token_if_needed

load_dotenv()
auto_refresh_token_if_needed()

def post_to_qbo(entity: str, payload: dict) -> str:
    """
    Posts the given payload to QBO for the specified entity.
    Returns the Target_Id (QBO Id) if successful, else None.
    """
    access_token = os.getenv("QBO_ACCESS_TOKEN")
    realm_id = os.getenv("QBO_REALM_ID")
    environment = os.getenv("QBO_ENVIRONMENT", "sandbox").lower()

    base_url = (
        "https://sandbox-quickbooks.api.intuit.com"
        if environment == "sandbox"
        else "https://quickbooks.api.intuit.com"
    )
    url = f"{base_url}/v3/company/{realm_id}/{entity.lower()}?minorversion=65"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        try:
            data = response.json()
            target_id = data.get(entity.capitalize(), {}).get("Id")
            return target_id
        except Exception as parse_err:
            print(f"⚠️ Parse error: {parse_err}")
            return None
    else:
        print(f"❌ Failed to post {entity} | Status {response.status_code}: {response.text}")
        return None
