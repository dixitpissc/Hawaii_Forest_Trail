import os
import logging
import requests
import pandas as pd
import json
from dotenv import load_dotenv
from datetime import datetime
from storage.sqlserver import sql
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import ProgressTimer

# === Setup logging ===
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG_FILE_PATH = os.path.join(LOG_DIR, log_filename)

logger = logging.getLogger("migration")
logger.setLevel(logging.INFO)
logger.propagate = False

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
if logger.hasHandlers():
    logger.handlers.clear()

file_handler = logging.FileHandler(LOG_FILE_PATH, mode='w', encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# === Load environment & refresh token ===
load_dotenv()
auto_refresh_token_if_needed()

access_token = os.getenv("QBO_ACCESS_TOKEN")
realm_id = os.getenv("QBO_REALM_ID")
environment = os.getenv("QBO_ENVIRONMENT", "sandbox")
base_url = "https://sandbox-quickbooks.api.intuit.com" if environment == "sandbox" else "https://quickbooks.api.intuit.com"
query_url = f"{base_url}/v3/company/{realm_id}/query"
post_url = f"{base_url}/v3/company/{realm_id}/account?minorversion=75"

headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

source_table = "Account"
source_schema = "dbo"
mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
mapping_table = "Map_Account"

session = requests.Session()
max_retries = 3

# === Helper functions ===
def fetch_qbo_account_by_name(account_name):
    """Fetch QBO account by Name, return Id and SyncToken"""
    if account_name is None:
        return None, None
    safe_name = account_name.replace("'", "''")
    query = f"SELECT Id, Name, SyncToken FROM Account WHERE Name='{safe_name}'"
    try:
        resp = session.get(query_url, headers=headers, params={"query": query, "minorversion": "75"})
        if resp.status_code == 200:
            accounts = resp.json().get("QueryResponse", {}).get("Account", [])
            if accounts:
                acc = accounts[0]
                return acc.get("Id"), acc.get("SyncToken")
    except Exception:
        logger.exception("Exception while fetching QBO account by name.")
    logger.warning(f"❌ Could not fetch QBO account by name: {account_name}")
    return None, None

def fetch_qbo_account_by_id(qbo_id):
    """Fetch QBO account by Id, return Id and SyncToken"""
    if not qbo_id:
        return None, None
    # Id is GUID-like; quote it safely
    query = f"SELECT Id, SyncToken FROM Account WHERE Id = '{str(qbo_id)}'"
    try:
        resp = session.get(query_url, headers=headers, params={"query": query, "minorversion": "75"})
        if resp.status_code == 200:
            accounts = resp.json().get("QueryResponse", {}).get("Account", [])
            if accounts:
                acc = accounts[0]
                return acc.get("Id"), acc.get("SyncToken")
    except Exception:
        logger.exception("Exception while fetching QBO account by id.")
    logger.warning(f"❌ Could not fetch QBO account by id: {qbo_id}")
    return None, None

def fetch_parent_value(parent_name):
    """Fetch ParentRef.value from QBO using ParentRef.Name"""
    if not parent_name or pd.isna(parent_name):
        return None
    parent_id, _ = fetch_qbo_account_by_name(parent_name)
    return parent_id

def generate_payload_from_source(row):
    """Generate full JSON payload for updating QBO account"""
    payload = {}
    # Id & SyncToken
    payload["Id"] = row["Target_Id"] or row["Source_Id"]
    payload["SyncToken"] = row["SyncToken"]
    payload["sparse"] = True
    payload["Active"] = True

    # Mandatory field
    payload["Name"] = row["Name"]

    # ParentRef.value
    if "ParentRef.value" in row and pd.notna(row["ParentRef.value"]):
        payload["ParentRef"] = {"value": str(row["ParentRef.value"])}

    # Copy other fields from source table (currently commented out in original)
    fields_to_copy = [
        "AccountType",
        "AccountSubType",
        "Classification"
        # place any fields you want to include, matching original script's intent
    ]
    for f in fields_to_copy:
        if f in row and pd.notna(row[f]):
            if f.startswith("CurrencyRef."):
                # handle nested CurrencyRef if needed
                pass
            else:
                payload[f] = row[f]
    return payload

# === Step 1: Prepare mapping table with latest QBO SyncToken & ParentRef.value ===
def prepare_mapping():
    logger.info("🚀 Preparing Map_Account with latest QBO data...")
    df_accounts = sql.fetch_table(source_table, source_schema)
    if df_accounts.empty:
        logger.warning("⚠️ No accounts found in source table.")
        return

    sql.ensure_schema_exists(mapping_schema)

    # Deduplicate by Id
    df_accounts = df_accounts.drop_duplicates(subset=["Id"]).copy()
    df_accounts.rename(columns={"Id": "Source_Id"}, inplace=True)
    df_accounts["Target_Id"] = df_accounts["Source_Id"]
    df_accounts["SyncToken"] = None
    df_accounts["ParentRef.value"] = None
    df_accounts["Payload_JSON"] = None
    df_accounts["Porter_Status"] = None
    df_accounts["Failure_Reason"] = None
    df_accounts["Retry_Count"] = 0

    timer = ProgressTimer(len(df_accounts), logger=logger)
    for idx, row in df_accounts.iterrows():
        # Fetch SyncToken for the account
        qbo_id, sync_token = fetch_qbo_account_by_name(row["Name"])
        df_accounts.at[idx, "SyncToken"] = sync_token
        df_accounts.at[idx, "Target_Id"] = qbo_id

        # Fetch ParentRef.value from QBO using ParentRef.Name
        parent_val = fetch_parent_value(row.get("ParentRef.Name"))
        df_accounts.at[idx, "ParentRef.value"] = parent_val

        # Generate JSON payload
        payload = generate_payload_from_source(df_accounts.loc[idx])
        df_accounts.at[idx, "Payload_JSON"] = json.dumps(payload, indent=2)

        df_accounts.at[idx, "Porter_Status"] = "Ready" if sync_token else "Failed"
        timer.update()
    timer.stop()

    # Insert into mapping table
    sql.insert_dataframe(df_accounts, mapping_table, mapping_schema)
    logger.info("✅ Map_Account prepared with SyncToken, ParentRef, and JSON payloads.")

# === Step 2: Update QBO accounts ===
def update_qbo_accounts():
    logger.info("🚀 Updating QBO accounts from Map_Account...")
    df_mapping = sql.fetch_table(mapping_table, mapping_schema)
    if df_mapping.empty:
        logger.warning("⚠️ Map_Account is empty.")
        return

    timer = ProgressTimer(len(df_mapping), logger=logger)
    for idx, row in df_mapping.iterrows():
        if not row.get("SyncToken") or not row.get("Target_Id"):
            logger.warning(f"⏭️ Skipping {row.get('Source_Id')} — missing SyncToken or QBO Id")
            timer.update()
            continue

        # Load payload from mapping (ensure it's a dict)
        try:
            payload = json.loads(row["Payload_JSON"]) if row.get("Payload_JSON") else generate_payload_from_source(row)
        except Exception:
            payload = generate_payload_from_source(row)

        success = False
        last_response_text = None

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Attempt {attempt}/{max_retries} updating {row['Source_Id']} ({row['Name']}) with SyncToken={payload.get('SyncToken')}")
                resp = session.post(post_url, headers=headers, json=payload, timeout=60)
                last_response_text = resp.text[:1000]  # keep truncated for logs

                # Successful update
                if resp.status_code in (200, 201):
                    df_mapping.at[idx, "Porter_Status"] = "Updated"
                    df_mapping.at[idx, "Failure_Reason"] = None
                    df_mapping.at[idx, "Retry_Count"] = attempt - 1
                    # update Payload_JSON in mapping to latest state
                    df_mapping.at[idx, "Payload_JSON"] = json.dumps(payload, indent=2)
                    logger.info(f"✅ Updated account {row['Source_Id']} ({row['Name']}) on attempt {attempt}")
                    success = True
                    break

                # Non-success: try to detect stale object error in JSON
                stale_error_detected = False
                try:
                    resp_json = resp.json()
                    fault = resp_json.get("Fault")
                    if fault:
                        errors = fault.get("Error", [])
                        if not isinstance(errors, list):
                            errors = [errors]
                        for err in errors:
                            code = str(err.get("code", "")).strip()
                            message = str(err.get("Message", "") or err.get("message", ""))
                            # detect stale object error via code or message
                            if code == "5010" or "Stale Object Error" in message:
                                stale_error_detected = True
                                logger.warning(f"Stale Object Error detected for {row['Source_Id']}: {message} (code={code})")
                                break
                except ValueError:
                    # resp.json() failed -- keep going to fallback textual match
                    pass

                # Fallback textual check (some responses are text/html)
                if not stale_error_detected and "Stale Object Error" in (resp.text or ""):
                    stale_error_detected = True
                    logger.warning(f"Stale Object Error detected in response text for {row['Source_Id']}")

                if stale_error_detected:
                    # increment retry count in mapping
                    df_mapping.at[idx, "Retry_Count"] = (df_mapping.at[idx, "Retry_Count"] or 0) + 1

                    # fetch latest SyncToken by QBO Id and update payload & mapping
                    qbo_id = row.get("Target_Id") or payload.get("Id")
                    _, latest_sync = fetch_qbo_account_by_id(qbo_id)
                    if latest_sync:
                        logger.info(f"Fetched latest SyncToken for {qbo_id}: {latest_sync} — retrying")
                        payload["SyncToken"] = latest_sync
                        # persist updated SyncToken to df_mapping so final insert writes it
                        df_mapping.at[idx, "SyncToken"] = latest_sync
                        df_mapping.at[idx, "Payload_JSON"] = json.dumps(payload, indent=2)
                        # try again (loop continues)
                        continue
                    else:
                        # cannot retrieve latest sync token -> fail
                        df_mapping.at[idx, "Porter_Status"] = "Failed"
                        df_mapping.at[idx, "Failure_Reason"] = f"Stale Object Error but could not fetch latest SyncToken for id={qbo_id}"
                        logger.error(f"❌ Stale Object Error but failed to fetch latest SyncToken for id={qbo_id}")
                        break

                # If not stale error, mark failed and capture reason
                df_mapping.at[idx, "Porter_Status"] = "Failed"
                df_mapping.at[idx, "Failure_Reason"] = f"HTTP {resp.status_code}: {last_response_text}"
                logger.warning(f"❌ Failed updating {row['Source_Id']} ({row['Name']}): HTTP {resp.status_code}: {last_response_text}")
                break

            except requests.exceptions.RequestException as e:
                # network timeout etc
                df_mapping.at[idx, "Retry_Count"] = (df_mapping.at[idx, "Retry_Count"] or 0) + 1
                logger.exception(f"Request exception on attempt {attempt} for {row['Source_Id']}: {e}")
                if attempt == max_retries:
                    df_mapping.at[idx, "Porter_Status"] = "Failed"
                    df_mapping.at[idx, "Failure_Reason"] = str(e)
                else:
                    # retry (loop will continue)
                    continue
            except Exception as e:
                logger.exception(f"Unexpected exception while updating {row['Source_Id']}")
                df_mapping.at[idx, "Porter_Status"] = "Failed"
                df_mapping.at[idx, "Failure_Reason"] = str(e)
                break

        # If all attempts exhausted and not success, ensure Failure_Reason captured
        if not success and not df_mapping.at[idx, "Failure_Reason"]:
            df_mapping.at[idx, "Failure_Reason"] = last_response_text or "Unknown error"

        timer.update()

    # Update mapping table with results
    sql.insert_dataframe(df_mapping, mapping_table, mapping_schema)
    timer.stop()
    logger.info("✅ QBO account updates completed.")

# === Main Execution ===
if __name__ == "__main__":
    prepare_mapping()       # Step 1: Build mapping table with payload
    # update_qbo_accounts()   # Step 2: Update accounts in QBO



# import os
# import logging
# import requests
# import pandas as pd
# import json
# from dotenv import load_dotenv
# from datetime import datetime
# from storage.sqlserver import sql
# from utils.token_refresher import auto_refresh_token_if_needed
# from utils.log_timer import ProgressTimer

# # === Setup logging ===
# LOG_DIR = "logs"
# os.makedirs(LOG_DIR, exist_ok=True)
# log_filename = f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
# LOG_FILE_PATH = os.path.join(LOG_DIR, log_filename)

# logger = logging.getLogger("migration")
# logger.setLevel(logging.INFO)
# logger.propagate = False

# formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
# if logger.hasHandlers():
#     logger.handlers.clear()

# file_handler = logging.FileHandler(LOG_FILE_PATH, mode='w', encoding='utf-8')
# file_handler.setFormatter(formatter)
# logger.addHandler(file_handler)

# console_handler = logging.StreamHandler()
# console_handler.setFormatter(formatter)
# logger.addHandler(console_handler)

# # === Load environment & refresh token ===
# load_dotenv()
# auto_refresh_token_if_needed()

# access_token = os.getenv("QBO_ACCESS_TOKEN")
# realm_id = os.getenv("QBO_REALM_ID")
# environment = os.getenv("QBO_ENVIRONMENT", "sandbox")
# base_url = "https://sandbox-quickbooks.api.intuit.com" if environment=="sandbox" else "https://quickbooks.api.intuit.com"
# query_url = f"{base_url}/v3/company/{realm_id}/query"
# post_url = f"{base_url}/v3/company/{realm_id}/account?minorversion=75"

# headers = {
#     "Authorization": f"Bearer {access_token}",
#     "Accept": "application/json",
#     "Content-Type": "application/json"
# }

# source_table = "Account"
# source_schema = "dbo"
# mapping_schema = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")
# mapping_table = "Map_Account"

# session = requests.Session()
# max_retries = 3

# # === Helper functions ===
# def fetch_qbo_account_by_name(account_name):
#     """Fetch QBO account by Name, return Id and SyncToken"""
#     query = f"SELECT Id, Name, SyncToken FROM Account WHERE Name='{account_name.replace('\'', '\'\'')}'"
#     resp = session.get(query_url, headers=headers, params={"query": query, "minorversion": "75"})
#     if resp.status_code == 200:
#         accounts = resp.json().get("QueryResponse", {}).get("Account", [])
#         if accounts:
#             acc = accounts[0]
#             return acc["Id"], acc["SyncToken"]
#     logger.warning(f"❌ Could not fetch QBO account: {account_name}")
#     return None, None

# def fetch_parent_value(parent_name):
#     """Fetch ParentRef.value from QBO using ParentRef.Name"""
#     if not parent_name or pd.isna(parent_name):
#         return None
#     parent_id, _ = fetch_qbo_account_by_name(parent_name)
#     return parent_id

# def generate_payload_from_source(row):
#     """Generate full JSON payload for updating QBO account"""
#     payload = {}
#     # Id & SyncToken
#     payload["Id"] = row["Target_Id"] or row["Source_Id"]
#     payload["SyncToken"] = row["SyncToken"]
#     payload["sparse"] = True
#     payload["Active"] = True

#     # Mandatory field
#     payload["Name"] = row["Name"]

#     # ParentRef.value
#     if "ParentRef.value" in row and pd.notna(row["ParentRef.value"]):
#         payload["ParentRef"] = {"value": str(row["ParentRef.value"])}

#     # Copy other fields from source table
#     fields_to_copy = [
#         # "SubAccount", 
#         # "FullyQualifiedName", 
#         # "Classification",
#         #   "AccountType",
#         # "AccountSubType", 
#         # # "CurrentBalance", 
#         # "CurrentBalanceWithSubAccounts",
#         # # "CurrencyRef.value", "CurrencyRef.name", 
#         # "Description", 
#         # "AcctNum"
#     ]
#     for f in fields_to_copy:
#         if f in row and pd.notna(row[f]):
#             # Handle nested CurrencyRef
#             if f.startswith("CurrencyRef."):
#                 key = f.split(".")[1]
#                 # if "CurrencyRef" not in payload:
#                 #     payload["CurrencyRef"] = {}
#                 # payload["CurrencyRef"][key] = row[f]
#             else:
#                 payload[f] = row[f]
#     return payload

# # === Step 1: Prepare mapping table with latest QBO SyncToken & ParentRef.value ===
# def prepare_mapping():
#     logger.info("🚀 Preparing Map_Account with latest QBO data...")
#     df_accounts = sql.fetch_table(source_table, source_schema)
#     if df_accounts.empty:
#         logger.warning("⚠️ No accounts found in source table.")
#         return

#     sql.ensure_schema_exists(mapping_schema)

#     # Deduplicate by Id
#     df_accounts = df_accounts.drop_duplicates(subset=["Id"]).copy()
#     df_accounts.rename(columns={"Id": "Source_Id"}, inplace=True)
#     df_accounts["Target_Id"] = df_accounts["Source_Id"]
#     df_accounts["SyncToken"] = None
#     df_accounts["ParentRef.value"] = None
#     df_accounts["Payload_JSON"] = None
#     df_accounts["Porter_Status"] = None
#     df_accounts["Failure_Reason"] = None
#     df_accounts["Retry_Count"] = 0

#     timer = ProgressTimer(len(df_accounts), logger=logger)
#     for idx, row in df_accounts.iterrows():
#         # Fetch SyncToken for the account
#         qbo_id, sync_token = fetch_qbo_account_by_name(row["Name"])
#         df_accounts.at[idx, "SyncToken"] = sync_token
#         df_accounts.at[idx, "Target_Id"] = qbo_id

#         # Fetch ParentRef.value from QBO using ParentRef.Name
#         parent_val = fetch_parent_value(row.get("ParentRef.Name"))
#         df_accounts.at[idx, "ParentRef.value"] = parent_val

#         # Generate JSON payload
#         payload = generate_payload_from_source(df_accounts.loc[idx])
#         df_accounts.at[idx, "Payload_JSON"] = json.dumps(payload, indent=2)

#         df_accounts.at[idx, "Porter_Status"] = "Ready" if sync_token else "Failed"
#         timer.update()
#     timer.stop()

#     # Insert into mapping table
#     sql.insert_dataframe(df_accounts, mapping_table, mapping_schema)
#     logger.info("✅ Map_Account prepared with SyncToken, ParentRef, and JSON payloads.")

# # === Step 2: Update QBO accounts ===
# def update_qbo_accounts():
#     logger.info("🚀 Updating QBO accounts from Map_Account...")
#     df_mapping = sql.fetch_table(mapping_table, mapping_schema)
#     if df_mapping.empty:
#         logger.warning("⚠️ Map_Account is empty.")
#         return

#     timer = ProgressTimer(len(df_mapping), logger=logger)
#     for idx, row in df_mapping.iterrows():
#         if not row["SyncToken"] or not row["Target_Id"]:
#             logger.warning(f"⏭️ Skipping {row['Source_Id']} — missing SyncToken or QBO Id")
#             timer.update()
#             continue

#         payload = json.loads(row["Payload_JSON"])
#         try:
#             resp = session.post(post_url, headers=headers, json=payload)
#             if resp.status_code == 200:
#                 df_mapping.at[idx, "Porter_Status"] = "Updated"
#                 logger.info(f"✅ Updated account {row['Source_Id']} ({row['Name']})")
#             else:
#                 df_mapping.at[idx, "Porter_Status"] = "Failed"
#                 df_mapping.at[idx, "Failure_Reason"] = resp.text[:250]
#                 logger.warning(f"❌ Failed updating {row['Source_Id']} ({row['Name']}): {resp.text[:250]}")
#         except Exception as e:
#             df_mapping.at[idx, "Porter_Status"] = "Failed"
#             df_mapping.at[idx, "Failure_Reason"] = str(e)
#             logger.error(f"❌ Exception updating {row['Source_Id']} ({row['Name']}): {e}")
#         finally:
#             timer.update()

#     # Update mapping table with results
#     sql.insert_dataframe(df_mapping, mapping_table, mapping_schema)
#     timer.stop()
#     logger.info("✅ QBO account updates completed.")

# # === Main Execution ===
# if __name__ == "__main__":
#     # prepare_mapping()       # Step 1: Build mapping table with payload
#     update_qbo_accounts()   # Step 2: Update accounts in QBO
