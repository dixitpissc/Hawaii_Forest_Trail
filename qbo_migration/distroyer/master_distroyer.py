# """
# QBO Entity Destroyer Script (master_distroyer.py)
# --------------------------------------------------
# Description:
# ------------
# This script is used to **bulk delete records from QuickBooks Online (QBO)** using the QBO API.
# It supports both **transactional and master entities** like Invoices, Payments, Customers, etc.
# Key Features:
# -------------
# - Deletes all records for selected QBO entities using the `?operation=delete` API.
# - Automatically fetches all existing record `Id` and `SyncToken` values using QBO SQL-like queries.
# - Supports pagination in batches (default: 1000 per fetch).
# - Uses a loop to **continue deleting until no records remain**, even if more than 1000 exist.
# - Integrates with `ProgressTimer` to show **elapsed time, ETA, and % complete** in real-time.
# - Automatically refreshes tokens when expired.
# - Logs all output to both **console** and **log files** (`logs/migration_*.log`).
# Usage:
# ------
# 1. Define the entities to delete in the `QBO_ENTITIES` list.
# 2. Ensure valid QBO credentials and `.env` variables are set:
#     - `QBO_ACCESS_TOKEN`
#     - `QBO_REALM_ID`
#     - `QBO_ENVIRONMENT`
# 3. Run the script:
#     ```bash
#     python master_distroyer.py
#     ```
# Dependencies:
# -------------
# - Python 3.x
# - `requests`, `python-dotenv`
# - Internal utilities: `utils.token_refresher`, `utils.log_timer`

# Warning:
# --------
# This script performs **permanent deletion** from QBO. Use with caution, especially in production environments.

# Author:
# -------
# Auto-generated and maintained within the ISSCortex Migration Toolkit
# """

# import requests
# from utils.log_timer import global_logger as logger, ProgressTimer
# from utils.token_refresher import get_qbo_context_migration,auto_refresh_token_if_needed

# auto_refresh_token_if_needed()
# qbo_ctx = get_qbo_context_migration()

# realm_id = qbo_ctx["REALM_ID"]
# environment = "production"
# base_url ="https://quickbooks.api.intuit.com"

# HEADERS = {
#     "Authorization": f"Bearer {qbo_ctx['ACCESS_TOKEN']}",
#     "Accept": "application/json",
#     "Content-Type": "application/text"
# }


# # Add the entities you want to delete
# QBO_ENTITIES = [
#     # Transaction Entities
#     # "Payment",
#     # "JournalEntry",
#     # "Purchase",
#     # "SalesReceipt",
#     # "VendorCredit",
#     # "RefundReceipt",
#     # "TimeActivity",
#     # "Transfer",
#     # "Deposit",
#     # "Estimate",
#     # "PurchaseOrder",
#     # "Invoice",
#     # "Bill",
#     # "CreditMemo",
#     # # "StatementCharge",  # rarely used
#     # "BillPayment",
#     # "VendorCredit",
#     ## Master Entities
#     # "Customer",
#     # "Vendor",
#     # "Employee",
#     # "Item",
#     # "Account",
#     # "Class",
#     # "Department",         # maps to Location
#     # "PaymentMethod",
#     # "Term",
#     # "TaxCode",
#     # "TaxRate"
# ]

# def get_qbo_headers():
#     """Headers for JSON-based operations (like delete)."""
#     return {
#         "Authorization": f"Bearer {qbo_ctx['ACCESS_TOKEN']}",
#         "Accept": "application/json",
#         "Content-Type": "application/json",  # ✅ for deletes
#     }


# def fetch_ids_and_tokens(entity: str, start_pos: int = 1, batch_size: int = 500):
#     """
#     Fetch a batch of Id + SyncToken pairs for an entity.
#     Uses pagination via STARTPOSITION to avoid infinite loop.
#     """
#     query_url = f"{base_url}/v3/company/{realm_id}/query"
#     query = f"SELECT Id, SyncToken FROM {entity} STARTPOSITION {start_pos} MAXRESULTS {batch_size}"

#     headers = {
#         "Authorization": f"Bearer {qbo_ctx['ACCESS_TOKEN']}",
#         "Accept": "application/json",
#         "Content-Type": "application/text",  # ✅ MUST be text for queries
#     }

#     response = session.post(query_url, headers=headers, data=query)
#     if response.status_code in (401, 403):
#         logger.warning(f"🔑 Token expired while fetching {entity}, refreshing...")
#         from utils.token_refresher import auto_refresh_token_if_needed
#         auto_refresh_token_if_needed()
#         return fetch_ids_and_tokens(entity, start_pos, batch_size)

#     if response.status_code != 200:
#         logger.error(f"❌ Failed to fetch {entity}: {response.status_code} - {response.text}")
#         return []

#     try:
#         data = response.json()
#         return [(r["Id"], r["SyncToken"]) for r in data.get("QueryResponse", {}).get(entity, [])]
#     except Exception as e:
#         logger.exception(f"⚠️ Error parsing response for {entity}: {e}")
#         return []


# def delete_all_for_entity(entity: str, batch_size: int = 500):
#     """
#     Deletes all records for the specified QBO entity safely with pagination.
#     """
#     logger.info(f"📥 Starting delete for {entity}...")
#     total_deleted, start_pos, empty_streak = 0, 1, 0

#     import concurrent.futures
#     while True:
#         records = fetch_ids_and_tokens(entity, start_pos, batch_size)
#         if not records:
#             empty_streak += 1
#             if empty_streak >= 2:  # double check before exit
#                 break
#             continue

#         empty_streak = 0
#         logger.info(f"🧹 Deleting {len(records)} {entity} records (batch starting at {start_pos})")
#         timer = ProgressTimer(len(records))

#         with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
#             futures = [executor.submit(delete_entity_record, entity, rid, token) for rid, token in records]
#             for fut in concurrent.futures.as_completed(futures):
#                 if fut.result():
#                     total_deleted += 1
#                 timer.update()

#         start_pos += batch_size

#     logger.info(f"✅ Finished deleting {total_deleted} records from {entity}")

# session = requests.Session()

# def get_qbo_headers():
#     return {
#         "Authorization": f"Bearer {qbo_ctx['ACCESS_TOKEN']}",
#         "Accept": "application/json",
#         "Content-Type": "application/json"
#     }

# def delete_entity_record(entity: str, entity_id: str, sync_token: str) -> bool:
#     """
#     Sends a DELETE operation to QBO for the given entity record using a fast session-based request.
#     Args:
#         entity (str): Entity name (e.g., "Customer").
#         entity_id (str): QBO record ID.
#         sync_token (str): QBO SyncToken for the record.
#     Returns:
#         bool: True if successfully deleted, False otherwise.
#     """    
#     # if _should_refresh_token():
#     #     refresh_qbo_token()

#     delete_url = f"{base_url}/v3/company/{realm_id}/{entity.lower()}?operation=delete"
#     payload = {"Id": entity_id, "SyncToken": sync_token}

#     try:
#         response = session.post(delete_url, headers=get_qbo_headers(), json=payload)
#         if response.status_code == 200:
#             logger.info(f"✅ Deleted {entity} → ID: {entity_id}")
#             return True
#         else:
#             logger.warning(f"❌ Failed to delete {entity} → ID: {entity_id}: {response.status_code} - {response.text}")
#             return False
#     except Exception as e:
#         logger.exception(f"❌ Exception while deleting {entity} → ID: {entity_id}: {str(e)}")
#         return False


# # def delete_all_for_entity(entity: str):
# #     """
# #     Continuously fetches and deletes all records for the specified QBO entity.
# #     Handles batching, progress tracking, and retries until no records remain.
# #     Args:
# #         entity (str): QBO entity to delete (e.g., "Payment").
# #     """
# #     logger.info(f"📥 Starting delete for {entity}...")
# #     total_deleted = 0

# #     import concurrent.futures
# #     while True:
# #         records = fetch_ids_and_tokens(entity)
# #         if not records:
# #             break

# #         logger.info(f"🧹 Deleting {len(records)} records from {entity}")
# #         timer = ProgressTimer(len(records))

# #         def delete_and_update(args):
# #             entity_id, sync_token = args
# #             result = delete_entity_record(entity, entity_id, sync_token)
# #             return result

# #         with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
# #             results = list(executor.map(delete_and_update, records))
# #             for _ in results:
# #                 total_deleted += 1
# #                 timer.update()

# #         logger.info(f"✅ Completed current batch for {entity}. Checking for more...")

# #     logger.info(f"✅ Finished deleting {total_deleted} records from {entity}")

# def main():
#     """
#     Main entry point for deletion.
#     Iterates over all entities listed in QBO_ENTITIES and deletes all records for each.
#     """
#     logger.info("🚀 Starting QBO bulk delete for all entities...\n" + "=" * 60)
#     for entity in QBO_ENTITIES:
#         logger.info(f"\n📂 Processing: {entity}")
#         delete_all_for_entity(entity)
#     logger.info("\n🏁 All deletions complete.")
