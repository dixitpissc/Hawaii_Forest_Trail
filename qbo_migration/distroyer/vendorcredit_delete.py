# """
# delete_vendorcredits_by_id.py

# Purpose:
#     Delete specific VendorCredit transactions in QuickBooks Online (QBO)
#     using a list of QBO VendorCredit Ids.

#     For each Id:
#       1. Read the VendorCredit to get the latest SyncToken.
#       2. Call the delete operation for that VendorCredit.
#       3. Log success or failure.

# Requirements:
#     - QBO auth helpers already in your project:
#         - utils.token_refresher.get_qbo_context_migration
#         - utils.token_refresher.auto_refresh_token_if_needed
#     - Optional logger:
#         - utils.log_timer.global_logger as logger
#     - .env / ControlTower etc. already configured as in your migration project.
# """

# import os
# import json
# import logging
# from typing import Tuple, Optional

# import requests
# from dotenv import load_dotenv

# from utils.token_refresher import get_qbo_context_migration, auto_refresh_token_if_needed
# from utils.log_timer import global_logger as logger  # same logger you already use

# # ----------------------------------------------------------------------
# # CONFIG
# # ----------------------------------------------------------------------

# # Edit this list with the QBO VendorCredit Ids you want to delete
# VENDORCREDIT_IDS_TO_DELETE = [
# '131282',
# '131286',
# '131287',
# '131289',
# '131291',
# '131298',
# '131298',
# '131299',
# '131299',
# '131300',
# '131300',
# '131301',
# '131301',
# '131302',
# '131302',
# '131303',
# '131303',
# '131304',
# '131304',
# '131305',
# '131305',
# '131306',
# '131306',
# '131307',
# '131307',
# '131308',
# '131308',
# '131309',
# '131309',
# '131310',
# '131310',
# '131311',
# '131311'
# ]

# # Default minor version (you can change if you’re using another)
# DEFAULT_MINOR_VERSION = "65"

# # Use a shared HTTP session
# session = requests.Session()

# load_dotenv()
# auto_refresh_token_if_needed()


# # ----------------------------------------------------------------------
# # HELPER FUNCTIONS
# # ----------------------------------------------------------------------

# def get_qbo_base_and_headers() -> Tuple[str, str, dict]:
#     """
#     Returns:
#         base_url:  e.g. https://quickbooks.api.intuit.com/v3/company/<realm>
#         minor_ver: minorversion string
#         headers:   Authorization + content headers
#     """
#     ctx = get_qbo_context_migration()
#     base = ctx["BASE_URL"].rstrip("/")
#     realm = ctx["REALM_ID"]
#     access_token = ctx["ACCESS_TOKEN"]
#     minor_ver = ctx.get("MINOR_VERSION", DEFAULT_MINOR_VERSION)

#     base_url = f"{base}/v3/company/{realm}"
#     headers = {
#         "Authorization": f"Bearer {access_token}",
#         "Accept": "application/json",
#         "Content-Type": "application/json"
#     }
#     return base_url, minor_ver, headers


# def _safe_json(resp: requests.Response):
#     try:
#         return resp.json()
#     except Exception:
#         return None


# def fetch_vendorcredit(vc_id: str) -> Tuple[Optional[dict], Optional[str]]:
#     """
#     Fetch a VendorCredit by Id to obtain SyncToken.

#     Returns:
#         (vc_obj, error_message)
#         - vc_obj: dict with VendorCredit if found, else None
#         - error_message: None if success, else reason string
#     """
#     for attempt in (1, 2):
#         base_url, minor_ver, headers = get_qbo_base_and_headers()
#         url = f"{base_url}/vendorcredit/{vc_id}?minorversion={minor_ver}"

#         try:
#             resp = session.get(url, headers=headers, timeout=20)
#         except Exception as e:
#             logger.error(f"❌ Exception while reading VendorCredit {vc_id}: {e}")
#             return None, f"Exception during GET: {e}"

#         if resp.status_code == 200:
#             data = _safe_json(resp) or {}
#             vc = data.get("VendorCredit") or data  # some responses may wrap, some may not
#             if not vc:
#                 return None, "No VendorCredit object in response."
#             return vc, None

#         if resp.status_code in (401, 403) and attempt == 1:
#             logger.warning(f"🔐 {resp.status_code} while fetching VendorCredit {vc_id}, refreshing token...")
#             auto_refresh_token_if_needed()
#             continue  # retry once after refresh

#         if resp.status_code == 404:
#             logger.warning(f"⚠️ VendorCredit {vc_id} not found (404).")
#             return None, "Not found (404)."

#         # Other error
#         body = resp.text or f"HTTP {resp.status_code}"
#         logger.error(f"❌ Failed to fetch VendorCredit {vc_id}: {body}")
#         return None, f"HTTP {resp.status_code}: {body}"

#     return None, "Failed after retry."


# def delete_vendorcredit(vc_id: str, sync_token: str) -> Tuple[bool, str]:
#     """
#     Perform QBO delete operation for a single VendorCredit.

#     Returns:
#         (success, message)
#     """
#     payload = {
#         "Id": str(vc_id),
#         "SyncToken": str(sync_token)
#     }

#     for attempt in (1, 2):
#         base_url, minor_ver, headers = get_qbo_base_and_headers()
#         url = f"{base_url}/vendorcredit?operation=delete&minorversion={minor_ver}"

#         try:
#             resp = session.post(url, headers=headers, json=payload, timeout=20)
#         except Exception as e:
#             logger.error(f"❌ Exception deleting VendorCredit {vc_id}: {e}")
#             return False, f"Exception during delete: {e}"

#         if resp.status_code == 200:
#             data = _safe_json(resp) or {}
#             vc = data.get("VendorCredit") or data
#             status = vc.get("status") if isinstance(vc, dict) else None
#             msg = f"Deleted (status={status})" if status else "Deleted."
#             logger.info(f"✅ VendorCredit {vc_id} deleted successfully. {msg}")
#             return True, msg

#         if resp.status_code in (401, 403) and attempt == 1:
#             logger.warning(f"🔐 {resp.status_code} while deleting VendorCredit {vc_id}, refreshing token...")
#             auto_refresh_token_if_needed()
#             continue  # retry once after refresh

#         # Other error
#         body = resp.text or f"HTTP {resp.status_code}"
#         logger.error(f"❌ Failed to delete VendorCredit {vc_id}: {body}")
#         return False, f"HTTP {resp.status_code}: {body}"

#     return False, "Failed after retry."


# def delete_vendorcredits_by_id(id_list):
#     """
#     High-level function: iterate over ids, fetch SyncToken, delete each VendorCredit.
#     """
#     if not id_list:
#         logger.warning("⚠️ No VendorCredit Ids provided to delete.")
#         return

#     logger.info(f"🧹 Starting VendorCredit delete for {len(id_list)} Id(s)...")

#     summary = {
#         "success": [],
#         "failed": []
#     }

#     for raw_id in id_list:
#         vc_id = str(raw_id).strip()
#         if not vc_id:
#             continue

#         logger.info(f"🔎 Processing VendorCredit Id={vc_id}...")

#         # 1) Fetch to get SyncToken
#         vc_obj, err = fetch_vendorcredit(vc_id)
#         if err or not vc_obj:
#             logger.error(f"❌ Skipping delete for {vc_id}: {err}")
#             summary["failed"].append((vc_id, err or "Unknown error"))
#             continue

#         sync_token = vc_obj.get("SyncToken")
#         if sync_token is None:
#             msg = "Missing SyncToken in VendorCredit response."
#             logger.error(f"❌ {msg} Id={vc_id}")
#             summary["failed"].append((vc_id, msg))
#             continue

#         # 2) Delete
#         ok, msg = delete_vendorcredit(vc_id, sync_token)
#         if ok:
#             summary["success"].append(vc_id)
#         else:
#             summary["failed"].append((vc_id, msg))

#     # Summary logs
#     logger.info("🏁 VendorCredit delete run completed.")
#     logger.info(f"✅ Success count: {len(summary['success'])}")
#     logger.info(f"❌ Failed count:  {len(summary['failed'])}")

#     if summary["failed"]:
#         logger.info("Failed details:")
#         for vid, reason in summary["failed"]:
#             logger.info(f"  - Id={vid}: {reason}")


# # ----------------------------------------------------------------------
# # MAIN
# # ----------------------------------------------------------------------

# def distroy_vendorcredit():
#     # TODO: edit VENDORCREDIT_IDS_TO_DELETE above before running
#     delete_vendorcredits_by_id(VENDORCREDIT_IDS_TO_DELETE)
