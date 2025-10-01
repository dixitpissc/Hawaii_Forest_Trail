"""
Sequence : x_00
Module: extraction initiator
Author: Dixit Prajapati
Created: 2025-07-27
Description: Handle extraction process with pause continue and exists functionality with logging
            and status handling in sql server
Production : Ready
Phase : 01
"""

from utils.log_timer import global_logger as logger  
from config.entities import entity_list, entity_sort_column
from extraction.D111_extractor import *

# Global or session-controlled variable updated by UI/backend
GLOBAL_CONTROL_STATUS = "continue"

def get_control_status() -> str:
    """Dynamically retrieves the current extraction control status."""
    return GLOBAL_CONTROL_STATUS  # Replace with actual UI-driven value


# # === Run full extraction with logging and control ===
# def run_full_extraction():
#     print("\n📁 QBO Data Migration | Mode: FULL SYNC")
#     print("=========================================")
#     print("🔧 Connecting to QuickBooks Online...")

#     for idx, entity in enumerate(entity_list, start=1):
#         logger.info(f"\n📦 Extracting: {entity}")
#         try:
#             sort_col = entity_sort_column.get(entity, "Id")
#             control_status = get_control_status()
#             records = fetch_all_records(entity, sort_col=sort_col, control_status=control_status)
#             normalize_and_store(records, entity)
#             row_count = len(records)
#             col_count = len(records[0]) if records else 0
#             log_extraction_status(idx, entity, row_count, col_count, "Success")
#         except Exception as e:
#             print(f"❌ Error processing {entity}: {e}")
#             log_extraction_status(idx, entity, 0, 0, "Failed", str(e))

#     logger.info("\n✅ Full QBO extraction completed.")


# === Run full extraction with logging and retry for failed entities ===
def run_full_extraction():
    print("\n📁 QBO Data Migration | Mode: FULL SYNC")
    print("=========================================")
    print("🔧 Connecting to QuickBooks Online...")

    failed_entities = []

    # === First pass: extract all entities ===
    for idx, entity in enumerate(entity_list, start=1):
        logger.info(f"\n📦 Extracting: {entity}")
        try:
            sort_col = entity_sort_column.get(entity, "Id")
            control_status = get_control_status()
            records = fetch_all_records(entity, sort_col=sort_col, control_status=control_status)
            normalize_and_store(records, entity)
            row_count = len(records)
            col_count = len(records[0]) if records else 0
            log_extraction_status(idx, entity, row_count, col_count, "Success")
        except Exception as e:
            logger.error(f"❌ Error processing {entity}: {e}")
            log_extraction_status(idx, entity, 0, 0, "Failed", str(e))
            failed_entities.append((idx, entity))

    # === Retry block for failed entities ===
    if failed_entities:
        logger.warning("\n🔁 Retrying failed extractions (up to 3 times each)...")

        for idx, entity in failed_entities:
            retry_success = False
            for attempt in range(1, 4):
                logger.info(f"🔁 Attempt {attempt}/3: Retrying {entity}")
                try:
                    sort_col = entity_sort_column.get(entity, "Id")
                    control_status = get_control_status()
                    records = fetch_all_records(entity, sort_col=sort_col, control_status=control_status)
                    normalize_and_store(records, entity)
                    row_count = len(records)
                    col_count = len(records[0]) if records else 0
                    log_extraction_status(idx, entity, row_count, col_count, "Retried-Success")
                    retry_success = True
                    break
                except Exception as e:
                    logger.error(f"❌ Retry {attempt} failed for {entity}: {e}")
                    if attempt < 3:
                        time.sleep(2 * attempt)  # exponential backoff
            if not retry_success:
                log_extraction_status(idx, entity, 0, 0, "Retried-Failed", f"All retries failed for {entity}")

    logger.info("\n✅ Full QBO extraction completed.")

