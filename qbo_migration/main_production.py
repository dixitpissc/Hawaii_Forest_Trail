"""
porter_migration_main.py

Usage:
    # Import this module anywhere without triggering work:
    from porter_migration_main import run_full_migration, run_entity, Payment_Method_Tporter, run_tracker

    # Start the migration live tracker (long-running loop):
    from porter_migration_main import run_tracker

    # Run the full migration sequence (when executed directly):
    python porter_migration_main.py

    # CLI examples:
    #   Start the long-running watchdog: python porter_migration_main.py --watchdog
    #   Run a subset of entities: python porter_migration_main.py --entities customer invoice payment

    # Or call a single entity from another module:
    Payment_Method_Tporter()

Design:
 - Lazy-imports each initiator module only when its corresponding function is called.
 - Prevents side-effects on module import.
 - Provides run_entity(name) to run by string name.
 - run_full_migration() runs the canonical sequence and logs results.
"""

from typing import Callable, Dict, Optional, List
import importlib
import traceback
import argparse
import threading
from utils.log_timer import global_logger as logger

# Import watchdog functions non-lazily so we can run a one-shot refresh at startup and start the background monitor.
from Watchdog.migration_live_tracker import run_tracker, refresh_migration_tracker

# Background watchdog thread management
_watchdog_thread: Optional[threading.Thread] = None
_watchdog_lock = threading.Lock()


def start_watchdog_background(daemon: bool = True) -> threading.Thread:
    """Start the long-running migration tracker in a background thread.

    If a background tracker is already running, this is a no-op and the existing thread is returned.
    """
    global _watchdog_thread
    with _watchdog_lock:
        if _watchdog_thread and _watchdog_thread.is_alive():
            logger.debug("Watchdog background thread already running")
            return _watchdog_thread

        logger.info("Starting migration live tracker in background thread")
        t = threading.Thread(target=run_tracker, daemon=daemon, name="migration_live_tracker")
        t.start()
        _watchdog_thread = t
        return t

# Watchdog functions are imported non-lazily above.
# Use a one-shot runner at startup or for testing that performs a single refresh without blocking.
def run_tracker_once():
    """Run a single refresh of the migration tracker (safe, non-blocking)."""
    logger.info("Running migration tracker one-shot (refresh_migration_tracker)")
    refresh_migration_tracker()


def run_tracker_background_once():
    """Ensure the background tracker is running and perform an immediate refresh.

    This is handy for startup: it creates the background thread once and immediately refreshes status.
    """
    start_watchdog_background()
    try:
        refresh_migration_tracker()
    except Exception:
        logger.exception("Initial one-shot refresh failed after starting background tracker")

# Optional: clear_cache is imported only when needed by functions to avoid side-effects on import
# from storage.sqlserver.sql import clear_cache  # DO NOT import at module-level to keep imports lazy

# --- Helper: lazy importer ---------------------------------------------------
def _lazy_call(module_path: str, func_name: str, *args, **kwargs):
    """
    Import module_path and call func_name on it. Returns whatever the function returns.
    Any import errors or runtime errors are caught and logged; exception is re-raised.
    """
    try:
        module = importlib.import_module(module_path)
    except Exception as e:
        logger.error(f"Failed to import module '{module_path}': {e}")
        logger.debug(traceback.format_exc())
        raise

    try:
        func = getattr(module, func_name)
    except AttributeError:
        msg = f"Module '{module_path}' has no attribute '{func_name}'"
        logger.error(msg)
        raise AttributeError(msg)

    try:
        logger.info(f"Calling {module_path}.{func_name}()")
        return func(*args, **kwargs)
    except Exception as e:
        logger.error(f"Error while running {module_path}.{func_name}: {e}")
        logger.debug(traceback.format_exc())
        raise

# --- Entity functions: thin wrappers that do lazy import ---------------------
# If you rename modules or functions, change module path and function name here only.

def Payment_Method_Tporter():
    return _lazy_call("porter_initiator.x_01_payment_method_initiator", "initiating_payment_method_migration")

def Term_Tporter():
    return _lazy_call("porter_initiator.x_02_term_initiator", "initiating_term_migration")

def Currency_Tporter():
    return _lazy_call("porter_initiator.x_03_currency_initiator", "initiating_currency_migration")

def Class_Tporter():
    return _lazy_call("porter_initiator.x_04_class_initiator", "initiating_class_migration")

def Department_Tporter():
    return _lazy_call("porter_initiator.x_05_department_initiator", "initiating_department_migration")

def Account_Tporter():
    return _lazy_call("porter_initiator.x_06_account_initiator", "initiating_account_migration")

def Item_Category_Tporter():
    return _lazy_call("porter_initiator.x_07_item_category_initiator", "initiating_item_category_migration")

def Item_Tporter():
    return _lazy_call("porter_initiator.x_08_item_initiator", "initiating_item_migration")

def Vendor_Tporter():
    return _lazy_call("porter_initiator.x_09_vendor_initiator", "initiating_vendor_migration")

def Customer_Tporter():
    return _lazy_call("porter_initiator.x_10_customer_initiator", "initiating_customer_migration")

def Employee_Tporter():
    return _lazy_call("porter_initiator.x_11_employee_initiator", "initiating_employee_migration")

def Invoice_Tporter():
    return _lazy_call("porter_initiator.x_12_invoice_initiator", "initiating_invoice_migration")

def Estimate_Tporter():
    return _lazy_call("porter_initiator.x_11_estimate_initiator", "initiating_estimate_migration")

def CreditMemo_Tporter():
    return _lazy_call("porter_initiator.x_13_creditmemo_initiator", "initiating_creditmemo_migration")

def Bill_Tporter():
    return _lazy_call("porter_initiator.x_14_bill_initiator", "initiating_bill_migration")

def VendorCredit_Tporter():
    return _lazy_call("porter_initiator.x_15_vendorcredit_initiator", "initiating_vendorcredit_migration")

def JournalEntry_Tporter():
    return _lazy_call("porter_initiator.x_16_journalentry_initiator", "initiating_journalentry_migration")

def Deposit_Tporter():
    return _lazy_call("porter_initiator.x_17_deposit_initiator", "initiating_deposit_migration")

def Purchase_Tporter():
    return _lazy_call("porter_initiator.x_18_purchase_initiator", "initiating_purchase_migration")

def PurchaseOrder_Tporter():
    return _lazy_call("porter_initiator.x_25_purchaseorder_initiator", "initiating_PurchaseOrder_migration")

def SalesReceipt_Tporter():
    return _lazy_call("porter_initiator.x_19_salesreceipt_initiator", "initiating_salesreceipt_migration")

def RefundReceipts_Tporter():
    return _lazy_call("porter_initiator.x_20_refundreceipts_initiator", "initiating_refundreceipt_migration")

def Payment_Tporter():
    return _lazy_call("porter_initiator.x_21_payment_initiator", "initiating_payment_migration")

def BillPayment_Tporter():
    return _lazy_call("porter_initiator.x_22_billpayment_initiator", "initiating_billpayment_migration")

def Transfer_Tporter():
    return _lazy_call("porter_initiator.x_23_transfer_initiator", "initiating_transfer_migration")

def Taxagency_Tporter():
    return _lazy_call("porter_initiator.x_09_taxagency_initiator", "initiating_taxagency_migration")

def Taxcode_Tporter():
    return _lazy_call("porter_initiator.x_09_taxcode_initiator", "initiating_taxcode_migration")

def Taxrate_Tporter():
    return _lazy_call("porter_initiator.x_09_taxrate_initiator", "initiating_taxrate_migration")

# If you have more entities (tax, estimate, purchase order...), add wrappers similarly.

# --- Registry: allows calling by name ---------------------------------------
_ENTITY_REGISTRY: Dict[str, Callable[[], Optional[object]]] = {
    "payment_method": Payment_Method_Tporter,
    "term": Term_Tporter,
    "currency": Currency_Tporter,
    "class": Class_Tporter,
    "department": Department_Tporter,
    "account": Account_Tporter,
    "item_category": Item_Category_Tporter,
    "item": Item_Tporter,
    "taxagency": Taxagency_Tporter,
    "taxcode":Taxcode_Tporter,
    "taxrate":Taxrate_Tporter,
    "vendor": Vendor_Tporter,
    "customer": Customer_Tporter,
    "employee": Employee_Tporter,
    
    "estimate":Estimate_Tporter,  # Fixed: lowercase to match processing
    "invoice": Invoice_Tporter,
    "creditmemo": CreditMemo_Tporter,
    "bill": Bill_Tporter,
    "vendorcredit": VendorCredit_Tporter,
    "journalentry": JournalEntry_Tporter,
    "deposit": Deposit_Tporter,
    "purchase": Purchase_Tporter,
    "purchaseorder": PurchaseOrder_Tporter,
    "salesreceipt": SalesReceipt_Tporter,
    "refundreceipt": RefundReceipts_Tporter,
    "payment": Payment_Tporter,
    "billpayment": BillPayment_Tporter,
    "transfer": Transfer_Tporter,
    "migration_tracker": run_tracker_once,
    "watchdog": run_tracker_once,
}

def run_entity(entity_key: str):
    """
    Run a migration for an entity by short name (case-insensitive).
    Returns the entity function's return value.
    Raises KeyError if unknown.

    Ensures the background migration tracker is running (so monitoring is available for single-entity runs).
    """
    # Ensure the background tracker is running so users have live visibility even for single entities
    try:
        start_watchdog_background()
    except Exception:
        logger.exception("Failed to ensure background watchdog before running entity")

    key = entity_key.strip().lower()
    if key not in _ENTITY_REGISTRY:
        raise KeyError(f"Unknown entity key '{entity_key}'. Valid keys: {sorted(_ENTITY_REGISTRY.keys())}")
    try:
        return _ENTITY_REGISTRY[key]()
    except Exception:
        logger.error(f"run_entity('{entity_key}') failed.")
        raise

# --- Full migration orchestration -------------------------------------------
def run_full_migration(entities: Optional[List[str]] = None, start_watchdog: bool = False):
    """
    Runs the migration sequence.
    - If entities is None -> runs a sensible default sequence (masters first, then transactions).
    - If entities is a list of entity keys (e.g. ['term','account','vendor']), runs only those in order.
    - If start_watchdog is True, starts the migration live tracker in a background daemon thread.
    """
    # Optionally start the migration live tracker in a background daemon thread.
    if start_watchdog:
        logger.info("Starting migration live tracker in background (daemon thread).")
        t = threading.Thread(target=run_tracker, daemon=True)
        t.start()

    # default canonical sequence (masters first, then transactional)
    default_sequence = [
        # "payment_method", 
        # "term",
        # "currency", 
        # "class", 
        # "department",
        # "account",
        # "item_category",
        #  "item", 
        # "taxagency",
        # "taxrate",
        # "taxcode",
        # "vendor", 
        # "customer", 
        # "employee",
        
        # # transactional
        
        # "estimate",  # Fixed: lowercase to match registry key
        # "purchaseorder",
        # "bill", 
        # "purchase", 
        # "vendorcredit", 

        # "invoice", 
        # "creditmemo", 
        # "salesreceipt", 
        # "journalentry",
        
        # "payment",
        # "transfer"
        # "deposit", 
        # "refundreceipt", 
        # "billpayment", 
        
    ]

    seq = [s.strip().lower() for s in (entities or default_sequence)]

    logger.info("🚀 Starting migration run_full_migration()")
    results = {}
    for key in seq:
        if key not in _ENTITY_REGISTRY:
            logger.warning(f"Skipping unknown entity key '{key}'")
            results[key] = {"status": "skipped", "reason": "unknown entity"}
            continue

        try:
            logger.info(f"--- Running migration for: {key} ---")
            val = run_entity(key)
            results[key] = {"status": "success", "result": val}
            logger.info(f"✅ Completed {key}")
        except Exception as e:
            results[key] = {"status": "failed", "error": str(e)}
            logger.error(f"❌ {key} failed: {e}")
            # depending on desired behavior, either continue or break. We continue by default.
            # If you want to abort on first error, uncomment the next line:
            # break

    logger.info("🟦 run_full_migration finished.")
    return results

# --- CLI entrypoint ---------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Porter migration runner")
    parser.add_argument('--watchdog', action='store_true', help='Start migration live tracker (blocks)')
    parser.add_argument('--start-watchdog', action='store_true', help='Start migration live tracker in background while running migrations (kept for backward compatibility)')
    parser.add_argument('--no-watchdog', action='store_true', help='Do not start the background migration live tracker')
    parser.add_argument('--entities', nargs='*', help='List of entity keys to run instead of default sequence')
    args = parser.parse_args()

    try:
        if args.watchdog:
            logger.info("Starting migration live tracker (watchdog). Use Ctrl+C to stop.")
            try:
                run_tracker()
            except KeyboardInterrupt:
                logger.info("Migration live tracker interrupted by user.")
        else:
            # Start background tracker by default unless explicitly disabled by --no-watchdog
            if not args.no_watchdog:
                run_tracker_background_once()
            res = run_full_migration(entities=args.entities if args.entities else None, start_watchdog=args.start_watchdog)
            logger.info("Migration results summary:")
            for entity, info in res.items():
                logger.info(f" - {entity}: {info.get('status')}")
    except Exception as exc:
        logger.error(f"Migration terminated with error: {exc}")
        raise
