import requests
import concurrent.futures
from utils.log_timer import global_logger as logger, ProgressTimer
from utils.token_refresher import get_qbo_context_migration, auto_refresh_token_if_needed

# -------------------------------------------------------------------
# 1. Initial setup: token, realm, session
# -------------------------------------------------------------------
auto_refresh_token_if_needed()
qbo_ctx = get_qbo_context_migration()

realm_id = qbo_ctx["REALM_ID"]
environment = "production"
base_url = "https://quickbooks.api.intuit.com"

# Use a shared session for performance
session = requests.Session()


# -------------------------------------------------------------------
# 2. Header helpers (always use fresh token)
# -------------------------------------------------------------------
def get_qbo_headers():
    """
    Headers for JSON-based operations (like delete).
    Uses fresh ACCESS_TOKEN from QBO context.
    """
    ctx = get_qbo_context_migration()
    return {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/json",  # for delete payloads
    }


def get_query_headers():
    """
    Headers for QBO SQL query operations.
    """
    ctx = get_qbo_context_migration()
    return {
        "Authorization": f"Bearer {ctx['ACCESS_TOKEN']}",
        "Accept": "application/json",
        "Content-Type": "application/text",  # MUST be text for QBO query
    }


# -------------------------------------------------------------------
# 3. WHERE clause builder (dynamic filter logic)
# -------------------------------------------------------------------
def build_where_clause(entity, mode, ids=None, docnumbers=None, date_from=None, date_to=None):
    """
    Build a QBO SQL WHERE clause based on the filter mode.

    mode:
      - 'all'        -> returns None (no WHERE)
      - 'id'         -> WHERE Id IN (...)
      - 'docnumber'  -> WHERE DocNumber IN (...)
      - 'date_range' -> WHERE TxnDate >= 'YYYY-MM-DD' AND TxnDate <= 'YYYY-MM-DD'
    """
    if mode == "all":
        return None

    if mode == "id":
        if not ids:
            raise ValueError("Mode 'id' requires at least one Id.")
        if isinstance(ids, (str, int)):
            ids_list = [str(ids)]
        else:
            ids_list = [str(x).strip() for x in ids if str(x).strip()]
        quoted = ",".join(f"'{v}'" for v in ids_list)
        return f"WHERE Id IN ({quoted})"

    if mode == "docnumber":
        if not docnumbers:
            raise ValueError("Mode 'docnumber' requires at least one DocNumber.")
        if isinstance(docnumbers, str):
            docs_list = [d.strip() for d in docnumbers.split(",") if d.strip()]
        else:
            docs_list = [str(x).strip() for x in docnumbers if str(x).strip()]
        quoted = ",".join(f"'{v}'" for v in docs_list)
        # Most transaction entities expose DocNumber
        return f"WHERE DocNumber IN ({quoted})"

    if mode == "date_range":
        if not date_from or not date_to:
            raise ValueError("Mode 'date_range' requires date_from and date_to.")
        # For transaction entities, TxnDate is the usual filter field
        return (
            f"WHERE TxnDate >= '{date_from}' "
            f"AND TxnDate <= '{date_to}'"
        )

    raise ValueError(f"Unsupported mode: {mode}")


# -------------------------------------------------------------------
# 4. Fetch Id + SyncToken with optional WHERE clause
# -------------------------------------------------------------------
def fetch_ids_and_tokens(entity, start_pos=1, batch_size=500, where_clause=None):
    """
    Fetch a batch of Id + SyncToken pairs for an entity with optional filtering.

    Uses pagination via STARTPOSITION to avoid infinite loop.
    """
    query_url = f"{base_url}/v3/company/{realm_id}/query"

    base_query = f"SELECT Id, SyncToken FROM {entity}"
    if where_clause:
        base_query += f" {where_clause}"

    query = f"{base_query} STARTPOSITION {start_pos} MAXRESULTS {batch_size}"

    def _do_request():
        headers = get_query_headers()
        return session.post(query_url, headers=headers, data=query)

    response = _do_request()

    # Handle expired token
    if response.status_code in (401, 403):
        logger.warning(f"🔑 Token expired while fetching {entity}, refreshing...")
        auto_refresh_token_if_needed()
        response = _do_request()

    if response.status_code != 200:
        logger.error(f"❌ Failed to fetch {entity}: {response.status_code} - {response.text}")
        return []

    try:
        data = response.json()
        rows = data.get("QueryResponse", {}).get(entity, [])
        return [(r["Id"], r["SyncToken"]) for r in rows]
    except Exception as e:
        logger.exception(f"⚠️ Error parsing response for {entity}: {e}")
        return []


# -------------------------------------------------------------------
# 5. Single-record delete
# -------------------------------------------------------------------
def delete_entity_record(entity, entity_id, sync_token):
    """
    Sends a DELETE operation to QBO for the given entity record.

    Args:
        entity (str): Entity name (e.g., "Customer", "Invoice").
        entity_id (str): QBO record ID.
        sync_token (str): QBO SyncToken for the record.

    Returns:
        bool: True if successfully deleted, False otherwise.
    """
    delete_url = f"{base_url}/v3/company/{realm_id}/{entity.lower()}?operation=delete"
    payload = {"Id": entity_id, "SyncToken": sync_token}

    try:
        response = session.post(delete_url, headers=get_qbo_headers(), json=payload)
        if response.status_code == 200:
            logger.info(f"✅ Deleted {entity} → ID: {entity_id}")
            return True
        else:
            logger.warning(
                f"❌ Failed to delete {entity} → ID: {entity_id}: "
                f"{response.status_code} - {response.text}"
            )
            return False
    except Exception as e:
        logger.exception(f"❌ Exception while deleting {entity} → ID: {entity_id}: {str(e)}")
        return False


# -------------------------------------------------------------------
# 6. Core bulk delete driver (uses mode + filters)
# -------------------------------------------------------------------
def delete_for_entity(
    entity,
    mode,
    ids=None,
    docnumbers=None,
    date_from=None,
    date_to=None,
    batch_size=500,
    max_workers=4,
):
    """
    Generic delete driver.

    - mode='all': deletes all rows for the entity
    - mode='id': deletes rows by Id list
    - mode='docnumber': deletes rows by DocNumber list
    - mode='date_range': deletes rows where TxnDate is between [date_from, date_to]
    """
    logger.info(f"📥 Starting delete for {entity} with mode={mode}...")

    where_clause = build_where_clause(
        entity=entity,
        mode=mode,
        ids=ids,
        docnumbers=docnumbers,
        date_from=date_from,
        date_to=date_to,
    )

    total_deleted = 0
    start_pos = 1
    empty_streak = 0

    while True:
        records = fetch_ids_and_tokens(
            entity=entity,
            start_pos=start_pos,
            batch_size=batch_size,
            where_clause=where_clause,
        )

        if not records:
            empty_streak += 1
            # Double-check empty before we stop
            if empty_streak >= 2:
                break
            start_pos += batch_size
            continue

        empty_streak = 0
        logger.info(
            f"🧹 Deleting {len(records)} {entity} records (batch starting at {start_pos})"
        )
        timer = ProgressTimer(len(records))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(delete_entity_record, entity, rid, token)
                for rid, token in records
            ]
            for fut in concurrent.futures.as_completed(futures):
                if fut.result():
                    total_deleted += 1
                timer.update()

        start_pos += batch_size

    logger.info(f"✅ Finished deleting {total_deleted} records from {entity}")


# -------------------------------------------------------------------
# 7. Simple public function: you call THIS with params in code
# -------------------------------------------------------------------
def run_dynamic_delete(
    entity,
    mode,
    ids=None,
    docnumbers=None,
    date_from=None,
    date_to=None,
    batch_size=500,
    max_workers=4,
):
    """
    Main function you should call from your code.

    Parameters:
        entity      : QBO entity string, e.g. "Invoice", "Bill", "Customer", "Payment"
        mode        : "all", "id", "docnumber", or "date_range"
        ids         : list of Ids (for mode="id")
        docnumbers  : list of DocNumbers (for mode="docnumber")
        date_from   : start date as 'YYYY-MM-DD' (for mode="date_range")
        date_to     : end date as 'YYYY-MM-DD'   (for mode="date_range")
    """
    logger.info(
        f"🚀 QBO Dynamic Delete | Entity={entity} | Mode={mode} | "
        f"Ids={ids} | DocNumbers={docnumbers} | From={date_from} | To={date_to}"
    )

    delete_for_entity(
        entity=entity,
        mode=mode,
        ids=ids,
        docnumbers=docnumbers,
        date_from=date_from,
        date_to=date_to,
        batch_size=batch_size,
        max_workers=max_workers,
    )

    logger.info("🏁 Delete operation complete.")


# -------------------------------------------------------------------
# 8. Example usage: edit these and run the file
# -------------------------------------------------------------------
# if __name__ == "__main__":
    # EXAMPLE 1: Delete ALL invoices
    # run_dynamic_delete(
    #     entity="Invoice",
    #     mode="all",
    # )

    # EXAMPLE 2: Delete specific Bills by Id
    # run_dynamic_delete(
    #     entity="Bill",
    #     mode="id",
    #     ids=["123", "456", "789"],
    # )

    # EXAMPLE 3: Delete Invoices by DocNumber
    # run_dynamic_delete(
    #     entity="Invoice",
    #     mode="docnumber",
    #     docnumbers=["INV-1001", "INV-1002"],
    # )

    # EXAMPLE 4: Delete Bills in a date range (TxnDate)
    # run_dynamic_delete(
    #     entity="Bill",
    #     mode="date_range",
    #     date_from="2024-01-01",
    #     date_to="2024-03-31",
    # )

# if __name__ == "__main__":
#     run_dynamic_delete(
#         entity="Invoice",
#         mode="id",
#         ids=["123", "456", "789"],
#     )

# if __name__ == "__main__":
#     run_dynamic_delete(
#         entity="VendorCredit",
#         mode="all",
#     )