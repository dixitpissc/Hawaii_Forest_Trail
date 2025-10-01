# utils/mapping_updater.py

import json
from storage.sqlserver import sql

def update_mapping_status(
    mapping_schema: str,
    mapping_table: str,
    source_id,
    status: str,
    target_id: str = None,
    failure_reason: str = None,
    payload: dict = None,
    increment_retry: bool = False
):
    """
    Generic update utility for QBO migration mapping tables.

    Args:
        mapping_schema (str): Schema name (e.g., 'porter_entities_mapping')
        mapping_table (str): Table name (e.g., 'Map_Class')
        source_id: The source record ID
        status (str): 'Success', 'Failed', or 'Exists'
        target_id (str, optional): QBO target ID (if available)
        failure_reason (str, optional): Error reason for failure
        payload (dict, optional): Payload sent to QBO, stored as JSON string
        increment_retry (bool): Whether to increase retry count
    """
    cols = []
    params = []

    if target_id:
        cols.append("Target_Id = ?")
        params.append(target_id)

    cols.append("Porter_Status = ?")
    params.append(status)

    if failure_reason is not None:
        cols.append("Failure_Reason = ?")
        params.append(failure_reason)

    if payload is not None:
        cols.append("Payload_JSON = ?")
        # params.append(json.dumps(payload))
        params.append(json.dumps(payload, indent=2))


    if increment_retry:
        cols.append("Retry_Count = ISNULL(Retry_Count, 0) + 1")

    query = f"""
        UPDATE [{mapping_schema}].[{mapping_table}]
        SET {', '.join(cols)}
        WHERE Source_Id = ?
    """
    params.append(source_id)
    sql.run_query(query, tuple(params))
