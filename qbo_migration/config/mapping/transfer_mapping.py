# config/mapping/transfer_mapping.py

TRANSFER_HEADER_MAPPING = {
    "SyncToken": "SyncToken",                     # Optional - default to "0" if missing
    "TxnDate": "TxnDate",                         # Required
    "Amount": "Amount",                           # Required
    "domain": "domain",                           # Optional - usually "QBO"
    "PrivateNote": "PrivateNote",                 # Optional
    "CurrencyRef.value": "CurrencyRef.value",           # Optional currency
    "FromAccountRef.value": "FromAccountRef.value",      # Source Account ID (map to Map_Account.Target_Id)
    "ToAccountRef.value": "ToAccountRef.value",          # Destination Account ID (map to Map_Account.Target_Id)
    "Id": "Id"                                     # Source row identifier
}
