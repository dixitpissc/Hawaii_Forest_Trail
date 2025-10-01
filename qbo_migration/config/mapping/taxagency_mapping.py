# config/mapping/taxagency_mapping.py

# Map QBO fields → Source column names
# Keep only fields that are valid to SEND on create.
#  Id, SyncToken, MetaData.*, domain, sparse are read-only from QBO and must not be sent on create.
TAXAGENCY_COLUMN_MAPPING = {
    "DisplayName":             "DisplayName",
    "TaxTrackedOnSales":       "TaxTrackedOnSales",
    "TaxTrackedOnPurchases":   "TaxTrackedOnPurchases",
    # Read-only fields present in source, useful to copy into Map_* for reference, but NOT sent in payload:
    "domain":                  "domain",
    "sparse":                  "sparse",
    "Id":                      "Id",
    "SyncToken":               "SyncToken",
    "MetaData.CreateTime":     "MetaData.CreateTime",
    "MetaData.LastUpdatedTime":"MetaData.LastUpdatedTime",
}
