# config/mapping/inventoryadjustment_mapping.py

# Header mapping: source table InventoryAdjustment
INVENTORYADJUSTMENT_HEADER_MAPPING = {
    "AdjustAccountRef.value": "AdjustAccountRef.value",
    "AdjustAccountRef.name":  "AdjustAccountRef.name",
    "domain":                 "domain",
    "sparse":                 "sparse",
    "Id":                     "Id",
    "SyncToken":              "SyncToken",
    "MetaData.CreateTime":    "MetaData_CreateTime",
    "MetaData.LastUpdatedTime":"MetaData_LastUpdatedTime",
    "DocNumber":              "DocNumber",
    "TxnDate":                "TxnDate",
    "DepartmentRef.value":    "DepartmentRef.value",
    "DepartmentRef.name":     "DepartmentRef.name",
}

# Line mapping: source table InventoryAdjustment_Line
ITEM_ADJUSTMENT_LINE_MAPPING = {
    "DetailType":                               "DetailType",
    "ItemAdjustmentLineDetail.ItemRef.value":   "ItemAdjustmentLineDetail.ItemRef.value",
    "ItemAdjustmentLineDetail.ItemRef.name":    "ItemAdjustmentLineDetail.ItemRef.name",
    "ItemAdjustmentLineDetail.QtyDiff":         "ItemAdjustmentLineDetail.QtyDiff",
    "Parent_Id":                                "Parent_Id",
    "TxnDate":                                  "TxnDate",
    "DocNumber":                                "DocNumber",
    "Parent_Entity":                            "Parent_Entity",
}
