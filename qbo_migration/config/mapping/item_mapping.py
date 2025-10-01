# Fields we keep in Map_Item (replication), with preferred QBO keys
ITEM_COLUMN_MAPPING = {
    # Core
    "Name": "Name",
    "Description": "Description",
    "PurchaseDesc": "PurchaseDesc",
    "Active": "Active",
    "Type": "Type",
    "Taxable": "Taxable",
    "SalesTaxIncluded": "SalesTaxIncluded",
    "PurchaseTaxIncluded": "PurchaseTaxIncluded",
    "UnitPrice": "UnitPrice",
    "PurchaseCost": "PurchaseCost",

    # Accounts (nested refs)
    "IncomeAccountRef.value": "IncomeAccountRef.value",
    # "IncomeAccountRef.name":  "IncomeAccountRef.name",
    "ExpenseAccountRef.value": "ExpenseAccountRef.value",
    # "ExpenseAccountRef.name":  "ExpenseAccountRef.name",
    "AssetAccountRef.value":   "AssetAccountRef.value",
    # "AssetAccountRef.name":    "AssetAccountRef.name",

    # Inventory / stock
    "TrackQtyOnHand": "TrackQtyOnHand",
    "QtyOnHand": "QtyOnHand",
    "ReorderPoint": "ReorderPoint",
    "InvStartDate": "InvStartDate",

    # Hierarchy
    "SubItem": "SubItem",
    "ParentRef.value": "ParentRef.value",
    # "ParentRef.name":  "ParentRef.name",

    # Read-only we still replicate to Map_Item (do NOT POST)
    # "FullyQualifiedName": "FullyQualifiedName",
    # "domain": "domain",
    # "sparse": "sparse",
    # "Id": "Id",
    # "SyncToken": "SyncToken",
    # "MetaData.CreateTime": "MetaData.CreateTime",
    # "MetaData.LastUpdatedTime": "MetaData.LastUpdatedTime",
    # "Level": "Level",
}

# Fields that must NEVER be sent to QBO in create/update payloads
ITEM_READONLY_FIELDS = {
    # system / read-only
    "FullyQualifiedName", "domain", "sparse", "Id", "SyncToken",
    "MetaData.CreateTime", "MetaData.LastUpdatedTime", "Level",
    # drop all *.name keys (we only post .value)
    "IncomeAccountRef.name", "ExpenseAccountRef.name", "AssetAccountRef.name",
    "ParentRef.name",
}

# For safety, allowlist of fields we will POST (everything else will be ignored)
# ---- QBO Item field controls ----
ITEM_POST_ALLOWLIST = {
    # scalars
    "Name", "Active", "Type", "Taxable", "SalesTaxIncluded", "PurchaseTaxIncluded",
    "UnitPrice", "PurchaseCost", "TrackQtyOnHand", "QtyOnHand", "ReorderPoint",
    "InvStartDate", "SubItem",
    # nested refs (value-only)
    "IncomeAccountRef.value", "ExpenseAccountRef.value", "AssetAccountRef.value",
    "ParentRef.value",
}


# ITEM_COLUMN_MAPPING = {
#     "Name": "Name",
#     "Description": "Description",
#     "Active": "Active",
#     "FullyQualifiedName": "FullyQualifiedName",
#     "Taxable": "Taxable",
#     "UnitPrice": "UnitPrice",
#     "Type": "Type",
#     "IncomeAccountRef.value": "IncomeAccountRef.value",
#     "IncomeAccountRef.name": "IncomeAccountRef.name",
#     "PurchaseCost": "PurchaseCost",
#     "TrackQtyOnHand": "TrackQtyOnHand",
#     "ExpenseAccountRef.value": "ExpenseAccountRef.value",
#     "ExpenseAccountRef.name": "ExpenseAccountRef.name",
#     "SubItem": "SubItem",
#     "ParentRef.value": "ParentRef.value",          # ✅ Required for category linking
#     "ParentRef.name": "ParentRef.name",
# }
