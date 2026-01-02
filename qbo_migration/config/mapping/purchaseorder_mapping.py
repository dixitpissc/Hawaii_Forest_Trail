# config/mapping/purchaseorder_mapping.py

# Header (PurchaseOrder-level) field mapping
PURCHASEORDER_HEADER_MAPPING = {
    "DocNumber": "DocNumber",
    # "SyncToken": "SyncToken",
    "POEmail.Address": "POEmail",            # Email string of POEmail.Address
    "APAccountRef.value": "APAccountRef.value",    # map from Map_Account (A/P account)
    "CurrencyRef.value": "CurrencyRef.value",      # map from Map_Currency
    "TxnDate": "TxnDate",
    "TotalAmt": "TotalAmt",
    "POStatus": "POStatus",
    "EmailStatus": "EmailStatus",
    "VendorRef.value": "VendorRef.value",           # map from Map_Vendor
    "VendorRef.name": "VendorRef.name",
    "Id": "Id",
    "PrivateNote": "PrivateNote",
    # "ExchangeRate": "ExchangeRate",
    # Address fields: full structure mapped as needed
    "ShipAddr.Line1": "ShipAddr.Line1",
    "ShipAddr.Line2": "ShipAddr.Line2",
    "ShipAddr.Line3": "ShipAddr.Line3",
    "ShipAddr.Line4": "ShipAddr.Line4",
    "ShipAddr.Line5": "ShipAddr.Line5",
    "ShipAddr.City": "ShipAddr.City",
    "ShipAddr.Country": "ShipAddr.Country",
    "ShipAddr.CountrySubDivisionCode": "ShipAddr.CountrySubDivisionCode",
    "ShipAddr.PostalCode": "ShipAddr.PostalCode",
    "VendorAddr.Line1": "VendorAddr.Line1",
    "VendorAddr.Line2": "VendorAddr.Line2",
    "VendorAddr.Line3": "VendorAddr.Line3",
    "VendorAddr.Line4": "VendorAddr.Line4",
    "VendorAddr.Line5": "VendorAddr.Line5",
    "VendorAddr.City": "VendorAddr.City",
    "VendorAddr.Country": "VendorAddr.Country",
    "VendorAddr.CountrySubDivisionCode": "VendorAddr.CountrySubDivisionCode",
    "VendorAddr.PostalCode": "VendorAddr.PostalCode",

    # Add more fields if your source includes others used by QBO PO
}

# Line item mapping for ItemBasedExpenseLineDetail lines (common PO line type)
ITEM_BASED_EXPENSE_LINE_MAPPING = {
    "ItemBasedExpenseLineDetail.ItemRef.value": "ItemBasedExpenseLineDetail.ItemRef.value",          # Map_Item
    "ItemBasedExpenseLineDetail.CustomerRef.value": "ItemBasedExpenseLineDetail.CustomerRef.value",  # Map_Customer (optional)
    "ItemBasedExpenseLineDetail.Qty": "ItemBasedExpenseLineDetail.Qty",
    "ItemBasedExpenseLineDetail.UnitPrice": "ItemBasedExpenseLineDetail.UnitPrice",
    "ItemBasedExpenseLineDetail.TaxCodeRef.value": "ItemBasedExpenseLineDetail.TaxCodeRef.value",
    "ItemBasedExpenseLineDetail.BillableStatus": "ItemBasedExpenseLineDetail.BillableStatus"
}

# If ProjectRef is used at line level
PROJECT_REF_MAPPING = {
    "ProjectRef.value": "ProjectRef.value"    # Map_Project
}

# Add any custom fields or additional mappings as necessary
