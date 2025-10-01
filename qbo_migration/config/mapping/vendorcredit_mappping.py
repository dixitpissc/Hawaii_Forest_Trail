# config/mapping/vendorcredit_mapping.py

VENDORCREDIT_HEADER_MAPPING = {
    "domain": "domain",
    "sparse": "sparse",
    "Id": "Id",
    "SyncToken": "SyncToken",
    "MetaData.CreateTime": "MetaData.CreateTime",
    "MetaData.LastUpdatedTime": "MetaData.LastUpdatedTime",
    "TxnDate": "TxnDate",
    # "DueDate": "DueDate",  # Keep it if it exists in other data sources
    # "RefNumber": "RefNumber",  # Optional fallback for DocNumber
    "DocNumber": "DocNumber",
    "CurrencyRef.value": "CurrencyRef.value",
    "CurrencyRef.name": "CurrencyRef.name",
    # "ExchangeRate": "ExchangeRate",
    "APAccountRef.value": "APAccountRef.value",
    "APAccountRef.name": "APAccountRef.name",
    "VendorRef.value": "VendorRef.value",
    "VendorRef.name": "VendorRef.name",
    "DepartmentRef.value": "DepartmentRef.value",
    "TotalAmt": "TotalAmt",
    "PrivateNote": "PrivateNote",
    "TaxFormNum": "TaxFormNum",  # Optional
    "TaxFormType": "TaxFormType"  # Optional
}

VENDORCREDIT_LINE_MAPPING = {
    "LineNum": "LineNum",
    "DetailType": "DetailType",
    "Amount": "Amount",
    "Description": "Description",
    "AccountBasedExpenseLineDetail.AccountRef.value": "AccountBasedExpenseLineDetail.AccountRef.value",
    "AccountBasedExpenseLineDetail.AccountRef.name": "AccountBasedExpenseLineDetail.AccountRef.name",
    "AccountBasedExpenseLineDetail.BillableStatus": "AccountBasedExpenseLineDetail.BillableStatus",
    "AccountBasedExpenseLineDetail.TaxCodeRef.value": "AccountBasedExpenseLineDetail.TaxCodeRef.value",
    "AccountBasedExpenseLineDetail.ClassRef.value": "AccountBasedExpenseLineDetail.ClassRef.value",
    "AccountBasedExpenseLineDetail.ClassRef.name": "AccountBasedExpenseLineDetail.ClassRef.name",
    "ItemBasedExpenseLineDetail.ItemRef.value": "ItemBasedExpenseLineDetail.ItemRef.value",
    "ItemBasedExpenseLineDetail.ItemRef.name": "ItemBasedExpenseLineDetail.ItemRef.name",
    "ItemBasedExpenseLineDetail.UnitPrice": "ItemBasedExpenseLineDetail.UnitPrice",
    "ItemBasedExpenseLineDetail.Qty": "ItemBasedExpenseLineDetail.Qty",
    "Parent_Id": "Parent_Id",
    "Parent_Entity": "Parent_Entity",
    "TxnDate": "TxnDate",
    "DocNumber": "DocNumber"
}
