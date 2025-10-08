# config/mapping/bill_mapping.py

# ✅ Bill Header Mapping — source columns from your SQL SELECT
BILL_HEADER_MAPPING = {
    "VendorRef.value": "VendorRef.value",
    "VendorRef.name": "VendorRef.name",
    "APAccountRef.value": "APAccountRef.value",
    "APAccountRef.name": "APAccountRef.name",
    "DepartmentRef.value": "DepartmentRef.value",
    "DepartmentRef.name": "DepartmentRef.name",
    "CurrencyRef.value": "CurrencyRef.value",
    "CurrencyRef.name": "CurrencyRef.name",
    "ExchangeRate": "ExchangeRate",
    "TxnDate": "TxnDate",
    "DocNumber": "DocNumber",
    "DueDate": "DueDate",
    "SalesTermRef.value": "SalesTermRef.value",
    "PrivateNote": "Memo"
}


# ✅ Bill Line Mapping — from your Bill_Line table
BILL_LINE_MAPPING = {
    "LineNum": "LineNum",
    "Description": "Description",
    "Amount": "Amount",
    "DetailType": "DetailType",

    # AccountBasedExpenseLineDetail
    "AccountRef.value": "AccountBasedExpenseLineDetail.AccountRef.value",
    "AccountRef.name": "AccountBasedExpenseLineDetail.AccountRef.name",
    "ClassRef.value": "AccountBasedExpenseLineDetail.ClassRef.value",
    "ClassRef.name": "AccountBasedExpenseLineDetail.ClassRef.name",
    "CustomerRef.value": "AccountBasedExpenseLineDetail.CustomerRef.value",
    "CustomerRef.name": "AccountBasedExpenseLineDetail.CustomerRef.name",
    "BillableStatus": "AccountBasedExpenseLineDetail.BillableStatus",
    "TaxCodeRef.value": "AccountBasedExpenseLineDetail.TaxCodeRef.value",
    "MarkupInfo.Percent": "AccountBasedExpenseLineDetail.MarkupInfo.Percent",

    # ItemBasedExpenseLineDetail (if used interchangeably in same dataset)
    "ItemBasedExpenseLineDetail.ItemRef.value": "ItemBasedExpenseLineDetail.ItemRef.value",
    "ItemBasedExpenseLineDetail.ItemRef.name": "ItemBasedExpenseLineDetail.ItemRef.name",
    "ItemBasedExpenseLineDetail.ClassRef.value": "ItemBasedExpenseLineDetail.ClassRef.value",
    "ItemBasedExpenseLineDetail.ClassRef.name": "ItemBasedExpenseLineDetail.ClassRef.name",
    "ItemBasedExpenseLineDetail.BillableStatus": "ItemBasedExpenseLineDetail.BillableStatus",
    "ItemBasedExpenseLineDetail.TaxCodeRef.value": "ItemBasedExpenseLineDetail.TaxCodeRef.value",
    "ItemBasedExpenseLineDetail.UnitPrice": "ItemBasedExpenseLineDetail.UnitPrice",
    "ItemBasedExpenseLineDetail.Qty": "ItemBasedExpenseLineDetail.Qty",
}
