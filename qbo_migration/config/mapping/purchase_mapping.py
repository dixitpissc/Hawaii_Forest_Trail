# config/mapping/purchase_mapping.py

PURCHASE_HEADER_MAPPING = {
    "AccountRef.value": "AccountRef.value",
    "PaymentMethodRef.value": "PaymentMethodRef.value",
    "PaymentType": "PaymentType",
    "TotalAmt": "TotalAmt",
    "TxnDate": "TxnDate",
    "CurrencyRef.value": "CurrencyRef.value",
    "EntityRef.value": "EntityRef.value",
    "EntityRef.type": "EntityRef.type",
    "DocNumber": "DocNumber",
    "PrivateNote": "PrivateNote",

    # Optional address fields (used if entity is Vendor with RemitToAddr)
    "RemitToAddr.Line1": "RemitToAddr.Line1",
    "RemitToAddr.Line2": "RemitToAddr.Line2",
    "RemitToAddr.Line3": "RemitToAddr.Line3",
    "RemitToAddr.City": "RemitToAddr.City",
    "RemitToAddr.Country": "RemitToAddr.Country",
    "RemitToAddr.CountrySubDivisionCode": "RemitToAddr.CountrySubDivisionCode",
    "RemitToAddr.PostalCode": "RemitToAddr.PostalCode",

    # Optional metadata (not posted, but retained for traceability)
    # "Id": "Id",
    # "SyncToken": "SyncToken",
    # "MetaData.CreateTime": "MetaData.CreateTime",
    # "MetaData.LastUpdatedTime": "MetaData.LastUpdatedTime"
}


PURCHASE_LINE_MAPPING = {
    "Id": "Id",
    "Description": "Description",
    "Amount": "Amount",
    "DetailType": "DetailType",

    # Account-based expense line details
    "AccountBasedExpenseLineDetail.AccountRef.value": "AccountBasedExpenseLineDetail.AccountRef.value",
    "AccountBasedExpenseLineDetail.ClassRef.value": "AccountBasedExpenseLineDetail.ClassRef.value",
    "AccountBasedExpenseLineDetail.BillableStatus": "AccountBasedExpenseLineDetail.BillableStatus",
    "AccountBasedExpenseLineDetail.TaxCodeRef.value": "AccountBasedExpenseLineDetail.TaxCodeRef.value",
    "AccountBasedExpenseLineDetail.CustomerRef.value": "AccountBasedExpenseLineDetail.CustomerRef.value",

    # Item-based expense line details (new)
    "ItemBasedExpenseLineDetail.ItemRef.value": "ItemBasedExpenseLineDetail.ItemRef.value",
    "ItemBasedExpenseLineDetail.ItemRef.name": "ItemBasedExpenseLineDetail.ItemRef.name",
    "ItemBasedExpenseLineDetail.ClassRef.value": "ItemBasedExpenseLineDetail.ClassRef.value",
    "ItemBasedExpenseLineDetail.UnitPrice": "ItemBasedExpenseLineDetail.UnitPrice",
    "ItemBasedExpenseLineDetail.Qty": "ItemBasedExpenseLineDetail.Qty",
    "ItemBasedExpenseLineDetail.TaxCodeRef.value": "ItemBasedExpenseLineDetail.TaxCodeRef.value",
    "ItemBasedExpenseLineDetail.CustomerRef.value": "ItemBasedExpenseLineDetail.CustomerRef.value",

    # Traceability attributes
    "Parent_Id": "Parent_Id",
    "TxnDate": "TxnDate",
    "DocNumber": "DocNumber",
    "Parent_Entity": "Parent_Entity"
}
