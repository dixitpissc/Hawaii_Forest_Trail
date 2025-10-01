# refundreceipt_mapping.py

REFUNDRECEIPT_HEADER_MAPPING = {
    "DocNumber": "DocNumber",
    "TxnDate": "TxnDate",
    "CurrencyRef.value": "CurrencyRef.value",
    "CustomerRef.value": "CustomerRef.value",
    "ApplyTaxAfterDiscount": "ApplyTaxAfterDiscount",
    "PrintStatus": "PrintStatus",
    "BillEmail.Address": "BillEmail.Address",
    "CustomerMemo.value": "CustomerMemo.value",
    "PaymentMethodRef.value": "PaymentMethodRef.value",
    "DepositToAccountRef.value": "DepositToAccountRef.value",
    "PaymentRefNum": "PaymentRefNum",
    "PrivateNote": "PrivateNote",
    "BillAddr.Line1": "BillAddr.Line1",
    "BillAddr.Line2": "BillAddr.Line2",
    "BillAddr.Line3": "BillAddr.Line3",
    "BillAddr.Line4": "BillAddr.Line4",
    "BillAddr.Line5": "BillAddr.Line5",
    "BillAddr.City": "BillAddr.City",
    "BillAddr.Country": "BillAddr.Country",
    "BillAddr.CountrySubDivisionCode": "BillAddr.CountrySubDivisionCode",
    "BillAddr.PostalCode": "BillAddr.PostalCode",
    "TxnTaxDetail.TotalTax": "TxnTaxDetail.TotalTax"
}

REFUNDRECEIPT_LINE_MAPPING = {
    "LineNum": "LineNum",
    "Description": "Description",
    "Amount": "Amount",
    "DetailType": "DetailType",
    "SalesItemLineDetail.ItemRef.value": "SalesItemLineDetail.ItemRef.value",
    "SalesItemLineDetail.ClassRef.value": "SalesItemLineDetail.ClassRef.value",
    "SalesItemLineDetail.ServiceDate": "SalesItemLineDetail.ServiceDate",
    "SalesItemLineDetail.UnitPrice": "SalesItemLineDetail.UnitPrice",
    "SalesItemLineDetail.Qty": "SalesItemLineDetail.Qty",
    "SalesItemLineDetail.TaxCodeRef.value": "SalesItemLineDetail.TaxCodeRef.value"
}
