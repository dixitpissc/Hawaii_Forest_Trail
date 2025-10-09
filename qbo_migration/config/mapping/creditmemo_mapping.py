CREDITMEMO_HEADER_MAPPING = {
    "DocNumber": "DocNumber",
    "TxnDate": "TxnDate",
    "CurrencyRef.value": "CurrencyRef.value",
    "CurrencyRef.name": "CurrencyRef.name",
    "CustomerRef.value": "CustomerRef.value",
    "CustomerRef.name": "CustomerRef.name",
    "ApplyTaxAfterDiscount": "ApplyTaxAfterDiscount",
    "PrintStatus": "PrintStatus",
    "EmailStatus": "EmailStatus",
    "BillEmail.Address": "BillEmail_Address",
    "BillEmailBcc.Address": "BillEmailBcc.Address",
    "CustomerMemo.value": "CustomerMemo.value",
    "PrivateNote": "PrivateNote",
    "TotalAmt": "TotalAmt",
    "RemainingCredit": "RemainingCredit",
    "Balance": "Balance",
    "DepartmentRef.value" : "DepartmentRef.value",
    "ShipDate": "ShipDate",

    # Bill Address Fields
    "BillAddr.Id": "BillAddr.Id",
    "BillAddr.Line1": "BillAddr_Line1",
    "BillAddr.Line2": "BillAddr_Line2",
    "BillAddr.Line3": "BillAddr_Line3",
    "BillAddr.Line4": "BillAddr_Line4",
    "BillAddr.Line5": "BillAddr_Line5",
    "BillAddr.City": "BillAddr_City",
    "BillAddr.Country": "BillAddr_Country",
    "BillAddr.CountrySubDivisionCode": "BillAddr_CountrySubDivisionCode",
    "BillAddr.PostalCode": "BillAddr_PostalCode",

    # Ship Address Fields
    "ShipAddr.Id": "ShipAddr.Id",
    "ShipAddr.Line1": "ShipAddr_Line1",
    "ShipAddr.Line2": "ShipAddr_Line2",
    "ShipAddr.Line3": "ShipAddr_Line3",
    "ShipAddr.Line4": "ShipAddr_Line4",
    "ShipAddr.City": "ShipAddr_City",
    "ShipAddr.Country": "ShipAddr_Country",
    "ShipAddr.CountrySubDivisionCode": "ShipAddr_CountrySubDivisionCode",
    "ShipAddr.PostalCode": "ShipAddr_PostalCode",

    # Ship-from (Address) — supported on sales forms
    "ShipFromAddr.Id": "ShipFromAddr.Id",
    "ShipFromAddr.Line1": "ShipFromAddr_Line1",
    "ShipFromAddr.Line2": "ShipFromAddr_Line2",
    "ShipFromAddr.Line3": "ShipFromAddr_Line3"
}


CREDITMEMO_LINE_MAPPING = {
    "LineNum": "LineNum",
    "Description": "Description",
    "Amount": "Amount",
    "DetailType": "DetailType",

    # Sales Item lines (existing)
    "SalesItemLineDetail.ServiceDate": "SalesItemLineDetail.ServiceDate",
    "SalesItemLineDetail.ItemRef.value": "SalesItemLineDetail.ItemRef.value",
    "SalesItemLineDetail.ItemRef.name": "SalesItemLineDetail.ItemRef.name",
    "SalesItemLineDetail.ClassRef.value": "SalesItemLineDetail.ClassRef.value",
    "SalesItemLineDetail.ClassRef.name": "SalesItemLineDetail.ClassRef.name",
    "SalesItemLineDetail.UnitPrice": "SalesItemLineDetail.UnitPrice",
    "SalesItemLineDetail.Qty": "SalesItemLineDetail.Qty",
    "SalesItemLineDetail.TaxCodeRef.value": "SalesItemLineDetail.TaxCodeRef.value",

    # Additional line detail types (new placeholders)
    "TaxLineDetail.TaxRateRef.value": "TaxLineDetail.TaxRateRef.value",
    "TaxLineDetail.PercentBased": "TaxLineDetail.PercentBased",
    "TaxLineDetail.TaxPercent": "TaxLineDetail.TaxPercent",

    "ItemBasedExpenseLineDetail.AccountRef.value": "ItemBasedExpenseLineDetail.AccountRef.value",
    "ItemBasedExpenseLineDetail.ItemRef.value": "ItemBasedExpenseLineDetail.ItemRef.value",
    "ItemBasedExpenseLineDetail.Qty": "ItemBasedExpenseLineDetail.Qty",
    "ItemBasedExpenseLineDetail.UnitPrice": "ItemBasedExpenseLineDetail.UnitPrice",

    "AccountBasedExpenseLineDetail.AccountRef.value": "AccountBasedExpenseLineDetail.AccountRef.value",

    "JournalEntryLineDetail.PostingType": "JournalEntryLineDetail.PostingType",
    "JournalEntryLineDetail.AccountRef.value": "JournalEntryLineDetail.AccountRef.value",

    "DescriptionOnly": "Description",
    "TDSLineDetail": "TDSLineDetail"  # India-specific withholding tax
}







# CREDITMEMO_HEADER_MAPPING = {
#     "DocNumber": "DocNumber",
#     "TxnDate": "TxnDate",
#     "CurrencyRef.value": "CurrencyRef.value",
#     "CustomerRef.name": "CustomerRef.name",
#     "ApplyTaxAfterDiscount": "ApplyTaxAfterDiscount",
#     "PrintStatus": "PrintStatus",
#     "EmailStatus": "EmailStatus",
#     "BillEmail.Address": "BillEmail.Address",
#     "CustomerMemo.value": "CustomerMemo.value",
#     "PrivateNote": "PrivateNote",
#     "TotalAmt": "TotalAmt",
#     "RemainingCredit": "RemainingCredit",
#     "Balance": "Balance",
#     "ShipAddr.Line1": "ShipAddr.Line1",
#     "ShipAddr.City": "ShipAddr.City",
#     "ShipAddr.Country": "ShipAddr.Country",
#     "ShipAddr.CountrySubDivisionCode": "ShipAddr.CountrySubDivisionCode",
#     "ShipAddr.PostalCode": "ShipAddr.PostalCode",
#     "BillAddr.Line1": "BillAddr.Line1",
#     "BillAddr.Line2": "BillAddr.Line2",
#     "BillAddr.Line3": "BillAddr.Line3",
#     "BillAddr.Line4": "BillAddr.Line4",
#     "BillAddr.Line5": "BillAddr.Line5",
#     "BillAddr.City": "BillAddr.City",
#     "BillAddr.Country": "BillAddr.Country",
#     "BillAddr.CountrySubDivisionCode": "BillAddr.CountrySubDivisionCode",
#     "BillAddr.PostalCode": "BillAddr.PostalCode"
# }

# CREDITMEMO_LINE_MAPPING = {
#     "LineNum": "LineNum",
#     "Description": "Description",
#     "Amount": "Amount",
#     "DetailType": "DetailType",
#     "SalesItemLineDetail.ItemRef.value": "Mapped_Item",
#     "SalesItemLineDetail.ItemRef.name": "SalesItemLineDetail.ItemRef.name",
#     "SalesItemLineDetail.ClassRef.value": "Mapped_Class",
#     "SalesItemLineDetail.UnitPrice": "SalesItemLineDetail.UnitPrice",
#     "SalesItemLineDetail.Qty": "SalesItemLineDetail.Qty",
#     "SalesItemLineDetail.ServiceDate": "SalesItemLineDetail.ServiceDate",
#     "SalesItemLineDetail.TaxCodeRef.value": "SalesItemLineDetail.TaxCodeRef.value"
# }
