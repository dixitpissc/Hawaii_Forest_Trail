# config/mapping/estimate_mapping.py

# Map QBO fields → source column names (AS-IS, dotted names)
ESTIMATE_HEADER_MAPPING = {
    # Scalars
    "DocNumber": "DocNumber",
    "TxnDate": "TxnDate",
    "ExchangeRate": "ExchangeRate",
    "TxnStatus": "TxnStatus",
    "TrackingNum": "TrackingNum",
    "GlobalTaxCalculation": "GlobalTaxCalculation",
    "TotalAmt": "TotalAmt",
    "HomeTotalAmt": "HomeTotalAmt",
    "PrintStatus": "PrintStatus",
    "EmailStatus": "EmailStatus",
    "ExpirationDate": "ExpirationDate",

    # Nested
    "CurrencyRef.value": "CurrencyRef.value",
    "CustomerMemo.value": "CustomerMemo.value",
    "BillEmail.Address": "BillEmail.Address",

    # BillAddr (dotted)
    "BillAddr.Id": "BillAddr.Id",
    "BillAddr.Line1": "BillAddr.Line1",
    "BillAddr.Line2": "BillAddr.Line2",
    "BillAddr.Line3": "BillAddr.Line3",
    "BillAddr.Line4": "BillAddr.Line4",
    "BillAddr.City": "BillAddr.City",
    "BillAddr.Country": "BillAddr.Country",
    "BillAddr.CountrySubDivisionCode": "BillAddr.CountrySubDivisionCode",
    "BillAddr.PostalCode": "BillAddr.PostalCode",

    # ShipAddr (dotted)
    "ShipAddr.Id": "ShipAddr.Id",
    "ShipAddr.Line1": "ShipAddr.Line1",
    "ShipAddr.Line2": "ShipAddr.Line2",
    "ShipAddr.Line3": "ShipAddr.Line3",
    "ShipAddr.Line4": "ShipAddr.Line4",
    "ShipAddr.Line5": "ShipAddr.Line5",
    "ShipAddr.City": "ShipAddr.City",
    "ShipAddr.Country": "ShipAddr.Country",
    "ShipAddr.CountrySubDivisionCode": "ShipAddr.CountrySubDivisionCode",
    "ShipAddr.PostalCode": "ShipAddr.PostalCode",
}

# Line mapping (against SOURCE Estimate_Line with dotted names)
ESTIMATE_LINE_MAPPING = {
    "Parent_Id": "Parent_Id",
    "DetailType": "DetailType",                         # Expect "SalesItemLineDetail"
    "Amount": "Amount",
    "Description": "Description",
    "SalesItemLineDetail.ItemRef.value": "SalesItemLineDetail.ItemRef.value",
    "SalesItemLineDetail.ItemRef.name": "SalesItemLineDetail.ItemRef.name",
    "SalesItemLineDetail.UnitPrice": "SalesItemLineDetail.UnitPrice",
    "SalesItemLineDetail.Qty": "SalesItemLineDetail.Qty",
    "SalesItemLineDetail.TaxCodeRef.value": "SalesItemLineDetail.TaxCodeRef.value",
    "SalesItemLineDetail.ClassRef.value": "SalesItemLineDetail.ClassRef.value",
    "SalesItemLineDetail.ClassRef.name": "SalesItemLineDetail.ClassRef.name",
    "SalesItemLineDetail.ServiceDate": "SalesItemLineDetail.ServiceDate",
    "LineNum": "LineNum",
    "TxnDate": "TxnDate",
    "DocNumber": "DocNumber",
}

ESTIMATE_SOURCE_TABLE = "Estimate"
ESTIMATE_LINE_SOURCE_TABLE = "Estimate_Line"
