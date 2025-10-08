INVOICE_HEADER_MAPPING = {
    "DocNumber": "DocNumber",
    "TxnDate": "TxnDate",
    "DueDate": "DueDate",
    "CustomerRef.value": "CustomerRef.value",
    "CurrencyRef.value": "CurrencyRef.value",
    "DepartmentRef.value":"DepartmentRef.value",
    "FreeFormAddress":"FreeFormAddress",

    # Billing Address
    "BillAddr.Line1": "BillAddr.Line1",
    "BillAddr.Line2": "BillAddr.Line2",
    "BillAddr.Line3": "BillAddr.Line3",
    "BillAddr.Line4": "BillAddr.Line4",
    "BillAddr.Line5": "BillAddr.Line5",
    "BillAddr.City": "BillAddr.City",
    "BillAddr.Country": "BillAddr.Country",
    "BillAddr.CountrySubDivisionCode": "BillAddr.CountrySubDivisionCode",
    "BillAddr.PostalCode": "BillAddr.PostalCode",

    # Shipping Address
    "ShipAddr.Line1": "ShipAddr.Line1",
    "ShipAddr.Line2": "ShipAddr.Line2",
    "ShipAddr.Line3": "ShipAddr.Line3",
    "ShipAddr.Line4": "ShipAddr.Line4",
    "ShipAddr.Line5": "ShipAddr.Line5",
    "ShipAddr.City": "ShipAddr.City",
    "ShipAddr.Country": "ShipAddr.Country",
    "ShipAddr.CountrySubDivisionCode": "ShipAddr.CountrySubDivisionCode",
    "ShipAddr.PostalCode": "ShipAddr.PostalCode",

    # Payment & Online Options
    "AllowOnlineACHPayment": "AllowOnlineACHPayment",
    "AllowOnlineCreditCardPayment": "AllowOnlineCreditCardPayment",
    "AllowIPNPayment": "AllowIPNPayment",

    # Terms
    "SalesTermRef.value": "SalesTermRef.value",

    # Status & Comms
    "PrintStatus": "PrintStatus",
    "EmailStatus": "EmailStatus",
    "BillEmail.Address": "BillEmail.Address",
    "PrivateNote": "PrivateNote",
    "CustomerMemo.value": "CustomerMemo.value",

    #
    "BillEmailBcc.Address":"BillEmailBcc.Address",
    "BillEmailCc.Address":"BillEmailCc.Address",

    #Ship From address
    "ShipFromAddr.Line1":"ShipFromAddr.Line1",
    "ShipFromAddr.Line2":"ShipFromAddr.Line2",
    "ShipFromAddr.Line3":"ShipFromAddr.Line3",
    "ShipFromAddr.Line4":"ShipFromAddr.Line4",
    "ShipFromAddr.Line5":"ShipFromAddr.Line5",

    # Accounting
    "GlobalTaxCalculation" : "GlobalTaxCalculation",
    "ApplyTaxAfterDiscount": "ApplyTaxAfterDiscount",
    "TotalAmt": "TotalAmt",
    "Balance": "Balance",
    "TxnTaxDetail.TxnTaxCodeRef.value":"TxnTaxDetail.TxnTaxCodeRef.value",
    # Payment Method
    "PaymentMethodRef.value": "PaymentMethodRef.value",

    # Addtitional Attributes
    "ShipDate" : "ShipDate",
    "TrackingNum" : "TrackingNum",
}

#Old
# SALES_ITEM_LINE_MAPPING = {
#     "LineNum": "LineNum",
#     "Amount": "Amount",
#     "Description": "Description",
#     "DetailType": "DetailType",
#     "SalesItemLineDetail.ItemRef.value": "SalesItemLineDetail.ItemRef.value",
#     "SalesItemLineDetail.UnitPrice": "SalesItemLineDetail.UnitPrice",
#     "SalesItemLineDetail.Qty": "SalesItemLineDetail.Qty",
#     "SalesItemLineDetail.ServiceDate": "SalesItemLineDetail.ServiceDate",
#     "SalesItemLineDetail.TaxCodeRef.value": "SalesItemLineDetail.TaxCodeRef.value",
#     "SalesItemLineDetail.ClassRef.value": "SalesItemLineDetail.ClassRef.value",
#     "SalesItemLineDetail.ItemAccountRef.value" : "SalesItemLineDetail.ItemAccountRef.value",
#     "SalesItemLineDetail.TaxClassificationRef.value":"SalesItemLineDetail.TaxClassificationRef.value"

# }

SALES_ITEM_LINE_MAPPING = {
    # Generic line fields
    "LineNum": "LineNum",                      
    "Amount": "Amount",
    "Description": "Description",
    "DetailType": "DetailType",                
    # SalesItemLineDetail core
    "SalesItemLineDetail.ItemRef.value": "SalesItemLineDetail.ItemRef.value",   
    "SalesItemLineDetail.UnitPrice": "SalesItemLineDetail.UnitPrice",
    "SalesItemLineDetail.Qty": "SalesItemLineDetail.Qty",
    "SalesItemLineDetail.ServiceDate": "SalesItemLineDetail.ServiceDate",
    "SalesItemLineDetail.TaxCodeRef.value": "SalesItemLineDetail.TaxCodeRef.value",
    "SalesItemLineDetail.ClassRef.value": "SalesItemLineDetail.ClassRef.value",
    "SalesItemLineDetail.ItemAccountRef.value" : "SalesItemLineDetail.ItemAccountRef.value",
    # MarkupInfo (supported on create)
    "SalesItemLineDetail.MarkupInfo.PercentBased": "SalesItemLineDetail.MarkupInfo.PercentBased",
    "SalesItemLineDetail.MarkupInfo.Value": "SalesItemLineDetail.MarkupInfo.Value",
    "SalesItemLineDetail.MarkupInfo.MarkUpIncomeAccountRef.value": "SalesItemLineDetail.MarkupInfo.MarkUpIncomeAccountRef.value",
}


# Mapping for GroupLineDetail
GROUP_LINE_MAPPING = {
    "LineNum": "LineNum",
    "Amount": "Amount",
    "DetailType": "DetailType",
    "GroupLineDetail.GroupItemRef.value": "GroupLineDetail.GroupItemRef.value",
    "GroupLineDetail.Quantity": "GroupLineDetail.Quantity",
    "GroupLineDetail.ClassRef.value": "GroupLineDetail.ClassRef.value"
}

# Mapping for DiscountLineDetail
DISCOUNT_LINE_MAPPING = {
    "LineNum": "LineNum",
    "Amount": "Amount",
    "DetailType": "DetailType",
    "DiscountLineDetail.PercentBased": "DiscountLineDetail.PercentBased",
    "DiscountLineDetail.DiscountPercent": "DiscountLineDetail.DiscountPercent",
    "DiscountLineDetail.DiscountAccountRef.value": "DiscountLineDetail.DiscountAccountRef.value",
    "DiscountLineDetail.ClassRef.value": "DiscountLineDetail.ClassRef.value"
}

# Mapping for SubTotalLineDetail
SUBTOTAL_LINE_MAPPING = {
    "LineNum": "LineNum",
    "Amount": "Amount",
    "DetailType": "DetailType",
    "SubTotalLineDetail.ItemRef.value": "SubTotalLineDetail.ItemRef.value"
}