# salesreceipt_mapping.py

SALESRECEIPT_HEADER_MAPPING = {
    # Core
    "DocNumber": "DocNumber",
    "TxnDate": "TxnDate",
    "CurrencyRef.value": "CurrencyRef.value",
    "CustomerRef.value": "Mapped_Customer",
    "ApplyTaxAfterDiscount": "ApplyTaxAfterDiscount",
    "PrintStatus": "PrintStatus",
    "EmailStatus": "EmailStatus",
    "BillEmail.Address": "BillEmail.Address",
    "CustomerMemo.value": "CustomerMemo.value",
    "PaymentMethodRef.value": "Mapped_PaymentMethod",
    "DepositToAccountRef.value": "Mapped_Account",
    "PaymentRefNum": "PaymentRefNum",
    "TotalAmt": "TotalAmt",
    "Balance": "Balance",

    # Addresses
    "BillAddr.Line1": "BillAddr.Line1",
    "BillAddr.Line2": "BillAddr.Line2",
    "BillAddr.Line3": "BillAddr.Line3",
    "BillAddr.Line4": "BillAddr.Line4",
    "BillAddr.City": "BillAddr.City",
    "BillAddr.Country": "BillAddr.Country",
    "BillAddr.CountrySubDivisionCode": "BillAddr.CountrySubDivisionCode",
    "BillAddr.PostalCode": "BillAddr.PostalCode",
    "ShipAddr.Line1": "ShipAddr.Line1",
    "ShipAddr.City": "ShipAddr.City",
    "ShipAddr.Country": "ShipAddr.Country",
    "ShipAddr.CountrySubDivisionCode": "ShipAddr.CountrySubDivisionCode",
    "ShipAddr.PostalCode": "ShipAddr.PostalCode",

    # Tax (header)
    # Keep the original total tax column (dot form) since your source SELECT uses it.
    "TxnTaxDetail.TotalTax": "TxnTaxDetail.TotalTax",

    # New: mapped TaxCode/TaxRate (these are produced in ensure_mapping_table)
    "TxnTaxDetail.TxnTaxCodeRef.value": "Mapped_TxnTaxCodeRef",
    "TxnTaxDetail.TaxLine[0].TaxLineDetail.TaxRateRef.value": "Mapped_TxnTaxRateRef",

    # Project
    "ProjectRef.value": "Mapped_Project"
}

SALESRECEIPT_LINE_MAPPING = {
    "LineNum": "LineNum",
    "Description": "Description",
    "Amount": "Amount",
    "DetailType": "DetailType",

    # Mapped refs from mapping tables
    "SalesItemLineDetail.ItemRef.value": "Mapped_Item",
    "SalesItemLineDetail.ClassRef.value": "Mapped_Class",

    # Native line fields
    "SalesItemLineDetail.ServiceDate": "SalesItemLineDetail.ServiceDate",
    "SalesItemLineDetail.UnitPrice": "SalesItemLineDetail.UnitPrice",
    "SalesItemLineDetail.Qty": "SalesItemLineDetail.Qty",
    "SalesItemLineDetail.TaxCodeRef.value": "SalesItemLineDetail.TaxCodeRef.value"
}


