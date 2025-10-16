# config/mapping/deposit_mapping.py

DEPOSIT_HEADER_MAPPING = {
    # Required/Recommended
    "DepositToAccountRef.value": "DepositToAccountRef.value",
    "DepositToAccountRef.name": "DepositToAccountRef.name",
    "TxnDate": "TxnDate",
    "TotalAmt": "TotalAmt",
    "CurrencyRef.value": "CurrencyRef.value",
    "CurrencyRef.name": "CurrencyRef.name",
    "ExchangeRate": "ExchangeRate",
    "PrivateNote": "PrivateNote",

    # Optional QBO-supported fields
    "DocNumber": "DocNumber",
    "TxnStatus": "TxnStatus",
    "DepartmentRef.value": "DepartmentRef.value",
    "DepartmentRef.name": "DepartmentRef.name",
    "GlobalTaxCalculation": "GlobalTaxCalculation",
    "TxnTaxDetail.TotalTax": "TxnTaxDetail.TotalTax"  # Optional tax summary
}

DEPOSIT_LINE_MAPPING = {
    # Core Line structure
    "LineNum": "LineNum",
    "Description": "Description",
    "Amount": "Amount",
    "DetailType": "DetailType",

    # DepositLineDetail subfields
    "DepositLineDetail.AccountRef.value": "DepositLineDetail.AccountRef.value",
    "DepositLineDetail.AccountRef.name": "DepositLineDetail.AccountRef.name",

    "DepositLineDetail.Entity.value": "DepositLineDetail.Entity.value",
    "DepositLineDetail.Entity.name": "DepositLineDetail.Entity.name",
    "DepositLineDetail.Entity.type": "DepositLineDetail.Entity.type",

    "DepositLineDetail.ClassRef.value": "DepositLineDetail.ClassRef.value",
    "DepositLineDetail.ClassRef.name": "DepositLineDetail.ClassRef.name",

    "DepositLineDetail.DepartmentRef.value": "DepositLineDetail.DepartmentRef.value",
    "DepositLineDetail.DepartmentRef.name": "DepositLineDetail.DepartmentRef.name",

    "DepositLineDetail.PaymentMethodRef.value": "DepositLineDetail.PaymentMethodRef.value",
    "DepositLineDetail.PaymentMethodRef.name": "DepositLineDetail.PaymentMethodRef.name",

    "DepositLineDetail.CheckNum": "DepositLineDetail.CheckNum",
    "DepositLineDetail.TaxCodeRef.value": "DepositLineDetail.TaxCodeRef.value",
    "DepositLineDetail.TaxAmount": "DepositLineDetail.TaxAmount",

    # New: LinkedTxn support (linking this deposit line to a source txn/line)
    # If your extractor provides multiple indices, add [1], [2], ... similarly.
    "LinkedTxn[0].TxnId": "LinkedTxn[0].TxnId",
    "LinkedTxn[0].TxnType": "LinkedTxn[0].TxnType",
    "LinkedTxn[0].TxnLineId": "LinkedTxn[0].TxnLineId",

    # New: Project reference (seen in your source)
    # If your source also exposes a name, add "ProjectRef.name" here as needed.
    "ProjectRef.value": "ProjectRef.value",

    # Control / Technical
    "Parent_Id": "Parent_Id",
    "Parent_Entity": "Parent_Entity",
    "TxnDate": "TxnDate",
    "DocNumber": "DocNumber"
}
