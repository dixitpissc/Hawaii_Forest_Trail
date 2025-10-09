# config/mapping/payment_mapping.py

PAYMENT_HEADER_MAPPING = {
    "CustomerRef.value": "CustomerRef.value",
    "CustomerRef.name": "CustomerRef.name",
    "DepositToAccountRef.value": "DepositToAccountRef.value",
    "PaymentRefNum": "PaymentRefNum",
    # "TxnSource":"TxnSource",
    "TotalAmt": "TotalAmt",
    "UnappliedAmt": "UnappliedAmt",
    "ProcessPayment": "ProcessPayment",
    "domain": "domain",
    "sparse": "sparse",
    "TxnDate": "TxnDate",
    "CurrencyRef.value": "CurrencyRef.value",
    "CurrencyRef.name": "CurrencyRef.name",
    "ExchangeRate": "ExchangeRate",
    "MetaData.CreateTime": "MetaData.CreateTime",
    "MetaData.LastUpdatedTime": "MetaData.LastUpdatedTime",

    "CreditCardPayment.CreditChargeInfo.Amount":"CreditCardPayment.CreditChargeInfo.Amount",
    "CreditCardPayment.CreditChargeInfo.NameOnAcct":"CreditCardPayment.CreditChargeInfo.NameOnAcct",
    "CreditCardPayment.CreditChargeInfo.ProcessPayment":"CreditCardPayment.CreditChargeInfo.ProcessPayment"
}

# config/mapping/payment_mapping.py

PAYMENT_LINE_MAPPING = {
    "TxnId": "LinkedTxn[0].TxnId",
    "TxnType": "LinkedTxn[0].TxnType",
    "Amount": "Amount",
    "LineEx_NameValue": [
        ("txnId", "LinkedTxn[0].TxnId"),
        ("txnOpenBalance", "LineEx.any[0].value.Value"),
        ("txnReferenceNumber", "LineEx.any[1].value.Value")
    ]
}
