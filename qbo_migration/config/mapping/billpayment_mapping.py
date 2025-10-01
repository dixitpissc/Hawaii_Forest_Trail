# config/mapping/billpayment_mapping.py

BILLPAYMENT_HEADER_MAPPING = {
    "VendorRef.value": "VendorRef.value",
    # "VendorRef.name": "VendorRef.name",
    "PayType": "PayType",
    "CheckPayment.BankAccountRef.value": "CheckPayment.BankAccountRef.value",
    "CheckPayment.BankAccountRef.name": "CheckPayment.BankAccountRef.name",
    "CheckPayment.PrintStatus": "CheckPayment.PrintStatus",
    "CreditCardPayment.CCAccountRef.value": "CreditCardPayment.CCAccountRef.value",
    "CreditCardPayment.CCAccountRef.name": "CreditCardPayment.CCAccountRef.name",
    "TotalAmt": "TotalAmt",
    "domain": "domain",
    "sparse": "sparse",
    "DocNumber": "DocNumber",
    "TxnDate": "TxnDate",
    "CurrencyRef.value": "CurrencyRef.value",
    "CurrencyRef.name": "CurrencyRef.name",
    "ExchangeRate": "ExchangeRate",
    "DepartmentRef.value": "DepartmentRef.value",
    "PrivateNote": "PrivateNote"
}

BILLPAYMENT_LINE_MAPPING = {
    "Amount": "Amount",
    "LinkedTxn[0].TxnId": "LinkedTxn[0].TxnId",
    "LinkedTxn[0].TxnType": "LinkedTxn[0].TxnType",
    "Parent_Id": "Parent_Id",
    "TxnDate": "TxnDate",
    "DocNumber": "DocNumber",
    "Parent_Entity": "Parent_Entity"
}  # Add more if line fields grow