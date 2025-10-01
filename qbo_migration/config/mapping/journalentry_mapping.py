JOURNALENTRY_HEADER_MAPPING = {
    "Adjustment": "Adjustment",
    # "domain": "domain",
    # "sparse": "sparse",
    # "Id": "Id",
    "DocNumber": "DocNumber",
    "TxnDate": "TxnDate",
    "CurrencyRef.value": "CurrencyRef.value",
    "CurrencyRef.name": "CurrencyRef.name",
    "ExchangeRate": "ExchangeRate",
    "PrivateNote": "PrivateNote",
    "Line": "Line"  # Included as placeholder for line structure
}

JOURNALENTRY_LINE_MAPPING = {
    "Id": "Id",
    "Description": "Description",
    "Amount": "Amount",
    "DetailType": "DetailType",
    "JournalEntryLineDetail.PostingType": "JournalEntryLineDetail.PostingType",
    "JournalEntryLineDetail.AccountRef.value": "JournalEntryLineDetail.AccountRef.value",
    "JournalEntryLineDetail.AccountRef.name": "JournalEntryLineDetail.AccountRef.name",
    "JournalEntryLineDetail.ClassRef.value": "JournalEntryLineDetail.ClassRef.value",
    "JournalEntryLineDetail.ClassRef.name": "JournalEntryLineDetail.ClassRef.name",
    "JournalEntryLineDetail.DepartmentRef.value": "JournalEntryLineDetail.DepartmentRef.value",
    "JournalEntryLineDetail.DepartmentRef.name": "JournalEntryLineDetail.DepartmentRef.name",
    "JournalEntryLineDetail.Entity.Type": "JournalEntryLineDetail.Entity.Type",
    "JournalEntryLineDetail.Entity.EntityRef.value": "JournalEntryLineDetail.Entity.EntityRef.value",
    "JournalEntryLineDetail.Entity.EntityRef.name": "JournalEntryLineDetail.Entity.EntityRef.name",
    "Parent_Id": "Parent_Id",
    "TxnDate": "TxnDate",
    "DocNumber": "DocNumber",
    "Parent_Entity": "Parent_Entity"
}

