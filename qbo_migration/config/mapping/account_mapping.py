# # config/mapping/account_mapping.py

# ACCOUNT_COLUMN_MAPPING = {
#     "Name": "Name",
#     "AccountType": "AccountType",
#     "AccountSubType": "AccountSubType",
#     "Description": "Description",
#     "FullyQualifiedName": "FullyQualifiedName",
#     "CurrencyRef.value": "CurrencyRef.value"  # Add this line
# }

# config/mapping/account_mapping.py

ACCOUNT_COLUMN_MAPPING = {
    "Name": "Name",
    "SubAccount": "SubAccount",
    # "ParentRef.value": "ParentRef.value",           # if SubAccount is True
    "AccountType": "AccountType",
    "AccountSubType": "AccountSubType",
    "AcctNum": "AcctNum",
    "Description": "Description",
    "CurrencyRef.value": "CurrencyRef.value",
    "Active": "Active"
}
