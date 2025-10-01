# entities.py

# Ordered entity list for QBO sync: Master → Transactions → System
entity_list = [
    # Master / List Entities (in dependency order)
    "Account",
    "Term",
    "PaymentMethod",
    "Currency",
    "CompanyCurrency",     # NEW: Added for multi-currency
    "Department",
    "Class",
    "TaxAgency",
    "TaxRate",
    "TaxCode",
    "Customer",
    "CustomerType",
    "Vendor",
    "Employee",
    "Item",
    "ItemCategory",
    # "CustomFieldDefinition",  # NEW: For custom fields
    # "JournalCode",            # NEW: Used in JournalEntry if enabled
    # "TimeActivity",

    # # Transaction Entities
    "Invoice",
    "Payment",
    "CreditMemo",
    "SalesReceipt",
    "RefundReceipt",
    "Estimate",

    "Bill",
    "BillPayment",
    "VendorCredit",
    "PurchaseOrder",
    "Purchase",

    "Deposit",
    "JournalEntry",
    "Transfer",
    "InventoryAdjustment",   # NEW: For inventory corrections
    # "ReimburseCharge",       # NEW: Employee reimbursements

    # # System / Company Setup Entities
    "CompanyInfo",
    "Preferences",
    "User",
    "Attachable",
    "ExchangeRate",
    "Budget",
    "Estimate",
    "taxclassification"
]


# Sort column per entity
entity_sort_column = {
    # Master Entities
    "Account": "Id",
    "Term": "MetaData.CreateTime",
    "PaymentMethod": "MetaData.CreateTime",
    "Currency": "Id",
    "CompanyCurrency": "Id",                 # NEW
    "Department": "MetaData.CreateTime",
    "Class": "MetaData.CreateTime",
    "TaxAgency": "MetaData.CreateTime",
    "TaxRate": "MetaData.CreateTime",
    "TaxCode": "MetaData.CreateTime",
    "Customer": "MetaData.CreateTime",
    "Vendor": "MetaData.CreateTime",
    "Employee": "MetaData.CreateTime",
    "Item": "MetaData.CreateTime",
    "CustomFieldDefinition": "Id",           # NEW
    "JournalCode": "MetaData.CreateTime",    # NEW
    "TimeActivity": "MetaData.CreateTime",

    # Transactions
    "Invoice": "MetaData.CreateTime",
    "Payment": "MetaData.CreateTime",
    "CreditMemo": "MetaData.CreateTime",
    "SalesReceipt": "MetaData.CreateTime",
    "RefundReceipt": "MetaData.CreateTime",
    "Estimate": "MetaData.CreateTime",

    "Bill": "MetaData.CreateTime",
    "BillPayment": "MetaData.CreateTime",
    "VendorCredit": "MetaData.CreateTime",
    "PurchaseOrder": "MetaData.CreateTime",
    "Purchase": "MetaData.CreateTime",

    "Deposit": "MetaData.CreateTime",
    "JournalEntry": "MetaData.CreateTime",
    "Transfer": "MetaData.CreateTime",
    "InventoryAdjustment": "MetaData.CreateTime",  # NEW
    "ReimburseCharge": "MetaData.CreateTime",      # NEW

    # System / Company
    "CompanyInfo": "Id",
    "Preferences": "Id",
    "User": "Id",
    "Attachable": "MetaData.CreateTime",
    "ExchangeRate": "EffectiveDate",
    "Budget": "StartDate",
    
    "Estimate" : "MetaData.CreateTime"

}



























#============================================================================================







# # entities.py

# # Ordered entity list for QBO sync: Master → Transactions → System
# entity_list = [
#     # Master / List Entities (in dependency order)
#     "Account",         # Required for all financial transactions
#     "Term",            # Used in customer/vendor payment terms
#     "PaymentMethod",   # Used in Payment & BillPayment
#     "Currency",        # Used if multicurrency is enabled
#     "Department",      # For departmental tracking
#     "Class",           # For classification tracking
#     "TaxAgency",       # Used in TaxRate & TaxCode
#     "TaxRate",         # Used in TaxCode
#     "TaxCode",         # Used in sales transactions
#     "Customer",        # Required for Invoices, Payments, etc.
#     "Vendor",          # Required for Bills, Purchase Orders
#     "Employee",        # Used in TimeActivity & Payroll
#     "Item",            # Used in Invoice, Bill, SalesReceipt lines
#     "TimeActivity",    # For time tracking (can link to employee/customer)

#     # Transaction Entities
#     "Invoice",
#     "Payment",
#     "CreditMemo",
#     "SalesReceipt",
#     "RefundReceipt",
#     "Estimate",

#     "Bill",
#     "BillPayment",
#     "VendorCredit",
#     "PurchaseOrder",
#     "Purchase",

#     "Deposit",
#     "JournalEntry",
#     "Transfer",

#     # System / Company Setup Entities
#     "CompanyInfo",
#     "Preferences",
#     "User",
#     "Attachable",
#     "ExchangeRate",
#     "Budget"
# ]


# # Sort column per entity
# entity_sort_column = {
#     # Master Entities
#     "Account": "Id",
#     "Term": "MetaData.CreateTime",
#     "PaymentMethod": "MetaData.CreateTime",
#     "Currency": "Id",
#     "Department": "MetaData.CreateTime",
#     "Class": "MetaData.CreateTime",
#     "TaxAgency": "MetaData.CreateTime",
#     "TaxRate": "MetaData.CreateTime",
#     "TaxCode": "MetaData.CreateTime",
#     "Customer": "MetaData.CreateTime",
#     "Vendor": "MetaData.CreateTime",
#     "Employee": "MetaData.CreateTime",
#     "Item": "MetaData.CreateTime",
#     "TimeActivity": "MetaData.CreateTime",

#     # Transactions
#     "Invoice": "MetaData.CreateTime",
#     "Payment": "MetaData.CreateTime",
#     "CreditMemo": "MetaData.CreateTime",
#     "SalesReceipt": "MetaData.CreateTime",
#     "RefundReceipt": "MetaData.CreateTime",
#     "Estimate": "MetaData.CreateTime",

#     "Bill": "MetaData.CreateTime",
#     "BillPayment": "MetaData.CreateTime",
#     "VendorCredit": "MetaData.CreateTime",
#     "PurchaseOrder": "MetaData.CreateTime",
#     "Purchase": "MetaData.CreateTime",

#     "Deposit": "MetaData.CreateTime",
#     "JournalEntry": "MetaData.CreateTime",
#     "Transfer": "MetaData.CreateTime",

#     # System / Company
#     "CompanyInfo": "Id",
#     "Preferences": "Id",
#     "User": "Id",
#     "Attachable": "MetaData.CreateTime",
#     "ExchangeRate": "EffectiveDate",
#     "Budget": "StartDate"
# }
