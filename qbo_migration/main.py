
# Entry point for orchestrating migration flow

# import os
# from dotenv import load_dotenv

# load_dotenv()

#Production Ready
#==================================== Extraction ==================================================

# from porter_initiator.x_00_extraction_Inititator import run_full_extraction

# if __name__ == "__main__":
#     run_full_extraction()

# ==================================################================================================

#Migration Module

# ==================================*** D01 - Class ***==========================================
# from migration.D01_class_migrator import migrate_classes
# def run_class_migration():
#     print("\n🚀 Starting class Migration Phase")
#     print("===================================")
#     migrate_classes()

# if __name__ == "__main__":
#     run_class_migration()
# ==================================******************===========================================

# ==================================*** D02 - Currency ***==========================================
# from migration.D02_currency_migrator import migrate_currencies
# def run_currency_migration():
#     migrate_currencies()
# if __name__ == "__main__":
#     run_currency_migration()
# ==================================******************===========================================

# ==================================*** D03 - Terms ***==========================================
# from migration.D03_term_migrator import migrate_terms
# def run_term_migration():
#     migrate_terms()
# if __name__ == "__main__":
#     run_term_migration()
# ==================================******************===========================================

# ==================================*** D04 - Departments ***==========================================
# from migration.D04_department_migrator import migrate_departments
# def run_department_migration():
#     migrate_departments()
# if __name__ == "__main__":
#     run_department_migration()
# ==================================******************===========================================

# ==================================*** D05 - Accounts ***=======================================
# from migration.D05_account_migrator import migrate_accounts,migrate_missing_accounts,retry_failed_accounts
# def run_accounts_migration():
#     # migrate_accounts()
#     retry_failed_accounts()

# if __name__ == "__main__":
#     run_accounts_migration()
    
# ==================================******************===========================================

# ==================================*** D06 - Vendors ***=======================================
# from migration.D06_vendor_migrator import conditionally_migrate_vendors
# def run_vendors_migration():
#     conditionally_migrate_vendors()

# if __name__ == "__main__":
#     run_vendors_migration()
# ==================================******************===========================================

# ==================================*** D07 - Item ***==========================================

# from migration.D07_item_migrator import migrate_items
# def run_item_migration():
#     migrate_items()

# if __name__ == "__main__":
#     run_item_migration()
# ==================================******************===========================================

# ==================================*** D08 - Customer ***==========================================
# from migration.D08_customer_migrator import migrate_customers

# def run_customer_migration():
#     print("\n🚀 Starting customer Migration Phase")
#     print("===================================")
#     migrate_customers()  # <== MUST BE HERE

# if __name__ == "__main__":
#     run_customer_migration()

# ==================================******************===========================================

# ==================================*** D09 - Employee ***==========================================
# from migration.D09_employee_migrator import migrate_employees
# def run_employees_migration():
#     migrate_employees()
# if __name__ == "__main__":
#     run_employees_migration()
# ==================================******************===========================================
 
# ==================================*** D10 - Payment Method ***==========================================
# from migration.D04_payment_method_migrator import migrate_payment_methods
# def run_payment_method_migration():
#     migrate_payment_methods()
# if __name__ == "__main__":
#     run_payment_method_migration()
# ==================================******************===========================================

# ==================================*** D11 - Invoice ***==========================================
# from migration.D11_invoice_migrator import resume_or_post_invoices
# def run_invoice_migration():
#     print("\n🚀 Starting Invoice Migration Phase")
#     print("===================================")
#     resume_or_post_invoices()

# if __name__ == "__main__":
#     run_invoice_migration()
# ==================================******************===========================================

# ==================================*** D12 - Payment ***========================================
# from migration.D12_payment_migrator import resume_or_post_payments
# def run_payment_migration():
#     print("\n🚀 Starting Payment Migration Phase")
#     print("===================================")
#     resume_or_post_payments()
# if __name__ == "__main__":
#     run_payment_migration()
# ==================================******************===========================================


# ==================================*** D15 - Bill ***========================================
# from migration.D15_bill_migrator import main
# def run_bills_migration():
#     main()

# if __name__ == "__main__":
#     run_bills_migration()
# ==================================******************===========================================


# ==================================*** D16 - Vendorcredit ***========================================
# from migration.D16_vendorcredit_migrator import migrate_vendorcredits
# def run_vendorcredit_migration():
#     migrate_vendorcredits()
# if __name__ == "__main__":
#     run_vendorcredit_migration()
# ==================================******************===========================================

# ==================================*** D16 - BillPayment ***========================================
# from migration.D17_billpayment_migrator import resume_or_post_billpayments
# def run_billpayment_migration():
#     resume_or_post_billpayments()
# if __name__ == "__main__":
#     run_billpayment_migration()
# ==================================******************===========================================


# Developing
# ==================================*** D16 - CreditMemo ***========================================
# from migration.D21_creditmemo_migrator import resume_or_post_creditmemos
# if __name__ == "__main__":
#     resume_or_post_creditmemos()
# ==================================******************===========================================


# ==================================*** D18 - Deposit ***========================================
# from migration.D18_deposit_migrator import resume_or_post_deposits
# if __name__ == "__main__":
#     resume_or_post_deposits()
# ==================================******************===========================================


# ==================================*** D19 - JournalEntry ***========================================
# from migration.D19_journalentry_migrator import resume_or_post_journalentries
# if __name__ == "__main__":
#     resume_or_post_journalentries()
# ==================================******************===========================================


# ==================================*** D20 - Purchase ***========================================
# from migration.D20_purchase_migrator import migrate_purchases
# if __name__ == "__main__":
#     migrate_purchases()
# ==================================******************===========================================

# from qbo_migration.validator.migration_status import generate_migration_status_report
# if __name__ == "__main__":
#     generate_migration_status_report()

# ==================================*** D22 - SalesReceipts ***========================================
# from migration.D22_salesreceipt_migrator import resume_or_post_salesreceipts
# if __name__ == "__main__":
#     resume_or_post_salesreceipts()
# ==================================******************===========================================


# ==================================*** D22 - RefundReceipts ***========================================
# from migration.D23_refundreceipt_migrator import resume_or_post_refundreceipts
# if __name__ == "__main__":
#     resume_or_post_refundreceipts()
# ==================================******************===========================================



# from validator.pre_validation.D00_qbo_preevaluation import pre_evaluation_summary

# if __name__ == "__main__":
#     pre_evaluation_summary()


# from distroyer.bill_delete import delete_all_qbo_bills
# from utils.token_refresher import auto_refresh_token_if_needed

# if __name__ == "__main__":
#     auto_refresh_token_if_needed()
#     delete_all_qbo_bills()



# def run_account_migration():
#     print("\n🚀 Starting Account Migration Phase")
#     print("===================================")
#     migrate_accounts()

# def run_term_migration():
#     print("\n🚀 Starting class Migration Phase")
#     print("===================================")
#     migrate_terms()


# def run_employee_migration():
#     print("\n🚀 Starting class Migration Phase")
#     print("===================================")
#     migrate_employees()

# def run_item_migration():
#     print("\n🚀 Starting class Migration Phase")
#     print("===================================")
#     migrate_items()

# def run_vendor_migration():
#     print("\n🚀 Starting class Migration Phase")
#     print("===================================")
#     migrate_vendors()


# def run_payment_migration():
#     print("\n🚀 Starting class Migration Phase")
#     print("===================================")
#     migrate_payments()


# from distroyer.master_distroyer import main

# if __name__ == "__main__":
#     # for val in range(1,100):
#     main()

# from migration.D11_invoice_migrator import resume_or_post_invoices
# def run_invoice_migration():
#     print("\n🚀 Starting Invoice Migration Phase")
#     print("===================================")
#     resume_or_post_invoices()

# if __name__ == "__main__":
#     run_invoice_migration()

# runner.py

# def run_invoice_migration():
#     print("\n🚀 Starting Invoice Migration Phase")
#     print("===================================")
#     resume_or_post_invoices()
#     print("🏁 Invoice Migration Completed")

# if __name__ == "__main__":
#     _should_refresh_token()
#     run_destroy_phase()
    # _should_refresh_token()
    # run_invoice_migration()

# from utils.dynamic_generation import generate_tokens_interactive

# if __name__ == "__main__":
#     generate_tokens_interactive()


# from validator.pre_validation.D01_class_validator import validate_class_table
# if __name__ == "__main__":
#     validate_class_table()




########################Validation#########################

# from validator.pre_validation.D05_account_validator import validate_account_table
# if __name__ == "__main__":
#     validate_account_table()

# from validator.pre_validation.D00_qbo_preevaluation import pre_evaluation_summary

# if __name__ == "__main__":
#     pre_evaluation_summary()



# from migration.D07_item_category_migrator import main

# if __name__ == "__main__":
#     main()



#POST VALIDATION 

# from validator.post_validation.validation import main
# if __name__ == "__main__":
#     main()

# from validator.extraction_summary.summary import summary

# if __name__ == "__main__":
#     summary()


# from extraction.D222_transaction_report import run_transaction_extraction
# if __name__ == "__main__":
#     run_transaction_extraction()


# from validator.pre_validation.D01_payment_method_validator import validate_paymentmethod_table
# if __name__ == "__main__":
#     validate_paymentmethod_table()


# from validator.post_analytics import generate_post_analytics

# if __name__ == "__main__":
#     generate_post_analytics()

