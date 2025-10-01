# config/mapping/taxcode_mapping.py

# QBO fields -> Source column names from [SOURCE_SCHEMA].[TaxCode]
# Send on create: Name, Active, Taxable, TaxGroup, Description, Sales/Purchase TaxRateList details
TAXCODE_COLUMN_MAPPING = {
    # Send on create (top-level)
    "Name":                       "Name",
    "Description":                "Description",
    "Active":                     "Active",
    "Taxable":                    "Taxable",
    "TaxGroup":                   "TaxGroup",

    # Sales details (0..1..)
    "Sales[0].TaxRateRef.value":  "SalesTaxRateList.TaxRateDetail[0]].TaxRateRef.value]",
    "Sales[0].TaxRateRef.name":   "SalesTaxRateList.TaxRateDetail[0]].TaxRateRef.name]",
    "Sales[0].TaxTypeApplicable": "SalesTaxRateList.TaxRateDetail[0]].TaxTypeApplicable]",
    "Sales[0].TaxOrder":          "SalesTaxRateList.TaxRateDetail[0]].TaxOrder]",

    "Sales[1].TaxRateRef.value":  "SalesTaxRateList.TaxRateDetail[1]].TaxRateRef.value]",
    "Sales[1].TaxRateRef.name":   "SalesTaxRateList.TaxRateDetail[1]].TaxRateRef.name]",
    "Sales[1].TaxTypeApplicable": "SalesTaxRateList.TaxRateDetail[1]].TaxTypeApplicable]",
    "Sales[1].TaxOrder":          "SalesTaxRateList.TaxRateDetail[1]].TaxOrder]",

    # Purchase details (0..1..)
    "Purchase[0].TaxRateRef.value":  "PurchaseTaxRateList.TaxRateDetail[0]].TaxRateRef.value]",
    "Purchase[0].TaxRateRef.name":   "PurchaseTaxRateList.TaxRateDetail[0]].TaxRateRef.name]",
    "Purchase[0].TaxTypeApplicable": "PurchaseTaxRateList.TaxRateDetail[0]].TaxTypeApplicable]",
    "Purchase[0].TaxOrder":          "PurchaseTaxRateList.TaxRateDetail[0]].TaxOrder]",

    "Purchase[1].TaxRateRef.value":  "PurchaseTaxRateList.TaxRateDetail[1]].TaxRateRef.value]",
    "Purchase[1].TaxRateRef.name":   "PurchaseTaxRateList.TaxRateDetail[1]].TaxRateRef.name]",
    "Purchase[1].TaxTypeApplicable": "PurchaseTaxRateList.TaxRateDetail[1]].TaxTypeApplicable]",
    "Purchase[1].TaxOrder":          "PurchaseTaxRateList.TaxRateDetail[1]].TaxOrder]",

    # Read-only/reference (copied to Map_* but NOT sent on create)
    "domain":                     "domain",
    "sparse":                     "sparse",
    "Id":                         "Id",
    "SyncToken":                  "SyncToken",
    "MetaData.CreateTime":        "MetaData.CreateTime",
    "MetaData.LastUpdatedTime":   "MetaData.LastUpdatedTime",
}
