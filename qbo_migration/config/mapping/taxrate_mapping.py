# config/mapping/taxrate_mapping.py

# QBO fields -> Source column names from [SOURCE_SCHEMA].[TaxRate]
# Only send allowed fields on create: Name, RateValue, AgencyRef.value, Active (optional), Description (optional)
# The others are copied to Map_* for reference only.
TAXRATE_COLUMN_MAPPING = {
    # Send on create
    "Name":                 "Name",
    "RateValue":            "RateValue",
    "AgencyRef.value":      "AgencyRef.value",
    "Active":               "Active",
    "Description":          "Description",

    # Reference / read-only fields to replicate, NOT sent on create
    "SpecialTaxType":             "SpecialTaxType",
    "DisplayType":                "DisplayType",
    "domain":                     "domain",
    "sparse":                     "sparse",
    "Id":                         "Id",
    "SyncToken":                  "SyncToken",
    "MetaData.CreateTime":        "MetaData.CreateTime",
    "MetaData.LastUpdatedTime":   "MetaData.LastUpdatedTime",
    "TaxReturnLineRef.value":     "TaxReturnLineRef.value",

    # Effective rate history columns (kept for reference)
    "EffectiveTaxRate[0].RateValue":      "EffectiveTaxRate[0]].RateValue]",
    "EffectiveTaxRate[0].EffectiveDate":  "EffectiveTaxRate[0]].EffectiveDate]",
    "EffectiveTaxRate[0].EndDate":        "EffectiveTaxRate[0]].EndDate]",
    "EffectiveTaxRate[1].RateValue":      "EffectiveTaxRate[1]].RateValue]",
    "EffectiveTaxRate[1].EffectiveDate":  "EffectiveTaxRate[1]].EffectiveDate]",
}
