# config/mapping/employee_mapping.py

EMPLOYEE_COLUMN_MAPPING = {
    "GivenName": "GivenName",
    "FamilyName": "FamilyName",
    "DisplayName": "DisplayName",
    "PrintOnCheckName": "PrintOnCheckName",
    "Active": "Active",
    "HiredDate": "HiredDate",
    "BillableTime": "BillableTime",
    "PrimaryEmailAddr.Address": "PrimaryEmailAddr.Address",
    
    # Address block (if present)
    "PrimaryAddr.Line1": "PrimaryAddr.Line1",
    "PrimaryAddr.City": "PrimaryAddr.City",
    "PrimaryAddr.CountrySubDivisionCode": "PrimaryAddr.CountrySubDivisionCode",
    "PrimaryAddr.PostalCode": "PrimaryAddr.PostalCode"
}
