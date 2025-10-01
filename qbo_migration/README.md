# QBO to QBO Migration Project

## Root Structure

- **generate_project_structure.py**  
  Script to automatically generate the required folder and file structure for the migration project.

- **Production_ETL_Resource_and_Cost_Estimation.csv / .xlsx**  
  Contains resource and cost estimation data for ETL processes in production.

- **QBO_To_QBO_Summary.xlsx**  
  Summary of migration status, entity mapping, and overall progress.

- **README.md**  
  Main documentation for the ISSCortex_Tporter project.

- **qbo_migration/**  
  Main migration engine and all supporting modules (see below for details).

---

## qbo_migration Folder Structure

- **auth/**  
  Handles authentication and token management for API access. Contains logic for refreshing tokens and initializing authentication.

- **config/**  
  Manages configuration entities and mapping logic. Includes entity definitions and mapping configurations for migration.

- **distroyer/**  
  Contains scripts for deleting various entities (bills, invoices, payments) from the target system, useful for cleanup and rollback.

- **extraction/**  
  Responsible for extracting data from the source system. Includes extractors for specific data types.

- **logs/**  
  Stores migration and entity-specific logs for auditing and troubleshooting.

- **migration/**  
  Core migration scripts for each entity (class, currency, term, department, payment method, account, vendor, item, customer, employee, invoice, payment, bill, vendor credit, deposit, journal entry, credit memo, bill payment, purchase, sales receipt, refund receipt). Each script handles the migration logic for its respective entity.

- **porter_initiator/**  
  Initiates extraction and migration processes for different entities. Contains orchestrators for starting migration flows.

- **storage/**  
  Contains subfolders for different storage backends (e.g., duckdb, sqlserver) used during migration.

- **transformation/**  
  Handles data transformation logic required before migration.

- **utils/**  
  Utility functions for logging, retry handling, mapping updates, token refresh, and other helper logic.

- **validator/**  
  Scripts for validating data before and after migration. Includes pre-extraction, pre-migration, and post-migration validation logic.

- **main.py**  
  Entry point for running the migration in development mode. Orchestrates extraction and migration phases.

- **main_production.py**  
  Entry point for running the migration in production mode. Uses initiators for each entity migration.

- **requirements.txt**  
  Lists Python dependencies for the project.

- **.env**  
  Stores environment variables and API credentials for secure access.

- **last_token_refresh.txt**  
  Stores the timestamp of the last token refresh for tracking authentication status.

---

## Usage Instructions

1. **Setup Environment:**
   - Install Python 3.8+ and required packages from `requirements.txt`.
   - Configure `.env` with your QBO credentials.

2. **Run Extraction:**
   - Use `main.py` or `main_production.py` to start extraction and migration.

3. **Monitor Logs:**
   - Check the `logs/` folder for migration status and error details.

4. **Validate Data:**
   - Use scripts in `validator/` for pre- and post-migration validation.

5. **Cleanup (if needed):**
   - Use scripts in `distroyer/` to remove unwanted or duplicate records.

---

## Contributors

- Project Lead: [Your Name]
- Team: ISSC Data Migration Team
- Contact: [your.email@domain.com]

---

## License

This project is proprietary to ISSC. For internal use only.
