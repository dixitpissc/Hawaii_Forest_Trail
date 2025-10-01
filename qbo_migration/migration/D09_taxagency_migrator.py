"""
Tax Agency Migrator - D09_taxagency_migrator.py

Purpose
-------
This module migrates TaxAgency records from a source SQL schema into QuickBooks Online (QBO)
via the QBO REST API. It builds QBO-safe JSON payloads, posts them, and records mapping and
migration status in a mapping table: `porter_entities_mapping.Map_TaxAgency`.

Key features & behavior
-----------------------
- Reads source TaxAgency records from `SOURCE_SCHEMA.TaxAgency`.
- Creates/refreshes a mapping table `MAPPING_SCHEMA.Map_TaxAgency` which includes:
  - Source_Id   : original source Id
  - Target_Id   : QBO Id (populated on success)
  - Porter_Status: Ready | Success | Failed
  - Retry_Count : integer
  - Failure_Reason : diagnostic text (short)
  - Payload_JSON : pretty-printed JSON payload that will be/was sent to QBO
  - Original_DisplayName : original DisplayName (keeps audit trail when we modify names)

- Payload construction:
  - Allowed fields for TaxAgency create: "DisplayName", "TaxTrackedOnSales",
    "TaxTrackedOnPurchases".
  - Sanitization is applied to DisplayName to remove HTML tags, unescape entities,
    normalize Unicode, remove zero-width/invisible/control characters, and collapse whitespace.
  - Initial truncation applied to 100 characters (QBO nominal limit).

- Posting & dynamic fallback algorithm (automated):
  1. Try posting the sanitized DisplayName (up to 100 chars).
  2. If QBO returns a length validation (code 6000 / "more than 100 characters" /
     "account_name"), try successive transformations **in this order**:
       a. Collapse repeated whitespace into single spaces (e.g. "a   b" -> "a b").
       b. Remove all whitespace entirely.
       c. Progressive truncation to smaller sizes: 100 → 80 → 60 → 50 → 40 → 30 → 20.
     Stop at the first transformation that QBO accepts.
  3. If QBO reports a duplicate name (code 6240) the script tries to retrieve the
     existing QBO Id from the error detail (`Id=<n>`). If not present, it queries QBO
     for the TaxAgency by DisplayName and uses the found Id. The row is then marked Success.
- On success, writes Target_Id, updates Payload_JSON to the accepted payload, and sets
  Porter_Status='Success'. When a fallback modified the DisplayName, Original_DisplayName
  is preserved for audit.
- On non-recoverable failures (network or non-length validation errors), sets Porter_Status='Failed',
  increments Retry_Count, and writes a brief Failure_Reason for diagnostics.

Design decisions & rationale
----------------------------
- The dynamic fallback approach favors data continuity — migration continues automatically
  while preserving the original DisplayName for auditing and later reconciliation.
- Sanitization is conservative: it avoids altering visible characters unless absolutely necessary,
  removing invisible/control chars and HTML artifacts that frequently trigger remote validation errors.
- The algorithm intentionally tries whitespace-based transforms before destructive truncation to
  preserve as much of the original name as possible.

Database side-effects
---------------------
- Creates or refreshes rows (via insert_invoice_map_dataframe) in:
  [{MAPPING_SCHEMA}].Map_TaxAgency
- Updates rows during payload generation and posting:
  - Payload_JSON is set to the sanitized/truncated payload that will be posted.
  - Original_DisplayName is stored once (if sanitized differs from original).
  - On success: Target_Id, Porter_Status set to 'Success', Failure_Reason cleared.
  - On failure: Porter_Status set to 'Failed', Retry_Count incremented, Failure_Reason set.

Configuration & prerequisites
-----------------------------
- Requires environment variables:
  - SOURCE_SCHEMA (default 'dbo')
  - MAPPING_SCHEMA (default 'porter_entities_mapping')
  - QBO_ENVIRONMENT (optional; 'sandbox' or 'production')
  - QBO auth provided by utils.token_refresher.get_qbo_context_migration()

- Dependencies (project-local modules):
  - storage.sqlserver.sql (for DB read/write helpers)
  - utils.token_refresher (for QBO token/context)
  - utils.log_timer (logger, ProgressTimer)
  - config.mapping.taxagency_mapping (column mapping)

How to run
----------
- Import and call migrate_taxagencies() or resume_or_post_taxagencies() from your migration
  runner script. Example:
      from migration.D01_taxagency_migrator import migrate_taxagencies
      migrate_taxagencies()

Diagnostics & support
--------------------
- The module records short diagnostic messages in Failure_Reason for failed rows.
- For deep troubleshooting, examine:
  - Map_TaxAgency.Payload_JSON (exact payload sent)
  - Map_TaxAgency.Original_DisplayName (pre-modification value)
  - Map_TaxAgency.Failure_Reason (short diagnostic)
- If QBO still rejects a row after the dynamic fallback, the row will be left in 'Failed' status
  with a diagnostic string — use that to open a support ticket with Intuit.

Notes & possible extensions
---------------------------
- Optionally append a deterministic short suffix (e.g., " (SRC{Source_Id})") after truncation
  to preserve uniqueness — this is not enabled by default but can be added.
- If you prefer quieter logs in production, adjust logger levels via utils.log_timer.global_logger.

Sequence : 08
Module: taxagency_migrator.py
Author: Dixit Prajapati
Created: 2025-09-18
Description: Handles migration of taxagency records from source system to QBO
Production : Ready
Phase : 02 - Multi User
"""

import os
import json
import re
import requests
import urllib.parse
import pandas as pd
import unicodedata
from html import unescape
from dotenv import load_dotenv

from storage.sqlserver import sql
from storage.sqlserver.sql import (
    ensure_schema_exists,
    insert_invoice_map_dataframe,
    sanitize_column_name,
    table_exists,
)
from utils.token_refresher import auto_refresh_token_if_needed, get_qbo_context_migration
from utils.log_timer import global_logger as logger, ProgressTimer
from config.mapping.taxagency_mapping import TAXAGENCY_COLUMN_MAPPING as column_mapping

# ──────────────────────────────────────────────────────────────
# Bootstrap
# ──────────────────────────────────────────────────────────────
load_dotenv()
auto_refresh_token_if_needed()

SOURCE_SCHEMA = os.getenv("SOURCE_SCHEMA", "dbo")
MAPPING_SCHEMA = os.getenv("MAPPING_SCHEMA", "porter_entities_mapping")

ensure_schema_exists(MAPPING_SCHEMA)

# ──────────────────────────────────────────────────────────────
# Sanitization utils (detect & remove invisible/control chars)
# ──────────────────────────────────────────────────────────────
_INVISIBLE_CHARS = (
    "\u200B"  # zero width space
    "\u200C"  # zero width non-joiner
    "\u200D"  # zero width joiner
    "\uFEFF"  # byte order mark
)
_INVISIBLE_RE = re.compile("[" + re.escape(_INVISIBLE_CHARS) + "]")
_TAG_RE = re.compile(r"<[^>]+>")

def sanitize_display_name(name: str) -> str:
    """
    Remove HTML tags/entities, normalize Unicode, remove invisible/control characters,
    and collapse repeated whitespace.
    Returns a sanitized string safe for length calculations and posting.
    """
    if name is None:
        return None
    s = str(name)
    s = unescape(s)
    s = _TAG_RE.sub("", s)
    s = unicodedata.normalize("NFKC", s)
    s = _INVISIBLE_RE.sub("", s)
    s = "".join(ch for ch in s if (ch == "\t" or ch == "\n" or ch == "\r" or (31 < ord(ch) < 0x110000)))
    s = " ".join(s.split())
    return s

def collapse_whitespace(name: str) -> str:
    """
    Replace any run of whitespace characters with a single space and trim ends.
    """    
    if name is None:
        return None
    return re.sub(r"\s+", " ", str(name)).strip()

def remove_all_whitespace(name: str) -> str:
    """
    Remove all whitespace characters (space, tab, newline) from the string.
    """    
    if name is None:
        return None
    return re.sub(r"\s+", "", str(name))

def debug_display_name_info(name: str) -> str:
    if name is None:
        return "None"
    s = str(name)
    repr_s = repr(s)
    char_len = len(s)
    byte_len = len(s.encode("utf-8"))
    hex_codes = " ".join(f"{ord(c):04x}" for c in s)
    return f"repr={repr_s}; chars={char_len}; bytes={byte_len}; hex={hex_codes}"

# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def _bool(val):
    if pd.isna(val):
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("1", "true", "t", "yes", "y"):
        return True
    if s in ("0", "false", "f", "no", "n"):
        return False
    return None

def _clean_nulls(df: pd.DataFrame) -> pd.DataFrame:
    return df.map(lambda x: None if isinstance(x, str) and x.strip() == "" else x)

def _src(col_key: str) -> str:
    return sanitize_column_name(column_mapping[col_key])

def truncate_display_name(name: str, max_len: int = 100) -> str:
    if name is None:
        return name
    s = str(name)
    if len(s) <= max_len:
        return s
    return s[:max_len]

def get_qbo_auth():
    ctx = get_qbo_context_migration()
    env = os.getenv("QBO_ENVIRONMENT", "sandbox")
    base_root = "https://sandbox-quickbooks.api.intuit.com" if env == "sandbox" else "https://quickbooks.api.intuit.com"
    realm = ctx["REALM_ID"]
    access_token = ctx["ACCESS_TOKEN"]
    base = f"{base_root}/v3/company/{realm}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    return base, headers

def find_existing_taxagency_by_name(display_name: str):
    if not display_name:
        return None
    base, headers = get_qbo_auth()
    safe_name = display_name.replace("'", "''")
    q = f"select * from TaxAgency where DisplayName = '{safe_name}'"
    url = f"{base}/query?query={urllib.parse.quote(q)}"
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            qr = data.get("QueryResponse", {})
            ta_list = qr.get("TaxAgency") or []
            if len(ta_list) > 0:
                return ta_list[0].get("Id")
    except Exception:
        logger.exception("Exception while querying existing TaxAgency")
    return None

# ──────────────────────────────────────────────────────────────
# Mapping table init
# ──────────────────────────────────────────────────────────────
def ensure_mapping_table():
    """
    Initialize or refresh MAPPING_SCHEMA.Map_TaxAgency from SOURCE_SCHEMA.TaxAgency.
    Ensures migration columns exist (Target_Id, Porter_Status, Retry_Count, Failure_Reason,
    Payload_JSON, Original_DisplayName) and populates Original_DisplayName for auditing.
    """

    logger.info("Initializing Map_TaxAgency from source...")
    df = sql.fetch_table("TaxAgency", SOURCE_SCHEMA)
    if df.empty:
        logger.warning("No rows found in source [TaxAgency].")
        return

    df = _clean_nulls(df.copy())

    if "Id" in df.columns:
        df["Source_Id"] = df["Id"]
    else:
        df["Source_Id"] = range(1, len(df) + 1)

    for col in ("Target_Id", "Porter_Status", "Retry_Count", "Failure_Reason", "Payload_JSON", "Original_DisplayName"):
        if col not in df.columns:
            df[col] = None

    df["Porter_Status"] = "Ready"
    df["Retry_Count"] = 0
    df["Failure_Reason"] = None
    df["Payload_JSON"] = None

    if "DisplayName" in df.columns:
        df["Original_DisplayName"] = df["DisplayName"]
    else:
        df["Original_DisplayName"] = None

    if table_exists("Map_TaxAgency", MAPPING_SCHEMA):
        sql.run_query(f"DELETE FROM [{MAPPING_SCHEMA}].[Map_TaxAgency]")

    insert_invoice_map_dataframe(df, "Map_TaxAgency", MAPPING_SCHEMA)
    logger.info(f"Inserted {len(df)} rows into {MAPPING_SCHEMA}.Map_TaxAgency")

# ──────────────────────────────────────────────────────────────
# Payload builder
# ──────────────────────────────────────────────────────────────
def build_payload(row: pd.Series) -> dict:
    """
    Build a QBO-compliant TaxAgency payload from a source row.

    - Reads source DisplayName and tax flags.
    - Sanitizes and truncates DisplayName (initial cap: 100 chars).
    - Persists Original_DisplayName if sanitization changed the input.
    - Returns dict suitable for JSON serialization and posting to QBO.
    """    
    src_display = row.get(_src("DisplayName"))
    if not src_display or str(src_display).strip() == "":
        return {}

    sanitized = sanitize_display_name(src_display)
    final_display = truncate_display_name(sanitized, max_len=100)

    try:
        sid = row.get("Source_Id")
        if str(sanitized) != str(src_display):
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_TaxAgency]
                SET Original_DisplayName = ?
                WHERE Source_Id = ?
            """, (str(src_display), sid))
    except Exception:
        logger.exception("Could not persist Original_DisplayName (non-fatal)")

    payload = {"DisplayName": final_display}

    t_sales = _bool(row.get(_src("TaxTrackedOnSales")))
    if t_sales is not None:
        payload["TaxTrackedOnSales"] = t_sales

    t_purch = _bool(row.get(_src("TaxTrackedOnPurchases")))
    if t_purch is not None:
        payload["TaxTrackedOnPurchases"] = t_purch

    return payload

# ──────────────────────────────────────────────────────────────
# Batch payload generation
# ──────────────────────────────────────────────────────────────
def generate_taxagency_payloads_in_batches(batch_size=500):
    """
    Generate and persist JSON payloads (Payload_JSON) for Map_TaxAgency rows marked 'Ready'.
    Processes rows in batches to avoid loading the entire table into memory.
    """    

    logger.info("Generating TaxAgency payloads...")
    while True:
        df = sql.fetch_table_with_params(
            f"""
            SELECT TOP {batch_size} *
            FROM [{MAPPING_SCHEMA}].[Map_TaxAgency]
            WHERE Porter_Status = 'Ready'
              AND (Payload_JSON IS NULL OR Payload_JSON = '')
            """,
            ()
        )
        if df.empty:
            break

        for _, row in df.iterrows():
            sid = row["Source_Id"]
            payload = build_payload(row)
            if not payload:
                reason = "Missing required DisplayName"
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_TaxAgency]
                    SET Porter_Status='Failed', Failure_Reason=?
                    WHERE Source_Id=?
                """, (reason, sid))
                continue

            payload_json = json.dumps(payload, indent=2, ensure_ascii=False)
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_TaxAgency]
                SET Payload_JSON=?, Failure_Reason=NULL
                WHERE Source_Id=?
            """, (payload_json, sid))

        logger.info("Batch payloads generated.")

# ──────────────────────────────────────────────────────────────
# Posting logic with dynamic whitespace/truncation fallback
# ──────────────────────────────────────────────────────────────
_session = requests.Session()

def _extract_qbo_error(resp_json):
    try:
        fault = resp_json.get("Fault") or {}
        errs = fault.get("Error") or []
        if errs and isinstance(errs, list):
            return errs
    except Exception:
        pass
    return []

def post_taxagency(row: pd.Series):
    """
    Post a single TaxAgency payload to QBO with dynamic fallback for length validation.

    Steps:
      1. Load Payload_JSON from Map_TaxAgency.
      2. Attempt to post the sanitized payload as-is.
      3. If QBO returns a length validation, try in order:
         - collapse repeated whitespace
         - remove all whitespace
         - progressively truncate to smaller lengths (100, 80, 60, 50, 40, 30, 20)
      4. If QBO reports a duplicate (6240), extract the Id or query QBO and mark success.
      5. On success, update Target_Id and Porter_Status='Success', persist the payload used.
      6. On non-recoverable failures, set Porter_Status='Failed', increment Retry_Count, and set Failure_Reason.

    The function preserves Original_DisplayName for audit when it modifies the DisplayName.
    """

    sid = row["Source_Id"]
    if row.get("Porter_Status") == "Success":
        return

    if int(row.get("Retry_Count") or 0) >= 7:
        logger.warning(f"Skipping Source_Id={sid} — retry limit reached.")
        return

    if not row.get("Payload_JSON"):
        logger.warning(f"Skipping Source_Id={sid} — missing Payload_JSON.")
        return

    auto_refresh_token_if_needed()
    base, headers = get_qbo_auth()
    url = f"{base}/taxagency"

    try:
        original_payload = json.loads(row["Payload_JSON"])
    except Exception as e:
        reason = f"Invalid Payload_JSON parse: {e}"
        sql.run_query(f"""
            UPDATE [{MAPPING_SCHEMA}].[Map_TaxAgency]
            SET Porter_Status='Failed',
                Retry_Count=ISNULL(Retry_Count,0)+1,
                Failure_Reason=?
            WHERE Source_Id=?
        """, (reason, sid))
        return

    # helper to do a single post attempt and return (resp, errs)
    def single_post(p):
        try:
            resp = _session.post(url, headers=headers, json=p, timeout=60)
        except Exception as e:
            return None, str(e)
        try:
            resp_json = resp.json()
        except Exception:
            resp_json = None
        errs = _extract_qbo_error(resp_json) if resp_json else []
        return resp, errs

    # sequence of transformations to try when we hit a length validation:
    # 1) as-is (already sanitized/truncated to 100)
    # 2) collapse whitespace (multiple -> single)
    # 3) remove all whitespace
    # 4) progressive truncation to shorter lengths
    tried_payloads = []

    # define progressive truncation sizes (after whitespace attempts)
    trunc_sizes = [100, 80, 60, 50, 40, 30, 20]

    # build candidate payload generator
    def generate_candidates(orig_display):
        # 1) as-is
        yield orig_display, "original"
        # 2) collapsed whitespace
        collapsed = collapse_whitespace(orig_display)
        if collapsed != orig_display:
            yield collapsed, "collapse_ws"
        # 3) remove all whitespace
        no_ws = remove_all_whitespace(orig_display)
        if no_ws != orig_display and no_ws != collapsed:
            yield no_ws, "remove_all_ws"
        # 4) truncated versions (progressively smaller)
        for size in trunc_sizes:
            truncated = truncate_display_name(orig_display, max_len=size)
            if truncated != orig_display:
                yield truncated, f"truncate_{size}"

    # attempt each candidate until success or exhausted
    orig_display = original_payload.get("DisplayName")
    final_success = False
    final_qid = None
    final_payload_used = None

    for candidate_display, reason_tag in generate_candidates(orig_display):
        trial_payload = dict(original_payload)
        trial_payload["DisplayName"] = candidate_display

        resp, errs_or_error = single_post(trial_payload)
        # network/exception
        if resp is None:
            errstr = f"HTTP exception: {errs_or_error}"
            sql.run_query(f"""
                UPDATE [{MAPPING_SCHEMA}].[Map_TaxAgency]
                SET Porter_Status='Failed',
                    Retry_Count=ISNULL(Retry_Count,0)+1,
                    Failure_Reason=?
                WHERE Source_Id=?
            """, (errstr, sid))
            return

        # success
        if resp.status_code in (200, 201):
            try:
                data = resp.json()
                qid = None
                if isinstance(data, dict):
                    qta = data.get("TaxAgency")
                    if qta and isinstance(qta, dict):
                        qid = qta.get("Id")
                payload_json = json.dumps(trial_payload, indent=2, ensure_ascii=False)
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_TaxAgency]
                    SET Payload_JSON=?, Target_Id=?, Porter_Status='Success', Failure_Reason=NULL
                    WHERE Source_Id=?
                """, (payload_json, qid, sid))
                final_success = True
                final_qid = qid
                final_payload_used = trial_payload
                break
            except Exception as e:
                reason = f"Parse success response error: {e}"
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_TaxAgency]
                    SET Porter_Status='Failed',
                        Retry_Count=ISNULL(Retry_Count,0)+1,
                        Failure_Reason=?
                    WHERE Source_Id=?
                """, (reason, sid))
                return

        # non-success: check errors list
        errs = errs_or_error if isinstance(errs_or_error, list) else []
        # handle duplicate
        duplicate_handled = False
        for err in errs:
            code = str(err.get("code") or "")
            detail = err.get("Detail") or err.get("Message") or ""
            if code == "6240" or "Duplicate Name Exists Error" in (err.get("Message") or ""):
                m = re.search(r"Id\s*=\s*(\d+)", detail)
                if m:
                    existing_id = m.group(1)
                    sql.run_query(f"""
                        UPDATE [{MAPPING_SCHEMA}].[Map_TaxAgency]
                        SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL
                        WHERE Source_Id=?
                    """, (existing_id, sid))
                    duplicate_handled = True
                    break
                else:
                    existing_id = find_existing_taxagency_by_name(candidate_display)
                    if existing_id:
                        sql.run_query(f"""
                            UPDATE [{MAPPING_SCHEMA}].[Map_TaxAgency]
                            SET Target_Id=?, Porter_Status='Success', Failure_Reason=NULL
                            WHERE Source_Id=?
                        """, (existing_id, sid))
                        duplicate_handled = True
                        break
                    else:
                        reason = f"Duplicate reported but couldn't resolve Id. Detail: {detail}"
                        sql.run_query(f"""
                            UPDATE [{MAPPING_SCHEMA}].[Map_TaxAgency]
                            SET Porter_Status='Failed',
                                Retry_Count=ISNULL(Retry_Count,0)+1,
                                Failure_Reason=?
                            WHERE Source_Id=?
                        """, (reason, sid))
                        duplicate_handled = True
                        break
        if duplicate_handled:
            return

        # detect length error
        length_issue = False
        for err in errs:
            code = str(err.get("code") or "")
            detail = err.get("Detail") or ""
            message = err.get("Message") or ""
            combined = f"{message} {detail}"
            if code == "6000" or "more than 100 characters" in combined.lower() or "account_name" in combined.lower():
                length_issue = True
                break

        if length_issue:
            # try next candidate in sequence
            continue

        # other failures: record and stop
        try:
            resp_json = resp.json()
            reason_text = json.dumps(resp_json)[:2000]
        except Exception:
            reason_text = resp.text[:2000]
        sql.run_query(f"""
            UPDATE [{MAPPING_SCHEMA}].[Map_TaxAgency]
            SET Porter_Status='Failed',
                Retry_Count=ISNULL(Retry_Count,0)+1,
                Failure_Reason=?
            WHERE Source_Id=?
        """, (reason_text, sid))
        return

    # if succeeded above, final_success True
    if final_success:
        # persist Original_DisplayName if it differs
        try:
            orig_display_db = row.get("Original_DisplayName") or original_payload.get("DisplayName")
            if final_payload_used and final_payload_used.get("DisplayName") != orig_display_db:
                sql.run_query(f"""
                    UPDATE [{MAPPING_SCHEMA}].[Map_TaxAgency]
                    SET Original_DisplayName = ?
                    WHERE Source_Id=?
                """, (orig_display_db, sid))
        except Exception:
            pass
        return

    # if we've exhausted candidates without success, store diagnostic and fail
    diag = {
        "attempted_display": original_payload.get("DisplayName"),
    }
    reason = f"Validation length error; dynamic fallback exhausted. DIAG: {json.dumps(diag)}"
    sql.run_query(f"""
        UPDATE [{MAPPING_SCHEMA}].[Map_TaxAgency]
        SET Porter_Status='Failed',
            Retry_Count=ISNULL(Retry_Count,0)+1,
            Failure_Reason=?
        WHERE Source_Id=?
    """, (reason[:2000], sid))
    return

# ──────────────────────────────────────────────────────────────
# Orchestration
# ──────────────────────────────────────────────────────────────
def migrate_taxagencies():
    """
    Full migration entrypoint:
      - Rebuilds the mapping table from source.
      - Generates payloads.
      - Attempts to post all eligible rows (Ready|Failed).
    """
    print("\nStarting TaxAgency Migration")
    ensure_mapping_table()
    generate_taxagency_payloads_in_batches(batch_size=500)

    rows = sql.fetch_table("Map_TaxAgency", MAPPING_SCHEMA)
    eligible = rows[rows["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("No eligible TaxAgency rows to post.")
        return

    pt = ProgressTimer(len(eligible))
    for _, r in eligible.iterrows():
        post_taxagency(r)
        pt.update()

    print("TaxAgency migration completed.")

def resume_or_post_taxagencies():
    """
    Resume-only migration:
      - Validates Map_TaxAgency exists and matches source row count.
      - Ensures payloads exist; otherwise falls back to migrate_taxagencies().
      - Posts only eligible rows.
    """
    if not table_exists("Map_TaxAgency", MAPPING_SCHEMA):
        migrate_taxagencies()
        return

    mapped_df = sql.fetch_table("Map_TaxAgency", MAPPING_SCHEMA)
    src_df = sql.fetch_table("TaxAgency", SOURCE_SCHEMA)

    if len(mapped_df) != len(src_df):
        migrate_taxagencies()
        return

    if mapped_df["Payload_JSON"].isnull().any():
        migrate_taxagencies()
        return

    eligible = mapped_df[mapped_df["Porter_Status"].isin(["Ready", "Failed"])].reset_index(drop=True)
    if eligible.empty:
        logger.info("No eligible TaxAgency rows to post.")
        return

    pt = ProgressTimer(len(eligible))
    for _, r in eligible.iterrows():
        post_taxagency(r)
        pt.update()

    print("TaxAgency posting completed.")
