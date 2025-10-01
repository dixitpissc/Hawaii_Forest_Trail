import os
import json
import math
import re
from decimal import Decimal, InvalidOperation, getcontext
from datetime import datetime

import requests
import pandas as pd
from dotenv import load_dotenv

# Project utilities
from utils.token_refresher import auto_refresh_token_if_needed
from utils.log_timer import global_logger as logger, ProgressTimer
from storage.sqlserver import sql

getcontext().prec = 28  # high precision for safe Decimal math

# ==============================
# Tunables / Env-driven options
# ==============================
REPORT_SCHEMA = os.getenv("REPORT_SCHEMA", "reporting")
REPORT_TABLE  = os.getenv("BalanceSheet_TABLE", "BalanceSheet")
SAVE_TO_SQL   = os.getenv("BalanceSheet_SAVE_TO_SQL", "true").lower() == "true"

# Report options (only sent when provided)
AS_OF_DATE    = os.getenv("BalanceSheet_AS_OF_DATE")               # yyyy-mm-dd (as-of)
DATE_MACRO    = os.getenv("BalanceSheet_DATE_MACRO")               # e.g. "This Fiscal Year-to-date"
ACCOUNTING    = os.getenv("BalanceSheet_ACCOUNTING_METHOD")        # may be ignored by BS
CURRENCY      = os.getenv("BalanceSheet_CURRENCY")                 # "USD"|"CAD"|...
COLUMNS_BY    = os.getenv("BalanceSheet_COLUMNS")                  # e.g. "Classes"
SUMMARIZE_BY  = os.getenv("BalanceSheet_SUMMARIZE_COLUMN_BY")      # legacy alias
MINOR_VERSION = os.getenv("QBO_MINOR_VERSION", "75")

# Balance sheet also supports ranges:
START_DATE   = os.getenv("BalanceSheet_START_DATE")   # yyyy-mm-dd
END_DATE     = os.getenv("BalanceSheet_END_DATE")     # yyyy-mm-dd

# Pagination (ignored by many reports; safe to send)
START_POS     = int(os.getenv("BalanceSheet_STARTPOSITION", "1"))
PAGE_SIZE     = int(os.getenv("BalanceSheet_PAGESIZE", "1000"))

# Insertion behavior
FORCE_TEXT    = os.getenv("BalanceSheet_FORCE_TEXT_INSERT", "false").lower() == "true"
COERCE_TABLE  = os.getenv("BalanceSheet_COERCE_TABLE_TO_TEXT", "true").lower() == "true"

# ==============================
# Bootstrap
# ==============================
load_dotenv()
auto_refresh_token_if_needed()

access_token = os.getenv("QBO_ACCESS_TOKEN")
realm_id     = os.getenv("QBO_REALM_ID")
environment  = os.getenv("QBO_ENVIRONMENT", "production")

if not access_token or not realm_id:
    raise RuntimeError("❌ Missing QBO_ACCESS_TOKEN or QBO_REALM_ID in environment.")

base_url    = "https://sandbox-quickbooks.api.intuit.com" if environment == "sandbox" else "https://quickbooks.api.intuit.com"
REPORT_NAME = "BalanceSheet"

# ──────────────────────────────────────────────────────────────
# SQL adapter (schema/table creation with fallback)
# ──────────────────────────────────────────────────────────────
import pyodbc

def _exec_sql(stmt: str):
    # (a) Try your storage layer helper names
    for attr in ("execute", "execute_non_query", "run", "exec", "raw", "non_query"):
        fn = getattr(sql, attr, None)
        if callable(fn):
            return fn(stmt)
    # (b) Fallback to direct pyodbc
    host = os.getenv("SQLSERVER_HOST", "localhost")
    port = os.getenv("SQLSERVER_PORT", "1433")
    user = os.getenv("SQLSERVER_USER", "sa")
    pwd  = os.getenv("SQLSERVER_PASSWORD", "")
    db   = os.getenv("SQLSERVER_DATABASE")
    if not db:
        raise RuntimeError("SQLSERVER_DATABASE is not set; cannot open fallback connection.")
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={host},{port};DATABASE={db};UID={user};PWD={pwd};TrustServerCertificate=yes"
    )
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(stmt)
            conn.commit()

def _ensure_schema(schema: str):
    _exec_sql(
        f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}') "
        f"EXEC('CREATE SCHEMA [{schema}]');"
    )

def _create_empty_table(schema: str, table: str, columns: list[str]):
    if not columns:
        return
    cols_sql = ", ".join([f"CAST(NULL AS NVARCHAR(255)) AS [{c}]" for c in columns])
    _exec_sql(
        f"IF OBJECT_ID('[{schema}].[{table}]') IS NULL "
        f"SELECT {cols_sql} INTO [{schema}].[{table}] WHERE 1=0;"
    )

def _coerce_table_to_text(schema: str, table: str, columns: list[str]):
    """
    Force existing columns to NVARCHAR(255) to avoid type errors
    when table was previously created with numeric types.
    """
    for col in columns:
        try:
            _exec_sql(
                f"IF COL_LENGTH(N'[{schema}].[{table}]', N'{col}') IS NOT NULL "
                f"ALTER TABLE [{schema}].[{table}] ALTER COLUMN [{col}] NVARCHAR(255) NULL;"
            )
        except Exception as e:
            logger.warning(f"⚠️ Could not coerce column [{schema}].[{table}].[{col}] to NVARCHAR(255): {e}")

# ──────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────
def _build_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

def _build_params() -> dict:
    p = {"minorversion": MINOR_VERSION, "startposition": START_POS, "pagesize": PAGE_SIZE}

    # --- date handling (choose ONE path) ---
    if START_DATE or END_DATE:
        if START_DATE: p["start_date"] = START_DATE
        if END_DATE:   p["end_date"]   = END_DATE
        # Optional but common when using ranges:
        if SUMMARIZE_BY:  p["summarize_column_by"] = SUMMARIZE_BY  # e.g., Month|Quarter|Year|Total
    else:
        if AS_OF_DATE: p["as_of_date"] = AS_OF_DATE
        if DATE_MACRO: p["date_macro"] = DATE_MACRO

    # other options you already had
    if ACCOUNTING:  p["accounting_method"] = ACCOUNTING
    if CURRENCY:    p["currency"]          = CURRENCY
    if COLUMNS_BY:  p["columns"]           = COLUMNS_BY
    return p


def _safe_get(d, *path, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    return default if cur is None else cur

def _normalize_name(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    return (
        s.replace(" ", "_")
         .replace("/", "_")
         .replace("-", "_")
         .replace(".", "_")
         .replace("__", "_")
    )

# ──────────────────────────────────────────────────────────────
# Fetch + unwrap
# ──────────────────────────────────────────────────────────────
def fetch_report_page(startposition: int = 1) -> dict:
    url = f"{base_url}/v3/company/{realm_id}/reports/{REPORT_NAME}"
    params = _build_params()
    params["startposition"] = startposition
    resp = requests.get(url, headers=_build_headers(access_token), params=params, timeout=60)
    preview = (resp.text or "")[:2000].replace("\n", " ")
    logger.info(f"🧾 Report HTTP {resp.status_code} | preview: {preview}")
    try:
        data = resp.json()
    except ValueError:
        ctype = resp.headers.get("content-type", "unknown")
        raise RuntimeError(f"QBO report returned non-JSON payload (content-type={ctype}). Preview: {preview}")
    if resp.status_code != 200:
        raise RuntimeError(f"QBO reports HTTP {resp.status_code}: {preview}")
    return data

def _unwrap_report(obj: dict) -> dict:
    if not isinstance(obj, dict):
        raise ValueError("Report payload isn't a JSON object.")
    if "Fault" in obj:
        errs = obj["Fault"].get("Error", []) or []
        if errs:
            e0 = errs[0]
            code = e0.get("code")
            msg  = e0.get("Message") or "Fault"
            det  = e0.get("Detail") or ""
            raise RuntimeError(f"QBO Fault {code}: {msg} — {det}")
        raise RuntimeError("QBO Fault with no details.")
    if "Report" in obj and isinstance(obj["Report"], dict):
        return obj["Report"]
    if "report" in obj and isinstance(obj["report"], dict):
        return obj["report"]
    if all(k in obj for k in ("Header", "Columns", "Rows")):
        return obj
    top = list(obj.keys())[:10]
    raise ValueError(f"Unexpected report payload shape; top-level keys: {top}")

# ──────────────────────────────────────────────────────────────
# Dynamic column extraction (Title + ColKey) → output schema
# ──────────────────────────────────────────────────────────────
def _extract_columns_meta(report: dict):
    """
    Returns per-column meta with stable names:
      [{"idx":0,"title":"", "colkey":"account", "safe":"col_0__account"}, ...]
    and output columns (value + id).
    """
    cols = _safe_get(report, "Columns", "Column", default=[]) or []
    meta = []
    out_columns = []
    for i, c in enumerate(cols):
        title  = c.get("ColTitle") or f"col_{i}"
        md     = c.get("MetaData", []) or []
        colkey = None
        for m in md:
            if (m or {}).get("Name") == "ColKey":
                colkey = m.get("Value")
                break
        base = _normalize_name(title)
        if colkey:
            base = f"{base}__{_normalize_name(colkey)}"
        meta.append({"idx": i, "title": title, "colkey": colkey, "safe": base})
        out_columns.append(base)
        out_columns.append(f"{base}__Id")
    return meta, out_columns

# ──────────────────────────────────────────────────────────────
# Row walker (captures ALL values + IDs + group path + summaries)
# ──────────────────────────────────────────────────────────────
def _walk_rows_dynamic(rows_container: dict, col_meta: list, current_group_path: list, rows_out: list):
    if not rows_container:
        return
    rows = rows_container.get("Row", []) or []
    for r in rows:
        rtype = r.get("type")
        if rtype == "Section":
            hdr = r.get("Header") or {}
            hdr_vals = [cd.get("value") for cd in (hdr.get("ColData", []) or []) if isinstance(cd, dict)]
            group_name = next((v for v in hdr_vals if v), None) or r.get("group") or "Group"
            new_gp = current_group_path + [group_name]
            _walk_rows_dynamic(r.get("Rows"), col_meta, new_gp, rows_out)
            summary = r.get("Summary") or {}
            sdata = summary.get("ColData", []) if isinstance(summary, dict) else []
            if sdata:
                row = {}
                for c in col_meta:
                    i = c["idx"]
                    base = c["safe"]
                    val = sdata[i].get("value") if i < len(sdata) else None
                    cid = sdata[i].get("id")    if i < len(sdata) else None
                    row[base] = val
                    row[f"{base}__Id"] = cid
                for gi, gname in enumerate(new_gp, start=1):
                    row[f"GroupLevel{gi}"] = gname
                row["RowType"] = "SectionSummary"
                rows_out.append(row)
        elif rtype == "Data":
            coldata = r.get("ColData", []) or []
            row = {}
            for c in col_meta:
                i = c["idx"]
                base = c["safe"]
                val = coldata[i].get("value") if i < len(coldata) else None
                cid = coldata[i].get("id")    if i < len(coldata) else None
                row[base] = val
                row[f"{base}__Id"] = cid
            for gi, gname in enumerate(current_group_path, start=1):
                row[f"GroupLevel{gi}"] = gname
            row["RowType"] = "Data"
            rows_out.append(row)
        else:
            continue

# ──────────────────────────────────────────────────────────────
# Value cleaning for DB
# ──────────────────────────────────────────────────────────────
_NUMERIC_RE = re.compile(r"""^\s*-?(\d{1,3}(,\d{3})*|\d+)?(\.\d+)?\s*$""")

def _clean_money(x):
    if x is None:
        return None
    if isinstance(x, (int, float, Decimal)):
        try:
            return Decimal(str(x))
        except Exception:
            return None
    s = str(x).strip()
    if not s or s in {"--", "—", "NA", "N/A", "null", "None"}:
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()
    s = s.replace(",", "")
    for sym in ["$", "₹", "£", "€", "CAD", "USD"]:
        s = s.replace(sym, "")
    s = s.strip()
    if not re.match(r"^-?\d+(\.\d+)?$", s):
        return None
    try:
        d = Decimal(s)
        return -d if neg else d
    except InvalidOperation:
        return None

def _is_numeric_like_series(ser: pd.Series, sample=200, threshold=0.9):
    if ser.empty:
        return False
    vals = ser.dropna().astype(str)
    if vals.empty:
        return False
    sample_vals = vals.head(sample)
    hits, total = 0, 0
    for v in sample_vals:
        v_norm = str(v).strip().replace(",", "")
        if re.match(r"^\(?-?\d+(\.\d+)?\)?$", v_norm):
            hits += 1
        total += 1
    return total > 0 and (hits / total) >= threshold

def _prepare_for_insert(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Replace '' with None and NaN with None
    - Convert numeric-like columns to Decimal
    - Never convert __Id, Option__, Report*, GroupLevel*, RowType
    """
    if df is None or df.empty:
        return df

    df2 = df.copy()

    # Replace '' -> None and NaN -> None
    df2 = df2.apply(lambda s: s.map(lambda v: None if (isinstance(v, str) and v.strip() == "") else v))
    df2 = df2.where(pd.notna(df2), None)

    # columns to skip numeric conversion
    skip = set([c for c in df2.columns
                if c.endswith("__Id") or
                   c.startswith("Option__") or
                   c.startswith("Report") or
                   c.startswith("GroupLevel") or
                   c == "RowType"])

    numeric_cols = []
    for col in df2.columns:
        if col in skip:
            continue
        try:
            if _is_numeric_like_series(df2[col]):
                numeric_cols.append(col)
        except Exception:
            pass

    for col in numeric_cols:
        try:
            df2[col] = df2[col].map(_clean_money)
        except Exception:
            pass

    return df2

# ──────────────────────────────────────────────────────────────
# Flatten (dynamic, lossless)
# ──────────────────────────────────────────────────────────────
def flatten_report(report_obj: dict) -> pd.DataFrame:
    report = _unwrap_report(report_obj)

    col_meta, dyn_cols = _extract_columns_meta(report)

    rows_out = []
    rows = report.get("Rows") or {}
    if rows.get("Row"):
        _walk_rows_dynamic(rows, col_meta, [], rows_out)

    # Collect GroupLevel* columns
    all_group_cols = set()
    for r in rows_out:
        for k in r.keys():
            if k.startswith("GroupLevel"):
                all_group_cols.add(k)
    group_cols = sorted(all_group_cols, key=lambda x: int(x.replace("GroupLevel", ""))) if all_group_cols else []

    # Header metadata + options
    header = report.get("Header", {}) or {}
    options = {f"Option__{_normalize_name((o or {}).get('Name'))}": (o or {}).get("Value")
               for o in (header.get("Option") or []) if isinstance(o, dict)}

    meta_cols = [
        "ReportTime", "ReportName", "ReportBasis", "ReportCurrency",
        "StartPeriod", "EndPeriod", "DateMacro"
    ]

    final_cols = dyn_cols + group_cols + ["RowType"] + meta_cols + list(options.keys())

    df = pd.DataFrame(rows_out)
    for c in final_cols:
        if c not in df.columns:
            df[c] = None

    # Fill meta
    df["ReportTime"]     = header.get("Time")
    df["ReportName"]     = header.get("ReportName")
    df["ReportBasis"]    = header.get("ReportBasis")
    df["ReportCurrency"] = header.get("Currency")
    df["StartPeriod"]    = header.get("StartPeriod")
    df["EndPeriod"]      = header.get("EndPeriod")
    df["DateMacro"]      = header.get("DateMacro")
    for k, v in options.items():
        df[k] = v

    if df.empty:
        df = pd.DataFrame(columns=final_cols)

    df = df[final_cols]
    return df

# ──────────────────────────────────────────────────────────────
# Fetch + flatten (single page; loop stub ready)
# ──────────────────────────────────────────────────────────────
def fetch_all_pages_and_flatten() -> pd.DataFrame:
    t1 = ProgressTimer("📥 Fetching Balance Sheet from QBO")
    try:
        if hasattr(t1, "start"): t1.start()
        first = fetch_report_page(startposition=START_POS)
    finally:
        for end_attr in ("end", "stop", "finish", "done"):
            if hasattr(t1, end_attr):
                getattr(t1, end_attr)()
                break

    t2 = ProgressTimer("🧱 Flattening report (dynamic)")
    try:
        if hasattr(t2, "start"): t2.start()
        df = flatten_report(first)
    finally:
        for end_attr in ("end", "stop", "finish", "done"):
            if hasattr(t2, end_attr):
                getattr(t2, end_attr)()
                break

    return df

# ──────────────────────────────────────────────────────────────
# Save
# ──────────────────────────────────────────────────────────────
def save_dataframe(df: pd.DataFrame, schema: str, table: str):
    try:
        _ensure_schema(schema)
    except Exception as e:
        logger.error(f"❌ Failed ensuring schema [{schema}]: {e}")
        raise

    SAVE_EMPTY = os.getenv("BalanceSheet_SAVE_EMPTY_SCHEMA", "true").lower() == "true"

    if df is None or df.empty:
        if SAVE_EMPTY:
            _create_empty_table(schema, table, list(df.columns) if df is not None else [])
            logger.info(f"📝 Created empty table [{schema}].[{table}] (0 rows).")
        else:
            logger.warning("⚠️ BalanceSheet: Nothing to save (empty DataFrame).")
        return

    # Prepare data
    prepared = df.astype(str) if FORCE_TEXT else _prepare_for_insert(df)

    # Try insert; on type error, coerce table to NVARCHAR and retry once
    try:
        sql.insert_dataframe(prepared, table, schema)
        logger.info(f"✅ Saved {len(prepared):,} rows into [{schema}].[{table}]")
    except Exception as e:
        msg = str(e)
        logger.error(f"❌ Insert failed: {msg}")
        if COERCE_TABLE:
            try:
                _coerce_table_to_text(schema, table, list(prepared.columns))
                logger.info(f"🔧 Coerced [{schema}].[{table}] columns to NVARCHAR(255); retrying insert...")
                sql.insert_dataframe(prepared.astype(str), table, schema)  # stringify to be extra-safe
                logger.info(f"✅ Saved {len(prepared):,} rows into [{schema}].[{table}] after coercion")
                return
            except Exception as e2:
                bad_preview = prepared.head(1).to_dict(orient="records")
                logger.error(f"❌ Retry insert failed; first-row preview: {bad_preview}")
                raise
        else:
            bad_preview = prepared.head(1).to_dict(orient="records")
            logger.error(f"❌ Failed inserting report rows; first-row preview: {bad_preview}")
            raise

# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────
def run_balance_sheet():
    logger.info("🚀 Starting Balance Sheet report pull")
    try:
        df = fetch_all_pages_and_flatten()
        logger.info(f"📊 Flattened {len(df):,} rows; sample cols: {list(df.columns)[:12]}")
        if SAVE_TO_SQL:
            save_dataframe(df, REPORT_SCHEMA, REPORT_TABLE)
        else:
            logger.info("💾 Skipping SQL save (BalanceSheet_SAVE_TO_SQL=false).")
        logger.info("✅ Done.")
        return df
    except Exception as e:
        msg = str(e)
        logger.error(f"❌ BalanceSheet report failed: {msg[:2000]}")
        raise

