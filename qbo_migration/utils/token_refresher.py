import os
import pyodbc
import requests
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
from dotenv import load_dotenv
load_dotenv()

USERID = 100250

# =========================
# ControlTower connection
# =========================
def _odbc_driver():
    return os.getenv("SQLSERVER_ODBC_DRIVER", "ODBC Driver 17 for SQL Server")

def _build_conn_str(*, host, port, database, user, password):
    return (
        f"DRIVER={{{_odbc_driver()}}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};PWD={password};"
        "TrustServerCertificate=yes;"
    )

def _get_controltower_connection():
    """
    Connects to the ControlTower catalog (holds user token table).
    Required env:
      CT_SQLSERVER_HOST, CT_SQLSERVER_PORT, CT_SQLSERVER_USER, CT_SQLSERVER_PASSWORD,
      CT_SQLSERVER_DATABASE   (e.g., ControlTower)
    """
    ct_conn_str = _build_conn_str(
        host=os.getenv("CT_SQLSERVER_HOST", "localhost"),
        port=os.getenv("CT_SQLSERVER_PORT", "1433"),
        database=os.getenv("CT_SQLSERVER_DATABASE", "ControlTower"),
        user=os.getenv("CT_SQLSERVER_USER", "sa"),
        password=os.getenv("CT_SQLSERVER_PASSWORD", ""),
    )
    return pyodbc.connect(ct_conn_str)

# =========================
# DB accessors
# =========================
def _fetch_token_row(user_id: str):
    """
    Reads token row from ct.QBO_UserBased_AccessToken_Migration.
    Schema/table names are fixed per your spec: schema 'ct', table 'QBO_UserBased_AccessToken_Migration'.
    """
    query = """
        SELECT TOP (1) TokenId, UserId, RealmId, AccessToken, RefreshToken, CreatedAtUtc
        FROM ct.QBO_UserBased_AccessToken_Migration
        WHERE UserId = ?
        ORDER BY CreatedAtUtc DESC
    """
    with _get_controltower_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, (user_id,))
        row = cur.fetchone()
        return row  # pyodbc.Row or None

def _upsert_token_row(user_id: str, realm_id: str, access_token: str, refresh_token: str):
    """
    Updates existing row for UserId or inserts if missing.
    CreatedAtUtc is server-side UTC now.
    """
    sql = """
    MERGE ct.QBO_UserBased_AccessToken_Migration AS t
    USING (SELECT ? AS UserId) AS s
      ON (t.UserId = s.UserId)
    WHEN MATCHED THEN
        UPDATE SET
            AccessToken = ?, 
            RefreshToken = ?, 
            RealmId = COALESCE(?, t.RealmId),
            CreatedAtUtc = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (UserId, RealmId, AccessToken, RefreshToken, CreatedAtUtc)
        VALUES (?, ?, ?, ?, SYSUTCDATETIME());
    """
    with _get_controltower_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, (user_id, access_token, refresh_token, realm_id,
                          user_id, realm_id, access_token, refresh_token))
        conn.commit()

# =========================
# Refresh logic (per user)
# =========================
def _should_refresh_from_db(user_id: str, threshold_minutes: int = 55) -> bool:
    row = _fetch_token_row(user_id)
    if not row:
        # No row => must refresh (or fail -> we'll attempt refresh path which will require refresh_token)
        return True
    created_at = row.CreatedAtUtc
    if created_at is None:
        return True
    # ensure aware
    if isinstance(created_at, datetime):
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
    else:
        # unexpected type -> force refresh
        return True
    now_utc = datetime.now(timezone.utc)
    return (now_utc - created_at) > timedelta(minutes=threshold_minutes)

def refresh_qbo_token_for_user(user_id: str) -> str | None:
    """
    Refreshes QBO token using the REFRESH TOKEN stored for this user in ControlTower.
    Updates the same row with new Access/Refresh tokens and CreatedAtUtc.
    Returns the fresh access_token (or None on failure).
    """
    user_id=USERID
    # Client credentials remain in env
    client_id = os.getenv("QBO_CLIENT_ID")
    client_secret = os.getenv("QBO_CLIENT_SECRET")
    if not client_id or not client_secret:
        print("❌ Missing QBO_CLIENT_ID / QBO_CLIENT_SECRET in environment.")
        return None

    row = _fetch_token_row(user_id)
    if not row or not row.RefreshToken:
        print(f"❌ No refresh token found in ct.QBO_UserBased_AccessToken_Migration for user '{user_id}'.")
        return None

    refresh_token = row.RefreshToken
    realm_id = row.RealmId  # keep existing realm id; refresh call doesn't return it

    token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    auth = (client_id, client_secret)
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    resp = requests.post(token_url, headers=headers, data=data, auth=auth, timeout=60)
    if resp.status_code == 200:
        tokens = resp.json()
        access_token = tokens.get("access_token")
        new_refresh_token = tokens.get("refresh_token", refresh_token)  # fallback if not rotated
        if not access_token:
            print("❌ Refresh succeeded but no access_token in response.")
            return None
        # Save back to DB (CreatedAtUtc will be updated to SYSUTCDATETIME())
        _upsert_token_row(user_id, realm_id, access_token, new_refresh_token)
        return access_token
    else:
        print(f"❌ Failed to refresh token for '{user_id}': {resp.status_code} {resp.text}")
        return None

def auto_refresh_token_if_needed(threshold_minutes: int = 55) -> str | None:
    """
    Returns a valid access token for the user, refreshing (and persisting) if needed.
    No .env writes, no timestamp file—fully DB-backed.
    """
    user_id=USERID
    if _should_refresh_from_db(user_id, threshold_minutes):
        print(f"⏱️  Token for '{user_id}' is older than {threshold_minutes} min — refreshing...")
        return refresh_qbo_token_for_user(user_id)
    # else return the current DB value
    row = _fetch_token_row(user_id)
    if row and row.AccessToken:
        print("✅ Token is still valid — no refresh needed.")
        return row.AccessToken
    # If row exists but missing access token, try refresh anyway
    print("ℹ️ No AccessToken found but row exists — attempting refresh...")
    return refresh_qbo_token_for_user(user_id)


#############################################################

# utils/qbo_tokens.py

# ---- defaults / constants ----
USER_ID_DEFAULT = USERID
MINOR_VERSION = 65

TABLES = {
    "extraction": "ct.QBO_UserBased_AccessToken_Extraction",
    "migration":  "ct.QBO_UserBased_AccessToken_Migration",
}

# ----------------------------
# SQL Server connection helper
# ----------------------------
def _sql_conn():
    host = os.getenv("CT_SQLSERVER_HOST", "localhost")
    port = os.getenv("CT_SQLSERVER_PORT", "1433")
    user = os.getenv("CT_SQLSERVER_USER", "sa")
    pwd  = os.getenv("CT_SQLSERVER_PASSWORD", "")
    db   = os.getenv("CT_SQLSERVER_DATABASE", "ControlTower")
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host},{port};DATABASE={db};UID={user};PWD={pwd};"
        "Encrypt=no;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=True)

# -----------------------
# Config (client) fetcher
# -----------------------
def _get_universal_config(conn) -> Optional[Dict[str, Any]]:
    sql = """
      SELECT TOP (1)
        QBO_ClientId, QBO_ClientSecret, QBO_Environment, RedirectUri, CreatedAt, UpdatedAt
      FROM ct.QBO_Universal_Token
      ORDER BY COALESCE(UpdatedAt, CreatedAt) DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        if not row:
            return None
        return {
            "client_id": row[0],
            "client_secret": row[1],
            "environment": (row[2] or "sandbox").lower(),
            "redirect_uri": row[3],
            "created_at": row[4],
            "updated_at": row[5],
        }

# ------------------------
# User token row utilities
# ------------------------
def _get_latest_user_token(conn, table: str, user_id: int, realm_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if realm_id:
        sql = f"""
          SELECT TOP (1) TokenId, UserId, RealmId, AccessToken, RefreshToken, CreatedAtUtc
          FROM {table}
          WHERE UserId = ? AND RealmId = ?
          ORDER BY CreatedAtUtc DESC, TokenId DESC
        """
        params = (user_id, realm_id)
    else:
        sql = f"""
          SELECT TOP (1) TokenId, UserId, RealmId, AccessToken, RefreshToken, CreatedAtUtc
          FROM {table}
          WHERE UserId = ?
          ORDER BY CreatedAtUtc DESC, TokenId DESC
        """
        params = (user_id,)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        if not row:
            return None
        return {
            "token_id": row[0],
            "user_id": row[1],
            "realm_id": row[2],
            "access_token": row[3],
            "refresh_token": row[4],
            "created_at_utc": row[5],
        }

def _insert_user_token(conn, table: str, user_id: int, realm_id: str, access_token: str, refresh_token: str) -> int:
    sql = f"""
      INSERT INTO {table} (UserId, RealmId, AccessToken, RefreshToken)
      OUTPUT INSERTED.TokenId
      VALUES (?, ?, ?, ?)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (user_id, realm_id, access_token, refresh_token))
        row = cur.fetchone()
        return int(row[0])

# -------------------
# Token health checks
# -------------------
def _qbo_base_url(environment: str) -> str:
    return "https://quickbooks.api.intuit.com" if (environment or "").lower() == "production" \
           else "https://sandbox-quickbooks.api.intuit.com"

def _is_access_token_valid(access_token: str, realm_id: str, environment: str) -> bool:
    try:
        base = _qbo_base_url(environment)
        url = f"{base}/v3/company/{realm_id}/companyinfo/{realm_id}?minorversion={MINOR_VERSION}"
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
        resp = requests.get(url, headers=headers, timeout=10)
        return resp.status_code == 200
    except requests.RequestException:
        return False

def _is_stale_by_time(created_at_utc, threshold_minutes: int = 50) -> bool:
    if not created_at_utc:
        return True
    try:
        if isinstance(created_at_utc, str):
            created_at_utc = datetime.fromisoformat(created_at_utc)
        if created_at_utc.tzinfo is None:
            created_at_utc = created_at_utc.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - created_at_utc > timedelta(minutes=threshold_minutes)
    except Exception:
        return True

# -------------------
# Intuit OAuth refresh
# -------------------
def _refresh_with_intuit(client_id: str, client_secret: str, refresh_token: str) -> Optional[Dict[str, str]]:
    token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    try:
        resp = requests.post(token_url, headers=headers, data=data, auth=(client_id, client_secret), timeout=15)
        if resp.status_code == 200:
            body = resp.json()
            return {
                "access_token": body["access_token"],
                "refresh_token": body.get("refresh_token", refresh_token),
            }
        else:
            print(f"❌ Intuit refresh failed: {resp.status_code} {resp.text}")
            return None
    except requests.RequestException as e:
        print(f"❌ Intuit refresh exception: {e}")
        return None

# ---------------------------
# Public, reusable entrypoints
# ---------------------------
def ensure_valid_access_token(
    user_id: int = USER_ID_DEFAULT,
    realm_id: Optional[str] = None,
    context: str = "extraction",
    threshold_minutes: int = 50
) -> Optional[Dict[str, Any]]:
    """
    Ensures you get a *valid* access token for the given context ('extraction'|'migration').
    Auto-creates schema/tables if missing.
    """
    table = TABLES[context]
    with _sql_conn() as conn:
        # ensure_token_tables(conn)   # ✅ always ensure tables
        cfg = _get_universal_config(conn)
        if not cfg:
            print("❌ No row in QBO_Universal_Token.")
            return None

        row = _get_latest_user_token(conn, table, user_id=user_id, realm_id=realm_id)
        if not row:
            print(f"❌ No token row in {table} for user_id={user_id}.")
            return None

        valid_live = _is_access_token_valid(row["access_token"], row["realm_id"], cfg["environment"])
        stale_time = _is_stale_by_time(row.get("created_at_utc"), threshold_minutes)
        needs_refresh = (not valid_live) or stale_time

        if needs_refresh:
            refreshed = _refresh_with_intuit(cfg["client_id"], cfg["client_secret"], row["refresh_token"])
            if not refreshed:
                return None
            _insert_user_token(conn, table, user_id=row["user_id"], realm_id=row["realm_id"],
                               access_token=refreshed["access_token"], refresh_token=refreshed["refresh_token"])
            access_token = refreshed["access_token"]
            refresh_token = refreshed["refresh_token"]
        else:
            access_token = row["access_token"]
            refresh_token = row["refresh_token"]

        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "realm_id": row["realm_id"],
            "client_id": cfg["client_id"],
            "client_secret": cfg["client_secret"],
            "environment": cfg["environment"],
            "redirect_uri": cfg["redirect_uri"],
        }

def get_qbo_values(user_id: int = USER_ID_DEFAULT, realm_id: Optional[str] = None, context: str = "extraction") -> Optional[Dict[str, Any]]:
    valid = ensure_valid_access_token(user_id=user_id, realm_id=realm_id, context=context)
    if not valid:
        return None
    return {
        "QBO_ACCESS_TOKEN": valid["access_token"],
        "QBO_REFRESH_TOKEN": valid["refresh_token"],
        "QBO_REALM_ID": valid["realm_id"],
        "QBO_CLIENT_ID": valid["client_id"],
        "QBO_CLIENT_SECRET": valid["client_secret"],
        "QBO_ENVIRONMENT": valid["environment"],
        "QBO_REDIRECT_URI": valid["redirect_uri"],
    }

def get_qbo_context(user_id: int = USER_ID_DEFAULT, realm_id: Optional[str] = None, context: str = "extraction") -> Dict[str, Any]:
    vals = get_qbo_values(user_id=user_id, realm_id=realm_id, context=context)
    if not vals:
        raise RuntimeError("Missing/invalid QBO credentials in DB")

    base = _qbo_base_url(vals["QBO_ENVIRONMENT"])
    realm = vals["QBO_REALM_ID"]
    access = vals["QBO_ACCESS_TOKEN"]
    return {
        "BASE_URL": base,
        "REALM_ID": realm,
        "ACCESS_TOKEN": access,
        "REFRESH_TOKEN": vals["QBO_REFRESH_TOKEN"],
        "CLIENT_ID": vals["QBO_CLIENT_ID"],
        "CLIENT_SECRET": vals["QBO_CLIENT_SECRET"],
        "ENVIRONMENT": vals["QBO_ENVIRONMENT"],
        "REDIRECT_URI": vals["QBO_REDIRECT_URI"],
        "QUERY_URL": f"{base}/v3/company/{realm}/query?minorversion={MINOR_VERSION}",
        "HEADERS": {
            "Authorization": f"Bearer {access}",
            "Accept": "application/json",
            "Content-Type": "application/text",
        },
    }

def qbo_query(sql: str, user_id: int = USER_ID_DEFAULT, realm_id: Optional[str] = None, context: str = "extraction") -> dict:
    ctx = get_qbo_context(user_id=user_id, realm_id=realm_id, context=context)
    resp = requests.post(ctx["QUERY_URL"], data=sql, headers=ctx["HEADERS"], timeout=60)
    if resp.status_code in (401, 403):
        ensure_valid_access_token(user_id=user_id, realm_id=realm_id, context=context)
        ctx = get_qbo_context(user_id=user_id, realm_id=realm_id, context=context)
        resp = requests.post(ctx["QUERY_URL"], data=sql, headers=ctx["HEADERS"], timeout=60)
    resp.raise_for_status()
    return resp.json()

def get_qbo_context_migration(user_id: int = USER_ID_DEFAULT, realm_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns a ready-to-use QBO context (BASE_URL, REALM_ID, ACCESS_TOKEN, HEADERS, etc.)
    for *migration* flows, sourcing tokens from:
      ControlTower.ct.QBO_UserBased_AccessToken_Migration

    This automatically:
      - fetches latest user token row for migration,
      - validates/refreshes via Intuit if stale/invalid,
      - returns a context dict identical to get_qbo_context(...)
        but bound to the 'migration' table.
    """
    return get_qbo_context(user_id=user_id, realm_id=realm_id, context="migration")

def get_qbo_runtime(user_id: int = USERID) -> Dict[str, Any]:
    """Fetch QBO context dynamically when extraction starts."""
    ctx = get_qbo_context(user_id=user_id, context="extraction")
    return {
        "ENVIRONMENT": ctx["ENVIRONMENT"],
        "REALM_ID": ctx["REALM_ID"],
        "ACCESS_TOKEN": ctx["ACCESS_TOKEN"],
        "BASE_URL": ctx["BASE_URL"],
        "QUERY_URL": ctx["QUERY_URL"],
        "HEADERS": ctx["HEADERS"],
    }
