# utils/token_refresher.py

import os
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

TOKEN_TIMESTAMP_FILE = "last_token_refresh.txt"

def refresh_qbo_token(env_path: str = ".env"):
    load_dotenv(dotenv_path=env_path)

    client_id = os.getenv("QBO_CLIENT_ID")
    client_secret = os.getenv("QBO_CLIENT_SECRET")
    refresh_token = os.getenv("QBO_REFRESH_TOKEN")
    environment = os.getenv("QBO_ENVIRONMENT", "sandbox")

    if not all([client_id, client_secret, refresh_token]):
        print("❌ Missing QBO_CLIENT_ID / CLIENT_SECRET / REFRESH_TOKEN in .env")
        return None

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

    response = requests.post(token_url, headers=headers, data=data, auth=auth)

    if response.status_code == 200:
        tokens = response.json()
        access_token = tokens["access_token"]
        new_refresh_token = tokens.get("refresh_token", refresh_token)  # fallback if not rotated

        # Update .env
        _update_env_var("QBO_ACCESS_TOKEN", access_token, env_path)
        _update_env_var("QBO_REFRESH_TOKEN", new_refresh_token, env_path)

        # Update current environment
        os.environ["QBO_ACCESS_TOKEN"] = access_token
        os.environ["QBO_REFRESH_TOKEN"] = new_refresh_token

        # Update timestamp file
        _record_token_refresh_time()

        print("🔄 QBO token refreshed and environment updated.")
        return access_token

    else:
        print(f"❌ Failed to refresh token: {response.status_code} {response.text}")
        return None


def auto_refresh_token_if_needed(threshold_minutes: int = 40, env_path: str = ".env"):
    if _should_refresh_token(threshold_minutes):
        print(f"⏱️  Token is older than {threshold_minutes} min — refreshing...")
        return refresh_qbo_token(env_path)
    else:
        print("✅ Token is still valid — no refresh needed.")
        return os.getenv("QBO_ACCESS_TOKEN")


def _should_refresh_token(threshold_minutes: int = 40) -> bool:
    if not os.path.exists(TOKEN_TIMESTAMP_FILE):
        return True
    try:
        with open(TOKEN_TIMESTAMP_FILE, "r") as f:
            last_str = f.read().strip()
        last_time = datetime.fromisoformat(last_str)
        now_utc = datetime.now(timezone.utc)
        # Convert naive to aware if needed
        if last_time.tzinfo is None:
            last_time = last_time.replace(tzinfo=timezone.utc)
        return now_utc - last_time > timedelta(minutes=threshold_minutes)
    except Exception:
        return True


def _record_token_refresh_time():
    with open(TOKEN_TIMESTAMP_FILE, "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())


def _update_env_var(key: str, value: str, env_path: str):
    updated = False
    lines = []

    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith(f"{key}="):
                    lines.append(f"{key}={value}\n")
                    updated = True
                else:
                    lines.append(line)

    if not updated:
        lines.append(f"{key}={value}\n")

    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(lines)
