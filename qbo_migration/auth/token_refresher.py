import os
import logging
from dotenv import load_dotenv
from intuitlib.client import AuthClient
from intuitlib.exceptions import AuthClientError

# Load environment variables
load_dotenv()

# Configure logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Globals
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = os.getenv("REDIRECT_URI")
QBO_ENVIRONMENT = os.getenv("QBO_ENVIRONMENT", "sandbox")

# Token env keys
ACCESS_TOKEN_ENV = "QBO_ACCESS_TOKEN"
REFRESH_TOKEN_ENV = "QBO_REFRESH_TOKEN"

def get_auth_client():
    """Initialize and return an AuthClient instance"""
    client = AuthClient(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        environment=QBO_ENVIRONMENT
    )
    client.access_token = os.getenv(ACCESS_TOKEN_ENV)
    client.refresh_token = os.getenv(REFRESH_TOKEN_ENV)
    return client

def update_env_file(access_token: str, refresh_token: str):
    """Update .env file with latest tokens"""
    try:
        with open(".env", "r") as f:
            lines = f.readlines()

        with open(".env", "w") as f:
            for line in lines:
                if not any(env in line for env in [ACCESS_TOKEN_ENV, REFRESH_TOKEN_ENV]):
                    f.write(line)
            f.write(f"{ACCESS_TOKEN_ENV}={access_token}\n")
            f.write(f"{REFRESH_TOKEN_ENV}={refresh_token}\n")
    except Exception as e:
        logger.error(f"Failed to update .env file: {e}")
        raise

def refresh_tokens():
    """Refresh and persist tokens using AuthClient"""
    client = get_auth_client()
    logger.info("Refreshing QuickBooks Online OAuth tokens...")
    try:
        client.refresh()
    except AuthClientError as e:
        logger.error(f"Token refresh failed: {e}")
        raise

    new_access_token = client.access_token
    new_refresh_token = client.refresh_token

    # Persist in .env
    update_env_file(new_access_token, new_refresh_token)

    # Update runtime environment
    os.environ[ACCESS_TOKEN_ENV] = new_access_token
    os.environ[REFRESH_TOKEN_ENV] = new_refresh_token

    logger.info("✅ OAuth tokens refreshed successfully.")
    return new_access_token
