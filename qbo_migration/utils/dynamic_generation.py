import os
import json
import requests
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv

def start_local_server(port=8000):
    """
    Starts a local HTTP server to receive the OAuth redirect with auth code.
    """
    class OAuthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            query = parse_qs(urlparse(self.path).query)
            self.server.auth_code = query.get("code", [None])[0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b" Auth successful. You can close this window.")

    httpd = HTTPServer(("localhost", port), OAuthHandler)
    print(f"🌐 Waiting for authorization code on http://localhost:{port} ...")
    try:
        httpd.handle_request()
    finally:
        httpd.server_close()
    return httpd.auth_code

def _update_env_var(key: str, value: str, env_path: str):
    """
    Updates the given key=value pair in the .env file.
    """
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

def generate_tokens_interactive(env_path=".env"):
    """
    Main function to:
    1. Launch browser for Intuit login
    2. Capture authorization code via local server
    3. Exchange code for tokens
    4. Update .env with QBO_ACCESS_TOKEN and QBO_REFRESH_TOKEN
    """
    load_dotenv(dotenv_path=env_path)

    client_id = os.getenv("QBO_CLIENT_ID")
    client_secret = os.getenv("QBO_CLIENT_SECRET")
    realm_id = os.getenv("QBO_REALM_ID")
    environment = os.getenv("QBO_ENVIRONMENT", "sandbox").lower()
    redirect_uri = "http://localhost:8000"

    if not all([client_id, client_secret, realm_id]):
        print("❌ Missing required environment variables in .env")
        return

    scope = "com.intuit.quickbooks.accounting"
    auth_url = (
        f"https://appcenter.intuit.com/connect/oauth2"
        f"?client_id={client_id}"
        f"&redirect_uri={redirect_uri}"
        f"&response_type=code"
        f"&scope={scope}"
        f"&state=security_token"
    )

    print("🌐 Opening browser for Intuit login...")
    print(f"🔗 If browser doesn't open, visit this URL manually:\n{auth_url}")
    webbrowser.open(auth_url)

    auth_code = start_local_server()
    if not auth_code:
        print("❌ Failed to capture authorization code.")
        return

    print(f"🔐 Auth code received. Exchanging for tokens...")

    token_url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": redirect_uri
    }
    auth = (client_id, client_secret)

    response = requests.post(token_url, headers=headers, data=data, auth=auth)

    if response.status_code == 200:
        tokens = response.json()
        access_token = tokens["access_token"]
        refresh_token = tokens["refresh_token"]

        _update_env_var("QBO_ACCESS_TOKEN", access_token, env_path)
        _update_env_var("QBO_REFRESH_TOKEN", refresh_token, env_path)

        os.environ["QBO_ACCESS_TOKEN"] = access_token
        os.environ["QBO_REFRESH_TOKEN"] = refresh_token

        print("\n Access & Refresh tokens successfully generated and saved to .env")
        return tokens
    else:
        print(f"❌ Token exchange failed: {response.status_code}")
        print(response.text)
        return None

# Run this directly
if __name__ == "__main__":
    generate_tokens_interactive()
