#!/usr/bin/env python3
"""
QBO Token Diagnostic Tool
Helps identify credential mismatches and token refresh issues.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qbo_migration.utils.token_refresher import debug_credential_sources, auto_refresh_token_if_needed
import json

def main():
    print("🔍 QBO Token Diagnostic Tool")
    print("=" * 50)
    
    # Check credential sources
    print("\n1️⃣ Checking credential sources...")
    debug_info = debug_credential_sources()
    print(json.dumps(debug_info, indent=2))
    
    if debug_info.get("mismatch_detected"):
        print("\n❌ MISMATCH DETECTED!")
        print("The Client ID in your environment variables differs from the database.")
        print("This will cause 'Incorrect Token type or clientID' errors.")
        print("\nRecommended actions:")
        print("1. Update your .env file to match the database Client ID")
        print("2. Or update the database to match your .env Client ID")
        print("3. Make sure the refresh token was created with the same Client ID you're using")
        return
    
    # Try token refresh
    print("\n2️⃣ Testing token refresh...")
    try:
        token = auto_refresh_token_if_needed()
        if token:
            print("✅ Token refresh successful!")
            print(f"Token: {token[:20]}...")
        else:
            print("❌ Token refresh failed!")
    except Exception as e:
        print(f"❌ Token refresh error: {e}")
    
    print("\n3️⃣ Diagnostics complete!")

if __name__ == "__main__":
    main()