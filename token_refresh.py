from app.utils.credentials_path import get_credentials_path
from app.utils.gdrive import get_gdrive_service

TOKEN_PATH = get_credentials_path("token.pickle")

print("🔄 Forcing full token refresh...")

# Delete existing token to force browser re-authentication
if TOKEN_PATH.exists():
    TOKEN_PATH.unlink()
    print(f"🗑️  Deleted old token: {TOKEN_PATH}")
else:
    print("ℹ️  No existing token found")

# This will open a browser for OAuth
print("🌐 Opening browser for Google authentication...")
service = get_gdrive_service()
print("✅ Token refreshed successfully! Valid for ~7 days.")