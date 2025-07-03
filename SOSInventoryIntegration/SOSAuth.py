import webbrowser
import requests
from flask import Flask, request
import urllib.parse
import threading
import time

# SOS Inventory OAuth Configuration
CLIENT_ID = "3fd9ac1b97be4bbd87419af3a25bc86c"
CLIENT_SECRET = "VMPfceYgfpFPINOzJQLa3Un5vXS4ITxegW2z"
REDIRECT_URI = "https://localhost:8080/callback"
AUTH_URL = "https://api.sosinventory.com/oauth2/authorize"
TOKEN_URL = "https://api.sosinventory.com/oauth2/token"

# Server Configuration
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 8080

# Global state for tokens
_access_token = None
_refresh_token = None
_used_codes = set()
_auth_completed = False
_server_thread = None


def _exchange_code_for_tokens(code):
    """Exchange authorization code for access tokens"""
    data = {
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "redirect_uri": REDIRECT_URI
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }

    try:
        print("📤 Exchanging authorization code for tokens...")
        response = requests.post(TOKEN_URL, data=data, headers=headers, timeout=30)

        print(f"📥 Token response status: {response.status_code}")

        if response.status_code != 200:
            return False, response.text

        tokens = response.json()

        if not tokens.get("access_token"):
            return False, f"No access token in response: {tokens}"

        return True, tokens

    except requests.exceptions.RequestException as e:
        return False, f"Request error: {str(e)}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def _handle_callback():
    """Handle the OAuth callback"""
    global _access_token, _refresh_token, _auth_completed

    try:
        code = request.args.get("code")
        if not code:
            return "Authorization failed. No code returned.", 400

        # Check if code was already used
        if code in _used_codes:
            return "❌ Authorization code already used. Please restart the auth flow.", 400

        _used_codes.add(code)
        print(f"✔ Received authorization code: {code}")

        # Exchange the code for tokens
        success, result = _exchange_code_for_tokens(code)

        if success:
            _access_token = result['access_token']
            _refresh_token = result.get('refresh_token')
            _auth_completed = True

            print(f"✅ Access Token: {_access_token}")
            print(f"🔁 Refresh Token: {_refresh_token}")
            print("\n🎉 SUCCESS! Authentication completed!")
            print("🔄 Continuing with API testing...")

            return """
            <html>
            <head><title>Authentication Successful</title></head>
            <body style="font-family: Arial, sans-serif; text-align: center; padding: 50px;">
                <h1 style="color: green;">🎉 Authentication Successful!</h1>
                <p style="font-size: 18px;">You can now close this browser window.</p>
                <p style="font-size: 16px; color: #666;">
                    The script will automatically continue with API testing.
                </p>
            </body>
            </html>
            """
        else:
            print(f"❌ Token exchange failed: {result}")
            return f"❌ Token request failed: {result}", 500

    except Exception as e:
        print(f"❌ Callback error: {str(e)}")
        return f"❌ Callback error: {str(e)}", 500


def _favicon():
    """Handle favicon requests"""
    return "", 404


def _run_flask_server(app):
    """Run Flask server in a separate thread"""
    try:
        app.run(host=SERVER_HOST, port=SERVER_PORT, ssl_context='adhoc', debug=False, use_reloader=False)
    except Exception as e:
        print(f"❌ Server error: {e}")


def authenticate():
    """Start the OAuth2 authentication flow"""
    global _access_token, _refresh_token, _auth_completed, _server_thread

    _used_codes.clear()
    _access_token = None
    _refresh_token = None
    _auth_completed = False

    print("🚀 Starting OAuth2 authentication flow...")
    print("📋 Steps:")
    print("   1. Browser will open automatically")
    print("   2. Complete authorization in the browser")
    print("   3. Wait for automatic continuation...")
    print()

    # Create Flask app
    app = Flask(__name__)
    app.add_url_rule("/callback", "callback", _handle_callback, methods=['GET'])
    app.add_url_rule("/favicon.ico", "favicon", _favicon, methods=['GET'])

    # Build authorization URL
    encoded_redirect = urllib.parse.quote(REDIRECT_URI, safe=':/?#[]@!$&\'()*+,;=')
    auth_url = (
        f"{AUTH_URL}?response_type=code"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={encoded_redirect}"
        f"&scope=read write"
    )

    print("🌐 Opening browser for authorization...")
    webbrowser.open(auth_url)

    # Start Flask server in a separate thread
    print("🔄 Starting local server on https://localhost:8080")
    _server_thread = threading.Thread(target=_run_flask_server, args=(app,), daemon=True)
    _server_thread.start()

    # Wait for authentication to complete or timeout
    timeout = 300  # 5 minutes
    start_time = time.time()

    print("⏳ Waiting for authentication completion...")
    while not _auth_completed and (time.time() - start_time) < timeout:
        time.sleep(1)
        if _auth_completed:
            break

    if _auth_completed and _access_token:
        print("✅ Authentication process completed successfully!")
        return True
    elif time.time() - start_time >= timeout:
        print("❌ Authentication timeout - please try again")
        return False
    else:
        print("❌ Authentication failed - no access token received")
        return False


def get_access_token():
    """Get the current access token"""
    return _access_token


def get_refresh_token():
    """Get the current refresh token"""
    return _refresh_token


def refresh_access_token():
    """Refresh the access token using the refresh token"""
    global _access_token, _refresh_token

    if not _refresh_token:
        return False, "No refresh token available"

    data = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": _refresh_token
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }

    try:
        print("🔄 Refreshing access token...")
        response = requests.post(TOKEN_URL, data=data, headers=headers, timeout=30)

        if response.status_code != 200:
            return False, response.text

        tokens = response.json()
        _access_token = tokens.get("access_token")

        # Update refresh token if a new one is provided
        if tokens.get("refresh_token"):
            _refresh_token = tokens.get("refresh_token")

        print("✅ Access token refreshed successfully!")
        return True, "Token refreshed successfully"

    except Exception as e:
        return False, str(e)