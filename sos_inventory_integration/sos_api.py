import requests

# SOS Inventory API Configuration
API_BASE_URL = "https://api.sosinventory.com/api/v2"


def make_request(method, endpoint, access_token, data=None, params=None):
    """Make an authenticated API request"""
    if not access_token:
        return False, "No access token provided"

    url = f"{API_BASE_URL}/{endpoint.lstrip('/')}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, params=params, timeout=30)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers, json=data, timeout=30)
        elif method.upper() == "PUT":
            response = requests.put(url, headers=headers, json=data, timeout=30)
        elif method.upper() == "DELETE":
            response = requests.delete(url, headers=headers, timeout=30)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        if response.status_code == 401:
            return False, "Authentication failed - token may be expired"

        if response.status_code >= 400:
            return False, f"API error ({response.status_code}): {response.text}"

        return True, response.json()

    except requests.exceptions.RequestException as e:
        return False, f"Request error: {str(e)}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def get_items(access_token, params=None):
    """Get inventory items"""
    return make_request("GET", "/item", access_token, params=params)


def get_item_by_id(item_id, access_token):
    """Get a specific item by ID"""
    return make_request("GET", f"/item/{item_id}", access_token)


def create_item(item_data, access_token):
    """Create a new inventory item"""
    return make_request("POST", "/item", access_token, data=item_data)


def update_item(item_id, item_data, access_token):
    """Update an existing inventory item"""
    return make_request("PUT", f"/item/{item_id}", access_token, data=item_data)


def delete_item(item_id, access_token):
    """Delete an inventory item"""
    return make_request("DELETE", f"/item/{item_id}", access_token)


def get_customers(access_token, params=None):
    """Get customers"""
    return make_request("GET", "/customer", access_token, params=params)


def get_orders(access_token, params=None):
    """Get orders"""
    return make_request("GET", "/order", access_token, params=params)


def get_vendors(access_token, params=None):
    """Get vendors"""
    return make_request("GET", "/vendor", access_token, params=params)


def test_connection(access_token):
    """Test the API connection"""
    success, result = get_items(access_token, params={"limit": 1})
    if success:
        return True, "API connection successful"
    else:
        return False, result