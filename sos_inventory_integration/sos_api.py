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


# ========== SALES ORDER FUNCTIONS ==========

def get_sales_orders(access_token, params=None):
    """
    Get sales orders

    Parameters:
    - access_token: Authentication token
    - params: Optional query parameters dict, can include:
        - start: Pagination cursor (row number)
        - maxresults: Max results to return (default: 200, max: 200)
        - status: "open" or "closed"
        - summary: If present, returns only summary attributes
        - query: Search string (searches number, comment, customerPO, customer name)
        - archived: "yes" or "no"
        - from/to: Date range filters (YYYY-MM-DDTHH:MM:SS format)
        - location: Location name filter
        - channel: Channel name filter
        - orderStage: Order stage name filter
        - createdsince/updatedsince: Date filters (YYYY-MM-DDTHH:MM:SS format)

    Returns:
    - Tuple (success: bool, result: dict or error_message: str)
    """
    return make_request("GET", "/salesorder", access_token, params=params)


def get_sales_order_by_id(sales_order_id, access_token):
    """Get a specific sales order by ID"""
    return make_request("GET", f"/salesorder/{sales_order_id}", access_token)


def search_sales_orders_by_query(search_string, access_token, additional_params=None):
    """
    Search sales orders using the query parameter

    Parameters:
    - search_string: String to search for in number, comment, customerPO, or customer name
    - access_token: Authentication token
    - additional_params: Optional dict with additional query parameters

    Returns:
    - Tuple (success: bool, result: dict or error_message: str)
    """
    params = {"query": search_string}

    if additional_params:
        params.update(additional_params)

    return get_sales_orders(access_token, params=params)


def get_sales_orders_by_status(status, access_token, additional_params=None):
    """
    Get sales orders filtered by status

    Parameters:
    - status: "open" or "closed"
    - access_token: Authentication token
    - additional_params: Optional dict with additional query parameters
    """
    params = {"status": status}

    if additional_params:
        params.update(additional_params)

    return get_sales_orders(access_token, params=params)


def get_sales_orders_by_date_range(from_date=None, to_date=None, access_token=None, additional_params=None):
    """
    Get sales orders filtered by date range

    Parameters:
    - from_date: Start date (YYYY-MM-DDTHH:MM:SS format)
    - to_date: End date (YYYY-MM-DDTHH:MM:SS format)
    - access_token: Authentication token
    - additional_params: Optional dict with additional query parameters
    """
    params = {}

    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date

    if additional_params:
        params.update(additional_params)

    return get_sales_orders(access_token, params=params)


def get_sales_orders_by_customer(customer_name, access_token, additional_params=None):
    """
    Get sales orders for a specific customer (uses query parameter)

    Parameters:
    - customer_name: Customer name to search for
    - access_token: Authentication token
    - additional_params: Optional dict with additional query parameters
    """
    return search_sales_orders_by_query(customer_name, access_token, additional_params)


def get_sales_orders_summary(access_token, additional_params=None):
    """
    Get sales orders with summary data only

    Parameters:
    - access_token: Authentication token
    - additional_params: Optional dict with additional query parameters
    """
    params = {"summary": "true"}

    if additional_params:
        params.update(additional_params)

    return get_sales_orders(access_token, params=params)


def get_recent_sales_orders(access_token, since_date, additional_params=None):
    """
    Get sales orders created or updated since a specific date

    Parameters:
    - access_token: Authentication token
    - since_date: Date string (YYYY-MM-DDTHH:MM:SS format)
    - additional_params: Optional dict with additional query parameters
    """
    params = {"updatedsince": since_date}

    if additional_params:
        params.update(additional_params)

    return get_sales_orders(access_token, params=params)


def test_connection(access_token):
    """Test the API connection using sales orders instead of items"""
    success, result = get_sales_orders(access_token, params={"maxresults": 1})
    if success:
        return True, "API connection successful"
    else:
        return False, result


# ========== HELPER FUNCTIONS ==========

def parse_sales_order_response(response_data):
    """
    Parse sales order response and extract useful information

    Parameters:
    - response_data: The response data from a sales order API call

    Returns:
    - Dict with parsed information or None if parsing fails
    """
    try:
        if not isinstance(response_data, dict):
            return None

        return {
            "count": response_data.get("count", 0),
            "total_count": response_data.get("totalCount", 0),
            "status": response_data.get("status", "unknown"),
            "message": response_data.get("message", ""),
            "orders": response_data.get("data", [])
        }
    except Exception:
        return None


def format_sales_order_summary(sales_order):
    """
    Format a sales order object into a readable summary

    Parameters:
    - sales_order: Single sales order dict from API response

    Returns:
    - Formatted string summary
    """
    try:
        number = sales_order.get("number", "Unknown")
        customer = sales_order.get("customerName", "Unknown Customer")
        status = sales_order.get("status", "Unknown Status")
        total = sales_order.get("total", 0)
        date = sales_order.get("transactionDate", "Unknown Date")

        return f"Order #{number} - {customer} - {status} - ${total} - {date}"
    except Exception:
        return "Unable to format sales order"