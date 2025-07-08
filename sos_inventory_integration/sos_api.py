import requests
import json

API_BASE_URL = "https://api.sosinventory.com/api/v2"


def make_request(method, endpoint, access_token, data=None, params=None):
    if not access_token:
        return False, "No access token provided"

    url = f"{API_BASE_URL}/{endpoint.lstrip('/')}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Host": "api.sosinventory.com"
    }

    print(f"[HTTP DEBUG] ===== HTTP REQUEST =====")
    print(f"[HTTP DEBUG] Method: {method}")
    print(f"[HTTP DEBUG] URL: {url}")
    print(f"[HTTP DEBUG] Token (first 20 chars): {access_token[:20]}..." if access_token else "No token")

    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, params=params, timeout=30)
        elif method.upper() == "PUT":
            response = requests.put(url, headers=headers, json=data, timeout=30)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        print(f"[HTTP DEBUG] Status Code: {response.status_code}")

        if response.status_code == 401:
            return False, "Authentication failed - token may be expired"

        if response.status_code >= 400:
            return False, f"API error ({response.status_code}): {response.text}"

        return True, response.json()

    except Exception as e:
        return False, f"Request error: {str(e)}"


def get_sales_order_by_id(sales_order_id, access_token):
    print(f"[API DEBUG] Getting sales order ID: {sales_order_id}")
    return make_request("GET", f"/salesorder/{sales_order_id}", access_token)


def update_sales_order(sales_order_id, sales_order_data, access_token):
    print(f"[API DEBUG] Updating sales order {sales_order_id}")
    return make_request("PUT", f"/salesorder/{sales_order_id}", access_token, data=sales_order_data)


def add_or_update_item_in_sales_order(sales_order_id, item_id, quantity_to_add, access_token):
    try:
        # Get current sales order
        success, response = get_sales_order_by_id(sales_order_id, access_token)
        if not success:
            return False, f"Could not retrieve sales order: {response}"

        # Extract the data portion
        current_data = response.get("data", response)

        # Get current lines
        lines = current_data.get("lines", [])

        # Find existing item or add new one
        existing_line_index = None
        for index, line in enumerate(lines):
            item_info = line.get("item", {})
            if isinstance(item_info, dict) and str(item_info.get("id")) == str(item_id):
                existing_line_index = index
                break

        if existing_line_index is not None:
            # Update existing line quantity
            current_quantity = lines[existing_line_index].get("quantity", 0)
            lines[existing_line_index]["quantity"] = current_quantity + quantity_to_add
            print(f"Updated existing line quantity: {current_quantity} -> {current_quantity + quantity_to_add}")
        else:
            # Add new line
            next_line_number = max([line.get("lineNumber", 0) for line in lines], default=0) + 1
            new_line = {
                "lineNumber": next_line_number,
                "item": {"id": item_id},
                "quantity": quantity_to_add,
                "unitprice": 0,
                "tax": {"taxable": False, "taxCode": None}
            }
            lines.append(new_line)
            print(f"Added new line: {new_line}")

        # Update the lines in current data
        current_data["lines"] = lines

        # Send update
        return update_sales_order(sales_order_id, current_data, access_token)

    except Exception as e:
        return False, f"Exception: {str(e)}"


def get_items(access_token, params=None):
    return make_request("GET", "/item", access_token, params=params)


def get_sales_orders(access_token, params=None):
    return make_request("GET", "/salesorder", access_token, params=params)


def search_sales_orders_by_query(search_string, access_token, additional_params=None):
    params = {"query": search_string}
    if additional_params:
        params.update(additional_params)
    return get_sales_orders(access_token, params=params)


def test_connection(access_token):
    success, result = get_sales_orders(access_token, params={"maxresults": 1})
    return (True, "API connection successful") if success else (False, result)


def parse_sales_order_response(response_data):
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
    try:
        number = sales_order.get("number", "Unknown")
        customer_info = sales_order.get("customer", {})
        customer_name = customer_info.get("name", "Unknown Customer") if isinstance(customer_info,
                                                                                    dict) else "Unknown Customer"
        total = sales_order.get("total", 0)
        date = sales_order.get("date", "Unknown Date")
        return f"Order #{number} - {customer_name} - ${total} - {date}"
    except Exception:
        return "Unable to format sales order"