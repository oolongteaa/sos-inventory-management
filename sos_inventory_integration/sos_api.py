import requests
import json
from datetime import datetime

API_BASE_URL = "https://api.sosinventory.com/api/v2"


def get_items(access_token, params=None):
    """
    Get inventory items with pagination support

    Parameters:
    - access_token: Authentication token
    - params: Optional query parameters dict, can include:
        - start: Pagination cursor (default: 0)
        - maxresults: Max results to return (default: 200, max: 200)
        - query: Search string for name, sku, or description
        - type: Item type filter
        - starred: Filter by starred items (0 or 1)

    Returns:
    - Tuple (success: bool, result: dict or error_message: str)
    """
    return make_request("GET", "/item", access_token, params=params)


def get_item_by_id(item_id, access_token):
    """
    Get a specific item by ID to retrieve price and other details

    Parameters:
    - item_id: The item ID to retrieve
    - access_token: Authentication token

    Returns:
    - Tuple (success: bool, result: dict or error_message: str)
    """
    print(f"[API DEBUG] Getting item details for ID: {item_id}")
    return make_request("GET", f"/item/{item_id}", access_token)


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


def get_current_date_string():
    """
    Get current date in YYYY-MM-DD format for SOS Inventory

    Returns:
    - str: Current date in ISO format
    """
    return datetime.now().strftime("%Y-%m-%d")


def get_sales_order_by_id(sales_order_id, access_token):
    print(f"[API DEBUG] Getting sales order ID: {sales_order_id}")
    return make_request("GET", f"/salesorder/{sales_order_id}", access_token)


def update_sales_order(sales_order_id, sales_order_data, access_token):
    print(f"[API DEBUG] Updating sales order {sales_order_id}")
    return make_request("PUT", f"/salesorder/{sales_order_id}", access_token, data=sales_order_data)


def get_item_price_and_details(item_id, access_token):
    """
    Get the selling price and other details for an item

    Parameters:
    - item_id: The item ID
    - access_token: Authentication token

    Returns:
    - Tuple (success: bool, details: dict or error_message: str)
      details dict contains: {"price": float, "name": str, "sku": str, "description": str}
    """
    try:
        success, response = get_item_by_id(item_id, access_token)
        if not success:
            return False, f"Could not retrieve item {item_id}: {response}"

        # Print the exact JSON response as received
        print(f"[API DEBUG] ===== RAW JSON RESPONSE FOR ITEM {item_id} =====")
        print(json.dumps(response, indent=2, ensure_ascii=False))
        print(f"[API DEBUG] ===== END RAW JSON RESPONSE =====")

        # Extract the data portion
        item_data = response.get("data", response)

        # Print the data portion separately if it exists
        if "data" in response:
            print(f"[API DEBUG] ===== ITEM DATA PORTION =====")
            print(json.dumps(item_data, indent=2, ensure_ascii=False))
            print(f"[API DEBUG] ===== END ITEM DATA PORTION =====")

        # Print individual field access for debugging
        print(f"[API DEBUG] Individual field access:")
        print(f"[API DEBUG]   ID: {item_data.get('id', 'N/A')}")
        print(f"[API DEBUG]   Name: {item_data.get('name', 'N/A')}")
        print(f"[API DEBUG]   Full Name: {item_data.get('fullname', 'N/A')}")
        print(f"[API DEBUG]   SKU: {item_data.get('sku', 'N/A')}")
        print(f"[API DEBUG]   Description: {item_data.get('description', 'N/A')}")
        print(f"[API DEBUG]   Type: {item_data.get('type', 'N/A')}")
        print(f"[API DEBUG]   Sales Price: {item_data.get('salesPrice', 'N/A')}")
        print(f"[API DEBUG]   Base Sales Price: {item_data.get('baseSalesPrice', 'N/A')}")
        print(f"[API DEBUG]   Purchase Cost: {item_data.get('purchaseCost', 'N/A')}")
        print(f"[API DEBUG]   On Hand: {item_data.get('onhand', 'N/A')}")
        print(f"[API DEBUG]   Available: {item_data.get('available', 'N/A')}")
        print(f"[API DEBUG]   Archived: {item_data.get('archived', 'N/A')}")

        # Get the sales price (correct field name)
        sales_price = item_data.get("salesPrice", 0)

        if sales_price is None:
            sales_price = 0

        try:
            price_float = float(sales_price)
            print(f"[API DEBUG] Item {item_id} parsed sales price: ${price_float}")
        except (ValueError, TypeError):
            print(f"[API DEBUG] Item {item_id} has invalid sales price: {sales_price}, using 0")
            price_float = 0.0

        # Return structured details
        details = {
            "price": price_float,
            "name": item_data.get('name', 'Unknown'),
            "fullname": item_data.get('fullname', ''),
            "sku": item_data.get('sku', ''),
            "description": item_data.get('description', ''),
            "type": item_data.get('type', ''),
            "salesPrice": price_float,
            "baseSalesPrice": item_data.get('baseSalesPrice', 0),
            "purchaseCost": item_data.get('purchaseCost', 0),
            "onhand": item_data.get('onhand', 0),
            "available": item_data.get('available', 0),
            "archived": item_data.get('archived', False)
        }

        return True, details

    except Exception as e:
        print(f"[API DEBUG] Error getting details for item {item_id}: {str(e)}")
        return False, f"Exception: {str(e)}"


def calculate_line_amount(quantity, unit_price):
    """
    Calculate the line amount (quantity × unit price)

    Parameters:
    - quantity: Item quantity
    - unit_price: Unit price

    Returns:
    - float: Calculated amount
    """
    try:
        amount = float(quantity) * float(unit_price)
        return round(amount, 2)  # Round to 2 decimal places for currency
    except (ValueError, TypeError):
        return 0.0


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

        # Get item details including price
        price_success, item_details = get_item_price_and_details(item_id, access_token)
        if not price_success:
            print(f"Warning: Could not get details for item {item_id}, using defaults: {item_details}")
            unit_price = 0.0
        else:
            unit_price = item_details.get("price", 0.0)

        # Get current date for line item due date
        current_date = get_current_date_string()

        # Find existing item or add new one
        existing_line_index = None
        for index, line in enumerate(lines):
            item_info = line.get("item", {})
            if isinstance(item_info, dict) and str(item_info.get("id")) == str(item_id):
                existing_line_index = index
                break

        if existing_line_index is not None:
            # Update existing line quantity and check/update price, amount, and due date
            current_quantity = lines[existing_line_index].get("quantity", 0)
            current_price = lines[existing_line_index].get("unitprice", 0.0)
            current_due_date = lines[existing_line_index].get("duedate", "")
            new_quantity = current_quantity + quantity_to_add

            lines[existing_line_index]["quantity"] = new_quantity
            lines[existing_line_index]["duedate"] = current_date

            # Compare prices and update if different
            try:
                current_price_float = float(current_price)
                if abs(current_price_float - unit_price) > 0.001:  # Use small tolerance for float comparison
                    lines[existing_line_index]["unitprice"] = unit_price
                    new_amount = calculate_line_amount(new_quantity, unit_price)
                    lines[existing_line_index]["amount"] = new_amount
                    print(
                        f"Updated existing line: quantity {current_quantity} -> {new_quantity}, price ${current_price_float} -> ${unit_price}, amount -> ${new_amount}, due date -> {current_date}")
                else:
                    # Price unchanged, but quantity changed, so recalculate amount
                    new_amount = calculate_line_amount(new_quantity, unit_price)
                    lines[existing_line_index]["amount"] = new_amount
                    print(
                        f"Updated existing line: quantity {current_quantity} -> {new_quantity}, amount -> ${new_amount}, due date {current_due_date} -> {current_date} (price unchanged: ${current_price_float})")
            except (ValueError, TypeError):
                # If current price is invalid, update it
                lines[existing_line_index]["unitprice"] = unit_price
                new_amount = calculate_line_amount(new_quantity, unit_price)
                lines[existing_line_index]["amount"] = new_amount
                print(
                    f"Updated existing line: quantity {current_quantity} -> {new_quantity}, fixed invalid price '{current_price}' -> ${unit_price}, amount -> ${new_amount}, due date -> {current_date}")
        else:
            # Add new line with retrieved price, calculated amount, and current due date
            next_line_number = max([line.get("lineNumber", 0) for line in lines], default=0) + 1
            line_amount = calculate_line_amount(quantity_to_add, unit_price)
            new_line = {
                "lineNumber": next_line_number,
                "item": {"id": item_id},
                "quantity": quantity_to_add,
                "unitprice": unit_price,
                "amount": line_amount,
                "duedate": current_date,
                "tax": {"taxable": False, "taxCode": None}
            }
            lines.append(new_line)
            print(f"Added new line with price, amount, and due date: {new_line}")

        # Update the lines in current data
        current_data["lines"] = lines

        # Send update
        return update_sales_order(sales_order_id, current_data, access_token)

    except Exception as e:
        return False, f"Exception: {str(e)}"


def add_multiple_items_to_sales_order(sales_order_id, items_to_add, access_token):
    """
    Add multiple items to a sales order based on Google Sheet data with pricing

    Parameters:
    - sales_order_id: ID of the sales order
    - items_to_add: List of dictionaries with item_id, quantity, and optional name
      Format: [{"item_id": "123", "quantity": 2, "name": "Item Name"}, ...]
    - access_token: SOS API access token

    Returns:
    - Tuple (success: bool, result: dict or error_message: str)
    """
    try:
        print(f"[API DEBUG] Adding {len(items_to_add)} items to sales order {sales_order_id}")

        # Get current sales order
        success, response = get_sales_order_by_id(sales_order_id, access_token)
        if not success:
            return False, f"Could not retrieve sales order: {response}"

        # Extract the data portion
        current_data = response.get("data", response)

        # Get current date for line item due dates
        current_date = get_current_date_string()
        print(f"[API DEBUG] Setting line item due dates to: {current_date}")

        # Get current lines
        lines = current_data.get("lines", [])

        items_added = 0
        items_updated = 0
        prices_updated = 0
        amounts_updated = 0
        due_dates_updated = 0
        price_errors = []

        for item_data in items_to_add:
            item_id = str(item_data.get("item_id", ""))
            quantity = item_data.get("quantity", 0)
            item_name = item_data.get("name", f"Item {item_id}")

            if not item_id or quantity <= 0:
                print(f"Skipping invalid item: {item_data}")
                continue

            # Get item details including price
            price_success, item_details = get_item_price_and_details(item_id, access_token)
            if not price_success:
                print(f"Warning: Could not get details for item {item_id} ({item_name}): {item_details}")
                unit_price = 0.0
                price_errors.append(f"Item {item_name} (ID: {item_id})")
            else:
                unit_price = item_details.get("price", 0.0)
                item_full_name = item_details.get("fullname", item_name)
                print(f"[API DEBUG] Retrieved price ${unit_price} for item {item_full_name} (ID: {item_id})")

            # Find existing item or add new one
            existing_line_index = None
            for index, line in enumerate(lines):
                item_info = line.get("item", {})
                if isinstance(item_info, dict) and str(item_info.get("id")) == item_id:
                    existing_line_index = index
                    break

            if existing_line_index is not None:
                # Update existing line quantity and check/update price, amount, and due date
                current_quantity = lines[existing_line_index].get("quantity", 0)
                current_price = lines[existing_line_index].get("unitprice", 0.0)
                current_amount = lines[existing_line_index].get("amount", 0.0)
                current_due_date = lines[existing_line_index].get("duedate", "")
                new_quantity = current_quantity + quantity

                lines[existing_line_index]["quantity"] = new_quantity

                # Update due date
                lines[existing_line_index]["duedate"] = current_date
                if current_due_date != current_date:
                    due_dates_updated += 1

                # Compare prices and update if different
                price_updated = False
                try:
                    current_price_float = float(current_price)
                    if abs(current_price_float - unit_price) > 0.001:  # Use small tolerance for float comparison
                        lines[existing_line_index]["unitprice"] = unit_price
                        price_updated = True
                        prices_updated += 1
                except (ValueError, TypeError):
                    # If current price is invalid, update it
                    lines[existing_line_index]["unitprice"] = unit_price
                    price_updated = True
                    prices_updated += 1
                    current_price_float = 0.0  # For logging

                # Calculate new amount (always update since quantity changed)
                new_amount = calculate_line_amount(new_quantity, unit_price)
                old_amount = current_amount
                lines[existing_line_index]["amount"] = new_amount
                amounts_updated += 1

                if price_updated:
                    print(
                        f"Updated existing item {item_name} (ID: {item_id}): quantity {current_quantity} -> {new_quantity}, price ${current_price_float} -> ${unit_price}, amount ${old_amount} -> ${new_amount}, due date {current_due_date} -> {current_date}")
                else:
                    print(
                        f"Updated existing item {item_name} (ID: {item_id}): quantity {current_quantity} -> {new_quantity}, amount ${old_amount} -> ${new_amount}, due date {current_due_date} -> {current_date} (price unchanged: ${unit_price})")

                items_updated += 1
            else:
                # Add new line with retrieved price, calculated amount, and current due date
                next_line_number = max([line.get("lineNumber", 0) for line in lines], default=0) + 1
                line_amount = calculate_line_amount(quantity, unit_price)
                new_line = {
                    "lineNumber": next_line_number,
                    "item": {"id": item_id},
                    "quantity": quantity,
                    "unitprice": unit_price,
                    "amount": line_amount,
                    "duedate": current_date,
                    "tax": {"taxable": False, "taxCode": None}
                }
                lines.append(new_line)
                print(
                    f"Added new item {item_name} (ID: {item_id}) with quantity: {quantity}, price: ${unit_price}, amount: ${line_amount}, due date: {current_date}")
                items_added += 1
                amounts_updated += 1
                due_dates_updated += 1

        # Update the lines in current data
        current_data["lines"] = lines

        # Send update
        success, result = update_sales_order(sales_order_id, current_data, access_token)

        if success:
            success_message = f"Successfully added {items_added} new items and updated {items_updated} existing items"
            if prices_updated > 0:
                success_message += f" (updated {prices_updated} prices"
                if amounts_updated > 0:
                    success_message += f", {amounts_updated} amounts"
                if due_dates_updated > 0:
                    success_message += f", {due_dates_updated} due dates"
                success_message += ")"
            elif amounts_updated > 0 or due_dates_updated > 0:
                updates = []
                if amounts_updated > 0:
                    updates.append(f"{amounts_updated} amounts")
                if due_dates_updated > 0:
                    updates.append(f"{due_dates_updated} due dates")
                success_message += f" (updated {', '.join(updates)})"

            if price_errors:
                success_message += f". Price lookup failed for: {', '.join(price_errors)}"
            print(success_message)

            return True, {
                "updated_order": result,
                "items_added": items_added,
                "items_updated": items_updated,
                "prices_updated": prices_updated,
                "amounts_updated": amounts_updated,
                "due_dates_updated": due_dates_updated,
                "due_date_set": current_date,
                "total_processed": items_added + items_updated,
                "price_errors": price_errors
            }
        else:
            return False, result

    except Exception as e:
        return False, f"Exception: {str(e)}"


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