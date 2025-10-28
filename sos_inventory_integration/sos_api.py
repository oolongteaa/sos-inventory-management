import requests
import json
from datetime import datetime
from copy import deepcopy

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


def make_request(method, endpoint, access_token, data=None, params=None, json=None):
    """
    Minimal change: keep existing behavior, but allow json= for POST/PUT.
    - If json is provided, send it as the JSON body.
    - Else, fall back to existing data argument (sent as JSON to preserve prior behavior).
    """
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
    print(f"[HTTP DEBUG] Params: {params}")

    try:
        method_upper = method.upper()
        if method_upper == "GET":
            response = requests.get(url, headers=headers, params=params, timeout=30)
        elif method_upper == "PUT":
            # Prefer json if provided, else use data (as JSON to match previous behavior)
            if json is not None:
                response = requests.put(url, headers=headers, params=params, json=json, timeout=30)
            else:
                response = requests.put(url, headers=headers, params=params, json=data, timeout=30)
        elif method_upper == "POST":
            if json is not None:
                response = requests.post(url, headers=headers, params=params, json=json, timeout=30)
            else:
                response = requests.post(url, headers=headers, params=params, json=data, timeout=30)
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


def add_item_to_sales_order(sales_order_id, item_id, quantity, access_token, force_new_line=False, line_date=None):
    """
    Add an item to a sales order

    Parameters:
    - sales_order_id: ID of the sales order
    - item_id: ID of the item to add
    - quantity: Quantity to add
    - access_token: SOS Inventory access token
    - force_new_line: If True, always create a new line item instead of updating existing quantity
    - line_date: Date to use for the line item (YYYY-MM-DD format), uses current date if None

    Returns:
    - Tuple: (success, result/error_message)
    """
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
            item_name = f"Item {item_id}"
        else:
            unit_price = item_details.get("price", 0.0)
            item_name = item_details.get("name", f"Item {item_id}")

        # Use provided date or current date for line item due date
        due_date = line_date if line_date else get_current_date_string()

        if force_new_line:
            # Always create a new line item, don't look for existing ones
            next_line_number = max([line.get("lineNumber", 0) for line in lines], default=0) + 1
            line_amount = calculate_line_amount(quantity, unit_price)
            new_line = {
                "lineNumber": next_line_number,
                "item": {"id": item_id},
                "quantity": quantity,
                "unitprice": unit_price,
                "amount": line_amount,
                "duedate": due_date,
                "tax": {"taxable": False, "taxCode": None}
            }
            lines.append(new_line)
            print(
                f"[FORCE NEW LINE] Added new line for {item_name} (ID: {item_id}) with quantity: {quantity}, price: ${unit_price}, amount: ${line_amount}, date: {due_date}")
        else:
            # Find existing item or add new one (original behavior)
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
                new_quantity = current_quantity + quantity

                lines[existing_line_index]["quantity"] = new_quantity
                lines[existing_line_index]["duedate"] = due_date

                # Compare prices and update if different
                try:
                    current_price_float = float(current_price)
                    if abs(current_price_float - unit_price) > 0.001:  # Use small tolerance for float comparison
                        lines[existing_line_index]["unitprice"] = unit_price
                        new_amount = calculate_line_amount(new_quantity, unit_price)
                        lines[existing_line_index]["amount"] = new_amount
                        print(
                            f"Updated existing line: quantity {current_quantity} -> {new_quantity}, price ${current_price_float} -> ${unit_price}, amount -> ${new_amount}, due date -> {due_date}")
                    else:
                        # Price unchanged, but quantity changed, so recalculate amount
                        new_amount = calculate_line_amount(new_quantity, unit_price)
                        lines[existing_line_index]["amount"] = new_amount
                        print(
                            f"Updated existing line: quantity {current_quantity} -> {new_quantity}, amount -> ${new_amount}, due date {current_due_date} -> {due_date} (price unchanged: ${unit_price})")
                except (ValueError, TypeError):
                    # If current price is invalid, update it
                    lines[existing_line_index]["unitprice"] = unit_price
                    new_amount = calculate_line_amount(new_quantity, unit_price)
                    lines[existing_line_index]["amount"] = new_amount
                    print(
                        f"Updated existing line: quantity {current_quantity} -> {new_quantity}, fixed invalid price '{current_price}' -> ${unit_price}, amount -> ${new_amount}, due date -> {due_date}")
            else:
                # Add new line with retrieved price, calculated amount, and provided due date
                next_line_number = max([line.get("lineNumber", 0) for line in lines], default=0) + 1
                line_amount = calculate_line_amount(quantity, unit_price)
                new_line = {
                    "lineNumber": next_line_number,
                    "item": {"id": item_id},
                    "quantity": quantity,
                    "unitprice": unit_price,
                    "amount": line_amount,
                    "duedate": due_date,
                    "tax": {"taxable": False, "taxCode": None}
                }
                lines.append(new_line)
                print(
                    f"Added new line: {item_name} (ID: {item_id}) with quantity: {quantity}, price: ${unit_price}, amount: ${line_amount}, date: {due_date}")

        # Update the lines in current data
        current_data["lines"] = lines

        # Send update
        update_success, update_result = update_sales_order(sales_order_id, current_data, access_token)

        if update_success:
            return True, f"Successfully added {item_name} x{quantity}" + (
                " [NEW LINE]" if force_new_line else "") + f" [Date: {due_date}]"
        else:
            return False, update_result

    except Exception as e:
        return False, f"Exception: {str(e)}"


def add_or_update_item_in_sales_order(sales_order_id, item_id, quantity_to_add, access_token):
    """
    Legacy function - maintained for backwards compatibility
    """
    return add_item_to_sales_order(sales_order_id, item_id, quantity_to_add, access_token, force_new_line=False)


def add_multiple_items_to_sales_order(sales_order_id, items_to_add, access_token):
    """
    Add multiple items to a sales order based on Google Sheet data with pricing

    Parameters:
    - sales_order_id: ID of the sales order
    - items_to_add: List of dictionaries with item_id, quantity, name, optional force_new_line, and optional row_date
      Format: [{"item_id": "123", "quantity": 2, "name": "Item Name", "force_new_line": False, "row_date": "2024-01-15"}, ...]
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

        # Get current lines
        lines = current_data.get("lines", [])

        items_added = 0
        items_updated = 0
        new_lines_forced = 0
        prices_updated = 0
        amounts_updated = 0
        due_dates_updated = 0
        price_errors = []

        for item_data in items_to_add:
            item_id = str(item_data.get("item_id", ""))
            quantity = item_data.get("quantity", 0)
            item_name = item_data.get("name", f"Item {item_id}")
            force_new_line = item_data.get("force_new_line", False)
            row_date = item_data.get("row_date")  # Get the date from the item data

            # Use row date or fall back to current date
            due_date = row_date if row_date else get_current_date_string()

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

            if force_new_line:
                # Always create a new line item, don't look for existing ones
                next_line_number = max([line.get("lineNumber", 0) for line in lines], default=0) + 1
                line_amount = calculate_line_amount(quantity, unit_price)
                new_line = {
                    "lineNumber": next_line_number,
                    "item": {"id": item_id},
                    "quantity": quantity,
                    "unitprice": unit_price,
                    "amount": line_amount,
                    "duedate": due_date,
                    "tax": {"taxable": False, "taxCode": None}
                }
                lines.append(new_line)
                print(
                    f"[FORCE NEW LINE] Added new line for {item_name} (ID: {item_id}) with quantity: {quantity}, price: ${unit_price}, amount: ${line_amount}, due date: {due_date}")
                items_added += 1
                new_lines_forced += 1
                amounts_updated += 1
                due_dates_updated += 1
            else:
                # Find existing item or add new one (original behavior)
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
                    lines[existing_line_index]["duedate"] = due_date
                    if current_due_date != due_date:
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
                            f"Updated existing item {item_name} (ID: {item_id}): quantity {current_quantity} -> {new_quantity}, price ${current_price_float} -> ${unit_price}, amount ${old_amount} -> ${new_amount}, due date {current_due_date} -> {due_date}")
                    else:
                        print(
                            f"Updated existing item {item_name} (ID: {item_id}): quantity {current_quantity} -> {new_quantity}, amount ${old_amount} -> ${new_amount}, due date {current_due_date} -> {due_date} (price unchanged: ${unit_price})")

                    items_updated += 1
                else:
                    # Add new line with retrieved price, calculated amount, and provided due date
                    next_line_number = max([line.get("lineNumber", 0) for line in lines], default=0) + 1
                    line_amount = calculate_line_amount(quantity, unit_price)
                    new_line = {
                        "lineNumber": next_line_number,
                        "item": {"id": item_id},
                        "quantity": quantity,
                        "unitprice": unit_price,
                        "amount": line_amount,
                        "duedate": due_date,
                        "tax": {"taxable": False, "taxCode": None}
                    }
                    lines.append(new_line)
                    print(
                        f"Added new item {item_name} (ID: {item_id}) with quantity: {quantity}, price: ${unit_price}, amount: ${line_amount}, due date: {due_date}")
                    items_added += 1
                    amounts_updated += 1
                    due_dates_updated += 1

        # Update the lines in current data
        current_data["lines"] = lines

        # Send update
        success, result = update_sales_order(sales_order_id, current_data, access_token)

        if success:
            success_message = f"Successfully added {items_added} new items and updated {items_updated} existing items"
            if new_lines_forced > 0:
                success_message += f" ({new_lines_forced} forced as new lines)"
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
                "new_lines_forced": new_lines_forced,
                "prices_updated": prices_updated,
                "amounts_updated": amounts_updated,
                "due_dates_updated": due_dates_updated,
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


def get_shipments(access_token, params=None):
    return make_request("GET", "/shipment", access_token, params=params)


def parse_shipment_response(result):
    if not isinstance(result, dict):
        return None

    status = result.get("status") or result.get("Status")
    message = result.get("message")

    if "data" in result and isinstance(result["data"], list):
        data = result["data"]
        return {
            "status": status or "ok",
            "count": result.get("count", len(data)),
            "total_count": result.get("totalCount", len(data)),
            "shipments": [normalize_shipment_shape(s) for s in data],
            "message": message,
        }

    if "id" in result and isinstance(result.get("lines"), list):
        return {
            "status": status or "ok",
            "count": 1,
            "total_count": 1,
            "shipments": [normalize_shipment_shape(result)],
            "message": message,
        }

    if isinstance(result.get("data"), dict) and "id" in result["data"]:
        sh = result["data"]
        return {
            "status": status or "ok",
            "count": 1,
            "total_count": 1,
            "shipments": [normalize_shipment_shape(sh)],
            "message": message,
        }

    return None


def normalize_shipment_shape(sh):
    if not isinstance(sh, dict):
        return {}

    sh = deepcopy(sh)

    defaults = {
        "id": None, "starred": 0, "syncToken": 0, "number": "", "date": None, "customer": None, "location": None,
        "billing": None, "shipping": None, "channel": None, "department": None, "priority": None,
        "assignedToUser": None, "shippingMethod": None, "trackingNumber": None, "linkedTransaction": None,
        "customerMessage": None, "comment": None, "customerNotes": "", "customFields": None, "customerPO": None,
        "shipBy": None, "shippingAmount": 0.0, "total": 0.0, "forceToShipStation": False, "archived": False,
        "summaryOnly": False, "hasSignature": False, "trackingLink": "", "keys": None, "values": None, "lines": [],
    }
    for k, v in defaults.items():
        sh.setdefault(k, v)

    if isinstance(sh.get("customer"), dict):
        sh["customer"].setdefault("id", None)
        sh["customer"].setdefault("name", None)
    if isinstance(sh.get("location"), dict):
        sh["location"].setdefault("id", None)
        sh["location"].setdefault("name", None)

    def ensure_addr(block):
        if not isinstance(block, dict):
            return {
                "company": None, "contact": None, "phone": None, "email": None, "addressName": "", "addressType": "",
                "address": {
                    "line1": None, "line2": None, "line3": None, "line4": None, "line5": None,
                    "city": None, "stateProvince": None, "postalCode": None, "country": None
                },
            }
        block.setdefault("company", None)
        block.setdefault("contact", None)
        block.setdefault("phone", None)
        block.setdefault("email", None)
        block.setdefault("addressName", "")
        block.setdefault("addressType", "")
        addr = block.get("address") or {}
        block["address"] = {
            "line1": addr.get("line1"), "line2": addr.get("line2"), "line3": addr.get("line3"),
            "line4": addr.get("line4"), "line5": addr.get("line5"), "city": addr.get("city"),
            "stateProvince": addr.get("stateProvince"), "postalCode": addr.get("postalCode"),
            "country": addr.get("country"),
        }
        return block

    sh["billing"] = ensure_addr(sh.get("billing"))
    sh["shipping"] = ensure_addr(sh.get("shipping"))

    norm_lines = []
    for ln in sh.get("lines") or []:
        if not isinstance(ln, dict):
            continue
        ln = deepcopy(ln)
        ln_defaults = {
            "id": None, "lineNumber": None, "item": None, "class": None, "job": None, "workcenter": None, "tax": None,
            "linkedTransaction": None, "description": None, "quantity": 0.0, "weight": 0.0, "volume": 0.0,
            "weightunit": "lb", "volumeunit": "cbm", "unitprice": 0.0, "amount": 0.0, "altAmount": 0.0,
            "picked": 0.0, "shipped": 0.0, "invoiced": 0.0, "produced": 0.0, "returned": 0.0, "cost": None,
            "margin": None, "listprice": 0.0, "percentdiscount": 0.0, "backOrdered": 0.0, "duedate": "",
            "uom": None, "bin": None, "lot": None, "serials": None,
        }
        for k, v in ln_defaults.items():
            ln.setdefault(k, v)

        if isinstance(ln.get("item"), dict):
            ln["item"].setdefault("id", None)
            ln["item"].setdefault("name", None)

        if isinstance(ln.get("linkedTransaction"), dict):
            lt = ln["linkedTransaction"]
            lt.setdefault("id", None)
            lt.setdefault("transactionType", None)
            lt.setdefault("refNumber", None)
            lt.setdefault("lineNumber", None)

        norm_lines.append(ln)

    sh["lines"] = norm_lines

    return sh


# FIXED: Helper function to correctly build address blocks.
def _build_shipment_address_block(addr_data):
    """Builds a valid address block from a dictionary."""
    if not addr_data or not isinstance(addr_data, dict):
        return {
            "company": None, "contact": None, "phone": None, "email": None,
            "addressName": "", "addressType": "",
            "address": {
                "line1": None, "line2": None, "line3": None, "line4": None, "line5": None,
                "city": None, "stateProvince": None, "postalCode": None, "country": None
            }
        }

    addr = addr_data.get("address", {})
    return {
        "company": addr_data.get("company"), "contact": addr_data.get("contact"),
        "phone": addr_data.get("phone"), "email": addr_data.get("email"),
        "addressName": addr_data.get("addressName", ""), "addressType": addr_data.get("addressType", ""),
        "address": {
            "line1": addr.get("line1"), "line2": addr.get("line2"), "line3": addr.get("line3"),
            "line4": addr.get("line4"), "line5": addr.get("line5"), "city": addr.get("city"),
            "stateProvince": addr.get("stateProvince"), "postalCode": addr.get("postalCode"),
            "country": addr.get("country"),
        }
    }


def build_fully_filled_shipment(
    *,
    number,
    date,
    ship_by,
    customer_id,
    location_id,
    lines,
    customer_name=None,
    location_name=None,
    billing_address=None,
    shipping_address=None,
    header_linked_tx=None,
    shipping_method_id=None,
    department_id=None,
    channel_id=None,
    priority=None,
    assigned_user_id=None,
    customer_message=None,
    comment=None,
    customer_po=None,
    shipping_amount=0.0,
):
    """
    Build a 'full' shipment object with explicit values for all writable fields.
    Does NOT include any 'id' fields at header or line per SOS rules.
    You must supply all values you care about.
    """
    payload = {
        "starred": 0, "syncToken": 0, "number": number or "", "date": date,
        "customer": {"id": customer_id, **({"name": customer_name} if customer_name else {})},
        "location": {"id": location_id, **({"name": location_name} if location_name else {})},
        "billing": _build_shipment_address_block(billing_address),
        "shipping": _build_shipment_address_block(shipping_address),
        "channel": {"id": channel_id} if channel_id else None,
        "department": {"id": department_id} if department_id else None,
        "priority": priority,
        "assignedToUser": {"id": assigned_user_id} if assigned_user_id else None,
        "shippingMethod": {"id": shipping_method_id} if shipping_method_id else None,
        "trackingNumber": None,
        "linkedTransaction": (
            {k: v for k, v in (header_linked_tx or {}).items() if k in ("id", "transactionType", "refNumber")}
            if header_linked_tx else None
        ),
        "customerMessage": customer_message, "comment": comment, "customerNotes": "",
        "customFields": None, "customerPO": customer_po, "shipBy": ship_by or "",
        "shippingAmount": float(shipping_amount or 0), "forceToShipStation": False,
        "archived": False, "summaryOnly": False, "hasSignature": False, "trackingLink": "",
        "keys": None, "values": None,
        # FIXED: Added missing fields from working payload
        "orderStage": None, "taxCode": None, "total": 0,
        "lines": [],
    }

    # Build lines with explicit fields
    for i, ln in enumerate(lines, 1):
        line_payload = {
            "lineNumber": i,  # FIXED: Add lineNumber, starting from 1
            "item": {"id": ln["item_id"]},
            "class": ({"id": ln["class_id"]} if ln.get("class_id") else None),
            "job": ({"id": ln["job_id"]} if ln.get("job_id") else None),
            "workcenter": None, "tax": None,
            "linkedTransaction": (
                {k: v for k, v in ln.get("line_linked_tx", {}).items()
                 if k in ("id", "transactionType", "refNumber", "lineNumber")}
                if ln.get("line_linked_tx") else None
            ),
            "description": ln.get("description"),
            "quantity": float(ln.get("quantity", 0)),
            "weight": 0.0, "volume": 0.0, "weightunit": "lb", "volumeunit": "cbm",
            "unitprice": float(ln.get("unitprice", 0)),
            "amount": float(ln.get("amount", 0)),
            "altAmount": 0, "picked": 0, "shipped": 0, "invoiced": 0, "produced": 0, "returned": 0,
            "cost": None, "margin": None,
            "listprice": float(ln.get("listprice", 0)),  # FIXED: Corrected key and default
            "percentdiscount": float(ln.get("percentdiscount", 0)),  # FIXED: Corrected default
            "backOrdered": 0.0,
            "duedate": ln.get("duedate"),  # FIXED: Default to None if not present
            "uom": ({"id": ln["uom_id"]} if ln.get("uom_id") else None),
            "bin": None, "lot": None,
            "serials": None,  # FIXED: Default to None
        }
        payload["lines"].append(line_payload)

    return payload


# FIXED: This function is modified to correctly handle the payload.
def create_shipment(payload, access_token, sanitize=True):
    """
    POST /shipment with a fully filled payload.
    - Sanitizes forbidden ids (header/lines) if present.
    - Ensures date strings include time.
    - Sends via json=.
    - Returns normalized shipment when possible.
    """
    body = deepcopy(payload)

    if sanitize:
        # Must not include shipment id on create
        body.pop("id", None)
        # Must not include line ids on create
        if isinstance(body.get("lines"), list):
            for ln in body["lines"]:
                if isinstance(ln, dict):
                    ln.pop("id", None)
                    # FIXED: DO NOT remove lineNumber. The API expects it on create.

    # Ensure dates include time if only YYYY-MM-DD provided
    for key in ("date", "shipBy"):
        if key in body and isinstance(body[key], str) and len(body[key]) == 10 and "T" not in body[key]:
            body[key] = f"{body[key]}T00:00:00"

    success, result = make_request("POST", "/shipment", access_token, params=None, json=body)
    if not success:
        return False, result

    parsed = parse_shipment_response(result)
    if parsed and parsed.get("shipments"):
        return True, parsed["shipments"][0]
    return True, result
