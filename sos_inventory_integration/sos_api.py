import requests
import json

API_BASE_URL = "https://api.sosinventory.com/api/v2"


def authenticate(username, password):
    """Authenticate and get access token with detailed logging"""
    print(f"[AUTH DEBUG] Attempting authentication for user: {username}")
    print(f"[AUTH DEBUG] API Base URL: {API_BASE_URL}")

    endpoint = "/auth"
    auth_data = {
        "username": username,
        "password": password
    }

    print(f"[AUTH DEBUG] Sending auth request to: {API_BASE_URL}{endpoint}")

    try:
        url = f"{API_BASE_URL}{endpoint}"
        response = requests.post(url, json=auth_data, timeout=30)

        print(f"[AUTH DEBUG] Auth response status: {response.status_code}")
        print(f"[AUTH DEBUG] Auth response: {response.text}")

        if response.status_code == 200:
            result = response.json()
            if "access_token" in result:
                token = result["access_token"]
                print(f"[AUTH DEBUG] Authentication successful, token: {token[:20]}...")
                return True, token
            else:
                print(f"[AUTH ERROR] No access_token in response: {result}")
                return False, "No access token in response"
        else:
            return False, f"Authentication failed ({response.status_code}): {response.text}"

    except Exception as e:
        print(f"[AUTH ERROR] Exception during authentication: {str(e)}")
        return False, f"Authentication error: {str(e)}"


def make_request(method, endpoint, access_token, data=None, params=None):
    """Make an authenticated API request with enhanced logging and token validation"""
    if not access_token:
        return False, "No access token provided"

    url = f"{API_BASE_URL}/{endpoint.lstrip('/')}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    print(f"[HTTP DEBUG] ===== HTTP REQUEST =====")
    print(f"[HTTP DEBUG] Method: {method}")
    print(f"[HTTP DEBUG] URL: {url}")
    print(f"[HTTP DEBUG] Token (first 20 chars): {access_token[:20]}..." if access_token else "No token")

    if data:
        print(f"[HTTP DEBUG] Request has data: {type(data)}")
        try:
            formatted_data = json.dumps(data, indent=2, default=str)
            print(f"[HTTP DEBUG] Request Data:\n{formatted_data}")
        except Exception as e:
            print(f"[HTTP DEBUG] Could not format request data: {e}")
            print(f"[HTTP DEBUG] Raw request data: {data}")

    if params:
        print(f"[HTTP DEBUG] Request params: {params}")

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

        print(f"[HTTP DEBUG] ===== HTTP RESPONSE =====")
        print(f"[HTTP DEBUG] Status Code: {response.status_code}")
        print(f"[HTTP DEBUG] Response Headers: {dict(response.headers)}")
        print(f"[HTTP DEBUG] Response Text: {response.text}")
        print(f"[HTTP DEBUG] ===== END HTTP RESPONSE =====")

        if response.status_code == 401:
            print(f"[HTTP DEBUG] 401 Unauthorized - Token may be expired")
            return False, "Authentication failed - token may be expired"

        if response.status_code == 403:
            print(f"[HTTP DEBUG] 403 Forbidden - Insufficient permissions")
            return False, "Access denied - insufficient permissions"

        if response.status_code >= 400:
            return False, f"API error ({response.status_code}): {response.text}"

        return True, response.json()

    except requests.exceptions.RequestException as e:
        print(f"[HTTP DEBUG] Request exception: {str(e)}")
        return False, f"Request error: {str(e)}"
    except Exception as e:
        print(f"[HTTP DEBUG] Unexpected exception: {str(e)}")
        return False, f"Unexpected error: {str(e)}"


def test_token_validity(access_token):
    """Test if the current token is valid with a simple API call"""
    print("[TOKEN TEST] Testing token validity...")

    # Use a simple, low-impact endpoint to test the token
    success, result = make_request("GET", "/item", access_token, params={"limit": 1})

    if success:
        print("[TOKEN TEST] ✓ Token is valid")
        return True
    elif "401" in str(result) or "Authentication failed" in str(result) or "Authorization" in str(result):
        print("[TOKEN TEST] ✗ Token is expired/invalid")
        return False
    else:
        print(f"[TOKEN TEST] ? Unexpected response: {result}")
        return False


def get_sales_order_by_id(sales_order_id, access_token):
    """Get a specific sales order by ID"""
    print(f"[API DEBUG] Getting sales order ID: {sales_order_id}")

    endpoint = f"/salesorder/{sales_order_id}"
    result = make_request("GET", endpoint, access_token)

    if result[0]:
        print(f"[API DEBUG] Successfully retrieved sales order")
        return result
    else:
        print(f"[API DEBUG] Failed to get sales order: {result[1]}")
        return result


def update_sales_order(sales_order_id, sales_order_data, access_token):
    """
    Update - Updates a specific sales order by setting the values of the parameters passed

    PUT /api/v2/salesorder/:id

    Parameters:
    - sales_order_id: ID of the sales order to update
    - sales_order_data: Dict containing the complete sales order object with updates
    - access_token: Authentication token

    Returns:
    - Tuple (success: bool, result: dict - updated SalesOrder object or error_message: str)
    """
    print(f"[API DEBUG] ===== UPDATE SALES ORDER REQUEST =====")
    print(f"[API DEBUG] URL sales_order_id: {sales_order_id} (type: {type(sales_order_id)})")
    print(f"[API DEBUG] Request data type: {type(sales_order_data)}")

    if isinstance(sales_order_data, dict):
        print(f"[API DEBUG] Request data keys: {list(sales_order_data.keys())}")

        # Check for problematic ID fields
        problematic_fields = ['id', 'starred', 'syncToken', 'number', 'date', 'subTotal', 'taxPercent', 'taxAmount',
                              'total', 'statusLink']
        found_problematic = {}
        for field in problematic_fields:
            if field in sales_order_data:
                found_problematic[field] = sales_order_data[field]

        if found_problematic:
            print(f"[API WARNING] Found problematic read-only fields in request: {found_problematic}")
        else:
            print(f"[API DEBUG] No read-only fields found in request ✓")

        # Log the complete request body structure
        print(f"[API DEBUG] ===== COMPLETE REQUEST BODY =====")
        try:
            formatted_json = json.dumps(sales_order_data, indent=2, default=str)
            print(formatted_json)
        except Exception as e:
            print(f"[API DEBUG] Could not format JSON: {e}")
            print(f"[API DEBUG] Raw data: {sales_order_data}")
        print(f"[API DEBUG] ===== END REQUEST BODY =====")

        # Log LINES specifically (not lineItems)
        lines = sales_order_data.get('lines', [])
        print(f"[API DEBUG] Lines count: {len(lines)}")
        for idx, line in enumerate(lines):
            print(f"[API DEBUG] Line {idx}: {line}")

    # Make the actual request with detailed logging
    endpoint = f"/salesorder/{sales_order_id}"
    result = make_request("PUT", endpoint, access_token, data=sales_order_data)

    print(f"[API DEBUG] ===== UPDATE RESULT =====")
    print(f"[API DEBUG] Success: {result[0]}")
    if result[0]:
        print(f"[API DEBUG] Response type: {type(result[1])}")
        if isinstance(result[1], dict):
            print(f"[API DEBUG] Response keys: {list(result[1].keys())}")
    else:
        print(f"[API DEBUG] Error response: {result[1]}")
    print(f"[API DEBUG] ===== END UPDATE SALES ORDER =====")

    return result


def add_or_update_item_in_sales_order(sales_order_id, item_id, quantity_to_add, access_token):
    """
    Add item to sales order or increase quantity if it already exists using GET/PUT approach
    Uses correct 'lines' field instead of 'lineItems'
    """
    try:
        print(f"[ITEM DEBUG] ===== ADD/UPDATE ITEM PROCESS =====")
        print(f"[ITEM DEBUG] sales_order_id: {sales_order_id} (type: {type(sales_order_id)})")
        print(f"[ITEM DEBUG] item_id: {item_id} (type: {type(item_id)})")
        print(f"[ITEM DEBUG] quantity_to_add: {quantity_to_add}")

        # Step 1: Get the complete sales order
        print(f"[ITEM DEBUG] Step 1: Getting sales order...")
        success, sales_order = get_sales_order_by_id(sales_order_id, access_token)

        if not success:
            return False, f"Could not retrieve sales order: {sales_order}"

        print(f"[ITEM DEBUG] Retrieved sales order successfully")
        print(f"[ITEM DEBUG] Original sales order keys: {list(sales_order.keys())}")

        # Step 2: Create a clean copy for update
        print(f"[ITEM DEBUG] Step 2: Creating clean copy...")

        # Copy the sales order but remove ALL read-only fields
        updated_sales_order = {}

        # These are the writable fields based on the API documentation
        writable_fields = [
            # Basic order info
            'customerPO', 'comment', 'customerNotes', 'customerMessage', 'statusMessage',

            # Financial fields
            'depositPercent', 'depositAmount', 'discountPercent', 'discountAmount',
            'shippingAmount', 'exchangeRate',

            # Flags
            'discountTaxable', 'shippingTaxable', 'dropShip', 'closed', 'archived', 'summaryOnly',

            # Reference objects (these contain IDs but are typically writable)
            'customer', 'location', 'billing', 'shipping', 'terms', 'salesRep',
            'channel', 'department', 'priority', 'assignedToUser', 'orderStage',
            'taxCode', 'currency', 'customFields',

            # The most important field - LINES (not lineItems)
            'lines'
        ]

        # Copy only writable fields
        for field in writable_fields:
            if field in sales_order:
                updated_sales_order[field] = sales_order[field]

        print(f"[ITEM DEBUG] Cleaned sales order keys: {list(updated_sales_order.keys())}")

        # Step 3: Handle LINES
        print(f"[ITEM DEBUG] Step 3: Processing lines...")
        lines = sales_order.get("lines", [])
        if not isinstance(lines, list):
            lines = []

        print(f"[ITEM DEBUG] Found {len(lines)} existing lines")

        # Clean each line - remove read-only fields
        cleaned_lines = []
        for idx, line in enumerate(lines):
            if isinstance(line, dict):
                print(f"[ITEM DEBUG] Processing line {idx}: {line}")

                # Create clean line - keep only writable fields
                clean_line = {}

                # Writable fields for lines based on the JSON structure
                line_writable_fields = [
                    'lineNumber', 'description', 'quantity', 'unitprice',
                    'percentdiscount', 'duedate',
                    # Object references (these should be preserved)
                    'item', 'class', 'job', 'workcenter', 'tax', 'uom', 'bin', 'lot'
                ]

                for field in line_writable_fields:
                    if field in line:
                        clean_line[field] = line[field]

                cleaned_lines.append(clean_line)
                print(f"[ITEM DEBUG] Cleaned line {idx}: {clean_line}")

        # Step 4: Find existing item or add new one
        print(f"[ITEM DEBUG] Step 4: Finding item {item_id} in lines...")
        existing_line_index = None

        for index, line in enumerate(cleaned_lines):
            item_info = line.get("item", {})
            if isinstance(item_info, dict):
                line_item_id = item_info.get("id")
                print(f"[ITEM DEBUG] Line {index} item ID: {line_item_id}")
                if str(line_item_id) == str(item_id):
                    existing_line_index = index
                    print(f"[ITEM DEBUG] Found existing item at line {index}")
                    break

        if existing_line_index is not None:
            # Update existing line quantity
            existing_line = cleaned_lines[existing_line_index]
            current_quantity = existing_line.get("quantity", 0)
            new_quantity = current_quantity + quantity_to_add

            cleaned_lines[existing_line_index]["quantity"] = new_quantity
            print(f"[ITEM DEBUG] Updated line {existing_line_index} quantity: {current_quantity} -> {new_quantity}")
        else:
            # Add new line
            max_line_number = 0
            for line in cleaned_lines:
                line_num = line.get("lineNumber", 0)
                if line_num > max_line_number:
                    max_line_number = line_num

            new_line_number = max_line_number + 1

            # Create new line with proper structure
            new_line = {
                "lineNumber": new_line_number,
                "item": {
                    "id": item_id
                },
                "quantity": quantity_to_add,
                "unitprice": 0,
                "tax": {
                    "taxable": False,
                    "taxCode": None
                }
            }

            cleaned_lines.append(new_line)
            print(f"[ITEM DEBUG] Added new line: {new_line}")

        # Step 5: Update the lines in the sales order
        updated_sales_order["lines"] = cleaned_lines

        print(f"[ITEM DEBUG] Step 5: Final preparation")
        print(f"[ITEM DEBUG] Final lines count: {len(cleaned_lines)}")
        print(f"[ITEM DEBUG] Final sales order keys: {list(updated_sales_order.keys())}")

        # Final validation - ensure no read-only fields
        read_only_fields = [
            'id', 'starred', 'syncToken', 'number', 'date',
            'subTotal', 'taxPercent', 'taxAmount', 'total', 'statusLink'
        ]

        found_readonly = [field for field in read_only_fields if field in updated_sales_order]
        if found_readonly:
            print(f"[ITEM WARNING] Found read-only fields, removing: {found_readonly}")
            for field in found_readonly:
                updated_sales_order.pop(field, None)

        # Step 6: Send the update
        print(f"[ITEM DEBUG] Step 6: Sending update...")
        return update_sales_order(sales_order_id, updated_sales_order, access_token)

    except Exception as e:
        print(f"[ITEM ERROR] Exception: {str(e)}")
        import traceback
        print(f"[ITEM ERROR] Traceback: {traceback.format_exc()}")
        return False, f"Exception: {str(e)}"