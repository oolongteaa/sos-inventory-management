#!/usr/bin/env python3
"""
Google Sheets to SOS Inventory Integration
Monitors a Google Sheet for completed rows, searches SOS Inventory sales orders
using Column B value + current month (e.g., "HA 101 July"), and adds items from
the spreadsheet based on item IDs in row 1 and quantities in the processed row
"""

import gspread
from google.oauth2.service_account import Credentials
import time
import hashlib
from datetime import datetime
from sos_inventory_integration import sos_api
from sos_inventory_integration import sos_auth

# Google Sheets Configuration
GOOGLE_CREDENTIALS_FILE = "cogent-scion-463416-v2-ae77628bbccc.json"  # Replace with your JSON file path
GOOGLE_SHEET_ID = "1yTdPlHPi8Xa1tADpJFJZa6-1ZFnFBMydp3AdnGgKEZs"  # Replace with your Google Sheet ID
WORKSHEET_INDEX = 0  # Use the first worksheet (0-indexed)

# Monitoring Configuration
CHECK_INTERVAL = 10  # seconds between sheet checks
DONE_COLUMN_NAME = "Done?"  # case-insensitive column name to monitor
SEARCH_COLUMN = "B"  # Column B contains the search string for sales orders

# Row configuration for spreadsheet structure
ITEM_ID_ROW = 0  # Row 1 (0-indexed) contains item IDs
ITEM_NAME_ROW = 1  # Row 2 (1-indexed) contains item names

# Color Configuration
SUCCESS_COLOR = {
    "red": 0.8,  # Light blue RGB values (normalized 0-1)
    "green": 0.9,
    "blue": 1.0
}

ERROR_COLOR = {
    "red": 1.0,  # Light red RGB values (normalized 0-1)
    "green": 0.8,
    "blue": 0.8
}

# Global state
_sos_access_token = None
_previous_completed_rows = None
_sheet_instance = None
_sheet_data_cache = None  # Cache sheet data to extract item IDs and names
_processed_rows_cache = set()  # Track processed rows to avoid duplicates
_done_column_info = None  # Store Done? column location


def print_separator(title=""):
    print("\n" + "=" * 60)
    if title:
        print(f" {title}")
        print("=" * 60)


def get_current_month_name():
    """Get the current month name (e.g., 'July', 'December')"""
    return datetime.now().strftime("%B")


def build_search_string(column_b_value):
    """
    Build search string from column B value + current month

    Parameters:
    - column_b_value: Value from column B of the Google Sheet

    Returns:
    - Search string in format "column_b_value current_month"
    """
    if not column_b_value:
        return None

    current_month = get_current_month_name()
    search_string = f"{column_b_value.strip()} {current_month}"

    return search_string


def find_done_column(data):
    """
    Find the 'Done?' column in any row of the sheet

    Parameters:
    - data: All sheet data (list of lists)

    Returns:
    - Dictionary with 'row', 'column', and 'found' keys
    """
    global _done_column_info

    if not data:
        return {'found': False, 'row': None, 'column': None}

    print("    [DEBUG] Searching for 'Done?' column...")

    # Search through all rows to find "Done?"
    for row_index, row in enumerate(data):
        for col_index, cell_value in enumerate(row):
            if cell_value and cell_value.strip().lower() == DONE_COLUMN_NAME.lower():
                _done_column_info = {
                    'found': True,
                    'row': row_index,
                    'column': col_index,
                    'row_number': row_index + 1,  # 1-indexed for display
                    'column_letter': chr(65 + col_index)  # Convert to A, B, C, etc.
                }
                print(
                    f"    [DEBUG] Found 'Done?' in row {row_index + 1}, column {chr(65 + col_index)} (index {col_index})")
                return _done_column_info

    print(f"    [DEBUG] 'Done?' column not found in sheet")
    return {'found': False, 'row': None, 'column': None}


def ensure_valid_sos_token():
    """Ensure we have a valid SOS access token, refresh if needed"""
    global _sos_access_token

    print("[TOKEN DEBUG] Checking SOS token validity...")

    # If we don't have a token at all, get one
    if not _sos_access_token:
        print("[TOKEN DEBUG] No token found, authenticating...")
        if sos_auth.authenticate():
            _sos_access_token = sos_auth.get_access_token()
            print("[TOKEN DEBUG] Initial authentication successful ✓")
            return True
        else:
            print("[TOKEN ERROR] Initial authentication failed")
            return False

    # Test the current token with a simple API call
    success, result = sos_api.test_connection(_sos_access_token)
    if success:
        print("[TOKEN DEBUG] Current token is valid ✓")
        return True
    else:
        print("[TOKEN DEBUG] Token expired/invalid, refreshing...")

        # Try to refresh the token
        if hasattr(sos_auth, 'refresh_access_token'):
            refresh_success, refresh_result = sos_auth.refresh_access_token()
            if refresh_success:
                _sos_access_token = sos_auth.get_access_token()
                print("[TOKEN DEBUG] Token refreshed successfully ✓")
                return True

        # If refresh failed or not available, re-authenticate
        print("[TOKEN DEBUG] Attempting full re-authentication...")
        if sos_auth.authenticate():
            _sos_access_token = sos_auth.get_access_token()
            print("[TOKEN DEBUG] Re-authentication successful ✓")
            return True
        else:
            print("[TOKEN ERROR] Re-authentication failed")
            return False


def extract_items_from_sheet_data(sheet_data, row_data):
    """
    Extract items and quantities from sheet data based on the processed row

    Parameters:
    - sheet_data: All sheet data (list of lists)
    - row_data: The specific row being processed

    Returns:
    - List of dictionaries with item_id, quantity, and name
    """
    try:
        if not sheet_data or len(sheet_data) < 3:
            print("ERROR: Sheet data insufficient - need at least 3 rows")
            return []

        # Get item IDs from row 1 (index 0)
        item_ids = sheet_data[ITEM_ID_ROW] if len(sheet_data) > ITEM_ID_ROW else []

        # Get item names from row 2 (index 1)
        item_names = sheet_data[ITEM_NAME_ROW] if len(sheet_data) > ITEM_NAME_ROW else []

        # Get quantities from the current row data
        quantities = row_data.get('data', [])

        print(f"    [DEBUG] Processing items from sheet:")
        print(f"      Item IDs row: {len(item_ids)} columns")
        print(f"      Item names row: {len(item_names)} columns")
        print(f"      Current row data: {len(quantities)} columns")

        items_to_add = []

        # Start from column index that corresponds to inventory items
        # Skip the first few columns that contain metadata (timestamp, unit, customer, etc.)
        start_column = 3  # Adjust based on your sheet structure

        for col_index in range(start_column, len(quantities)):
            # Get quantity from current row
            quantity_value = quantities[col_index] if col_index < len(quantities) else None

            # Skip if no quantity or invalid quantity
            if not quantity_value or quantity_value == '' or quantity_value == 0:
                continue

            try:
                quantity = float(quantity_value)
                if quantity <= 0:
                    continue
            except (ValueError, TypeError):
                print(f"      Warning: Invalid quantity '{quantity_value}' in column {col_index}")
                continue

            # Get corresponding item ID and name
            item_id = item_ids[col_index] if col_index < len(item_ids) else None
            item_name = item_names[col_index] if col_index < len(item_names) else None

            # Skip if no item ID
            if not item_id or item_id == '':
                continue

            # Clean up the data
            item_id_clean = str(item_id).strip()
            item_name_clean = str(item_name).strip() if item_name else f"Item {item_id_clean}"

            items_to_add.append({
                "item_id": item_id_clean,
                "quantity": quantity,
                "name": item_name_clean,
                "column": col_index
            })

            print(f"      Found: {item_name_clean} (ID: {item_id_clean}) - Qty: {quantity}")

        print(f"    [DEBUG] Total items to add: {len(items_to_add)}")
        return items_to_add

    except Exception as e:
        print(f"ERROR: Exception extracting items from sheet: {str(e)}")
        return []


def setup_google_sheets():
    """Setup Google Sheets API access and target the first worksheet"""
    global _sheet_instance
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=scope)
        client = gspread.authorize(creds)

        # Open the spreadsheet by key
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)

        # Get all worksheets to show available options
        worksheets = spreadsheet.worksheets()
        print(f"Found {len(worksheets)} worksheet(s) in the spreadsheet:")
        for i, ws in enumerate(worksheets):
            print(f"  {i}: {ws.title} (ID: {ws.id})")

        # Use the first worksheet (index 0)
        if len(worksheets) > WORKSHEET_INDEX:
            sheet = worksheets[WORKSHEET_INDEX]
            print(f"Using worksheet: '{sheet.title}' (index {WORKSHEET_INDEX})")
        else:
            print(f"ERROR: Worksheet index {WORKSHEET_INDEX} not found")
            return None

        _sheet_instance = sheet
        print("SUCCESS: Google Sheets connection established")

        # Test the connection by getting basic info
        try:
            row_count = sheet.row_count
            col_count = sheet.col_count
            print(f"Worksheet dimensions: {row_count} rows x {col_count} columns")
        except Exception as e:
            print(f"WARNING: Could not get worksheet dimensions: {e}")

        return sheet
    except Exception as e:
        print(f"ERROR: Failed to setup Google Sheets: {e}")
        return None


def create_row_signature(row_data):
    """Create a unique signature for a row to track if it's been processed"""
    try:
        # Use row number + first few columns as signature
        data = row_data.get('data', [])
        row_num = row_data.get('row_number', 0)

        # Create signature from row number and key columns
        signature_parts = [str(row_num)]

        # Add first 5 columns to signature (adjust as needed)
        for i in range(min(5, len(data))):
            signature_parts.append(str(data[i]).strip())

        signature = "|".join(signature_parts)
        return hashlib.md5(signature.encode()).hexdigest()
    except Exception as e:
        print(f"ERROR: Could not create row signature: {e}")
        return None


def color_row(sheet, row_number, color_type="success"):
    """Color the entire row - blue for success, red for error"""
    try:
        # Get the sheet dimensions to determine the range
        all_values = sheet.get_all_values()
        if not all_values:
            print(f"WARNING: Could not determine sheet dimensions for row {row_number}")
            return False

        # Find the actual number of columns by checking the header row or the longest row
        num_columns = 0
        for row in all_values:
            if len(row) > num_columns:
                num_columns = len(row)

        # If still no columns found, default to a reasonable number
        if num_columns == 0:
            num_columns = 50  # Default to 50 columns

        # Convert column number to letter notation (handles beyond Z)
        def num_to_col_letters(n):
            """Convert column number to Excel-style letters (1=A, 26=Z, 27=AA, etc.)"""
            result = ""
            while n > 0:
                n -= 1  # Make it 0-indexed
                result = chr(65 + (n % 26)) + result
                n //= 26
            return result

        end_column = num_to_col_letters(num_columns)

        # Define the range (e.g., "A5:AC5" for row 5 with 29 columns)
        range_name = f"A{row_number}:{end_column}{row_number}"

        # Choose color based on type
        if color_type == "error":
            color = ERROR_COLOR
            color_name = "light red"
        else:
            color = SUCCESS_COLOR
            color_name = "light blue"

        print(f"Coloring range: {range_name} ({color_name})")

        # Create the format request
        format_request = {
            "backgroundColor": color
        }

        # Apply the formatting
        sheet.format(range_name, format_request)
        print(f"SUCCESS: Colored row {row_number} {color_name} (columns A to {end_column})")
        return True

    except Exception as e:
        print(f"ERROR: Failed to color row {row_number}: {e}")
        return False


def setup_sos_inventory():
    """Setup SOS Inventory API access"""
    global _sos_access_token

    print_separator("SOS INVENTORY AUTHENTICATION")
    print("Setting up SOS Inventory API access...")

    if not ensure_valid_sos_token():
        print("ERROR: SOS Inventory authentication failed")
        return False

    print("SUCCESS: SOS Inventory authentication successful!")

    # Test the connection
    success, result = sos_api.test_connection(_sos_access_token)
    if success:
        print("SUCCESS: SOS Inventory API connection verified")
        return True
    else:
        print(f"ERROR: SOS Inventory API test failed: {result}")
        return False


def fetch_sheet_data(sheet):
    """Fetch all sheet data and compute a hash for change detection"""
    global _sheet_data_cache
    try:
        # Get fresh data from the sheet
        data = sheet.get_all_values()
        _sheet_data_cache = data  # Cache the data for item extraction

        # Create hash for change detection - include timestamp for better detection
        timestamp = str(int(time.time() * 1000))  # milliseconds
        flat_data = "".join([",".join(row) for row in data]) + timestamp
        data_hash = hashlib.md5(flat_data.encode()).hexdigest()

        print(f"    [DEBUG] Fetched {len(data)} rows from sheet")
        return data_hash, data
    except Exception as e:
        print(f"ERROR: Error fetching sheet data: {e}")
        return None, None


def filter_completed_rows(data):
    """Filter rows where the 'Done?' column is 'Yes'"""
    global _done_column_info

    if not data:
        return []

    # Find the "Done?" column
    done_info = find_done_column(data)
    if not done_info['found']:
        print(f"WARNING: '{DONE_COLUMN_NAME}' column not found in the sheet")
        return []

    done_column_index = done_info['column']
    done_row_index = done_info['row']

    print(f"    [DEBUG] Found 'Done?' at row {done_row_index + 1}, column {chr(65 + done_column_index)}")
    print(
        f"    [DEBUG] Looking for 'Yes' values in column {chr(65 + done_column_index)} below row {done_row_index + 1}")

    # Filter rows where "Done?" is "Yes" (case-insensitive)
    # Only check rows BELOW the "Done?" header row
    completed_rows = []
    for row_index, row in enumerate(data):
        # Skip rows at or above the "Done?" header row
        if row_index <= done_row_index:
            continue

        # Check if the row has enough columns and if "Done?" is "Yes"
        if (len(row) > done_column_index and
                row[done_column_index].strip().lower() == "yes"):
            # Add row number for reference (using the original header row for context)
            row_with_meta = {
                'row_number': row_index + 1,  # 1-indexed row number
                'data': row,
                'headers': data[0] if len(data) > 0 else [],  # Use first row as headers
                'done_column_index': done_column_index
            }
            completed_rows.append(row_with_meta)
            print(f"    [DEBUG] Found completed row {row_index + 1}: Done? = '{row[done_column_index]}'")

    print(f"    [DEBUG] Found {len(completed_rows)} completed rows total")
    return completed_rows


def get_new_completed_rows(current_completed, previous_completed):
    """Compare completed rows to find new ones, avoiding duplicates"""
    global _processed_rows_cache

    if previous_completed is None:
        # On first run, don't process existing completed rows
        print("    [DEBUG] First run - not processing existing completed rows")
        return []

    new_rows = []

    for row in current_completed:
        # Create a signature for this row
        signature = create_row_signature(row)
        if not signature:
            continue

        # Check if we've already processed this row
        if signature in _processed_rows_cache:
            print(f"    [DEBUG] Row {row['row_number']} already processed (cached)")
            continue

        # Check if this row was in the previous set
        row_was_previously_completed = False
        if previous_completed:
            for prev_row in previous_completed:
                prev_signature = create_row_signature(prev_row)
                if prev_signature == signature:
                    row_was_previously_completed = True
                    break

        # If it wasn't previously completed, it's new
        if not row_was_previously_completed:
            new_rows.append(row)
            _processed_rows_cache.add(signature)
            print(f"    [DEBUG] New completed row detected: {row['row_number']}")

    return new_rows


def get_column_b_value(row_data):
    """Extract the value from column B (index 1)"""
    try:
        data = row_data.get('data', [])
        if len(data) > 1:  # Column B is index 1 (0=A, 1=B, etc.)
            return data[1].strip()
        else:
            print(f"WARNING: Row {row_data['row_number']} doesn't have a column B value")
            return None
    except Exception as e:
        print(f"ERROR: Error extracting column B value from row {row_data['row_number']}: {e}")
        return None


def validate_row_data(row_data):
    """Validate that row data can be processed"""
    try:
        headers = row_data.get('headers', [])
        data = row_data.get('data', [])

        if not data:
            print(f"ERROR: No data found for row {row_data['row_number']}")
            return False

        # Check if we have at least column B (index 1)
        if len(data) < 2:
            print(f"ERROR: Row {row_data['row_number']} doesn't have enough columns (need at least B)")
            return False

        # Check if column B has meaningful data
        column_b_value = get_column_b_value(row_data)
        if not column_b_value:
            print(f"ERROR: Row {row_data['row_number']} has empty or invalid column B value")
            return False

        return True

    except Exception as e:
        print(f"ERROR: Error validating row data for row {row_data.get('row_number', 'unknown')}: {e}")
        return False


def add_items_to_sales_order(sales_order_id, items_to_add):
    """
    Add multiple items to sales order using the API function with pricing

    Parameters:
    - sales_order_id: ID of the sales order
    - items_to_add: List of item dictionaries from extract_items_from_sheet_data

    Returns:
    - Tuple (success: bool, message: str)
    """
    try:
        print(f"    [DEBUG] Adding {len(items_to_add)} items to sales order {sales_order_id}")

        # Ensure we have a valid token before proceeding
        if not ensure_valid_sos_token():
            return False, "Failed to obtain valid SOS authentication token"

        # Use the API function that handles multiple items with pricing
        success, result = sos_api.add_multiple_items_to_sales_order(
            sales_order_id, items_to_add, _sos_access_token
        )

        if success:
            # Extract update statistics including price, amount, and due date updates
            items_added = result.get("items_added", 0)
            items_updated = result.get("items_updated", 0)
            prices_updated = result.get("prices_updated", 0)
            amounts_updated = result.get("amounts_updated", 0)
            due_dates_updated = result.get("due_dates_updated", 0)
            due_date_set = result.get("due_date_set", "")
            total_processed = result.get("total_processed", 0)
            price_errors = result.get("price_errors", [])

            print(f"    Successfully processed {total_processed} items:")
            print(f"      - New items added: {items_added}")
            print(f"      - Existing items updated: {items_updated}")

            if prices_updated > 0:
                print(f"      - Prices updated: {prices_updated}")

            if amounts_updated > 0:
                print(f"      - Line amounts updated: {amounts_updated}")

            if due_dates_updated > 0:
                print(f"      - Due dates updated: {due_dates_updated} (set to {due_date_set})")

            if price_errors:
                print(f"      - Price lookup errors: {len(price_errors)} items")
                for item in price_errors:
                    print(f"        * {item}")

            message = f"Processed {total_processed} items ({items_added} new, {items_updated} updated"
            if prices_updated > 0:
                message += f", {prices_updated} prices updated"
            if amounts_updated > 0:
                message += f", {amounts_updated} amounts updated"
            if due_dates_updated > 0:
                message += f", {due_dates_updated} due dates updated to {due_date_set}"
            message += ")"
            if price_errors:
                message += f", {len(price_errors)} price errors"

            return True, message
        else:
            print(f"    Failed to add items to sales order: {result}")
            return False, result

    except Exception as e:
        print(f"    Exception while processing sales order: {str(e)}")
        return False, f"Exception while adding items: {str(e)}"

def process_completed_row(row_data):
    """Process a newly completed row by searching SOS Inventory sales orders and adding items from spreadsheet"""
    global _sheet_instance, _sheet_data_cache

    print(f"\nProcessing Row {row_data['row_number']}:")

    success = False
    error_reason = None

    try:
        # First validate the row data
        if not validate_row_data(row_data):
            error_reason = "Invalid row data"
            success = False
        else:
            # Display the row data
            data = row_data['data']
            headers = row_data.get('headers', [])

            print("Row contents:")
            for i, value in enumerate(data):
                if value.strip():  # Only show non-empty values
                    header = headers[i] if i < len(headers) else f"Column {chr(65 + i)}"
                    print(f"  {header}: {value}")

            # Get the search string from column B + current month
            column_b_value = get_column_b_value(row_data)
            search_string = build_search_string(column_b_value)

            if not search_string:
                error_reason = "Could not build search string from column B"
                success = False
            else:
                current_month = get_current_month_name()
                print(f"\nColumn B value: '{column_b_value}'")
                print(f"Current month: '{current_month}'")
                print(f"Full search string: '{search_string}'")

                # Extract items from the sheet data
                if not _sheet_data_cache:
                    error_reason = "No sheet data available for item extraction"
                    success = False
                else:
                    items_to_add = extract_items_from_sheet_data(_sheet_data_cache, row_data)

                    if not items_to_add:
                        error_reason = "No items found to add from this row"
                        success = False
                    else:
                        # Search SOS Inventory for sales orders and add items
                        success, error_reason = search_and_update_sales_orders(row_data, search_string, items_to_add)

    except Exception as e:
        print(f"ERROR: Exception while processing row {row_data['row_number']}: {e}")
        success = False
        error_reason = f"Processing exception: {str(e)}"

    # Color the row based on success/failure
    if _sheet_instance:
        if success:
            color_success = color_row(_sheet_instance, row_data['row_number'], "success")
            if color_success:
                print(f"Row {row_data['row_number']} has been colored light blue (SUCCESS)")
        else:
            color_success = color_row(_sheet_instance, row_data['row_number'], "error")
            if color_success:
                print(f"Row {row_data['row_number']} has been colored light red (ERROR: {error_reason})")

    if success:
        print(f"SUCCESS: Sales order processing successful for row {row_data['row_number']}")
    else:
        print(f"ERROR: Processing failed for row {row_data['row_number']} - {error_reason}")

    return success


def search_and_update_sales_orders(row_data, search_string, items_to_add):
    """Search SOS Inventory sales orders and add items from spreadsheet to found orders"""
    print(f"Searching SOS Inventory sales orders for row {row_data['row_number']}...")

    try:
        # Ensure we have a valid access token
        if not ensure_valid_sos_token():
            print("  ERROR: Failed to obtain valid SOS Inventory access token")
            return False, "No access token"

        print(f"  Items to add: {len(items_to_add)}")
        for item in items_to_add:
            print(f"    - {item['name']} (ID: {item['item_id']}) x {item['quantity']}")

        # Search for sales orders containing the search string
        print(f"  Searching sales orders containing: '{search_string}'...")

        # Use the updated search function with a reasonable limit
        success, result = sos_api.search_sales_orders_by_query(
            search_string,
            _sos_access_token,
            additional_params={"maxresults": 50}
        )

        if not success:
            print(f"    ERROR: Failed to search sales orders: {result}")
            return False, f"Search failed: {result}"

        # Parse the response using the helper function
        parsed_data = sos_api.parse_sales_order_response(result)
        if not parsed_data:
            print("    ERROR: Unable to parse sales order response")
            return False, "Unable to parse response"

        orders = parsed_data["orders"]
        total_count = parsed_data["total_count"]

        print(f"    Found {len(orders)} sales orders containing '{search_string}' (total: {total_count})")

        if not orders:
            print(f"    No sales orders found containing '{search_string}'")
            print(
                f"    Tried searching for: Column B ('{row_data.get('data', ['', ''])[1] if len(row_data.get('data', [])) > 1 else 'N/A'}') + Current Month ('{get_current_month_name()}')")
            return False, "No sales orders found"

        # Display the matching orders
        print("    Matching sales orders:")
        for i, order in enumerate(orders[:10]):  # Show first 10 matches
            summary = sos_api.format_sales_order_summary(order)
            print(f"      {i + 1}. {summary}")

        if len(orders) > 10:
            print(f"      ... and {len(orders) - 10} more orders")

        if total_count > len(orders):
            print(f"      (Total matches: {total_count}, showing first {len(orders)})")

        # Process each found sales order - add the items from spreadsheet
        print(f"\n    Adding {len(items_to_add)} items to found sales orders (with pricing)...")

        successful_updates = 0
        failed_updates = 0
        update_details = []

        for i, order in enumerate(orders[:5]):  # Process first 5 orders to avoid overwhelming the system
            order_id = order.get("id")
            order_number = order.get("number", "Unknown")

            if not order_id:
                print(f"      Order {i + 1} ({order_number}): No ID found, skipping")
                failed_updates += 1
                continue

            print(f"      Processing Order {i + 1} ({order_number}, ID: {order_id})...")

            # Add items to this sales order
            item_success, item_message = add_items_to_sales_order(order_id, items_to_add)

            if item_success:
                print(f"        SUCCESS: {item_message}")
                successful_updates += 1
                update_details.append(f"Order {order_number}: {item_message}")
            else:
                print(f"        ERROR: {item_message}")
                failed_updates += 1
                update_details.append(f"Order {order_number}: FAILED - {item_message}")

        # Summary of updates
        print(f"\n    Update Summary:")
        print(f"      Successful updates: {successful_updates}")
        print(f"      Failed updates: {failed_updates}")
        print(f"      Total orders processed: {successful_updates + failed_updates}")
        print(f"      Items processed per order: {len(items_to_add)}")

        if update_details:
            print(f"    Update Details:")
            for detail in update_details:
                print(f"      - {detail}")

        # Determine overall success
        if successful_updates > 0:
            return True, None  # At least one update succeeded
        elif failed_updates > 0:
            return False, f"All {failed_updates} update attempts failed"
        else:
            return False, "No orders were processed"

    except Exception as e:
        print(f"  ERROR: Error searching/updating sales orders: {e}")
        return False, f"Exception: {str(e)}"


def monitor_sheet():
    """Main monitoring loop"""
    global _previous_completed_rows

    print_separator("SHEET MONITORING")

    # Setup connections
    sheet = setup_google_sheets()
    if not sheet:
        return False

    if not setup_sos_inventory():
        return False

    current_month = get_current_month_name()

    print(f"\nStarting sheet monitoring...")
    print(f"Sheet ID: {GOOGLE_SHEET_ID}")
    print(f"Worksheet: '{sheet.title}' (index {WORKSHEET_INDEX})")
    print(f"Check interval: {CHECK_INTERVAL} seconds")
    print(f"Monitoring column: '{DONE_COLUMN_NAME}' (will search entire sheet for this header)")
    print(f"Search column: {SEARCH_COLUMN}")
    print(f"Current month: {current_month}")
    print(f"Search format: '[Column B Value] {current_month}' (e.g., 'HA 101 {current_month}')")
    print(f"Item processing: Extract from spreadsheet using item IDs in row 1")
    print("Will search SOS Inventory sales orders and add items from spreadsheet when new rows are completed")
    print("Uses OAuth2 Bearer token authentication with automatic token refresh")
    print("Uses GET sales order → modify lines → PUT sales order approach")
    print("Returns updated sales order object for verification")
    print("SUCCESS: Rows will be colored light blue")
    print("FAILURE: Rows will be colored light red")
    print("Handles new rows added to sheet dynamically")
    print("Searches for 'Done?' header in any row, then looks for 'Yes' values below it")
    print("Automatically retrieves and applies item pricing from SOS Inventory")
    print("Press Ctrl+C to stop monitoring")

    prev_hash = None

    try:
        while True:
            current_hash, data = fetch_sheet_data(sheet)

            if current_hash is None:
                print("ERROR: Failed to fetch sheet data, retrying...")
                time.sleep(30)
                continue

            if current_hash != prev_hash:
                print(f"\nSheet updated! ({time.strftime('%Y-%m-%d %H:%M:%S')})")

                # Get current completed rows
                current_completed_rows = filter_completed_rows(data)

                # Find newly completed rows
                new_completed_rows = get_new_completed_rows(current_completed_rows, _previous_completed_rows)

                if new_completed_rows:
                    print(f"Found {len(new_completed_rows)} newly completed row(s)")

                    # Process each new completed row
                    for row in new_completed_rows:
                        try:
                            process_completed_row(row)
                        except Exception as e:
                            print(f"ERROR: Critical error processing row {row['row_number']}: {e}")
                            # Color the row red for critical errors
                            if _sheet_instance:
                                color_row(_sheet_instance, row['row_number'], "error")
                else:
                    if _previous_completed_rows is not None:
                        print("Sheet updated but no new completions found")

                # Update tracking variables
                prev_hash = current_hash
                _previous_completed_rows = current_completed_rows
            else:
                # Print a simple status update every few checks
                current_time = time.strftime('%H:%M:%S')
                print(f"{current_time} - Monitoring... (no changes detected)")

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user")
        return True
    except Exception as e:
        print(f"\nERROR: Monitoring error: {e}")
        return False


def main():
    """Main function"""
    current_month = get_current_month_name()

    print("Google Sheets to SOS Inventory Integration (Sales Order Search + Spreadsheet Item Addition)")
    print("This will monitor your Google Sheet, search SOS Inventory sales orders,")
    print("and add items from the spreadsheet based on item IDs and quantities")
    print("Uses OAuth2 Bearer token authentication with GET → modify → PUT approach")
    print("Handles new rows added dynamically and targets the first worksheet")
    print("Searches for 'Done?' header anywhere in sheet, processes 'Yes' values below it")
    print("Automatically retrieves and applies item pricing from SOS Inventory")

    print_separator("CONFIGURATION")
    print(f"Google Credentials: {GOOGLE_CREDENTIALS_FILE}")
    print(f"Google Sheet ID: {GOOGLE_SHEET_ID}")
    print(f"Target Worksheet: First worksheet (index {WORKSHEET_INDEX})")
    print(f"Monitoring Column: {DONE_COLUMN_NAME} (searches entire sheet for this header)")
    print(f"Search Column: {SEARCH_COLUMN}")
    print(f"Current Month: {current_month}")
    print(f"Item ID Row: {ITEM_ID_ROW + 1} (Row 1)")
    print(f"Item Name Row: {ITEM_NAME_ROW + 1} (Row 2)")
    print(f"Check Interval: {CHECK_INTERVAL} seconds")
    print("Mode: Sales Order Search + Spreadsheet Item Addition via GET/PUT")
    print("Search Pattern: [Column B] + [Current Month]")
    print("Item Action: Add items from spreadsheet based on item IDs and quantities")
    print("Method: GET sales order → modify lines → PUT sales order → verify result")
    print("Authentication: OAuth2 Bearer token with automatic refresh")
    print("API Base URL: https://api.sosinventory.com/api/v2/")
    print("Authorization Header: Bearer {access_token}")
    print("Host Header: api.sosinventory.com")
    print("New Row Handling: Automatically detects and processes new rows added to sheet")
    print("Duplicate Prevention: Uses row signatures to avoid reprocessing")
    print("Header Detection: Searches entire sheet for 'Done?' header, processes rows below it")
    print("Pricing: Automatically retrieves item selling prices from SOS Inventory API")
    print("Color coding:")
    print("  - Light blue: Successful sales order search and item addition")
    print("  - Light red: Errors (SOS API failure, invalid data, no orders found, etc.)")

    # Start monitoring
    success = monitor_sheet()

    if success:
        print("\nIntegration completed successfully")
    else:
        print("\nIntegration failed")


if __name__ == "__main__":
    main()