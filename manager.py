#!/usr/bin/env python3
"""
Google Sheets to SOS Inventory Integration - Multi-Sheet Version

Monitors one or more Google Sheets for completed rows, searches SOS Inventory
sales orders using Column B value + month from Column A date (e.g., "HA 101 July"),
and adds items from the spreadsheet based on item IDs in row 1 and quantities in
the processed row.

Key changes vs original:
- Support for multiple sheets via SHEET_CONFIGS.
- Per-sheet context (SheetContext) to avoid global sheet state.
- One monitoring thread per sheet (monitor_single_sheet).
"""

import gspread
from google.oauth2.service_account import Credentials
import time
import hashlib
from datetime import datetime
import threading

from sos_inventory_integration import sos_api
from sos_inventory_integration import sos_auth

# ---------------------------------------------------------------------------
# Google Sheets Configuration
# ---------------------------------------------------------------------------

GOOGLE_CREDENTIALS_FILE = "cogent-scion-463416-v2-ae77628bbccc.json"  # Path to your JSON credentials

# List of sheets to monitor. Add as many as you need.
SHEET_CONFIGS = [
    {
        "id": "1yTdPlHPi8Xa1tADpJFJZa6-1ZFnFBMydp3AdnGgKEZs",
        "worksheet_index": 0,  # 0-based index of the worksheet
        "name": "Chateau",     # Optional name for logs
        "check_interval": 10,  # Seconds between checks for this sheet
    },
    # Example of a second sheet:
    {
    "id": "1mMKAiu8kf-z-j4bgeZ1ptSeFObsljGhKjJqjfGB7XhU",
    "worksheet_index": 0,  # 0-based index of the worksheet
    "name": "Vero",  # Optional name for logs
    "check_interval": 10,  # Seconds between checks for this sheet
    },
]

# Monitoring Configuration (defaults, can be overridden per sheet via "check_interval")
CHECK_INTERVAL = 10  # seconds between sheet checks
DONE_COLUMN_NAME = "Done?"  # case-insensitive column name to monitor
SEARCH_COLUMN = "C"  # Column C contains the search string for sales orders
DATE_COLUMN = "B"  # Column B contains the creation date

# Row configuration for spreadsheet structure
ITEM_ID_ROW = 0   # Row 1 (0-indexed) contains item IDs
ITEM_NAME_ROW = 1 # Row 2 (0-indexed) contains item names

# Color Configuration
SUCCESS_COLOR = {
    "red": 0.8,   # Light blue RGB values (normalized 0-1)
    "green": 0.9,
    "blue": 1.0
}

ERROR_COLOR = {
    "red": 1.0,   # Light red RGB values (normalized 0-1)
    "green": 0.8,
    "blue": 0.8
}

# ---------------------------------------------------------------------------
# Global SOS Inventory state (shared across sheets)
# ---------------------------------------------------------------------------
_sos_access_token = None


# ---------------------------------------------------------------------------
# Per-sheet context
# ---------------------------------------------------------------------------

class SheetContext:
    """Holds all per-sheet state needed for monitoring and processing."""
    def __init__(self, sheet, config):
        self.sheet = sheet
        self.config = config
        self.previous_completed_rows = None
        self.sheet_data_cache = None
        self.processed_rows_cache = set()
        self.done_column_info = None
        self.prev_hash = None


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def print_separator(title=""):
    print("\n" + "=" * 60)
    if title:
        print(f" {title}")
        print("=" * 60)


def parse_month_from_date(date_string):
    """
    Parse month name from a date string in column A

    Parameters:
    - date_string: Date string from column A

    Returns:
    - Month name (e.g., "July", "September") or None if parsing fails
    """
    if not date_string or date_string.strip() == "":
        print(f"    [DEBUG] Empty date string provided")
        return None

    try:
        date_str = str(date_string).strip()
        print(f"    [DEBUG] Parsing month from date: '{date_str}'")

        # Remove any time portion if present
        date_part = date_str.split(' ')[0] if ' ' in date_str else date_str

        # Try different date formats
        date_formats = [
            "%Y-%m-%d",  # 2024-01-15
            "%m/%d/%Y",  # 1/15/2024 or 01/15/2024
            "%d/%m/%Y",  # 15/1/2024 or 15/01/2024
            "%m-%d-%Y",  # 1-15-2024 or 01-15-2024
            "%d-%m-%Y",  # 15-1-2024 or 15-01-2024
            "%Y/%m/%d",  # 2024/01/15
            "%d/%m/%y",  # 15/1/24 or 15/01/24
            "%m/%d/%y",  # 1/15/24 or 01/15/24
        ]

        parsed_date = None
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_part, fmt)
                print(f"    [DEBUG] Successfully parsed with format '{fmt}': {parsed_date}")
                break
            except ValueError:
                continue

        if parsed_date:
            month_name = parsed_date.strftime("%B")  # Full month name (e.g., "July")
            print(f"    [DEBUG] Extracted month: '{month_name}'")
            return month_name
        else:
            print(f"    [DEBUG] Could not parse date '{date_str}' with any known format")
            return None

    except Exception as e:
        print(f"    [DEBUG] Error parsing date '{date_string}': {e}")
        return None


def build_search_string(column_b_value, column_a_date):
    """
    Build search string from column B value + month from column A date.
    Generates both full month and abbreviated month (e.g., "October" and "Oct").
    """

    if not column_b_value:
        print("    [DEBUG] No column B value provided")
        return None, None

    # Extract month from column A date
    month_name = parse_month_from_date(column_a_date)
    if not month_name:
        print(f"    [DEBUG] Could not extract month from column A date: '{column_a_date}'")
        return None, None

    # Get abbreviated month (first 3 letters, capitalized correctly)
    month_abbrev = month_name[:3] if len(month_name) >= 3 else month_name
    month_abbrev = month_abbrev.title()  # e.g., "Oct"

    search_string = column_b_value.strip()
    print(f"    [DEBUG] Built search components - Base: '{search_string}', Full month: '{month_name}', Short month: '{month_abbrev}'")

    # Return both full and abbreviated month names
    return search_string, (month_name, month_abbrev)


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


# ---------------------------------------------------------------------------
# Google Sheets setup and per-sheet operations
# ---------------------------------------------------------------------------

def setup_google_sheets(sheet_id, worksheet_index):
    """Setup Google Sheets API access and target a given worksheet"""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_FILE, scopes=scope)
        client = gspread.authorize(creds)

        # Open the spreadsheet by key
        spreadsheet = client.open_by_key(sheet_id)

        # Get all worksheets to show available options
        worksheets = spreadsheet.worksheets()
        print(f"Found {len(worksheets)} worksheet(s) in the spreadsheet {sheet_id}:")
        for i, ws in enumerate(worksheets):
            print(f"  {i}: {ws.title} (ID: {ws.id})")

        # Use the requested worksheet
        if len(worksheets) > worksheet_index:
            sheet = worksheets[worksheet_index]
            print(f"Using worksheet: '{sheet.title}' (index {worksheet_index})")
        else:
            print(f"ERROR: Worksheet index {worksheet_index} not found in spreadsheet {sheet_id}")
            return None

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
        print(f"ERROR: Failed to setup Google Sheets for {sheet_id}: {e}")
        return None


def fetch_sheet_data(ctx: SheetContext):
    """Fetch all sheet data and compute a hash for change detection"""
    try:
        data = ctx.sheet.get_all_values()
        ctx.sheet_data_cache = data  # Cache the data for item extraction

        # Create hash for change detection - include timestamp for better detection
        timestamp = str(int(time.time() * 1000))  # milliseconds
        flat_data = "".join([",".join(row) for row in data]) + timestamp
        data_hash = hashlib.md5(flat_data.encode()).hexdigest()

        sheet_name = ctx.config.get('name', ctx.config['id'])
        print(f"[{sheet_name}] [DEBUG] Fetched {len(data)} rows from sheet")
        return data_hash, data
    except Exception as e:
        sheet_name = ctx.config.get('name', ctx.config['id'])
        print(f"[{sheet_name}] ERROR: Error fetching sheet data: {e}")
        return None, None


def find_done_column(ctx: SheetContext, data):
    """
    Find the 'Done?' column in any row of the sheet

    Parameters:
    - ctx: SheetContext
    - data: All sheet data (list of lists)

    Returns:
    - Dictionary with 'row', 'column', and 'found' keys
    """
    if not data:
        return {'found': False, 'row': None, 'column': None}

    print("    [DEBUG] Searching for 'Done?' column...")

    for row_index, row in enumerate(data):
        for col_index, cell_value in enumerate(row):
            if cell_value and cell_value.strip().lower() == DONE_COLUMN_NAME.lower():
                ctx.done_column_info = {
                    'found': True,
                    'row': row_index,
                    'column': col_index,
                    'row_number': row_index + 1,
                    'column_letter': chr(65 + col_index)
                }
                print(
                    f"    [DEBUG] Found 'Done?' in row {row_index + 1}, column {chr(65 + col_index)} (index {col_index})"
                )
                return ctx.done_column_info

    print(f"    [DEBUG] 'Done?' column not found in sheet")
    return {'found': False, 'row': None, 'column': None}


def filter_completed_rows(ctx: SheetContext, data):
    """Filter rows where the 'Done?' column is 'Yes'"""
    if not data:
        return []

    done_info = find_done_column(ctx, data)
    if not done_info['found']:
        print(f"WARNING: '{DONE_COLUMN_NAME}' column not found in the sheet")
        return []

    done_column_index = done_info['column']
    done_row_index = done_info['row']

    print(f"    [DEBUG] Found 'Done?' at row {done_row_index + 1}, column {chr(65 + done_column_index)}")
    print(
        f"    [DEBUG] Looking for 'Yes' values in column {chr(65 + done_column_index)} below row {done_row_index + 1}")

    completed_rows = []
    for row_index, row in enumerate(data):
        # Skip rows at or above the "Done?" header row
        if row_index <= done_row_index:
            continue

        if (len(row) > done_column_index and row[done_column_index].strip().lower() == "yes"):
            row_with_meta = {
                'row_number': row_index + 1,
                'data': row,
                'headers': data[0] if len(data) > 0 else [],
                'done_column_index': done_column_index
            }
            completed_rows.append(row_with_meta)
            print(f"    [DEBUG] Found completed row {row_index + 1}: Done? = '{row[done_column_index]}'")

    print(f"    [DEBUG] Found {len(completed_rows)} completed rows total")
    return completed_rows


def create_row_signature(row_data):
    """Create a unique signature for a row to track if it's been processed"""
    try:
        data = row_data.get('data', [])
        row_num = row_data.get('row_number', 0)

        signature_parts = [str(row_num)]

        # Add first 5 columns to signature (adjust as needed)
        for i in range(min(5, len(data))):
            signature_parts.append(str(data[i]).strip())

        signature = "|".join(signature_parts)
        return hashlib.md5(signature.encode()).hexdigest()
    except Exception as e:
        print(f"ERROR: Could not create row signature: {e}")
        return None


def get_new_completed_rows(ctx: SheetContext, current_completed):
    """Compare completed rows to find new ones, avoiding duplicates"""
    if ctx.previous_completed_rows is None:
        # On first run, don't process existing completed rows
        print("    [DEBUG] First run - not processing existing completed rows")
        return []

    new_rows = []

    for row in current_completed:
        signature = create_row_signature(row)
        if not signature:
            continue

        if signature in ctx.processed_rows_cache:
            print(f"    [DEBUG] Row {row['row_number']} already processed (cached)")
            continue

        row_was_previously_completed = False
        if ctx.previous_completed_rows:
            for prev_row in ctx.previous_completed_rows:
                prev_signature = create_row_signature(prev_row)
                if prev_signature == signature:
                    row_was_previously_completed = True
                    break

        if not row_was_previously_completed:
            new_rows.append(row)
            ctx.processed_rows_cache.add(signature)
            print(f"    [DEBUG] New completed row detected: {row['row_number']}")

    return new_rows


def get_column_b_value(row_data):
    """Extract the value from column C (index 2)"""
    try:
        data = row_data.get('data', [])
        if len(data) > 2:
            return data[2].strip()
        else:
            print(f"WARNING: Row {row_data['row_number']} doesn't have a column C value")
            return None
    except Exception as e:
        print(f"ERROR: Error extracting column C value from row {row_data['row_number']}: {e}")
        return None


def get_column_a_value(row_data):
    """Extract the value from column B (index 1)"""
    try:
        data = row_data.get('data', [])
        if len(data) > 1:
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
        data = row_data.get('data', [])

        if not data:
            print(f"ERROR: No data found for row {row_data['row_number']}")
            return False

        if len(data) < 3:
            print(f"ERROR: Row {row_data['row_number']} doesn't have enough columns (need at least B and C)")
            return False

        column_a_value = get_column_a_value(row_data)
        if not column_a_value:
            print(f"ERROR: Row {row_data['row_number']} has empty or invalid column B value (date)")
            return False

        column_b_value = get_column_b_value(row_data)
        if not column_b_value:
            print(f"ERROR: Row {row_data['row_number']} has empty or invalid column C value")
            return False

        return True

    except Exception as e:
        print(f"ERROR: Error validating row data for row {row_data.get('row_number', 'unknown')}: {e}")
        return False


def color_row(sheet, row_number, color_type="success"):
    """Color the entire row - blue for success, red for error"""
    try:
        all_values = sheet.get_all_values()
        if not all_values:
            print(f"WARNING: Could not determine sheet dimensions for row {row_number}")
            return False

        num_columns = 0
        for row in all_values:
            if len(row) > num_columns:
                num_columns = len(row)

        if num_columns == 0:
            num_columns = 50  # Default

        def num_to_col_letters(n):
            result = ""
            while n > 0:
                n -= 1
                result = chr(65 + (n % 26)) + result
                n //= 26
            return result

        end_column = num_to_col_letters(num_columns)
        range_name = f"A{row_number}:{end_column}{row_number}"

        if color_type == "error":
            color = ERROR_COLOR
            color_name = "light red"
        else:
            color = SUCCESS_COLOR
            color_name = "light blue"

        print(f"Coloring range: {range_name} ({color_name})")

        format_request = {
            "backgroundColor": color
        }

        sheet.format(range_name, format_request)
        print(f"SUCCESS: Colored row {row_number} {color_name} (columns A to {end_column})")
        return True

    except Exception as e:
        print(f"ERROR: Failed to color row {row_number}: {e}")
        return False


# ---------------------------------------------------------------------------
# Item extraction and SOS Inventory API integration
# ---------------------------------------------------------------------------

def extract_items_from_sheet_data(sheet_data, row_data):
    """
    Extract items and quantities from sheet data based on the processed row

    Parameters:
    - sheet_data: All sheet data (list of lists)
    - row_data: The specific row being processed

    Returns:
    - List of dictionaries with item_id, quantity, name, force_new_line flag, and row_date
    """
    try:
        if not sheet_data or len(sheet_data) < 3:
            print("ERROR: Sheet data insufficient - need at least 3 rows")
            return []

        item_ids = sheet_data[ITEM_ID_ROW] if len(sheet_data) > ITEM_ID_ROW else []
        item_names = sheet_data[ITEM_NAME_ROW] if len(sheet_data) > ITEM_NAME_ROW else []
        quantities = row_data.get('data', [])

        row_date = None
        if len(quantities) > 1 and quantities[1]:
            row_date_str = str(quantities[1]).strip()
            try:
                from datetime import datetime

                date_part = row_date_str.split(' ')[0] if ' ' in row_date_str else row_date_str

                date_formats = [
                    "%Y-%m-%d",
                    "%m/%d/%Y",
                    "%d/%m/%Y",
                    "%m-%d-%Y",
                    "%d-%m-%Y",
                ]

                parsed_date = None
                for fmt in date_formats:
                    try:
                        parsed_date = datetime.strptime(date_part, fmt)
                        break
                    except ValueError:
                        continue

                if parsed_date:
                    row_date = parsed_date.strftime("%Y-%m-%d")
                    print(f"    [DEBUG] Parsed row date: {row_date_str} -> {row_date}")
                else:
                    print(f"    [DEBUG] Could not parse date '{row_date_str}', will use current date")

            except Exception as e:
                print(f"    [DEBUG] Error parsing date '{row_date_str}': {e}, will use current date")

        print(f"    [DEBUG] Processing items from sheet:")
        print(f"      Item IDs row: {len(item_ids)} columns")
        print(f"      Item names row: {len(item_names)} columns")
        print(f"      Current row data: {len(quantities)} columns")
        print(f"      Row date: {row_date if row_date else 'current date'}")

        items_to_add = []

        # Start from column index that corresponds to inventory items
        start_column = 3  # Adjust based on your sheet structure

        for col_index in range(start_column, len(quantities)):
            quantity_value = quantities[col_index] if col_index < len(quantities) else None

            if not quantity_value or quantity_value == '' or quantity_value == 0:
                continue

            try:
                quantity = float(quantity_value)
                if quantity <= 0:
                    continue
            except (ValueError, TypeError):
                print(f"      Warning: Invalid quantity '{quantity_value}' in column {col_index}")
                continue

            item_id = item_ids[col_index] if col_index < len(item_ids) else None
            item_name = item_names[col_index] if col_index < len(item_names) else None

            if not item_id or item_id == '':
                continue

            item_id_clean = str(item_id).strip()

            if item_id_clean == "0":
                print(f"      Skipping item with ID '0' in column {col_index}")
                continue

            item_name_clean = str(item_name).strip() if item_name else f"Item {item_id_clean}"

            # Always force new line
            force_new_line = True

            if _sos_access_token:
                price_success, item_details = sos_api.get_item_price_and_details(item_id_clean, _sos_access_token)
                if price_success:
                    sales_price = item_details.get("price", 0.0)
                    print(
                        f"      Item {item_name_clean} (ID: {item_id_clean}) has sales price ${sales_price} - will create new line")
                else:
                    print(f"      Warning: Could not check sales price for item {item_id_clean}: {item_details}")

            items_to_add.append({
                "item_id": item_id_clean,
                "quantity": quantity,
                "name": item_name_clean,
                "column": col_index,
                "force_new_line": force_new_line,
                "row_date": row_date
            })

            print(f"      Found: {item_name_clean} (ID: {item_id_clean}) - Qty: {quantity} [NEW LINE]" +
                  (f" [Date: {row_date}]" if row_date else " [Date: current]"))

        print(f"    [DEBUG] Total items to add: {len(items_to_add)}")
        return items_to_add

    except Exception as e:
        print(f"ERROR: Exception extracting items from sheet: {str(e)}")
        return []


def add_items_to_sales_order(sales_order_id, items_to_add):
    """
    Add items to a sales order in SOS Inventory - ALWAYS creates new lines

    Parameters:
    - sales_order_id: The ID of the sales order
    - items_to_add: List of items with item_id, quantity, name, force_new_line flag, and row_date

    Returns:
    - Tuple: (success, message)
    """
    try:
        successful_additions = []
        failed_additions = []

        for item in items_to_add:
            item_id = item["item_id"]
            quantity = item["quantity"]
            item_name = item["name"]
            row_date = item.get("row_date")

            print(f"        Adding item as NEW LINE: {item_name} (ID: {item_id}) x {quantity}" +
                  (f" [Date: {row_date}]" if row_date else " [Date: current]"))

            success, result = sos_api.add_item_to_sales_order(
                sales_order_id,
                item_id,
                quantity,
                _sos_access_token,
                force_new_line=True,
                line_date=row_date
            )

            if success:
                successful_additions.append(f"{item_name} x{quantity}")
                print(f"          SUCCESS: Added {item_name} x{quantity} as NEW LINE")
            else:
                failed_additions.append(f"{item_name} x{quantity}: {result}")
                print(f"          ERROR: Failed to add {item_name} x{quantity} - {result}")

        if successful_additions:
            success_msg = f"Added {len(successful_additions)} items as NEW LINES: {', '.join(successful_additions)}"
            if failed_additions:
                fail_msg = f"Failed to add {len(failed_additions)} items: {', '.join(failed_additions)}"
                return True, f"{success_msg}. {fail_msg}"
            else:
                return True, success_msg
        else:
            fail_msg = f"Failed to add all {len(failed_additions)} items: {', '.join(failed_additions)}"
            return False, fail_msg

    except Exception as e:
        return False, f"Exception adding items to sales order: {str(e)}"


def search_and_update_sales_orders(row_data, search_string, month_tuple, items_to_add):
    """
    Search SOS Inventory sales orders and add items from spreadsheet to found orders.
    Supports both full and abbreviated month names (e.g., 'October' and 'Oct').
    """
    print(f"Searching SOS Inventory sales orders for row {row_data['row_number']}...")

    try:
        if not ensure_valid_sos_token():
            print("  ERROR: Failed to obtain valid SOS Inventory access token")
            return False, "No access token"

        full_month, short_month = month_tuple

        print(f"  Items to add as NEW LINES: {len(items_to_add)}")
        for item in items_to_add:
            print(f"    - {item['name']} (ID: {item['item_id']}) x {item['quantity']} [NEW LINE]")

        search_patterns = [
            f"{search_string} {full_month}",
            f"{search_string}  {full_month}",
            f"{search_string} {short_month}",
            f"{search_string}  {short_month}"
        ]

        all_orders = []
        orders_found_by_pattern = {}

        for i, pattern in enumerate(search_patterns):
            print(f"  Trying pattern {i + 1}: '{pattern}'...")

            success, result = sos_api.search_sales_orders_by_query(
                pattern,
                _sos_access_token,
                additional_params={"maxresults": 50}
            )

            if success:
                parsed_data = sos_api.parse_sales_order_response(result)
                if parsed_data:
                    orders = parsed_data["orders"]
                    orders_found_by_pattern[pattern] = len(orders)
                    print(f"    Found {len(orders)} orders with pattern '{pattern}'")

                    for order in orders:
                        if not any(existing_order.get("id") == order.get("id") for existing_order in all_orders):
                            all_orders.append(order)
                else:
                    orders_found_by_pattern[pattern] = 0
                    print(f"    No valid response for pattern '{pattern}'")
            else:
                orders_found_by_pattern[pattern] = 0
                print(f"    Search failed for pattern '{pattern}': {result}")

        total_count = len(all_orders)
        print(f"    Total unique orders found: {total_count}")

        for pattern, count in orders_found_by_pattern.items():
            if count > 0:
                print(f"      '{pattern}': {count} orders")

        if not all_orders:
            print(f"    No sales orders found with any month variant (full or short)")
            return False, "No sales orders found"

        print("    Matching sales orders:")
        for i, order in enumerate(all_orders[:10]):
            summary = sos_api.format_sales_order_summary(order)
            print(f"      {i + 1}. {summary}")

        if len(all_orders) > 10:
            print(f"      ... and {len(all_orders) - 10} more orders")

        first_order = all_orders[0]
        order_id = first_order.get("id")
        order_number = first_order.get("number", "Unknown")

        if not order_id:
            print(f"      First order ({order_number}): No ID found")
            return False, "First order has no ID"

        print(f"\n    Processing first order only: {order_number} (ID: {order_id})...")
        print(f"    Adding {len(items_to_add)} items to this sales order as NEW LINES...")

        item_success, item_message = add_items_to_sales_order(order_id, items_to_add)

        if item_success:
            print(f"        SUCCESS: {item_message}")
            return True, item_message
        else:
            print(f"        ERROR: {item_message}")
            return False, f"Failed to update order {order_number}: {item_message}"

    except Exception as e:
        print(f"  ERROR: Error searching/updating sales orders: {e}")
        return False, f"Exception: {str(e)}"


def process_completed_row(ctx: SheetContext, row_data):
    """Process a newly completed row — searches SOS orders and adds items."""
    sheet_name = ctx.config.get('name', ctx.config['id'])
    print(f"\n[{sheet_name}] Processing Row {row_data['row_number']}:")

    try:
        if not validate_row_data(row_data):
            raise ValueError("Invalid row data (missing or bad columns)")

        column_a_date = get_column_a_value(row_data)
        column_b_value = get_column_b_value(row_data)

        print(f"  Column B (date): {column_a_date}")
        print(f"  Column C (property/ID): {column_b_value}")

        search_components = build_search_string(column_b_value, column_a_date)
        if not search_components or not search_components[0] or not search_components[1]:
            raise ValueError("Could not build search string (missing column values)")

        search_string, month_tuple = search_components
        full_month, short_month = month_tuple
        print(f"  Month variants: {full_month} / {short_month}")
        print(f"  Search patterns will include both variants.")

        if not ctx.sheet_data_cache:
            raise ValueError("No sheet data cache available")

        items_to_add = extract_items_from_sheet_data(ctx.sheet_data_cache, row_data)
        if not items_to_add:
            print("  No items found to add — marking as successful.")
            color_row(ctx.sheet, row_data['row_number'], "success")
            return True

        success, message = search_and_update_sales_orders(
            row_data,
            search_string,
            month_tuple,
            items_to_add
        )

        if success:
            print(f"SUCCESS: Row {row_data['row_number']}: {message}")
            color_row(ctx.sheet, row_data['row_number'], "success")
        else:
            print(f"ERROR: Row {row_data['row_number']}: {message}")
            color_row(ctx.sheet, row_data['row_number'], "error")

        return success

    except Exception as e:
        print(f"ERROR: Exception while processing row {row_data.get('row_number','?')}: {e}")
        color_row(ctx.sheet, row_data['row_number'], "error")
        return False


# ---------------------------------------------------------------------------
# SOS Inventory setup
# ---------------------------------------------------------------------------

def setup_sos_inventory():
    """Setup SOS Inventory API access"""
    global _sos_access_token

    print_separator("SOS INVENTORY AUTHENTICATION")
    print("Setting up SOS Inventory API access...")

    if not ensure_valid_sos_token():
        print("ERROR: SOS Inventory authentication failed")
        return False

    print("SUCCESS: SOS Inventory authentication successful!")

    success, result = sos_api.test_connection(_sos_access_token)
    if success:
        print("SUCCESS: SOS Inventory API connection verified")
        return True
    else:
        print(f"ERROR: SOS Inventory API test failed: {result}")
        return False


# ---------------------------------------------------------------------------
# Monitoring loops
# ---------------------------------------------------------------------------

def monitor_single_sheet(ctx: SheetContext):
    """Main monitoring loop for a single sheet."""
    sheet_name = ctx.config.get('name', ctx.config['id'])
    interval = ctx.config.get('check_interval', CHECK_INTERVAL)

    print_separator(f"SHEET MONITORING - {sheet_name}")

    print(f"Starting sheet monitoring for: {sheet_name}")
    print(f"Sheet ID: {ctx.config['id']}")
    print(f"Worksheet: '{ctx.sheet.title}' (index {ctx.config['worksheet_index']})")
    print(f"Check interval: {interval} seconds")
    print(f"Monitoring column: '{DONE_COLUMN_NAME}' (search entire sheet for this header)")
    print(f"Search column: {SEARCH_COLUMN}")
    print(f"Date column: {DATE_COLUMN}")
    print(f"Search format: '[Column B Value] [Month from Column A Date]' (e.g., 'HA 101 September')")
    print("Item processing: Extract from spreadsheet using item IDs in row 1")
    print("IMPORTANT: ALWAYS creates NEW LINES - never updates existing item quantities")
    print("Press Ctrl+C in main terminal to stop all monitoring")

    try:
        while True:
            current_hash, data = fetch_sheet_data(ctx)

            if current_hash is None:
                print("ERROR: Failed to fetch sheet data, retrying...")
                time.sleep(interval)
                continue

            if current_hash != ctx.prev_hash:
                print(f"\n[{sheet_name}] Sheet updated! ({time.strftime('%Y-%m-%d %H:%M:%S')})")

                current_completed_rows = filter_completed_rows(ctx, data)
                new_completed_rows = get_new_completed_rows(ctx, current_completed_rows)

                if new_completed_rows:
                    print(f"[{sheet_name}] Found {len(new_completed_rows)} newly completed row(s)")
                    for row in new_completed_rows:
                        try:
                            process_completed_row(ctx, row)
                        except Exception as e:
                            print(f"[{sheet_name}] ERROR: Critical error processing row {row['row_number']}: {e}")
                            color_row(ctx.sheet, row['row_number'], "error")
                else:
                    if ctx.previous_completed_rows is not None:
                        print(f"[{sheet_name}] Sheet updated but no new completions found")

                ctx.prev_hash = current_hash
                ctx.previous_completed_rows = current_completed_rows
            else:
                current_time = time.strftime('%H:%M:%S')
                print(f"[{sheet_name}] {current_time} - Monitoring... (no changes detected)")

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n\n[{sheet_name}] Monitoring stopped by user")
        return True
    except Exception as e:
        print(f"\n[{sheet_name}] ERROR: Monitoring error: {e}")
        return False


def monitor_all_sheets():
    """Setup and start monitoring for all sheets in SHEET_CONFIGS."""
    print_separator("GLOBAL SETUP")

    # Setup SOS once (shared)
    if not setup_sos_inventory():
        print("ERROR: SOS Inventory authentication failed; aborting.")
        return False

    threads = []

    for cfg in SHEET_CONFIGS:
        sheet = setup_google_sheets(cfg["id"], cfg["worksheet_index"])
        if not sheet:
            print(f"ERROR: Could not set up Google Sheet {cfg['id']}")
            continue

        ctx = SheetContext(sheet, cfg)
        t = threading.Thread(target=monitor_single_sheet, args=(ctx,), daemon=True)
        t.start()
        threads.append(t)

        print(f"Started monitoring thread for sheet {cfg.get('name', cfg['id'])}")

    if not threads:
        print("No sheets started; exiting.")
        return False

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping all sheet monitors...")
        return True


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def main():
    """Main entry point"""
    print("Google Sheets to SOS Inventory Integration (Sales Order Search + Spreadsheet Item Addition)")
    print("Multi-Sheet Monitoring Version")
    print_separator("CONFIGURATION")
    print(f"Google Credentials: {GOOGLE_CREDENTIALS_FILE}")
    print("Sheets to monitor:")
    for cfg in SHEET_CONFIGS:
        print(f"  - ID: {cfg['id']}, worksheet index: {cfg['worksheet_index']}, name: {cfg.get('name', '(unnamed)')}")
    print(f"Monitoring Column: {DONE_COLUMN_NAME} (searches entire sheet for this header)")
    print(f"Search Column: {SEARCH_COLUMN}")
    print(f"Date Column: {DATE_COLUMN} (used to extract month for sales order search)")
    print(f"Item ID Row: {ITEM_ID_ROW + 1} (Row 1)")
    print(f"Item Name Row: {ITEM_NAME_ROW + 1} (Row 2)")
    print(f"Default Check Interval: {CHECK_INTERVAL} seconds (can be overridden per sheet)")
    print("IMPORTANT: ALWAYS creates NEW LINES - preserves existing items and dates")

    success = monitor_all_sheets()

    if success:
        print("\nIntegration monitoring stopped cleanly")
    else:
        print("\nIntegration failed")


if __name__ == "__main__":
    main()