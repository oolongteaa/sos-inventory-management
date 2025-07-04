#!/usr/bin/env python3
"""
Google Sheets to SOS Inventory Integration
Monitors a Google Sheet for completed rows and tests SOS Inventory API connection
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import hashlib
from SOSInventoryIntegration import SOSInventoryAPI
from SOSInventoryIntegration import SOSAuth

# Google Sheets Configuration
GOOGLE_CREDENTIALS_FILE = "cogent-scion-463416-v2-ae77628bbccc.json"  # Replace with your JSON file path
GOOGLE_SHEET_ID = "15EVhfeVlxirHSYZdtqRjrqmhOFE93pW0EeqQxw4nKDE"  # Replace with your Google Sheet ID

# Monitoring Configuration
CHECK_INTERVAL = 10  # seconds between sheet checks
DONE_COLUMN_NAME = "Done?"  # case-insensitive column name to monitor

# Global state
_sos_access_token = None
_previous_completed_rows = None


def print_separator(title=""):
    print("\n" + "=" * 60)
    if title:
        print(f" {title}")
        print("=" * 60)


def setup_google_sheets():
    """Setup Google Sheets API access"""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_FILE, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(GOOGLE_SHEET_ID).sheet1
        print("SUCCESS: Google Sheets connection established")
        return sheet
    except Exception as e:
        print(f"ERROR: Failed to setup Google Sheets: {e}")
        return None


def setup_sos_inventory():
    """Setup SOS Inventory API access"""
    global _sos_access_token

    print_separator("SOS INVENTORY AUTHENTICATION")
    print("Setting up SOS Inventory API access...")

    if SOSAuth.authenticate():
        _sos_access_token = SOSAuth.get_access_token()
        print("SUCCESS: SOS Inventory authentication successful!")

        # Test the connection
        success, result = SOSInventoryAPI.test_connection(_sos_access_token)
        if success:
            print("SUCCESS: SOS Inventory API connection verified")
            return True
        else:
            print(f"ERROR: SOS Inventory API test failed: {result}")
            return False
    else:
        print("ERROR: SOS Inventory authentication failed")
        return False


def fetch_sheet_data(sheet):
    """Fetch all sheet data and compute a hash for change detection"""
    try:
        data = sheet.get_all_values()
        flat_data = "".join([",".join(row) for row in data])
        data_hash = hashlib.md5(flat_data.encode()).hexdigest()
        return data_hash, data
    except Exception as e:
        print(f"ERROR: Error fetching sheet data: {e}")
        return None, None


def filter_completed_rows(data):
    """Filter rows where the 'Done?' column is 'Yes'"""
    if not data:
        return []

    # Find the "Done?" column index
    header_row = data[0]
    done_column_index = None

    for i, header in enumerate(header_row):
        if header.strip().lower() == DONE_COLUMN_NAME.lower():
            done_column_index = i
            break

    if done_column_index is None:
        print(f"WARNING: '{DONE_COLUMN_NAME}' column not found in the sheet")
        return []

    # Filter rows where "Done?" is "Yes" (case-insensitive)
    completed_rows = []
    for row_index, row in enumerate(data):
        if row_index == 0:  # Skip header row
            continue

        # Check if the row has enough columns and if "Done?" is "Yes"
        if (len(row) > done_column_index and
                row[done_column_index].strip().lower() == "yes"):
            # Add row number for reference
            row_with_meta = {
                'row_number': row_index + 1,
                'data': row,
                'headers': header_row
            }
            completed_rows.append(row_with_meta)

    return completed_rows


def get_new_completed_rows(current_completed, previous_completed):
    """Compare completed rows to find new ones"""
    if previous_completed is None:
        return []  # Don't process all rows on first run

    # Convert to sets for comparison (using just the data part)
    current_set = set(tuple(row['data']) for row in current_completed)
    previous_set = set(tuple(row['data']) for row in previous_completed)

    # Find rows that are in current but not in previous
    new_data_tuples = current_set - previous_set

    # Get the full row objects for new rows
    new_rows = []
    for row in current_completed:
        if tuple(row['data']) in new_data_tuples:
            new_rows.append(row)

    return new_rows


def process_completed_row(row_data):
    """Process a newly completed row by testing SOS Inventory API"""
    print(f"\nProcessing Row {row_data['row_number']}:")

    # Display the row data
    headers = row_data['headers']
    data = row_data['data']

    print("Row contents:")
    for i, (header, value) in enumerate(zip(headers, data)):
        if value.strip():  # Only show non-empty values
            print(f"  {header}: {value}")

    # Test SOS Inventory API connection and get some data
    success = test_sos_inventory_for_row(row_data)

    if success:
        print(f"SUCCESS: SOS Inventory API test successful for row {row_data['row_number']}")
    else:
        print(f"ERROR: SOS Inventory API test failed for row {row_data['row_number']}")

    return success


def test_sos_inventory_for_row(row_data):
    """Test SOS Inventory API by calling various GET endpoints"""
    print(f"Testing SOS Inventory API for row {row_data['row_number']}...")

    try:
        # Test API with Get items (limit to 3 for testing)
        print("  Testing get_items()...")
        success, items = SOSInventoryAPI.get_items(_sos_access_token, params={"limit": 3})
        if success:
            item_count = len(items.get('data', []))
            print(f"    SUCCESS: Retrieved {item_count} items")
            if item_count > 0:
                # Show first item as example
                first_item = items.get('data', [])[0]
                print(f"    Sample item: {first_item.get('name', 'Unknown')} (ID: {first_item.get('id', 'N/A')})")
        else:
            print(f"    ERROR: Failed to get items: {items}")
            return False

        return True

    except Exception as e:
        print(f"  ERROR: Error testing SOS Inventory API: {e}")
        return False


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

    print(f"\nStarting sheet monitoring...")
    print(f"Sheet ID: {GOOGLE_SHEET_ID}")
    print(f"Check interval: {CHECK_INTERVAL} seconds")
    print(f"Monitoring column: '{DONE_COLUMN_NAME}'")
    print("Will test SOS Inventory API when new rows are completed")
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
                            print(f"ERROR: Error processing row {row['row_number']}: {e}")
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
    print("Google Sheets to SOS Inventory Integration (Read-Only Testing)")
    print("This will monitor your Google Sheet and test SOS Inventory API when rows are completed")

    print_separator("CONFIGURATION")
    print(f"Google Credentials: {GOOGLE_CREDENTIALS_FILE}")
    print(f"Google Sheet ID: {GOOGLE_SHEET_ID}")
    print(f"Monitoring Column: {DONE_COLUMN_NAME}")
    print(f"Check Interval: {CHECK_INTERVAL} seconds")
    print("Mode: READ-ONLY (testing API connections only)")

    # Start monitoring
    success = monitor_sheet()

    if success:
        print("\nIntegration completed successfully")
    else:
        print("\nIntegration failed")


if __name__ == "__main__":
    main()