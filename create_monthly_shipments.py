import sys
import time
from datetime import datetime, date, timedelta
import json

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    ZoneInfo = None

# Import your modules
from sos_inventory_integration import sos_auth
from sos_inventory_integration import sos_api


# Tunables
HTTP_RETRIES = 3           # basic retry for transient timeouts
RETRY_BACKOFF_SECONDS = 3  # linear backoff between retries
MAX_SHIPMENTS = 10          # create shipments for first N orders

# Option A configuration (timezone-safe dates)
LOCAL_TZ_NAME = "America/New_York"
SEND_AS_DATETIME = True  # send "YYYY-MM-DDT12:00:00-04:00" if True (recommended)

# Number formatting options
PREFIX = "* "               # e.g., "SHIP-" or "FULF "
MAX_NUMBER_LEN = 21       # hard cap for the final number string (including prefix)

# Month name and common abbreviations we will search for in the Sales Order number
MONTH_MATCHES = {
    1:  ["january", "jan"],
    2:  ["february", "feb"],
    3:  ["march", "mar"],
    4:  ["april", "apr"],
    5:  ["may"],
    6:  ["june", "jun"],
    7:  ["july", "jul"],
    8:  ["august", "aug"],
    9:  ["september", "sept", "sep"],
    10: ["october", "oct"],
    11: ["november", "nov"],
    12: ["december", "dec"],
}

# Abbreviated month names for constructing the next SO number
MONTH_ABBR = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}


def month_date_range(year: int, month: int):
    """Return start/end ISO strings for the given month: YYYY-MM-DDT00:00:00 and YYYY-MM-DDT23:59:59."""
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    start_str = start.strftime("%Y-%m-%dT00:00:00")
    end_str = end.strftime("%Y-%m-%dT23:59:59")
    return start_str, end_str


def next_month_first_midday_local_iso(year: int, month: int) -> str:
    """
    Return ISO 8601 string for the first day of the next month at 12:00 local time.
    """
    if month == 12:
        nm_year, nm_month = year + 1, 1
    else:
        nm_year, nm_month = year, month + 1

    if SEND_AS_DATETIME and ZoneInfo is not None:
        try:
            tz = ZoneInfo(LOCAL_TZ_NAME)
            dt_local = datetime(nm_year, nm_month, 1, 12, 0, 0, tzinfo=tz)
            return dt_local.isoformat()
        except Exception:
            pass  # fall through to naive

    dt_naive = datetime(nm_year, nm_month, 1, 12, 0, 0)
    return dt_naive.isoformat(timespec="seconds")


def so_number_matches_month(number: str, month: int) -> bool:
    if not number:
        return False
    hay = str(number).lower()
    patterns = MONTH_MATCHES.get(month, [])
    return any(p in hay for p in patterns)


def fetch_sales_orders_for_month(access_token: str, year: int, month: int, page_size: int = 200):
    """
    Fetch all sales orders for the given month by pagination with retries.
    Then filter to those whose 'number' contains the month name or abbreviation.
    Returns a list of order dicts.
    """
    start_dt, end_dt = month_date_range(year, month)
    print(f"[INFO] Fetching sales orders between {start_dt} and {end_dt}")

    orders = []
    start_cursor = 0

    while True:
        params = {
            "start": start_cursor,
            "maxresults": page_size,
            "dateFrom": start_dt,
            "dateTo": end_dt,
        }

        last_err = None
        for attempt in range(1, HTTP_RETRIES + 1):
            success, result = sos_api.get_sales_orders(
                access_token,
                params=params
            )
            if success:
                break
            last_err = result
            print(f"[WARN] get_sales_orders failed (attempt {attempt}/{HTTP_RETRIES}): {result}")
            if attempt < HTTP_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)

        if not success:
            raise RuntimeError(f"Failed to get sales orders after {HTTP_RETRIES} attempts: {last_err}")

        parsed = sos_api.parse_sales_order_response(result)
        if not parsed:
            break

        batch = (parsed.get("orders") or [])
        orders.extend(batch)

        count = parsed.get("count", 0)
        total = parsed.get("total_count", 0)
        print(f"[INFO] Retrieved {len(batch)} orders in this page (count={count}, total={total})")

        if len(batch) < page_size:
            break
        start_cursor += len(batch)

    print(f"[INFO] Total orders collected for month (pre-filter): {len(orders)}")

    filtered = [o for o in orders if so_number_matches_month(o.get("number"), month)]
    print(f"[INFO] Orders after month-name filter: {len(filtered)} "
          f"(patterns: {', '.join(MONTH_MATCHES.get(month, []))})")
    return filtered


def build_lines_for_shipment_from_sales_order(order):
    lines = []
    so_lines = order.get("lines") or []
    for ln in so_lines:
        item = ln.get("item") or {}
        item_id = item.get("id")
        if not item_id:
            continue

        quantity = ln.get("quantity", 0) or 0
        unitprice = ln.get("unitprice", 0) or 0
        amount = ln.get("amount", 0) or 0
        description = ln.get("description")
        duedate = ln.get("duedate")

        lines.append({
            "item_id": item_id,
            "quantity": quantity,
            "unitprice": unitprice,
            "amount": amount,
            "description": description,
            "duedate": duedate,
        })
    return lines


def pick_customer_and_location_from_order(order):
    cust = order.get("customer") or {}
    loc = order.get("location") or {}
    customer_id = cust.get("id")
    location_id = loc.get("id")
    customer_name = cust.get("name")
    location_name = loc.get("name")
    return customer_id, location_id, customer_name, location_name


# Normalize month names first (case-insensitive) to avoid unnecessary truncation
def normalize_month_in_number(s: str) -> str:
    """
    Replace long month names with preferred abbreviations (case-insensitive):
    September -> Sept, October -> Oct, November -> Nov.
    """
    if not s:
        return s
    replacements = [
        ("september", "Sept"),
        ("october", "Oct"),
        ("november", "Nov"),
    ]
    out = s
    lower = out.lower()
    for long_lower, abbr in replacements:
        temp = []
        i = 0
        while i < len(out):
            if lower[i:i+len(long_lower)] == long_lower:
                temp.append(abbr)
                i += len(long_lower)
            else:
                temp.append(out[i])
                i += 1
        out = "".join(temp)
        lower = out.lower()
    return out


def build_shipment_number_from_so(so_number: str, max_len: int = MAX_NUMBER_LEN, prefix: str = PREFIX) -> str:
    """
    Build the shipment number:
    - Normalize month names.
    - Prepend prefix.
    - Enforce max length on the final string.
    - Keep spaces.
    """
    base = (so_number or "").strip()
    base = normalize_month_in_number(base)

    # Combine prefix + base
    combined = f"{prefix}{base}" if prefix else base

    # Enforce max length on full string
    if len(combined) > max_len:
        combined = combined[:max_len]

    return combined


def create_shipment_from_order(order, access_token, target_year: int, target_month: int):
    """
    Create a shipment from a Sales Order.
    Returns (success: bool, result: dict|str) where result is the created shipment on success,
    or an error message on failure.
    """
    number = order.get("number") or order.get("id") or "UNKNOWN"
    print(f"[STEP] Preparing shipment for Sales Order: #{number}")

    # 1) Build lines
    lines = build_lines_for_shipment_from_sales_order(order)
    if not lines:
        return False, f"Sales order #{number} has no item lines."

    # 2) Required IDs
    customer_id, location_id, customer_name, location_name = pick_customer_and_location_from_order(order)
    if not customer_id:
        return False, f"Sales order #{number} is missing customer id."
    if not location_id:
        return False, f"Sales order #{number} is missing location id."

    # 3) Dates
    target_dt_iso = next_month_first_midday_local_iso(target_year, target_month)
    ship_date = target_dt_iso
    ship_by = target_dt_iso

    # 4) Optional fields
    customer_po = order.get("customerPO") or order.get("customerPo") or None
    billing_addr = order.get("billing")
    shipping_addr = order.get("shipping")

    # 5) Shipment number
    shipment_number = build_shipment_number_from_so(
        order.get("number") or order.get("id") or "",
        max_len=MAX_NUMBER_LEN,
        prefix=PREFIX
    )

    # 6) Build payload
    try:
        payload = sos_api.build_fully_filled_shipment(
            number=shipment_number,
            date=ship_date,
            ship_by=ship_by,
            customer_id=customer_id,
            location_id=location_id,
            lines=lines,
            customer_name=customer_name,
            location_name=location_name,
            billing_address=billing_addr,
            shipping_address=shipping_addr,
            customer_po=customer_po,
            header_linked_tx={
                "id": order.get("id"),
                "transactionType": "SalesOrder",
                "refNumber": number
            } if order.get("id") else None,
            customer_message=f"Shipment created from Sales Order #{number}",
            comment="Auto-generated by integration script",
            shipping_amount=0.0,
        )
    except Exception as e:
        return False, f"Failed to build shipment payload for SO #{number}: {e}"

    # 7) Debug: payload
    try:
        print("[DEBUG] ===== SHIPMENT PAYLOAD (PRE-API) =====")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        print("[DEBUG] ===== END SHIPMENT PAYLOAD =====")
    except Exception as e:
        print(f"[DEBUG] Could not pretty-print payload: {e}")

    # 8) Create shipment
    try:
        success, result = sos_api.create_shipment(payload, access_token, sanitize=True)
    except Exception as e:
        return False, f"Exception during shipment creation for SO #{number}: {e}"

    if not success:
        return False, f"API error creating shipment for SO #{number}: {result}"

    # 9) Return the created shipment
    shipment = result if isinstance(result, dict) else {}
    return True, shipment


# --------------------------
# New Sales Order creation
# --------------------------

def _next_month(year: int, month: int):
    if month == 12:
        return (year + 1, 1)
    return (year, month + 1)


def _detect_month_in_text(text: str):
    """
    Return (month_number, start_index, end_index) of the month substring in text, case-insensitive.
    Prefers longer matches (e.g., 'September' over 'Sep'), then first occurrence.
    """
    if not text:
        return None
    low = text.lower()
    # Build candidate patterns with preference: full names first, then 'sept', then 3-letter abbrs, etc.
    patterns = [
        ("september", 9), ("october", 10), ("november", 11), ("december", 12),
        ("january", 1), ("february", 2), ("march", 3), ("april", 4), ("august", 8),
        ("july", 7), ("june", 6), ("may", 5),
        ("sept", 9),  # handle 'Sept' specifically
        ("j an", None)  # placeholder to keep list syntax valid; will be ignored
    ]
    # Construct full search list including standard abbreviations
    patterns = [(p, m) for (p, m) in patterns if m is not None]
    for mnum, abbrs in MONTH_MATCHES.items():
        for p in sorted(set(abbrs), key=lambda s: -len(s)):  # longer first
            patterns.append((p, mnum))

    best = None
    seen_spans = set()
    for pat, mnum in patterns:
        i = low.find(pat)
        if i != -1:
            span = (i, i + len(pat))
            if span in seen_spans:
                continue
            seen_spans.add(span)
            # Prefer longer match by default since we seeded with long names first
            if best is None:
                best = (mnum, i, i + len(pat))
            else:
                # If this match starts earlier, prefer it; else keep first found
                if i < best[1]:
                    best = (mnum, i, i + len(pat))
    return best


def build_next_month_so_number(original_number: str, target_year: int, target_month: int) -> str:
    """
    Replace only the month token in the original SO number with next month's abbreviation.
    If no month token is found, append the next month's abbreviation at the end, separated by a space.
    """
    if not original_number:
        return MONTH_ABBR.get(target_month, "Jan")

    nm_year, nm_month = _next_month(target_year, target_month)
    next_abbr = MONTH_ABBR.get(nm_month, "Jan")

    # Try to find an existing month substring
    found = _detect_month_in_text(original_number)
    if found:
        _mnum, s, e = found
        return f"{original_number[:s]}{next_abbr}{original_number[e:]}"
    # No explicit month present; append
    sep = "" if original_number.endswith(" ") else " "
    return f"{original_number}{sep}{next_abbr}"


def build_sales_order_payload_from_original(original_order: dict, new_number: str, target_year: int, target_month: int):
    """
    Build a minimal Sales Order payload using info from the original order.
    Includes a single line for Toilet Paper Roll (id=2), non-taxable.
    """
    customer = original_order.get("customer") or {}
    location = original_order.get("location") or {}

    customer_id = customer.get("id")
    location_id = location.get("id")

    if not customer_id or not location_id:
        raise ValueError("Original order is missing customer or location id.")

    # Use next month first day for 'date' at 12:00 local (safe for UI)
    nm_year, nm_month = _next_month(target_year, target_month)
    if SEND_AS_DATETIME and ZoneInfo is not None:
        try:
            tz = ZoneInfo(LOCAL_TZ_NAME)
            dt_local = datetime(nm_year, nm_month, 1, 12, 0, 0, tzinfo=tz)
            date_iso = dt_local.isoformat()
        except Exception:
            date_iso = f"{nm_year:04d}-{nm_month:02d}-01T12:00:00"
    else:
        date_iso = f"{nm_year:04d}-{nm_month:02d}-01T12:00:00"

    # Build the minimal header and one line per requirements
    payload = {
        "number": new_number,
        "date": date_iso,
        "customer": {"id": customer_id, **({"name": customer.get("name")} if customer.get("name") else {})},
        "location": {"id": location_id, **({"name": location.get("name")} if location.get("name") else {})},
        "billing": original_order.get("billing") or None,
        "shipping": original_order.get("shipping") or None,
        "customerPO": original_order.get("customerPO") or original_order.get("customerPo") or None,
        "closed": False,
        "archived": False,
        "lines": [
            {
                "lineNumber": 1,
                "item": {"id": 2, "name": "Toilet Paper Roll"},
                "class": None,
                "job": None,
                "workcenter": None,
                "tax": {
                    "taxable": False,
                    "taxCode": None,
                    "taxExemptReasonId": None
                },
                "description": "Toilet Paper Roll",
                "quantity": 1,
                "unitprice": 0,      # We are not fetching price; can be left 0 unless you prefer to lookup
                "amount": 0,         # quantity * unitprice
                "duedate": f"{nm_year:04d}-{nm_month:02d}-01",
            }
        ]
    }

    return payload


def create_sales_order(new_so_payload: dict, access_token: str):
    """
    Create a new Sales Order using the existing API make_request via sos_api.
    We must not modify the API; we call its make_request directly.
    """
    try:
        success, result = sos_api.make_request(
            "POST",
            "/salesorder",
            access_token,
            json=new_so_payload
        )
        return success, result
    except Exception as e:
        return False, f"Exception creating sales order: {e}"


# --------------------------
# Testing mode (by numbers)
# --------------------------

def fetch_sales_orders_by_numbers(access_token: str, numbers_csv: str):
    """
    Fetch Sales Orders by exact number list (comma-separated) using only existing API functions.
    - Uses search_sales_orders_by_query(query, access_token) for each number.
    - Picks an exact case-insensitive match from the returned data.
    """
    if not numbers_csv or not numbers_csv.strip():
        return []

    raw_numbers = [n.strip() for n in numbers_csv.split(",")]
    targets = [n for n in raw_numbers if n]

    results = []
    seen = set()
    for num in targets:
        key = num.lower()
        if key in seen:
            continue
        seen.add(key)

        ok, res = sos_api.search_sales_orders_by_query(num, access_token, additional_params={"maxresults": 200})
        if not ok or not isinstance(res, dict):
            print(f"[WARN] Could not search Sales Orders for '{num}': {res}")
            continue

        parsed = sos_api.parse_sales_order_response(res)
        candidates = (parsed.get("orders") if parsed else []) or []

        match = None
        for o in candidates:
            if (o.get("number") or "").strip().lower() == key:
                match = o
                break

        if match:
            results.append(match)
            print(f"[INFO] Found Sales Order by number: {num} (id={match.get('id')})")
        else:
            print(f"[WARN] No exact Sales Order match for '{num}' in search results (returned {len(candidates)} candidates).")

    return results


def main(year: int, month: int, numbers_csv: str = ""):
    print("[STEP] Authenticating...")
    if not sos_auth.authenticate():
        print("[ERROR] Authentication failed or timed out.")
        sys.exit(1)

    access_token = sos_auth.get_access_token()
    if not access_token:
        print("[ERROR] No access token received.")
        sys.exit(1)

    ok, msg = sos_api.test_connection(access_token)
    print(f"[INFO] Connectivity test: {msg if ok else 'FAILED'}")
    if not ok:
        sys.exit(1)

    if numbers_csv and numbers_csv.strip():
        print("[STEP] Testing mode: using provided Sales Order numbers.")
        orders = fetch_sales_orders_by_numbers(access_token, numbers_csv)
        if not orders:
            print("[INFO] No Sales Orders found for the provided numbers.")
            return
    else:
        print("[STEP] Fetching monthly sales orders...")
        orders = fetch_sales_orders_for_month(access_token, year, month)
        if not orders:
            print("[INFO] No matching sales orders found for the selected month after filtering by number.")
            return

    to_process = orders[:MAX_SHIPMENTS]
    print(f"[STEP] Creating shipments for up to {MAX_SHIPMENTS} orders. Actual count: {len(to_process)}")

    successes = 0
    failures = 0
    for idx, order in enumerate(to_process, start=1):
        so_number = order.get("number") or order.get("id") or f"INDEX-{idx}"
        print(f"\n[ORDER {idx}] Starting shipment creation for Sales Order #{so_number}")
        try:
            success, result = create_shipment_from_order(order, access_token, year, month)
            if success:
                shipment = result
                print("[SUCCESS] Shipment created!")
                print(f"  Shipment Number: {shipment.get('number') or shipment.get('id')}")
                print(f"  Shipment ID: {shipment.get('id')}")
                print(f"  Customer: {(shipment.get('customer') or {}).get('name')}")
                print(f"  Date: {shipment.get('date')}")
                print(f"  Ship By: {shipment.get('ship_by') or shipment.get('shipBy')}")
                print(f"  Lines: {len(shipment.get('lines') or [])}")
                successes += 1

                # After successful shipment: create the next month's Sales Order
                try:
                    new_number = build_next_month_so_number(so_number, year, month)
                    new_so_payload = build_sales_order_payload_from_original(order, new_number, year, month)

                    print("[DEBUG] ===== NEW SALES ORDER PAYLOAD =====")
                    print(json.dumps(new_so_payload, indent=2, ensure_ascii=False))
                    print("[DEBUG] ===== END NEW SALES ORDER PAYLOAD =====")

                    so_ok, so_res = create_sales_order(new_so_payload, access_token)
                    if so_ok:
                        # Try to format or at least surface the new SO id/number
                        created_data = so_res.get("data", so_res) if isinstance(so_res, dict) else {}
                        new_id = created_data.get("id") if isinstance(created_data, dict) else None
                        new_num = created_data.get("number") if isinstance(created_data, dict) else new_number
                        print("[SUCCESS] Created new Sales Order for next month.")
                        print(f"  New SO Number: {new_num}")
                        print(f"  New SO ID: {new_id}")
                    else:
                        print(f"[WARN] Failed to create new Sales Order for next month: {so_res}")
                except Exception as e_new_so:
                    print(f"[WARN] Exception while creating next month's Sales Order: {e_new_so}")

            else:
                print(f"[ERROR] Could not create shipment for SO #{so_number}: {result}")
                failures += 1
        except Exception as e:
            print(f"[ERROR] Exception during shipment creation for SO #{so_number}: {e}")
            failures += 1

    print("\n[SUMMARY]")
    print(f"  Successful shipments: {successes}")
    print(f"  Failed shipments: {failures}")

if __name__ == "__main__":
    YEAR = 2025
    MONTH = 9

    # Extract --numbers early and remove it from argv so legacy parsing doesn't get confused
    numbers_arg = ""
    if "--numbers" in sys.argv:
        idx = sys.argv.index("--numbers")
        try:
            numbers_arg = sys.argv[idx + 1].strip()
        except Exception:
            print("[ERROR] --numbers flag provided but missing value. Example: --numbers \"H 113 Oct,BR 3 Oct\"")
            sys.exit(1)
        # Remove the flag and its value before positional parsing
        del sys.argv[idx:idx + 2]

    # Now handle legacy positional args for year and month
    if len(sys.argv) >= 3:
        try:
            YEAR = int(sys.argv[1])
            MONTH = int(sys.argv[2])
        except Exception:
            print("Usage: python create_shipments_from_first_five_orders_in_month.py <year> <month> [--numbers 'SO1,SO2']")
            sys.exit(1)

    # Run main: if numbers_arg is set, main will use testing mode and NOT fetch by month
    main(YEAR, MONTH, numbers_csv=numbers_arg)