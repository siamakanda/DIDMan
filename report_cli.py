import json
import os
import sys
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Tuple

# Define the structure for the cached data and the final aggregated report
CacheData = Dict[str, List[List[str]]]
AggregatedReportLine = Dict[str, Any]

# =======================================================================
# #################### ⚙️ CONFIGURATION BLOCK ⚙️ ####################
# =======================================================================

CONFIG: Dict[str, Any] = {
    # 1. CACHE INPUTS (Static location for the JSON file)
    'CACHE_DIRECTORY': 'cache', 
    'GLOBAL_CACHE_FILENAME': 'spreadsheet_data_cache.json',
    
    # 2. DATA INDICES (Based on your provided sample data structure)
    'DATE_COLUMN_INDEX': 1,      # '12/2/2025'
    'PRICE_COLUMN_INDEX': 3,     # '$0.20'
    'SHEET_DATE_FORMAT': '%m/%d/%Y', 
    
    # 3. TERMINAL REPORT HEADERS & PLACEHOLDERS
    'TERMINAL_HEADERS': ['Serial', 'Client', 'DID Qty', 'DID Rate', 'Total Price', 'Expiring in Days', 'Status'],
    'DEFAULT_STATUS': 'Active', 
}

# =======================================================================
# #################### 🛠️ CORE UTILITY FUNCTIONS 🛠️ ####################
# =======================================================================

# --- CACHING FUNCTIONS ---

def get_cache_filepath(config: Dict[str, Any]) -> str:
    """Generates the static global cache filepath."""
    return os.path.join(config['CACHE_DIRECTORY'], config['GLOBAL_CACHE_FILENAME'])

def load_data_from_cache(config: Dict[str, Any]) -> CacheData:
    """Loads ALL sheet data from the single local JSON cache file."""
    filepath = get_cache_filepath(config)
    
    if not os.path.exists(filepath):
        print(f"\n❌ FILE ERROR: Global cache file **{config['GLOBAL_CACHE_FILENAME']}** not found in {config['CACHE_DIRECTORY']}.")
        print("💡 Action: Please run the data fetching script first to create the cache file.")
        sys.exit(1)
        
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data: CacheData = json.load(f)
            print(f"✅ Cache Status: Data loaded successfully from {filepath}")
            return data
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR while loading cache: {e}")
        sys.exit(1)

# --- INPUT & CALCULATION FUNCTIONS ---

def get_target_day_input() -> int:
    """Prompts the user for the day number and validates the input."""
    while True:
        try:
            target_day_input = input("\n➡️ **REQUIRED INPUT:** Please enter the DAY NUMBER (1-31) you want to report on: ")
            target_day_number = int(target_day_input)
            if not 1 <= target_day_number <= 31:
                print("🚨 Input Error: Day must be between 1 and 31.")
                continue
            return target_day_number
        except ValueError:
            print("🚨 Input Error: Please enter a whole number.")

def filter_rows_by_day(sheet_data: List[List[str]], target_day: int, config: Dict[str, Any]) -> List[List[str]]:
    """Filters rows where the date column matches the target day."""
    matching_rows = []
    date_col_index = config['DATE_COLUMN_INDEX']
    date_format = config['SHEET_DATE_FORMAT']
    
    for row in sheet_data[1:]: 
        if len(row) > date_col_index:
            sheet_date_str = row[date_col_index].strip()
            
            try:
                sheet_date_obj = datetime.strptime(sheet_date_str, date_format)
                if sheet_date_obj.day == target_day:
                    matching_rows.append(row) 
            except ValueError:
                continue 
    return matching_rows

def calculate_expiring_days(target_day: int) -> int:
    """Calculates the difference of days between the entered day and today."""
    today = date.today()
    try:
        target_date_obj = date(today.year, today.month, target_day)
    except ValueError:
        return 0 
    
    delta: timedelta = target_date_obj - today
    return delta.days

def aggregate_report_data(global_cache: CacheData, target_day: int, config: Dict[str, Any]) -> List[AggregatedReportLine]:
    """
    Processes the raw cache data to calculate the summary metrics per client (sheet).
    """
    final_report_lines: List[AggregatedReportLine] = []
    price_index = config['PRICE_COLUMN_INDEX']
    
    expiring_in_days = calculate_expiring_days(target_day)

    for sheet_name, sheet_data in global_cache.items():
        matching_rows = filter_rows_by_day(sheet_data, target_day, config)
        
        if not matching_rows:
            continue

        # 1. DID Qty (Count of matching rows)
        did_qty = len(matching_rows)
        total_price = 0.0
        
        # 2. Total Price (Sum of Column 3)
        for row in matching_rows:
            try:
                # Remove '$' and ',' then convert to float
                price_str = row[price_index].replace('$', '').replace(',', '').strip()
                total_price += float(price_str)
            except (IndexError, ValueError):
                continue
        
        # 3. DID Rate (Calculated Average Price)
        did_rate = total_price / did_qty if did_qty > 0 else 0.0

        # 4. Construct the Aggregated Line
        final_report_lines.append({
            'Client': sheet_name,
            'DID Qty': did_qty,
            'DID Rate': did_rate,
            'Total Price': total_price,
            'Expiring in Days': expiring_in_days,
            'Status': config['DEFAULT_STATUS']
        })
        
    return final_report_lines

# --- TERMINAL FORMATTING (THIS WAS LIKELY MISSING) ---

def format_and_print_terminal_report(aggregated_data: List[AggregatedReportLine], target_day: int, config: Dict[str, Any]) -> None:
    """
    Formats the aggregated report data into an aligned, printable terminal table.
    """
    if not aggregated_data:
        print("\n\n--- TERMINAL REPORT ---")
        print("No aggregated data found for the target day.")
        return

    headers = config['TERMINAL_HEADERS']
    widths = [len(h) for h in headers]
    printable_rows = []
    
    # Prepare data rows for printing and determine dynamic column widths
    for i, line in enumerate(aggregated_data):
        serial = str(i + 1)
        
        # Format values
        did_qty = str(line['DID Qty'])
        did_rate = f"{line['DID Rate']:.3f}"
        total_price = f"${line['Total Price']:.2f}"
        expiring_days = str(line['Expiring in Days'])

        processed_row = [
            serial, line['Client'], did_qty, did_rate, total_price, 
            expiring_days, line['Status']
        ]
        printable_rows.append(processed_row)
        
        # Update widths
        for j, cell in enumerate(processed_row):
            widths[j] = max(widths[j], len(str(cell)))

    # Add padding to widths
    widths = [w + 2 for w in widths]

    # --- PRINT TO TERMINAL ---
    
    print("\n" + "="*85)
    print(f"✨ **DAILY TRANSACTION SUMMARY REPORT for Day {target_day}** ✨")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data Source: {config['GLOBAL_CACHE_FILENAME']}")
    print("-" * 85)
    
    # Print Header
    header_line = "".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(header_line)
    print("-" * 85)

    # Print Data Rows
    for p_row in printable_rows:
        row_line = "".join(str(cell).ljust(widths[i]) for i, cell in enumerate(p_row))
        print(row_line)
    
    print("-" * 85)
    print(f"Total Unique Clients Reported: {len(printable_rows)}")
    print("="*85)
    
# =======================================================================
# #################### 🚀 MAIN REPORT FLOW 🚀 #######################
# =======================================================================

def generate_terminal_report_from_cache(config: Dict[str, Any] = CONFIG) -> None:
    """
    Main function to orchestrate loading data from the static cache, aggregating, 
    and printing the terminal report.
    """
    
    # 1. Setup and Input
    target_day_number = get_target_day_input()
    
    # 2. Load Data (from static cache directory)
    global_cache = load_data_from_cache(config)
    
    # 3. Aggregate Metrics
    print("\nCalculating metrics per client...")
    aggregated_data = aggregate_report_data(global_cache, target_day_number, config)

    # 4. Format and Print the Final Report
    format_and_print_terminal_report(aggregated_data, target_day_number, config)

if __name__ == '__main__':
    generate_terminal_report_from_cache()