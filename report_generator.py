import json
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Tuple

# --- TYPE DEFINITIONS ---
CacheData = Dict[str, List[List[str]]]
AggregatedReportLine = Dict[str, Any]

# --- CONFIGURATION (Duplicate for standalone running/importing) ---
CONFIG: Dict[str, Any] = {
    'DATE_COLUMN_INDEX': 1,
    'PRICE_COLUMN_INDEX': 3,
    'SHEET_DATE_FORMAT': '%m/%d/%Y', 
    'TERMINAL_HEADERS': ['Serial', 'Client', 'DID Qty', 'DID Rate', 'Total Price', 'Expiring in Days', 'Status'],
    'DEFAULT_STATUS': 'Active', 
}

# =======================================================================
# #################### 📈 REPORTING FUNCTIONS ###########################
# =======================================================================

def get_target_day_input() -> int:
    """Prompts the user for the day number and validates the input."""
    while True:
        try:
            target_day_input = input("\n➡️ **REQUIRED INPUT:** Please enter the DAY NUMBER (1-31) for the report (or 'q' to quit): ")
            if target_day_input.lower() == 'q':
                return 0
            target_day_number = int(target_day_input)
            if not 1 <= target_day_number <= 31:
                print("🚨 Input Error: Day must be between 1 and 31.")
                continue
            return target_day_number
        except ValueError:
            print("🚨 Input Error: Please enter a whole number (1-31) or 'q'.")

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
    """Calculates the summary metrics per client (sheet) from the cache."""
    final_report_lines: List[AggregatedReportLine] = []
    price_index = config['PRICE_COLUMN_INDEX']
    expiring_in_days = calculate_expiring_days(target_day)

    for sheet_name, sheet_data in global_cache.items():
        matching_rows = filter_rows_by_day(sheet_data, target_day, config)
        
        if not matching_rows:
            continue

        did_qty = len(matching_rows)
        total_price = 0.0
        
        for row in matching_rows:
            try:
                price_str = row[price_index].replace('$', '').replace(',', '').strip()
                total_price += float(price_str)
            except (IndexError, ValueError):
                continue
        
        did_rate = total_price / did_qty if did_qty > 0 else 0.0

        final_report_lines.append({
            'Client': sheet_name, 'DID Qty': did_qty, 'DID Rate': did_rate, 
            'Total Price': total_price, 'Expiring in Days': expiring_in_days, 
            'Status': config['DEFAULT_STATUS']
        })
        
    return final_report_lines

def format_and_print_terminal_report(aggregated_data: List[AggregatedReportLine], target_day: int, config: Dict[str, Any]) -> None:
    """Formats the aggregated report data into an aligned, printable terminal table."""
    if not aggregated_data:
        print(f"\n\n--- TERMINAL REPORT ---")
        print(f"No aggregated data found for day {target_day}.")
        return

    headers = config['TERMINAL_HEADERS']
    widths = [len(h) for h in headers]
    printable_rows = []
    
    for i, line in enumerate(aggregated_data):
        serial = str(i + 1)
        did_qty = str(line['DID Qty'])
        did_rate = f"{line['DID Rate']:.3f}"
        total_price = f"${line['Total Price']:.2f}"
        expiring_days = str(line['Expiring in Days'])

        processed_row = [
            serial, line['Client'], did_qty, did_rate, total_price, 
            expiring_days, line['Status']
        ]
        printable_rows.append(processed_row)
        
        for j, cell in enumerate(processed_row):
            widths[j] = max(widths[j], len(str(cell)))

    widths = [w + 2 for w in widths]

    # --- PRINT TO TERMINAL ---
    
    print("\n" + "="*85)
    print(f"✨ **DAILY TRANSACTION SUMMARY REPORT for Day {target_day}** ✨")
    print("-" * 85)
    
    header_line = "".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(header_line)
    print("-" * 85)

    for p_row in printable_rows:
        row_line = "".join(str(cell).ljust(widths[i]) for i, cell in enumerate(p_row))
        print(row_line)
    
    print("-" * 85)
    print(f"Total Unique Clients Reported: {len(printable_rows)}")
    print("="*85)


def run_report_generator(global_cache: CacheData, config: Dict[str, Any]) -> None:
    
    """
    MASTER FUNCTION: Runs the interactive reporting loop from the provided cache.
    """
    print("\n--- Starting Interactive Reporting Mode ---")
    
    while True:
        target_day_number = get_target_day_input()
        
        if target_day_number == 0:
            print("\n👋 Exiting Reporting. Goodbye!")
            break
            
        print(f"\nProcessing report request for Day **{target_day_number}**...")
        
        # Aggregate and display report from the loaded cache
        aggregated_data = aggregate_report_data(global_cache, target_day_number, config)
        format_and_print_terminal_report(aggregated_data, target_day_number, config)

if __name__ == '__main__':
    # This won't run correctly standalone without a cache loaded, but
    # it provides the structure.
    print("Run this script via cli_master.py for full functionality.")