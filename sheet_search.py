import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime
import csv
import os
import time
import sys 
import json # New import for handling JSON cache files

from typing import Dict, List, Any, Tuple, Optional

# =======================================================================
# #################### ⚙️ CONFIGURATION BLOCK ⚙️ ####################
# =======================================================================

CONFIG: Dict[str, Any] = {
    # 1. AUTHENTICATION FILE
    'SERVICE_ACCOUNT_KEY_FILE': 'service_account_key.json', 

    # 2. SPREADSHEET DETAILS
    'SPREADSHEET_NAME': 'DIDMan', 
    'SPREADSHEET_ID': "1Ph6TUddAK1yVuM0Fbe434mxLuu7b1pXgtpsrJ_fu8bo",

    # 3. COLUMN SETTINGS
    'DATE_COLUMN_INDEX': 1,  # Column B (0-indexed). Must contain dates in the format below.
    'SHEET_DATE_FORMAT': '%m/%d/%Y', 
    'SLEEP_TIME_SECONDS': 0.5, # Rate limit pause (increase this if you hit API limits)

    # 4. REPORT STRUCTURE
    'CUSTOM_HEADERS': {
        0: 'DID Number',
        1: 'Date',
        2: '1 & DID',
        3: 'Price',
        4: 'Vendor'
    },
    # Indices for the Client-Facing Report (must exist in CUSTOM_HEADERS)
    'CLIENT_COLUMNS_INDICES': [0, 1, 2],
    
    # 5. CACHE SETTINGS (New)
    'USE_CACHE': True,
}

# =======================================================================
# #################### 🛠️ CORE UTILITY FUNCTIONS 🛠️ ####################
# =======================================================================

# --- CACHING FUNCTIONS ---

def get_cache_filepath(sheet_name: str, daily_output_dir: str) -> str:
    """Generates a standardized cache filepath."""
    cache_filename = f"{sheet_name.replace(' ', '_')}_cache.json"
    # Ensure the cache file is saved next to the output reports
    return os.path.join(daily_output_dir, cache_filename)

def load_data_from_cache(sheet_name: str, daily_output_dir: str) -> Optional[List[List[str]]]:
    """Tries to load sheet data from a local JSON cache file."""
    filepath = get_cache_filepath(sheet_name, daily_output_dir)
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                print(f"  Status: Data loaded from local cache: {filepath}")
                return data
        except Exception as e:
            print(f"  ❌ CACHE ERROR: Failed to read cache file {filepath}. Fetching from API. Error: {e}")
            return None
    return None

def save_data_to_cache(sheet_name: str, daily_output_dir: str, data: List[List[str]]) -> None:
    """Saves sheet data to a local JSON cache file."""
    filepath = get_cache_filepath(sheet_name, daily_output_dir)
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        print(f"  Status: Sheet data saved to local cache: {filepath}")
    except Exception as e:
        print(f"  ❌ CACHE WARNING: Could not save cache file {filepath}. Error: {e}")

# --- INPUT/OUTPUT/AUTH FUNCTIONS (Unchanged from last optimization) ---

def get_target_day_input() -> int:
    """Prompts the user for the day number and validates the input."""
    while True:
        try:
            target_day_input = input("\n➡️ **REQUIRED INPUT:** Please enter the DAY NUMBER (1-31) you want to search (e.g., 18): ")
            target_day_number = int(target_day_input)
            
            if not 1 <= target_day_number <= 31:
                print("🚨 Input Error: Day must be between 1 and 31.")
                continue
            return target_day_number
        except ValueError:
            print("🚨 Input Error: Please enter a whole number.")

def authenticate_and_get_worksheets(config: Dict[str, Any]) -> List[gspread.Worksheet]:
    """Authenticates and returns a list of all worksheets, handling user-facing errors."""
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(config['SERVICE_ACCOUNT_KEY_FILE'], scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(config['SPREADSHEET_NAME'])
        all_worksheets = spreadsheet.worksheets() 
        
        print(f"\n✅ Connection Status: Successfully connected to spreadsheet. Found **{len(all_worksheets)}** worksheets.")
        return all_worksheets
    
    except FileNotFoundError:
        print(f"\n❌ FATAL ERROR: Authentication file '{config['SERVICE_ACCOUNT_KEY_FILE']}' not found.")
        print("💡 Action: Please ensure the JSON key file is in the script directory.")
        sys.exit(1)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"\n❌ FATAL ERROR: Spreadsheet named '{config['SPREADSHEET_NAME']}' was not found.")
        print("💡 Action: Please verify SPREADSHEET_NAME/ID in the CONFIG.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ UNEXPECTED CONNECTION ERROR: {e}")
        sys.exit(1)

def get_output_directory(target_day: int) -> str:
    """
    Determines and creates the output directory path, nested inside a 'temp/' folder.
    """
    now = date.today()
    try:
        target_date_obj = date(now.year, now.month, target_day)
    except ValueError:
        print(f"\n❌ VALIDATION ERROR: Day **{target_day}** is not valid for the current month.")
        sys.exit(1)

    daily_output_dir = target_date_obj.strftime('temp/%Y-%m-%d')
    
    try:
        os.makedirs(daily_output_dir, exist_ok=True)
        print(f"📁 Output Directory: Reports will be saved to the folder **{daily_output_dir}**\n")
        return daily_output_dir
    except OSError as e:
        print(f"\n❌ SYSTEM ERROR: Failed to create directory {daily_output_dir}: {e}")
        sys.exit(1)

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

def write_csv_report(filepath: str, header: List[str], data_rows: List[List[str]]) -> None:
    """Generic function to write data and header to a specified CSV path."""
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(header) 
            writer.writerows(data_rows) 
    except OSError as e:
        print(f"❌ WARNING: Failed to write CSV file **{filepath}**: {e}")

# =======================================================================
# #################### 🚀 MAIN EXECUTION FLOW 🚀 #####################
# =======================================================================

def find_rows_with_target_day_interactive(config: Dict[str, Any] = CONFIG) -> None:
    """
    Coordinates the entire process, checking local cache before fetching data.
    """
    
    report: Dict[str, Any] = {
        'total_sheets': 0, 'total_matches': 0, 'files_created': 0,
        'sheets_processed': 0, 'sheets_skipped_empty': 0, 'sheets_skipped_error': 0,
        'sheets_loaded_from_cache': 0, # New metric
        'internal_rows': []
    }
    
    # 1. Setup and Input
    target_day_number = get_target_day_input()
    print(f"\n--- Initializing Search: Target Day **{target_day_number}** ---")
    
    daily_output_dir = get_output_directory(target_day_number)
    
    # 2. Authentication and Connection
    all_worksheets = authenticate_and_get_worksheets(config)
    report['total_sheets'] = len(all_worksheets)
    
    # 3. Process each Worksheet
    for worksheet in all_worksheets:
        sheet_name = worksheet.title
        print(f"\nProcessing Sheet: **{sheet_name}**")
        print("---")
        
        sheet_data = None
        
        # --- CACHE CHECK (New Logic) ---
        if config['USE_CACHE']:
            sheet_data = load_data_from_cache(sheet_name, daily_output_dir)
            if sheet_data:
                report['sheets_loaded_from_cache'] += 1
        
        # --- FETCH FROM API IF CACHE MISS ---
        if sheet_data is None:
            try:
                # API Call with Rate Limiting
                sheet_data = worksheet.get_all_values()
                report['sheets_processed'] += 1
                time.sleep(config['SLEEP_TIME_SECONDS'])
                
                if sheet_data and config['USE_CACHE']:
                    save_data_to_cache(sheet_name, daily_output_dir, sheet_data)
                
            except gspread.exceptions.APIError:
                report['sheets_skipped_error'] += 1
                print(f"  ❌ API ERROR: Rate limit likely reached. Skipping sheet **{sheet_name}**.")
                continue
            except Exception as e:
                report['sheets_skipped_error'] += 1
                print(f"  ❌ UNEXPECTED ERROR: Failed to process sheet **{sheet_name}**: {e}. Skipping.")
                continue

        # --- DATA PROCESSING ---
        if not sheet_data or len(sheet_data) <= 1:
            report['sheets_skipped_empty'] += 1
            print("  Status: Sheet is empty or header-only. Skipped.")
            continue
        
        # Filtering
        matching_rows_in_sheet = filter_rows_by_day(sheet_data, target_day_number, config)

        if matching_rows_in_sheet:
            match_count = len(matching_rows_in_sheet)
            report['total_matches'] += match_count
            print(f"  🎉 **Match Found:** {match_count} record(s) identified.")
            
            # 3A. Prepare for Internal Report
            for row_data in matching_rows_in_sheet:
                report['internal_rows'].append([sheet_name] + row_data) 
                
            # 3B. Generate External Client Alert CSV
            client_filename = f"{sheet_name}_Alerts.csv"
            client_output_file = os.path.join(daily_output_dir, client_filename)
            
            # Prepare Data/Header for Client Report
            client_report_data = [
                [row[i] for i in config['CLIENT_COLUMNS_INDICES'] if i < len(row)]
                for row in matching_rows_in_sheet
            ]
            client_header = [config['CUSTOM_HEADERS'][i] for i in config['CLIENT_COLUMNS_INDICES']]
            
            # Write CSV
            write_csv_report(client_output_file, client_header, client_report_data)
            report['files_created'] += 1
            print(f"  > **Client Alert Saved:** {client_output_file}")

        else:
            print("  Status: No matching records found.")
    
    # 4. Generate Internal Full Report (Single CSV)
    if report['internal_rows']:
        # Filename based on the day number
        internal_filename = f"Internal_Full_Report_{target_day_number:02d}.csv"
        internal_output_file = os.path.join(daily_output_dir, internal_filename)

        # Determine dynamic header based on widest row
        max_data_cols = max(len(row) for row in report['internal_rows']) - 1
        internal_header = ['Sheet Name'] 

        for i in range(max_data_cols):
            internal_header.append(config['CUSTOM_HEADERS'].get(i, f'Column {chr(65 + i)}'))
        
        write_csv_report(internal_output_file, internal_header, report['internal_rows']) 
        print(f"\n✅ Final Report Generated: **{internal_output_file}**")
        report['files_created'] += 1
    else:
        print("\nℹ️ No matching records found across all sheets. Internal Report skipped.")

    # 5. Final Summary
    sheets_processed_success = report['sheets_processed'] + report['sheets_loaded_from_cache'] - report['sheets_skipped_empty'] - report['sheets_skipped_error']
    
    print("\n" + "="*50)
    print("✨ **PROCESSING COMPLETE SUMMARY** ✨")
    print("="*50)
    print(f"Target Day Searched: **{target_day_number}** (Folder: {daily_output_dir})")
    print(f"Total Matching Records: **{report['total_matches']}**")
    print(f"Total Files Created: **{report['files_created']}** (1 Internal Report + {report['files_created'] - 1} Client Alerts)")
    print("-" * 50)
    print(f"Total Sheets Checked: {report['total_sheets']}")
    print(f"Sheets Fetched from API: {report['sheets_processed']}")
    print(f"Sheets Loaded from Cache: {report['sheets_loaded_from_cache']}")
    print(f"Sheets Processed Successfully: {sheets_processed_success}")
    print(f"Sheets Skipped (Empty/Header): {report['sheets_skipped_empty']}")
    print(f"Sheets Skipped (Error/API): {report['sheets_skipped_error']}")
    print("="*50)
    print("Script finished execution.")


if __name__ == '__main__':
    find_rows_with_target_day_interactive()