import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime
import csv
import os
import time
import sys 
import json 
from typing import Dict, List, Any, Tuple, Optional

CacheData = Dict[str, List[List[str]]]

# =======================================================================
# #################### ⚙️ CONFIGURATION BLOCK ⚙️ ####################
# =======================================================================

CONFIG: Dict[str, Any] = {
    # 1. AUTHENTICATION & SPREADSHEET
    'SERVICE_ACCOUNT_KEY_FILE': 'service_account_key.json', 
    'SPREADSHEET_NAME': 'DIDMan', 
    'SPREADSHEET_ID': "1Ph6TUddAK1yVuM0Fbe434mxLuu7b1pXgtpsrJ_fu8bo",

    # 2. FILE MANAGEMENT
    'CACHE_DIRECTORY': 'cache', 
    'GLOBAL_CACHE_FILENAME': 'spreadsheet_data_cache.json',
    
    # 3. AUTOMATION SETTING
    # The script will search for TODAY's day number in the Date column.
    'TARGET_DAY_AUTOMATIC': date.today().day, 
    
    # 4. COLUMN & PROCESSING SETTINGS
    'DATE_COLUMN_INDEX': 1,
    'SHEET_DATE_FORMAT': '%m/%d/%Y', 
    'SLEEP_TIME_SECONDS': 0.5,
    'USE_CACHE': True,
    
    # 5. REPORT STRUCTURE (For optional CSV generation)
    'CUSTOM_HEADERS': {
        0: 'DID Number', 1: 'Date', 2: '1 & DID', 3: 'Price', 4: 'Vendor'
    },
    'CLIENT_COLUMNS_INDICES': [0, 1, 2]
}

# =======================================================================
# #################### 🛠️ CORE UTILITY FUNCTIONS 🛠️ ####################
# =======================================================================

# --- CACHING FUNCTIONS ---

def get_cache_filepath(config: Dict[str, Any]) -> str:
    """Generates the static global cache filepath and ensures the directory exists."""
    cache_dir = config['CACHE_DIRECTORY']
    cache_filename = config['GLOBAL_CACHE_FILENAME']
    
    try:
        os.makedirs(cache_dir, exist_ok=True)
    except OSError as e:
        print(f"❌ SYSTEM ERROR: Failed to create cache directory '{cache_dir}': {e}")
        sys.exit(1)

    return os.path.join(cache_dir, cache_filename)

def load_data_from_cache(config: Dict[str, Any]) -> CacheData:
    """Tries to load ALL sheet data from the single local JSON cache file."""
    filepath = get_cache_filepath(config)
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data: CacheData = json.load(f)
                print(f"  Status: Global cache loaded successfully from: {filepath}")
                return data
        except Exception as e:
            print(f"  ❌ CACHE ERROR: Failed to read global cache. Starting empty. Error: {e}")
            return {}
    return {}

def save_data_to_cache(cache_data: CacheData, config: Dict[str, Any]) -> None:
    """Saves the entire cache structure to a single local JSON file."""
    filepath = get_cache_filepath(config)
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=4)
        print(f"  Status: All sheet data saved/updated in the global cache: {filepath}")
    except Exception as e:
        print(f"  ❌ CACHE WARNING: Could not save global cache file {filepath}. Error: {e}")


# --- OTHER UTILITY FUNCTIONS ---

# *** REMOVED: get_target_day_input() ***

def authenticate_and_get_worksheets(config: Dict[str, Any]) -> List[gspread.Worksheet]:
    """Authenticates and returns a list of all worksheets."""
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(config['SERVICE_ACCOUNT_KEY_FILE'], scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(config['SPREADSHEET_NAME'])
        all_worksheets = spreadsheet.worksheets() 
        print(f"\n✅ Connection Status: Successfully connected. Found **{len(all_worksheets)}** worksheets.")
        return all_worksheets
    except Exception as e:
        print(f"\n❌ FATAL CONNECTION ERROR: {e}")
        sys.exit(1)

def get_output_directory(target_day: int) -> str:
    """Determines and creates the *daily report* output directory (temp/%Y-%m-%d)."""
    now = date.today()
    try:
        target_date_obj = date(now.year, now.month, target_day)
    except ValueError:
        # This should only happen if today's date is invalid, which is impossible, 
        # but kept for robustness.
        print(f"\n❌ VALIDATION ERROR: Day **{target_day}** is not valid for the current month.")
        sys.exit(1)

    daily_output_dir = target_date_obj.strftime('temp/%Y-%m-%d')
    
    try:
        os.makedirs(daily_output_dir, exist_ok=True)
        print(f"📁 Daily Report Directory: Reports will be saved to **{daily_output_dir}** (if enabled).\n")
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
    """Function to write data to a specified CSV path (Currently unused but kept for compatibility)."""
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
    
    report: Dict[str, Any] = {
        'total_sheets': 0, 'total_matches': 0, 'files_created': 0,
        'sheets_processed': 0, 'sheets_skipped_empty': 0, 'sheets_skipped_error': 0,
        'sheets_loaded_from_cache': 0, 'internal_rows': []
    }
    
    # 1. Setup and Input (AUTOMATICALLY SETS THE DAY)
    target_day_number = config['TARGET_DAY_AUTOMATIC']
    print(f"\n--- Initializing Data Fetch for Day **{target_day_number}** (Today's Day) ---")
    
    # The output directory is created, but the cache is saved elsewhere.
    daily_output_dir = get_output_directory(target_day_number)
    
    # 2. Load Cache (Global Check)
    global_cache = {}
    if config['USE_CACHE']:
        global_cache = load_data_from_cache(config)
    
    # 3. Authentication and Connection
    all_worksheets = authenticate_and_get_worksheets(config)
    report['total_sheets'] = len(all_worksheets)
    
    # 4. Process each Worksheet
    for worksheet in all_worksheets:
        sheet_name = worksheet.title
        print(f"\nProcessing Sheet: **{sheet_name}**")
        print("---")
        
        sheet_data: Optional[List[List[str]]] = None
        
        # --- CACHE CHECK ---
        if config['USE_CACHE'] and sheet_name in global_cache:
            sheet_data = global_cache[sheet_name]
            report['sheets_loaded_from_cache'] += 1
            print("  Status: Data loaded from global cache.")
        
        # --- FETCH FROM API IF CACHE MISS ---
        if sheet_data is None:
            try:
                sheet_data = worksheet.get_all_values()
                report['sheets_processed'] += 1
                time.sleep(config['SLEEP_TIME_SECONDS'])
                
                if sheet_data and config['USE_CACHE']:
                    global_cache[sheet_name] = sheet_data 
                
            except gspread.exceptions.APIError:
                report['sheets_skipped_error'] += 1
                print(f"  ❌ API ERROR: Rate limit likely reached. Skipping sheet **{sheet_name}**.")
                continue
            except Exception as e:
                report['sheets_skipped_error'] += 1
                print(f"  ❌ UNEXPECTED ERROR: Failed to process sheet **{sheet_name}**: {e}. Skipping.")
                continue

        # --- DATA PROCESSING (Filtering for summary) ---
        if not sheet_data or len(sheet_data) <= 1:
            report['sheets_skipped_empty'] += 1
            print("  Status: Sheet is empty or header-only. Skipped.")
            continue
        
        matching_rows_in_sheet = filter_rows_by_day(sheet_data, target_day_number, config)

        if matching_rows_in_sheet:
            match_count = len(matching_rows_in_sheet)
            report['total_matches'] += match_count
            print(f"  🎉 **Match Found:** {match_count} record(s) identified.")
        else:
            print("  Status: No matching records found.")
            
    # 5. Save Cache (Write entire updated dictionary back to the STATIC cache file)
    if config['USE_CACHE']:
        save_data_to_cache(global_cache, config)
    
    # 6. Final Summary
    sheets_processed_success = report['total_sheets'] - report['sheets_skipped_empty'] - report['sheets_skipped_error']
    
    print("\n" + "="*50)
    print("✨ **DATA FETCH COMPLETE SUMMARY** ✨")
    print("="*50)
    print(f"Target Day Searched (Today's Day): **{target_day_number}**")
    print(f"Total Sheets Fetched: {report['sheets_processed']}")
    print(f"Total Sheets Loaded from Cache: {report['sheets_loaded_from_cache']}")
    print("-" * 50)
    print(f"Total Records Matching Day {target_day_number}: **{report['total_matches']}**")
    print("="*50)
    print("Script finished execution.")


if __name__ == '__main__':
    find_rows_with_target_day_interactive()