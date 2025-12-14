import json
import os
import sys
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional

# --- TYPE DEFINITIONS ---
CacheData = Dict[str, List[List[str]]]

# --- CONFIGURATION (Duplicate for standalone running/importing) ---
CONFIG: Dict[str, Any] = {
    'SERVICE_ACCOUNT_KEY_FILE': 'service_account_key.json', 
    'SPREADSHEET_NAME': 'DIDMan', 
    'SPREADSHEET_ID': "1Ph6TUddAK1yVuM0Fbe434mxLuu7b1pXgtpsrJ_fu8bo",
    'CACHE_DIRECTORY': 'cache', 
    'GLOBAL_CACHE_FILENAME': 'spreadsheet_data_cache.json',
    'CACHE_LIFETIME_HOURS': 24,
    'DATE_COLUMN_INDEX': 1,
    'SHEET_DATE_FORMAT': '%m/%d/%Y', 
    'TARGET_DAY_AUTOMATIC': date.today().day, 
    'SLEEP_TIME_SECONDS': 0.5,
}

# =======================================================================
# #################### 📦 CACHE & FETCH FUNCTIONS ####################
# =======================================================================

def get_cache_filepath(config: Dict[str, Any]) -> str:
    """Generates the static global cache filepath."""
    cache_dir = config['CACHE_DIRECTORY']
    cache_filename = config['GLOBAL_CACHE_FILENAME']
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, cache_filename)

def check_cache_status(config: Dict[str, Any]) -> Tuple[bool, datetime]:
    """Checks if the cache file exists and if it is still valid."""
    filepath = get_cache_filepath(config)
    
    if not os.path.exists(filepath):
        return False, datetime.min
    
    mod_timestamp = os.path.getmtime(filepath)
    mod_time = datetime.fromtimestamp(mod_timestamp)
    
    lifetime = timedelta(hours=config['CACHE_LIFETIME_HOURS'])
    is_valid = (datetime.now() - mod_time) < lifetime
    
    return is_valid, mod_time

def load_data_from_cache(config: Dict[str, Any]) -> CacheData:
    """Tries to load ALL sheet data from the single local JSON cache file."""
    filepath = get_cache_filepath(config)
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def save_data_to_cache(cache_data: CacheData, config: Dict[str, Any]) -> None:
    """Saves the entire cache structure to a single local JSON file."""
    filepath = get_cache_filepath(config)
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, indent=4)
        print(f"  Status: Cache successfully saved/updated at {filepath}")
    except Exception as e:
        print(f"  ❌ CACHE WARNING: Could not save cache file. Error: {e}")

def authenticate_and_get_worksheets(config: Dict[str, Any]) -> List[gspread.Worksheet]:
    """Authenticates and returns a list of all worksheets."""
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(config['SERVICE_ACCOUNT_KEY_FILE'], scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(config['SPREADSHEET_NAME'])
        return spreadsheet.worksheets()
    except Exception as e:
        print(f"\n❌ FATAL CONNECTION ERROR: {e}")
        # In the master app, we want to return an empty list, not sys.exit(1)
        return []

def run_data_fetch(config: Dict[str, Any] = CONFIG) -> None:
    """
    MASTER FUNCTION: Executes the full data pulling process from Google Sheets and saves to cache.
    """
    print("\n--- 🌐 Starting Data Fetch from Google Sheets ---")
    
    global_cache = load_data_from_cache(config)
    all_worksheets = authenticate_and_get_worksheets(config)
    
    if not all_worksheets:
        print("❌ Fetch failed: Could not connect or authenticate.")
        return

    sheets_fetched = 0
    
    for worksheet in all_worksheets:
        sheet_name = worksheet.title
        
        # We only fetch if data is missing for that sheet to minimize API calls
        if sheet_name not in global_cache:
            try:
                sheet_data = worksheet.get_all_values()
                global_cache[sheet_name] = sheet_data
                sheets_fetched += 1
                time.sleep(config['SLEEP_TIME_SECONDS'])
            except Exception:
                print(f"  ❌ API ERROR: Skipping sheet **{sheet_name}**.")
                continue

    save_data_to_cache(global_cache, config)
    print(f"✅ Data Fetch Complete. Fetched/Updated **{sheets_fetched}** sheets.")

if __name__ == '__main__':
    # If run directly, fetch today's data and update the cache
    run_data_fetch()