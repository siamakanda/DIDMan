# cli_master.py

import sys
from datetime import datetime
from typing import Dict, Any, List
from datetime import date # Import date for TARGET_DAY_AUTOMATIC

# Import functions and modules from separate files
try:
    import data_fetcher
    import report_generator
except ImportError:
    print("❌ ERROR: Ensure 'data_fetcher.py' and 'report_generator.py' are in the same directory.")
    sys.exit(1)


# =======================================================================
# #################### ⚙️ MASTER CONFIGURATION BLOCK ⚙️ ####################
# This combines all necessary settings for both the fetcher and the reporter.
# =======================================================================
MASTER_CONFIG: Dict[str, Any] = {
    # 1. AUTHENTICATION & SPREADSHEET (Fetcher Keys)
    'SERVICE_ACCOUNT_KEY_FILE': 'service_account_key.json', 
    'SPREADSHEET_NAME': 'DIDMan', 
    'SPREADSHEET_ID': "1Ph6TUddAK1yVuM0Fbe434mxLuu7b1pXgtpsrJ_fu8bo",

    # 2. FILE MANAGEMENT & CACHING (Shared Keys)
    'CACHE_DIRECTORY': 'cache', 
    'GLOBAL_CACHE_FILENAME': 'spreadsheet_data_cache.json',
    'CACHE_LIFETIME_HOURS': 24, # Cache is considered old after 24 hours

    # 3. DATA INDICES (Reporter Keys)
    'DATE_COLUMN_INDEX': 1,
    'PRICE_COLUMN_INDEX': 3,
    'SHEET_DATE_FORMAT': '%m/%d/%Y', 
    
    # 4. AUTOMATION & THROTTLING (Fetcher Keys)
    'TARGET_DAY_AUTOMATIC': date.today().day, 
    'SLEEP_TIME_SECONDS': 0.5,
    
    # 5. REPORT HEADERS & PLACEHOLDERS (Reporter Keys)
    'TERMINAL_HEADERS': ['Serial', 'Client', 'DID Qty', 'DID Rate', 'Total Price', 'Expiring in Days', 'Status'],
    'DEFAULT_STATUS': 'Active', 
}
# We MUST use this complete dictionary from now on.

# =======================================================================
# #################### 🚀 MASTER ORCHESTRATION 🚀 ####################
# =======================================================================

def cli_app(config: Dict[str, Any] = MASTER_CONFIG) -> None:
    """
    Main orchestration function to check cache, pull data if necessary, 
    and run the reporting loop by calling functions from imported modules.
    """
    
    # 1. CHECK CACHE STATUS
    is_cache_valid, mod_time = data_fetcher.check_cache_status(config)
    
    if not is_cache_valid:
        # ... (logging remains the same)
        data_fetcher.run_data_fetch(config)
    else:
        print(f"\n✅ CACHE HIT: Cache is valid (Last updated: {mod_time.strftime('%Y-%m-%d %H:%M:%S')})")


    # 2. LOAD CACHE
    global_cache = data_fetcher.load_data_from_cache(config)
    if not global_cache:
        print("\n❌ FATAL: Could not load any data from cache. Exiting.")
        return

    # 3. START INTERACTIVE REPORTING LOOP
    # The complete MASTER_CONFIG is passed here, ensuring report_generator has all keys.
    report_generator.run_report_generator(global_cache, config)

if __name__ == '__main__':
    cli_app()