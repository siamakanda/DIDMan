import pandas as pd
import os
import time
import gspread
import re
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, text  # <--- Update this import


# --- CONFIGURATION ---
CONFIG = {
    'SERVICE_ACCOUNT_KEY_FILE': 'service_account_key.json', 
    'SPREADSHEET_ID': "1Ph6TUddAK1yVuM0Fbe434mxLuu7b1pXgtpsrJ_fu8bo",
    'SLEEP_TIME_SECONDS': 1, 
    'CACHE_DB_PATH': 'cache/did_manager.db',
    'CACHE_EXPIRY_MINUTES': 15
}

# --- FUNCTIONS ---

def authenticate_and_get_worksheets(config):
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(config['SERVICE_ACCOUNT_KEY_FILE'], scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(config['SPREADSHEET_ID'])
        print(f"✅ Connected to '{spreadsheet.title}'. Found {len(spreadsheet.worksheets())} worksheets.")
        return spreadsheet.worksheets()
    except Exception as e:
        raise RuntimeError(f"❌ AUTH ERROR: {e}")

def save_worksheets_to_sqlite(worksheets, db_name):
    os.makedirs(os.path.dirname(db_name), exist_ok=True)
    engine = create_engine(f'sqlite:///{db_name}')
    
    for sheet in worksheets:
        try:
            data = sheet.get_all_values()
            if not data or len(data) < 1:
                continue
                
            df = pd.DataFrame(data[1:], columns=data[0])

            # 1. Clean Columns
            df = df.loc[:, df.columns != ''] 
            new_cols = []
            for i, col in enumerate(df.columns):
                if not col or col.strip() == "":
                    new_cols.append(f"unnamed_{i}")
                else:
                    # Remove any non-alphanumeric character for SQL column safety
                    clean_col = re.sub(r'\W+', '_', col).lower().strip('_')
                    new_cols.append(clean_col)
            df.columns = new_cols

            # 2. Add Meta-data
            df['gsheet_row_index'] = range(2, len(df) + 2)
            df['is_synced'] = 1
            
            # 3. Clean Table Name
            table_name = re.sub(r'\W+', '_', sheet.title).lower().strip('_')
            
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"✅ Cached: {table_name}")
            
            time.sleep(CONFIG['SLEEP_TIME_SECONDS']) 
                       
        except Exception as e:
            print(f"❌ Error in '{sheet.title}': {e}")
    
    engine.dispose()

def is_cache_fresh(file_path, max_age_minutes):
    if not os.path.exists(file_path):
        return False
    file_age_min = (time.time() - os.path.getmtime(file_path)) / 60
    return file_age_min < max_age_minutes

def display_dashboard():
    engine = create_engine(f'sqlite:///{CONFIG["CACHE_DB_PATH"]}')
    with engine.connect() as conn:
        query = text("SELECT name FROM sqlite_master WHERE type='table';")
        tables = conn.execute(query).fetchall()
        
        print(f"\n{'Table Name':<45} | {'Rows':<10}")
        print("-" * 60)
        
        for table in tables:
            table_name = table[0]
            # Wrap table_name in double quotes to handle characters like '/' or '-'
            count_query = text(f'SELECT COUNT(*) FROM "{table_name}"')
            try:
                row_count = conn.execute(count_query).scalar()
                print(f"{table_name:<45} | {row_count:<10}")
            except Exception as e:
                print(f"{table_name:<45} | ❌ Error: {e}")
            
    engine.dispose()

# --- EXECUTION ---
if __name__ == "__main__":
    if is_cache_fresh(CONFIG['CACHE_DB_PATH'], CONFIG['CACHE_EXPIRY_MINUTES']):
        print(f"🚀 Cache is fresh (<{CONFIG['CACHE_EXPIRY_MINUTES']}m). Using local SQL.")
        display_dashboard()
    else:
        print("♻️ Cache old/missing. Syncing with Google Sheets...")
        try:
            worksheets = authenticate_and_get_worksheets(CONFIG)
            save_worksheets_to_sqlite(worksheets, CONFIG['CACHE_DB_PATH'])
        except Exception as e:
            print(e)