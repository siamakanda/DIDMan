import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime
import csv
import os
import time
from typing import Dict, List, Any, Tuple, Optional

# --- CONFIGURATION (Centralized as dictionary for easy global access) ---

CONFIG: Dict[str, Any] = {
    # 1. AUTHENTICATION FILE
    'SERVICE_ACCOUNT_KEY_FILE': 'service_account_key.json', 

    # 2. SPREADSHEET DETAILS
    'SPREADSHEET_NAME': 'DIDMan', 
    'SPREADSHEET_ID': "1Ph6TUddAK1yVuM0Fbe434mxLuu7b1pXgtpsrJ_fu8bo",

    # 3. COLUMN SETTINGS
    'DATE_COLUMN_INDEX': 1,  # Column B (0-indexed)
    'SHEET_DATE_FORMAT': '%m/%d/%Y', 
    'SLEEP_TIME_SECONDS': 0.5, 

    # 4. CUSTOM HEADER DEFINITION
    'CUSTOM_HEADERS': {
        0: 'DID Number',
        1: 'Date',
        2: '1 & DID',
        3: 'Price',
        4: 'Vendor'
    },
    # Define the indices for the client-facing report
    'CLIENT_COLUMNS_INDICES': [0, 1, 2] # Columns A, B, C
}

# --- I/O AND AUTHENTICATION FUNCTIONS ---

def authenticate_and_get_worksheets(config: Dict[str, Any]) -> Tuple[List[gspread.Worksheet], str]:
    """
    Authenticates with Google Sheets and returns a list of all worksheets.
    Raises RuntimeError on connection failure.
    """
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(config['SERVICE_ACCOUNT_KEY_FILE'], scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(config['SPREADSHEET_NAME'])
        all_worksheets = spreadsheet.worksheets() 
        
        print(f"✅ Connected to spreadsheet '{config['SPREADSHEET_NAME']}'. Found {len(all_worksheets)} worksheets.")
        return all_worksheets, config['SPREADSHEET_NAME']
    
    except FileNotFoundError:
        raise RuntimeError(f"\n❌ FATAL ERROR: Service account key file '{config['SERVICE_ACCOUNT_KEY_FILE']}' not found.")
    except gspread.exceptions.SpreadsheetNotFound:
        raise RuntimeError(f"\n❌ FATAL ERROR: Spreadsheet '{config['SPREADSHEET_NAME']}' not found.")
    except Exception as e:
        raise RuntimeError(f"\n❌ UNEXPECTED CONNECTION ERROR: {e}")

def get_target_day_input() -> int:
    """Prompts the user for the day number and validates the input."""
    while True:
        try:
            target_day_input = input("❓ Please enter the day number you want to search (e.g., 18): ")
            target_day_number = int(target_day_input)
            
            if not 1 <= target_day_number <= 31:
                print("🚨 Day must be between 1 and 31.")
                continue
            print(f"\n✅ Target day number to search: **{target_day_number}**")
            return target_day_number
        except ValueError:
            print("🚨 Invalid input. Please enter a whole number.")

def get_output_directory(target_day: int) -> str:
    """Determines and creates the output directory path."""
    now = date.today()
    try:
        target_date_obj = date(now.year, now.month, target_day)
    except ValueError:
        raise ValueError(f"❌ Error: The entered day ({target_day}) is not valid for the current month/year.")

    daily_output_dir = target_date_obj.strftime('temp/%Y-%m-%d')
    
    try:
        os.makedirs(daily_output_dir, exist_ok=True)
        print(f"📁 Output Directory (Target Date): **{daily_output_dir}**\n")
        return daily_output_dir
    except OSError as e:
        raise RuntimeError(f"❌ Error creating directory {daily_output_dir}: {e}")

# --- DATA PROCESSING FUNCTIONS ---

def filter_rows_by_day(sheet_data: List[List[str]], target_day: int, config: Dict[str, Any]) -> List[List[str]]:
    """Filters rows where the date column matches the target day."""
    matching_rows = []
    date_col_index = config['DATE_COLUMN_INDEX']
    date_format = config['SHEET_DATE_FORMAT']
    
    for row in sheet_data: 
        if len(row) > date_col_index:
            sheet_date_str = row[date_col_index].strip()
            
            try:
                sheet_date_obj = datetime.strptime(sheet_date_str, date_format)
                if sheet_date_obj.day == target_day:
                    matching_rows.append(row) 
            except ValueError:
                # Skip rows with invalid or missing dates
                continue 
    return matching_rows

def write_csv_report(filepath: str, header: List[str], data_rows: List[List[str]]) -> None:
    """Generic function to write data and header to a specified CSV path."""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header) 
        writer.writerows(data_rows) 
        
# --- MAIN COORDINATION FUNCTION ---

def find_rows_with_target_day_functional(config: Dict[str, Any] = CONFIG) -> None:
    """
    Coordinates the process of fetching data, filtering rows by day, and 
    generating internal and external CSV reports.
    """
    
    report: Dict[str, Any] = {
        'total_sheets': 0, 'total_matches': 0, 'files_created': 0,
        'sheets_processed': 0, 'sheets_skipped_empty': 0, 'sheets_skipped_error': 0,
        'internal_rows': [] # Stores ALL data rows for the Internal Report
    }
    
    try:
        # 1. Setup and Input
        target_day_number = get_target_day_input()
        daily_output_dir = get_output_directory(target_day_number)
        
        # 2. Authentication and Connection
        all_worksheets, spreadsheet_name = authenticate_and_get_worksheets(config)
        report['total_sheets'] = len(all_worksheets)
        
        # 3. Process each Worksheet
        for worksheet in all_worksheets:
            sheet_name = worksheet.title
            print(f"\n--- Searching Sheet: **{sheet_name}** ---")
            
            try:
                # API Call with Rate Limiting
                data = worksheet.get_all_values()
                report['sheets_processed'] += 1
                time.sleep(config['SLEEP_TIME_SECONDS'])
                
                if not data:
                    report['sheets_skipped_empty'] += 1
                    continue
                
                # Filtering
                matching_rows_in_sheet = filter_rows_by_day(data, target_day_number, config)

                if matching_rows_in_sheet:
                    match_count = len(matching_rows_in_sheet)
                    report['total_matches'] += match_count
                    print(f"🎉 Found **{match_count}** row(s) matching day **{target_day_number}**.")
                    
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
                    print(f"   (External Alert saved to: **{client_output_file}**)")

                else:
                    print("😔 No matching rows found for the target day.")

            except gspread.exceptions.APIError:
                report['sheets_skipped_error'] += 1
                print(f"❌ Error processing sheet '{sheet_name}': API Error (Quota exceeded likely).")
            except Exception as e:
                report['sheets_skipped_error'] += 1
                print(f"❌ Unexpected Error processing sheet '{sheet_name}': {e}")
        
        # 4. Generate Internal Full Report (Single CSV)
        if report['internal_rows']:
            internal_filename = f"Internal_Full_Report_{daily_output_dir}.csv"
            internal_output_file = os.path.join(daily_output_dir, internal_filename)

            # Determine dynamic header based on widest row
            max_data_cols = max(len(row) for row in report['internal_rows']) - 1
            internal_header = ['Sheet Name'] 

            for i in range(max_data_cols):
                internal_header.append(config['CUSTOM_HEADERS'].get(i, f'Column {chr(65 + i)}'))
            
            write_csv_report(internal_output_file, internal_header, report['internal_rows']) 
            
            print(f"\n✅ Generated Internal Full Report: **{internal_output_file}**")
            report['files_created'] += 1

        # 5. Final Summary
        sheets_processed_success = report['sheets_processed'] - report['sheets_skipped_empty'] - report['sheets_skipped_error']
        
        print("\n" + "="*50)
        print("✨ **DETAILED EXECUTION REPORT** ✨")
        print("="*50)
        print(f"Target Search Day: {target_day_number} (Folder: {daily_output_dir})")
        print(f"Total Matches Found: {report['total_matches']} records")
        print(f"Total Files Created: {report['files_created']}")
        print("-"*50)
        print(f"Total Sheets in Spreadsheet: {report['total_sheets']}")
        print(f"Sheets Processed Successfully: {sheets_processed_success}")
        print(f"Sheets Skipped (Empty/Header): {report['sheets_skipped_empty']}")
        print(f"Sheets Skipped (Error): {report['sheets_skipped_error']}")
        print("="*50)

    except (RuntimeError, ValueError) as e:
        print(e)
    except Exception as e:
        print(f"\n❌ UNEXPECTED FATAL ERROR: {e}")

if __name__ == '__main__':
    find_rows_with_target_day_functional()