import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime
import csv
import os
import time # <<< ADDED for API rate limiting

# ====================================================================
# --- CONFIGURATION (EDIT ONLY IF SPREADSHEET DETAILS CHANGE) ---
# ====================================================================

# 1. AUTHENTICATION FILE
SERVICE_ACCOUNT_KEY_FILE = 'service_account_key.json' 

# 2. SPREADSHEET DETAILS
SPREADSHEET_NAME = 'DID_Automation' 

# 3. COLUMN SETTINGS
# The index of the column containing the date (A=0, B=1, C=2, etc.)
DATE_COLUMN_INDEX = 1  # Column B
# The format used in your Google Sheet (MM/DD/YYYY) - still needed for parsing
SHEET_DATE_FORMAT = '%m/%d/%Y' 

# 4. CUSTOM HEADER DEFINITION
CUSTOM_HEADERS = {
    0: 'Number', 
    1: 'Date',    
    2: '1&Number',  
    3: 'Price',   
    4: 'Vendor'   
}
LAST_CUSTOM_HEADER_INDEX = max(CUSTOM_HEADERS.keys())

# API Quota Rate Limiter
SLEEP_TIME_SECONDS = 1 

# ====================================================================
# --- MAIN FUNCTION ---
# ====================================================================

def find_rows_with_target_day():
    """
    Prompts user for the target day, authenticates, iterates through ALL 
    worksheets, searches Column B for the target day number, creates a 
    daily directory, saves client CSV files, and prints a detailed report.
    """
    
    # 1. Get Target Day Input from User
    while True:
        try:
            target_day_input = input("❓ Please enter the day number you want to search (e.g., 18): ")
            target_day_number = int(target_day_input)
            
            if not 1 <= target_day_number <= 31:
                print("🚨 Day must be between 1 and 31.")
                continue
            break
        except ValueError:
            print("🚨 Invalid input. Please enter a whole number.")
            
    # Get current month and year to construct the full target date
    now = date.today()
    current_year = now.year
    current_month = now.month

    # 2. Construct the specific target date object and output path
    try:
        target_date_obj = date(current_year, current_month, target_day_number)
    except ValueError as e:
        print(f"❌ Error: The entered day ({target_day_number}) is not valid for the current month/year.")
        print("Please check the month's maximum days.")
        return

    DAILY_OUTPUT_DIR = target_date_obj.strftime('%Y-%m-%d')
    
    # Report tracking variables
    report = {
        'total_sheets': 0,
        'sheets_processed': 0,
        'sheets_skipped_empty': 0,
        'sheets_skipped_error': 0,
        'total_matches': 0,
        'files_created': 0
    }
    
    print(f"\n✅ Target day number to search: **{target_day_number}**")
    print(f"📁 Output Directory (Target Date): **{DAILY_OUTPUT_DIR}**\n")
    
    # Create Directory
    try:
        os.makedirs(DAILY_OUTPUT_DIR, exist_ok=True)
        print(f"✅ Successfully ensured directory '{DAILY_OUTPUT_DIR}' exists.")
    except OSError as e:
        print(f"❌ Error creating directory {DAILY_OUTPUT_DIR}: {e}")
        return
        
    try:
        # 3. Authentication
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_KEY_FILE, scope)
        client = gspread.authorize(creds)
        print("✅ Successfully authenticated with Google.")

        # 4. Open the spreadsheet and get all worksheets
        spreadsheet = client.open(SPREADSHEET_NAME)
        all_worksheets = spreadsheet.worksheets() 
        report['total_sheets'] = len(all_worksheets)
        print(f"✅ Connected to spreadsheet '{SPREADSHEET_NAME}'. Found {report['total_sheets']} worksheets to check.")
        
        # 5. Loop through each worksheet
        for worksheet in all_worksheets:
            sheet_name = worksheet.title
            print(f"\n--- Searching Client Sheet: **{sheet_name}** ---")

            try:
                # API CALL: Rate Limit check implemented by time.sleep()
                data = worksheet.get_all_values()
                report['sheets_processed'] += 1
                
                if not data:
                    print("Skipping (Sheet is empty).")
                    report['sheets_skipped_empty'] += 1
                    time.sleep(SLEEP_TIME_SECONDS) # Sleep even on empty sheets to maintain rate
                    continue
                
                matching_rows_in_sheet = []
                for row in data: 
                    if len(row) > DATE_COLUMN_INDEX:
                        sheet_date_str = row[DATE_COLUMN_INDEX].strip()
                        
                        try:
                            sheet_date_obj = datetime.strptime(sheet_date_str, SHEET_DATE_FORMAT)
                            if sheet_date_obj.day == target_day_number:
                                matching_rows_in_sheet.append(row) 
                        except ValueError:
                            continue 

                # 6. Output Results and Write to Client-Specific CSV
                if matching_rows_in_sheet:
                    match_count = len(matching_rows_in_sheet)
                    report['total_matches'] += match_count
                    
                    print(f"🎉 Found **{match_count}** row(s) matching day **{target_day_number}**.")
                    
                    client_filename = f"{sheet_name}_matches.csv"
                    client_output_file = os.path.join(DAILY_OUTPUT_DIR, client_filename)
                    
                    # CSV Header Generation
                    max_data_cols = max(len(row) for row in matching_rows_in_sheet)
                    final_header = [] 
                    for i in range(max_data_cols):
                        final_header.append(CUSTOM_HEADERS.get(i, f'Column {chr(65 + i)}'))
                            
                    # CSV Writing
                    with open(client_output_file, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(final_header) 
                        writer.writerows(matching_rows_in_sheet) 
                        
                    report['files_created'] += 1
                    print(f"💾 Results saved to: **{client_output_file}**")
                else:
                    print("😔 No matching rows found for the target day.")

            except gspread.exceptions.APIError as e:
                print(f"❌ Error processing sheet '{sheet_name}': {e}")
                report['sheets_skipped_error'] += 1
            except Exception as e:
                print(f"❌ Unexpected Error processing sheet '{sheet_name}': {e}")
                report['sheets_skipped_error'] += 1
            
            # Rate Limiting Pause
            time.sleep(SLEEP_TIME_SECONDS)
                
        # 7. Final Summary (Detailed Report)
        print("\n" + "="*50)
        print("✨ **DETAILED EXECUTION REPORT** ✨")
        print("="*50)
        print(f"Target Search Day: {target_day_number} (Folder: {DAILY_OUTPUT_DIR})")
        print(f"Total Matches Found: {report['total_matches']} records")
        print(f"Total CSV Files Created: {report['files_created']}")
        print("-"*50)
        print(f"Total Sheets in Spreadsheet: {report['total_sheets']}")
        print(f"Sheets Successfully Processed: {report['sheets_processed'] - report['sheets_skipped_empty']}")
        print(f"Sheets Skipped (Empty): {report['sheets_skipped_empty']}")
        print(f"Sheets Skipped (Error): {report['sheets_skipped_error']}")
        print("="*50)

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"\n❌ FATAL ERROR: Spreadsheet '{SPREADSHEET_NAME}' not found.")
    except FileNotFoundError:
        print(f"\n❌ FATAL ERROR: Service account key file '{SERVICE_ACCOUNT_KEY_FILE}' not found.")
    except Exception as e:
        print(f"\n❌ UNEXPECTED FATAL ERROR: {e}")

if __name__ == '__main__':
    find_rows_with_target_day()