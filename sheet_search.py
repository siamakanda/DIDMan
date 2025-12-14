import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime
import csv
import os
import time

# ====================================================================
# --- CONFIGURATION (EDIT ONLY IF SPREADSHEET DETAILS CHANGE) ---
# ====================================================================

# 1. AUTHENTICATION FILE
SERVICE_ACCOUNT_KEY_FILE = 'service_account_key.json' 

# 2. SPREADSHEET DETAILS
SPREADSHEET_NAME = 'DIDMan' 
SPREADSHEET_ID = "1Ph6TUddAK1yVuM0Fbe434mxLuu7b1pXgtpsrJ_fu8bo"

# 3. COLUMN SETTINGS
DATE_COLUMN_INDEX = 1  # Column B
SHEET_DATE_FORMAT = '%m/%d/%Y' 
SLEEP_TIME_SECONDS = 0.5  # Pause between API calls to avoid rate limits

# 4. CUSTOM HEADER DEFINITION (Used for both reports, and dynamic headers)
CUSTOM_HEADERS = {
    0: 'DID Number',  # Column A
    1: 'Date',    # Column B
    2: '1 & DID', # Column C (Used explicitly for External Report)
    3: 'Price',   # Column D
    4: 'Vendor'   # Column E
}
# Define the indices for the client-facing report (External Report)
CLIENT_COLUMNS_INDICES = [0, 1, 2] # Columns A (Number), B (Date), C (1&Number)

# ====================================================================
# --- MAIN FUNCTION ---
# ====================================================================

def find_rows_with_target_day():
    """
    Prompts user for target day, extracts data, and generates two distinct 
    reports: one full combined CSV for internal use, and multiple 
    3-column CSVs for client alerts.
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
            
    # Date Construction
    now = date.today()
    try:
        target_date_obj = date(now.year, now.month, target_day_number)
    except ValueError as e:
        print(f"❌ Error: The entered day ({target_day_number}) is not valid for the current month/year.")
        return

    DAILY_OUTPUT_DIR = target_date_obj.strftime('%Y-%m-%d')
    
    # Report tracking variables
    report = {
        'total_sheets': 0, 'total_matches': 0, 'files_created': 0,
        'sheets_processed': 0, 'sheets_skipped_empty': 0, 'sheets_skipped_error': 0,
        'internal_rows': [] # Stores ALL data rows for the Internal Report
    }
    
    print(f"\n✅ Target day number to search: **{target_day_number}**")
    print(f"📁 Output Directory (Target Date): **{DAILY_OUTPUT_DIR}**\n")
    
    # Create Directory
    try:
        os.makedirs(DAILY_OUTPUT_DIR, exist_ok=True)
    except OSError as e:
        print(f"❌ Error creating directory {DAILY_OUTPUT_DIR}: {e}")
        return
        
    try:
        # 2. Authentication and Connection
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_KEY_FILE, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(SPREADSHEET_NAME)
        all_worksheets = spreadsheet.worksheets() 
        report['total_sheets'] = len(all_worksheets)
        print(f"✅ Connected to spreadsheet '{SPREADSHEET_NAME}'. Found {report['total_sheets']} worksheets.")
        
        # 3. Loop through each worksheet
        for worksheet in all_worksheets:
            sheet_name = worksheet.title
            print(f"\n--- Searching Sheet: **{sheet_name}** ---")

            try:
                # API Call with Rate Limiting
                data = worksheet.get_all_values()
                report['sheets_processed'] += 1
                report['sheets_processed'] += 1
                
                if not data:
                    report['sheets_skipped_empty'] += 1
                    time.sleep(SLEEP_TIME_SECONDS)
                    continue
                
                matching_rows_in_sheet = []

                for row in data: 
                    if len(row) > DATE_COLUMN_INDEX:
                        sheet_date_str = row[DATE_COLUMN_INDEX].strip()
                        
                        try:
                            sheet_date_obj = datetime.strptime(sheet_date_str, SHEET_DATE_FORMAT)
                            if sheet_date_obj.day == target_day_number:
                                matching_rows_in_sheet.append(row) 
                            if sheet_date_obj.day == target_day_number:
                                matching_rows_in_sheet.append(row) 
                        except ValueError:
                            continue 

                # 4. Process Matches and Generate Reports
                if matching_rows_in_sheet:
                    match_count = len(matching_rows_in_sheet)
                    report['total_matches'] += match_count
                    print(f"🎉 Found **{match_count}** row(s) matching day **{target_day_number}**.")
                    
                    # --- 4A. PREPARE DATA FOR INTERNAL REPORT (ALL COLUMNS + Sheet Name) ---
                    for row_data in matching_rows_in_sheet:
                        # [Sheet Name, Col A, Col B, Col C, ...]
                        report['internal_rows'].append([sheet_name] + row_data) 

                    # --- 4B. GENERATE EXTERNAL CLIENT ALERT CSV (3 Columns Only) ---
                    client_filename = f"{sheet_name}_Alerts.csv"
                    client_output_file = os.path.join(DAILY_OUTPUT_DIR, client_filename)
                    
                    # 1. Filter the data to only include the required columns (A, B, C)
                    client_report_data = []
                    for row_data in matching_rows_in_sheet:
                        # Get only the data from the indices defined in CLIENT_COLUMNS_INDICES
                        filtered_row = [row_data[i] for i in CLIENT_COLUMNS_INDICES if i < len(row_data)]
                        client_report_data.append(filtered_row)
                    
                    # 2. Prepare the static 3-column header
                    client_header = [CUSTOM_HEADERS[i] for i in CLIENT_COLUMNS_INDICES]
                    
                    # 3. Write CSV
                    with open(client_output_file, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(client_header) 
                        writer.writerows(client_report_data) 
                        
                    report['files_created'] += 1
                    print(f"   (External Alert saved to: **{client_output_file}**)")
                else:
                    print("😔 No matching rows found for the target day.")

            except gspread.exceptions.APIError as e:
                report['sheets_skipped_error'] += 1
                print(f"❌ Error processing sheet '{sheet_name}': API Error (Quota exceeded likely).")
            except Exception as e:
                report['sheets_skipped_error'] += 1
                print(f"❌ Unexpected Error processing sheet '{sheet_name}': {e}")
            
            # Rate Limiting Pause
            time.sleep(SLEEP_TIME_SECONDS)
        
        # 5. --- GENERATE INTERNAL FULL REPORT (Single CSV) ---
        if report['internal_rows']:
            internal_filename = f"Internal_Full_Report_{DAILY_OUTPUT_DIR}.csv"
            internal_output_file = os.path.join(DAILY_OUTPUT_DIR, internal_filename)

            # 1. Determine the widest row and create dynamic header
            max_data_cols = max(len(row) for row in report['internal_rows']) - 1 # Subtract 1 for Sheet Name
            internal_header = ['Sheet Name'] 

            for i in range(max_data_cols):
                # Use custom header if defined, otherwise use dynamic Col Letter
                internal_header.append(CUSTOM_HEADERS.get(i, f'Column {chr(65 + i)}'))
            
            # 2. Write CSV
            with open(internal_output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(internal_header) 
                writer.writerows(report['internal_rows']) 
            
            print(f"\n✅ Generated Internal Full Report: **{internal_output_file}**")
            report['files_created'] += 1


        # 6. Final Summary (Detailed Report)
        print("\n" + "="*50)
        print("✨ **DETAILED EXECUTION REPORT** ✨")
        print("="*50)
        print(f"Target Search Day: {target_day_number} (Folder: {DAILY_OUTPUT_DIR})")
        print(f"Total Matches Found: {report['total_matches']} records")
        print(f"Total Files Created: {report['files_created']} (1 Internal + {report['files_created'] - 1} Client Alerts)")
        print("-"*50)
        print(f"Total Sheets in Spreadsheet: {report['total_sheets']}")
        print(f"Sheets Processed Successfully: {report['sheets_processed'] - report['sheets_skipped_empty'] - report['sheets_skipped_error']}")
        print(f"Sheets Skipped (Empty): {report['sheets_skipped_empty']}")
        print(f"Sheets Skipped (Error): {report['sheets_skipped_error']}")
        print("="*50)

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"\n❌ FATAL ERROR: Spreadsheet '{SPREADSHEET_NAME}' not found.")
        print(f"\n❌ FATAL ERROR: Spreadsheet '{SPREADSHEET_NAME}' not found.")
    except FileNotFoundError:
        print(f"\n❌ FATAL ERROR: Service account key file '{SERVICE_ACCOUNT_KEY_FILE}' not found.")
        print(f"\n❌ FATAL ERROR: Service account key file '{SERVICE_ACCOUNT_KEY_FILE}' not found.")
    except Exception as e:
        print(f"\n❌ UNEXPECTED FATAL ERROR: {e}")
        print(f"\n❌ UNEXPECTED FATAL ERROR: {e}")

if __name__ == '__main__':
    find_rows_with_target_day()
