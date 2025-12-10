import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import date, datetime
import csv
import os

# ====================================================================
# --- CONFIGURATION (MUST BE EDITED BEFORE RUNNING) ---
# ====================================================================

# 1. AUTHENTICATION FILE
SERVICE_ACCOUNT_KEY_FILE = 'service_account_key.json' 

# 2. SPREADSHEET DETAILS
SPREADSHEET_NAME = 'DID_Automation' 

# 3. DATE/COLUMN SETTINGS
# The index of the column containing the date (A=0, B=1, C=2, etc.)
DATE_COLUMN_INDEX = 1  # Column B
# The format used in your Google Sheet (MM/DD/YYYY) - still needed for parsing
SHEET_DATE_FORMAT = '%m/%d/%Y' 

# ====================================================================
# --- MAIN FUNCTION ---
# ====================================================================

def find_rows_with_today_date():
    """
    Authenticates, iterates through ALL worksheets, searches Column B 
    for rows where the DAY NUMBER matches the current day, and writes 
    the results for each sheet to a SEPARATE, named CSV file.
    """
    
    # Get today's day number
    today_day_number = date.today().day
    print(f"✅ Target day number to search: **{today_day_number}** (Ignoring month and year)\n")
    
    # Variable to track total matches across all sheets
    total_matches = 0
    
    try:
        # 2. Authentication
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_KEY_FILE, scope)
        client = gspread.authorize(creds)
        print("✅ Successfully authenticated with Google.")

        # 3. Open the spreadsheet and get all worksheets
        spreadsheet = client.open(SPREADSHEET_NAME)
        all_worksheets = spreadsheet.worksheets() 
        print(f"✅ Connected to spreadsheet '{SPREADSHEET_NAME}'. Found {len(all_worksheets)} worksheets to check.")
        
        # 4. Loop through each worksheet
        for worksheet in all_worksheets:
            sheet_name = worksheet.title
            print(f"\n--- Searching Client Sheet: **{sheet_name}** ---")

            try:
                data = worksheet.get_all_values()
                
                if not data:
                    print("Skipping (Sheet is empty).")
                    continue
                
                matching_rows_in_sheet = []
                for i, row in enumerate(data): 
                    row_number = i + 1 
                    
                    if len(row) > DATE_COLUMN_INDEX:
                        sheet_date_str = row[DATE_COLUMN_INDEX].strip()
                        
                        # --- DAY MATCH LOGIC ---
                        try:
                            sheet_date_obj = datetime.strptime(sheet_date_str, SHEET_DATE_FORMAT)
                            sheet_day_number = sheet_date_obj.day
                            
                            if sheet_day_number == today_day_number:
                                matching_rows_in_sheet.append((row_number, row))
                        except ValueError:
                            # Skip rows that are not in the expected date format
                            continue 

                # 5. Output Results and Write to Client-Specific CSV
                if matching_rows_in_sheet:
                    total_matches += len(matching_rows_in_sheet)
                    match_count = len(matching_rows_in_sheet)
                    print(f"🎉 Found **{match_count}** row(s) matching day **{today_day_number}**.")
                    
                    # Define output file name
                    client_output_file = f"{sheet_name}_matches_day_{today_day_number}.csv"
                    
                    # --- CSV PREPARATION AND WRITING ---
                    # Determine header based on the widest row in this sheet's matches
                    max_cols = max(len(row_data) for row_num, row_data in matching_rows_in_sheet)
                    data_cols = [f'Column {chr(65 + i)}' for i in range(max_cols)]
                    final_header = ['Row Number'] + data_cols 

                    with open(client_output_file, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(final_header) 
                        
                        # Write rows: [Row Number, Col A, Col B, Col C, ...]
                        writer.writerows([[row_num] + row_data for row_num, row_data in matching_rows_in_sheet])
                        
                    print(f"💾 Results for this client saved to: **{client_output_file}**")
                else:
                    print("😔 No matching rows found for the target day.")

            except Exception as e:
                print(f"❌ Error processing sheet '{sheet_name}': {e}")
                
        # 6. Final Summary
        print("\n================================")
        print(f"✨ **FINAL SUMMARY:** Found a total of **{total_matches}** matching rows across all clients.")
        print(f"Check your directory for the individual client CSV files.")
        print("================================")
            
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"❌ Error: Spreadsheet '{SPREADSHEET_NAME}' not found. Check the name and sharing permissions.")
    except FileNotFoundError:
        print(f"❌ Error: Service account key file '{SERVICE_ACCOUNT_KEY_FILE}' not found. Check the path.")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

if __name__ == '__main__':
    find_rows_with_today_date()


    git config --global user.name siamakanda
    git config --global user.email siamakanda5@gmail.com
