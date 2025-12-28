"""
DID Manager - Google Sheets Automation Tool
Version: 2.5 - Fixed Blank Column Names
"""

import pandas as pd
import os
import time
import gspread
import re
import logging
import shutil
from datetime import datetime
from sqlalchemy import create_engine, text
from google.oauth2.service_account import Credentials

# ===== LOGGING SETUP =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('did_manager.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===== CONFIGURATION =====
class Config:
    def __init__(self):
        self.SERVICE_ACCOUNT_KEY_FILE = 'service_account_key.json'
        self.CACHE_DB_PATH = 'cache/did_manager.db'
        self.EXPORT_DIR = 'exports'
        self.SPREADSHEET_ID = "1bY8_Oq-s2yskrWZL2NQF_JESMfOXcrmCVcKyRyNfYXc"
        self.SCOPES = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        self.API_DELAY = 1.2
        self.SHEET_DATE_FORMAT = '%m/%d/%Y'
        
    def validate(self):
        if not os.path.exists(self.SERVICE_ACCOUNT_KEY_FILE):
            raise FileNotFoundError(f"❌ Service account key not found: {self.SERVICE_ACCOUNT_KEY_FILE}")
        os.makedirs(os.path.dirname(self.CACHE_DB_PATH), exist_ok=True)
        os.makedirs(self.EXPORT_DIR, exist_ok=True)
        return True

# ===== MAIN MANAGER CLASS =====
class DIDManager:
    def __init__(self, config: Config):
        self.config = config
        self.gs_client = None
        self.db_engine = None
        self.spreadsheet = None
        
        if self.config.validate():
            self._initialize()
    
    def _initialize(self):
        try:
            creds = Credentials.from_service_account_file(
                self.config.SERVICE_ACCOUNT_KEY_FILE,
                scopes=self.config.SCOPES
            )
            self.gs_client = gspread.authorize(creds)
            self.spreadsheet = self.gs_client.open_by_key(self.config.SPREADSHEET_ID)
            self.db_engine = create_engine(f'sqlite:///{self.config.CACHE_DB_PATH}')
            logger.info("✅ DID Manager initialized")
        except Exception as e:
            logger.error(f"❌ Initialization failed: {e}")
            raise

    def sync_all_sheets(self) -> bool:
        """Sync worksheets with backup and summary"""
        try:
            if os.path.exists(self.config.CACHE_DB_PATH):
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_path = f"{self.config.CACHE_DB_PATH}.backup_{timestamp}"
                shutil.copy2(self.config.CACHE_DB_PATH, backup_path)
                print(f"📦 Backup created: {os.path.basename(backup_path)}")
            
            worksheets = self.spreadsheet.worksheets()
            stats = {"synced": 0, "skipped": 0, "failed": 0, "total_rows": 0}
            
            print(f"\n🔄 Syncing {len(worksheets)} worksheets...")
            
            for i, sheet in enumerate(worksheets, 1):
                status, row_count = self._sync_single_sheet(sheet)
                stats[status] += 1
                stats["total_rows"] += row_count
                
                status_symbol = {"synced": "✅", "skipped": "⚠️ ", "failed": "❌"}[status]
                print(f"{i:3d}/{len(worksheets)}: {sheet.title[:30]:<30} {status_symbol} ({row_count} rows)")
                
                if i % 10 == 0: 
                    print(f"   📊 Progress: {i}/{len(worksheets)} worksheets")
                time.sleep(self.config.API_DELAY)
            
            if os.path.exists(self.config.CACHE_DB_PATH):
                os.utime(self.config.CACHE_DB_PATH, None)
            
            # Display summary
            print(f"\n" + "="*40)
            print(f"{'📊 SYNC SUMMARY':^40}")
            print("="*40)
            print(f"📁 Total Worksheets:     {len(worksheets):>10}")
            print(f"✅ Successfully Synced:  {stats['synced']:>10}")
            print(f"⚠️  Skipped (Empty):      {stats['skipped']:>10}")
            print(f"❌ Failed (Errors):      {stats['failed']:>10}")
            print(f"📈 Total Rows Synced:    {stats['total_rows']:>10}")
            print("="*40)
            
            if stats['synced'] == 0:
                print("\n⚠️  Warning: No worksheets were synced successfully")
            elif stats['failed'] > 0:
                print(f"\n⚠️  Warning: {stats['failed']} worksheets failed to sync")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Sync failed: {e}")
            print(f"\n❌ Sync failed: {e}")
            return False

    def _sync_single_sheet(self, sheet) -> tuple:
        """Sync single sheet - FIXED: Ensure no blank column names"""
        title = sheet.title.strip()
        safe_name = re.sub(r'[^\w]', '_', title).strip('_').lower()
        
        try:
            data = sheet.get_all_values()
            
            # Check if sheet has any data
            if not data or not any(any(cell.strip() for cell in row) for row in data):
                logger.warning(f"Sheet '{title}' is empty")
                return "skipped", 0
            
            df = pd.DataFrame(data)
            
            # Check if first row is header
            is_header_row = False
            if len(df) > 0:
                first_row = df.iloc[0]
                # Convert to string safely
                first_row_strs = []
                for val in first_row:
                    try:
                        first_row_strs.append(str(val).lower().strip())
                    except:
                        first_row_strs.append("")
                
                # Check for header keywords
                header_keywords = ['did', 'date', 'phone', 'number', 'price', 'vendor', 'cost', 'rate']
                header_score = sum(1 for cell in first_row_strs 
                                 if any(keyword in cell for keyword in header_keywords))
                is_header_row = header_score >= 2
            
            if is_header_row:
                # Use first row as headers
                headers = []
                for idx, cell in enumerate(data[0]):
                    header = str(cell).strip()
                    if not header:  # If header is empty, create one
                        header = f"Column_{idx}"
                    headers.append(header)
                df = pd.DataFrame(data[1:], columns=headers)
            else:
                # Use numeric headers
                df.columns = [str(i) for i in range(len(df.columns))]
            
            if df.empty:
                return "skipped", 0
            
            # FIX: Ensure all column names are non-empty strings
            df = self._clean_column_names(df)
            
            # Clean and standardize
            df = self._clean_dataframe(df)
            df = self._standardize_column_names(df)
            df = df.dropna(how='all')
            
            if df.empty:
                return "skipped", 0
            
            # FIX: Check for and handle duplicate column names BEFORE saving to SQL
            df = self._fix_duplicate_columns(df)
            
            # Save to database
            with self.db_engine.begin() as conn:
                df.to_sql(safe_name, conn, if_exists='replace', index=False)
            
            logger.info(f"Synced: {title} -> {safe_name} ({len(df)} rows)")
            return "synced", len(df)
            
        except Exception as e:
            logger.error(f"Failed to sync '{title}': {e}")
            return "failed", 0

    def _clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all column names are valid, non-empty strings"""
        df = df.copy()
        
        new_columns = []
        for i, col in enumerate(df.columns):
            try:
                col_str = str(col).strip()
                if not col_str:  # If column name is empty
                    col_str = f"Column_{i}"
                # Ensure valid SQL column name (alphanumeric and underscores)
                col_str = re.sub(r'[^\w]', '_', col_str)
                # Ensure it doesn't start with a number
                if col_str[0].isdigit():
                    col_str = f"Col_{col_str}"
                new_columns.append(col_str)
            except:
                new_columns.append(f"Column_{i}")
        
        df.columns = new_columns
        return df

    def _fix_duplicate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle duplicate column names by appending numbers"""
        df = df.copy()
        columns = df.columns.tolist()
        
        # Track counts of each column name
        counts = {}
        new_columns = []
        
        for col in columns:
            if col not in counts:
                counts[col] = 1
                new_columns.append(col)
            else:
                counts[col] += 1
                new_columns.append(f"{col}_{counts[col]}")
        
        df.columns = new_columns
        return df

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean dataframe values"""
        df = df.copy()
        
        for col in df.columns:
            try:
                # Convert to string safely
                df[col] = df[col].apply(lambda x: str(x).strip() if pd.notna(x) else "")
            except:
                # If conversion fails, leave as is
                pass
        
        return df

    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names to common format"""
        column_map = {}
        
        for col in df.columns:
            col_str = str(col).lower().strip()
            
            if 'did' in col_str and 'plus' not in col_str and 'ext' not in col_str:
                column_map[col] = 'DID'
            elif 'date' in col_str:
                column_map[col] = 'Date'
            elif 'plus' in col_str:
                column_map[col] = 'DID_Plus'
            elif any(x in col_str for x in ['price', 'rate', 'cost', '$']):
                column_map[col] = 'Price'
            elif any(x in col_str for x in ['vendor', 'provider', 'carrier']):
                column_map[col] = 'Vendor'
        
        if column_map:
            df = df.rename(columns=column_map)
        
        return df

    def list_clients(self):
        """Display all clients with statistics"""
        try:
            with self.db_engine.connect() as conn:
                tables = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                ).fetchall()
                
                if not tables:
                    print("❌ No data found. Run Sync first.")
                    return
                
                print(f"\n{'CLIENT NAME':<35} | {'ROWS':<8} | {'SAMPLE DATA':<30}")
                print("-" * 80)
                
                for t in tables:
                    t_name = t[0]
                    try:
                        count = conn.execute(text(f'SELECT COUNT(*) FROM "{t_name}"')).scalar()
                        sample = conn.execute(text(f'SELECT * FROM "{t_name}" LIMIT 1')).fetchone()
                        
                        status = "🟢" if count > 0 else "⚪"
                        
                        if sample:
                            # Show first 3 non-empty values
                            preview_parts = []
                            for val in sample[:3]:
                                if val and str(val).strip() and str(val).lower() not in ['nan', 'none', 'null']:
                                    preview_parts.append(str(val)[:15])
                            preview = " | ".join(preview_parts) if preview_parts else "Has data"
                        else:
                            preview = "Empty"
                        
                        print(f"{status} {t_name:<33} | {count:<8} | {preview}")
                    except Exception:
                        print(f"❌ {t_name:<33} | ERROR")
                
                print(f"\n📊 Total: {len(tables)} clients")
                
        except Exception as e:
            print(f"❌ Error listing clients: {e}")

    def filter_by_day(self, day: int):
        """Filter records by day of month"""
        if not 1 <= day <= 31:
            print("❌ Day must be between 1 and 31")
            return
        
        print(f"\n📅 Filtering records for day {day}...")
        start_time = time.time()
        results = []
        client_hits = {}
        
        try:
            with self.db_engine.connect() as conn:
                tables = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table'")
                ).fetchall()
                
                for t in tables:
                    table_name = t[0]
                    try:
                        df = pd.read_sql(text(f'SELECT * FROM "{table_name}"'), conn)
                        
                        if df.empty:
                            continue
                        
                        # Find date column
                        date_col = self._find_date_column(df)
                        if not date_col:
                            continue
                        
                        # Parse dates
                        dates = pd.to_datetime(
                            df[date_col].astype(str).str.strip(),
                            format=self.config.SHEET_DATE_FORMAT,
                            errors='coerce'
                        )
                        
                        # Fallback to other formats if needed
                        if dates.isna().all():
                            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m-%d-%Y', '%d-%m-%Y']:
                                dates = pd.to_datetime(df[date_col], format=fmt, errors='coerce')
                                if not dates.isna().all():
                                    break
                        
                        matches = df[dates.dt.day == day].copy()
                        
                        if not matches.empty:
                            # Select only relevant columns for display
                            display_cols = []
                            for col in matches.columns:
                                col_lower = str(col).lower()
                                if any(keyword in col_lower for keyword in 
                                      ['did', 'date', 'price', 'vendor', '0', '1', '2', '3', '4']):
                                    display_cols.append(col)
                            
                            if display_cols:
                                matches_display = matches[display_cols].copy()
                                matches_display.insert(0, 'Client', table_name)
                                results.append(matches_display)
                                client_hits[table_name] = len(matches)
                                
                    except Exception:
                        continue
            
            # Display results
            if results:
                final_df = pd.concat(results, ignore_index=True)
                elapsed_time = time.time() - start_time
                
                print(f"\n✅ Found {len(final_df)} record(s) in {len(client_hits)} client(s)")
                print(f"⏱️  Completed in {elapsed_time:.2f} seconds")
                print("=" * 100)
                
                # Clean column names for display
                final_df.columns = [self._clean_display_column(col) for col in final_df.columns]
                
                # Remove completely empty columns
                final_df = final_df.loc[:, (final_df != '').any(axis=0)]
                final_df = final_df.dropna(axis=1, how='all')
                
                # Show results
                print(final_df.to_string(index=False, max_rows=50))
                
                # Summary
                print(f"\n📊 Summary by client:")
                for client, count in sorted(client_hits.items(), key=lambda x: x[1], reverse=True):
                    print(f"  • {client}: {count} records")
                    
            else:
                print(f"❌ No records found for day {day}")
                
        except Exception as e:
            print(f"❌ Filter failed: {e}")

    def _find_date_column(self, df: pd.DataFrame) -> str:
        """Find date column in dataframe"""
        # Check for standard names
        for col in df.columns:
            col_str = str(col).lower()
            if 'date' in col_str:
                return col
        
        # Check for common column indices (B = 1, C = 2, etc.)
        if '1' in df.columns:  # Column B
            return '1'
        elif '2' in df.columns:  # Column C
            return '2'
        
        # Try to identify by content pattern
        for col in df.columns:
            sample = df[col].dropna().head(5)
            if len(sample) > 0:
                sample_str = sample.astype(str)
                if any('/' in str(val) for val in sample_str):
                    return col
        
        return None

    def _clean_display_column(self, col_name: str) -> str:
        """Clean column name for display"""
        col_str = str(col_name)
        
        # Map numeric columns to meaningful names
        col_map = {
            '0': 'DID',
            '1': 'Date',
            '2': 'DID_Plus',
            '3': 'Price',
            '4': 'Vendor',
            '5': 'Status',
            '6': 'Notes'
        }
        
        if col_str in col_map:
            return col_map[col_str]
        
        # Clean other column names
        if col_str.startswith('_'):
            return col_str[1:].replace('_', ' ').title()
        else:
            return col_str.replace('_', ' ').title()

    def search_by_did(self, term: str):
        """Search for phone number across all clients"""
        if not term.strip():
            print("❌ Please enter a search term")
            return
        
        print(f"\n🔍 Searching for '{term}'...")
        results = []
        
        with self.db_engine.connect() as conn:
            tables = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table'")
            ).fetchall()
            
            for t in tables:
                try:
                    df = pd.read_sql(text(f'SELECT * FROM "{t[0]}"'), conn)
                    
                    # Search in all columns
                    mask = False
                    for col in df.columns:
                        try:
                            mask = mask | df[col].astype(str).str.contains(term, case=False, na=False)
                        except:
                            continue
                    
                    matches = df[mask].copy()
                    if not matches.empty:
                        matches.insert(0, 'Client', t[0])
                        results.append(matches)
                        
                except Exception:
                    continue
        
        if results:
            final = pd.concat(results, ignore_index=True)
            
            # Clean display
            final.columns = [self._clean_display_column(col) for col in final.columns]
            final = final.loc[:, (final != '').any(axis=0)]
            final = final.dropna(axis=1, how='all')
            
            print(f"\n✅ Found {len(final)} match(es):")
            print("=" * 80)
            print(final.to_string(index=False, max_rows=50))
        else:
            print("❌ No matches found.")

    def export_client_data(self, client_name: str):
        """Export client data to CSV"""
        if not client_name.strip():
            print("❌ Please enter a client name")
            return
        
        safe_name = re.sub(r'[^\w]', '_', client_name).strip('_').lower()
        
        try:
            with self.db_engine.connect() as conn:
                # Check if table exists
                exists = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name=:name"),
                    {'name': safe_name}
                ).fetchone()
                
                if not exists:
                    print(f"❌ Client '{client_name}' not found in cache")
                    return
                
                df = pd.read_sql(text(f'SELECT * FROM "{safe_name}"'), conn)
                
                if df.empty:
                    print(f"❌ Client '{client_name}' has no data")
                    return
                
                # Clean column names for export
                df.columns = [self._clean_display_column(col) for col in df.columns]
                
                # Create export directory
                os.makedirs(self.config.EXPORT_DIR, exist_ok=True)
                
                # Generate filename
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"export_{safe_name}_{timestamp}.csv"
                filepath = os.path.join(self.config.EXPORT_DIR, filename)
                
                # Export
                df.to_csv(filepath, index=False, encoding='utf-8')
                
                # Display success
                file_size = os.path.getsize(filepath) / 1024
                
                print(f"\n✅ Export successful!")
                print(f"📁 File: {filename}")
                print(f"📊 Records: {len(df)} rows, {len(df.columns)} columns")
                print(f"💾 Size: {file_size:.1f} KB")
                print(f"📍 Location: {os.path.abspath(filepath)}")
                
                # Show preview with proper column names
                print(f"\n📋 Preview (first 3 rows):")
                print("=" * 80)
                print(df.head(3).to_string(index=False))
                
        except Exception as e:
            print(f"❌ Export failed: {e}")

    def display_menu(self):
        """Display interactive menu"""
        while True:
            # Cache status
            cache_status = "⚪ No cache"
            if os.path.exists(self.config.CACHE_DB_PATH):
                age_min = (time.time() - os.path.getmtime(self.config.CACHE_DB_PATH)) / 60
                if age_min < 30:
                    cache_status = f"🟢 Fresh ({int(age_min)}m ago)"
                elif age_min < 1440:
                    cache_status = f"🟡 Stale ({int(age_min)}m ago)"
                else:
                    cache_status = f"🔴 Outdated ({int(age_min/60)}h ago)"
            
            print("\n" + "="*50)
            print(f"{'📞 DID MANAGER v2.5':^50}")
            print(f"{cache_status:^50}")
            print("="*50)
            print("1. 📋 List All Clients")
            print("2. 🔄 Sync from Google Sheets")
            print("3. 🔍 Search by DID/Phone")
            print("4. 📅 Today's Records")
            print("5. 🗓️ Filter by Day (1-31)")
            print("6. 💾 Export Client Data")
            print("7. 📊 Cache Statistics")
            print("q. 🚪 Quit")
            
            choice = input("\nSelect option: ").strip().lower()
            
            if choice == '1':
                self.list_clients()
            elif choice == '2':
                self.sync_all_sheets()
            elif choice == '3':
                term = input("Enter DID/Phone to search: ").strip()
                if term:
                    self.search_by_did(term)
                else:
                    print("❌ Please enter a search term")
            elif choice == '4':
                today = datetime.now().day
                print(f"\n📅 Today is day {today}")
                self.filter_by_day(today)
            elif choice == '5':
                try:
                    day = int(input("Enter day (1-31): ").strip())
                    if 1 <= day <= 31:
                        self.filter_by_day(day)
                    else:
                        print("❌ Day must be between 1 and 31")
                except ValueError:
                    print("❌ Please enter a valid number")
            elif choice == '6':
                client = input("Enter client name to export: ").strip()
                if client:
                    self.export_client_data(client)
                else:
                    print("❌ Please enter a client name")
            elif choice == '7':
                self._show_cache_stats()
            elif choice == 'q':
                print("\n👋 Goodbye!")
                break
            else:
                print("❌ Invalid option")
            
            input("\nPress Enter to continue...")

    def _show_cache_stats(self):
        """Show cache statistics"""
        if not os.path.exists(self.config.CACHE_DB_PATH):
            print("❌ No cache database found")
            return
        
        try:
            with self.db_engine.connect() as conn:
                tables = conn.execute(
                    text("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                ).scalar() or 0
                
                # Count total rows
                total_rows = 0
                if tables > 0:
                    table_list = conn.execute(
                        text("SELECT name FROM sqlite_master WHERE type='table'")
                    ).fetchall()
                    
                    for table in table_list:
                        try:
                            count = conn.execute(text(f'SELECT COUNT(*) FROM "{table[0]}"')).scalar() or 0
                            total_rows += count
                        except:
                            continue
                
                file_size = os.path.getsize(self.config.CACHE_DB_PATH) / 1024
                mod_time = datetime.fromtimestamp(os.path.getmtime(self.config.CACHE_DB_PATH))
                
                print(f"\n📊 CACHE STATISTICS")
                print("="*40)
                print(f"Last Modified: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Total Clients: {tables}")
                print(f"Total Records: {total_rows:,}")
                print(f"Database Size: {file_size:.1f} KB")
                print("="*40)
                
        except Exception as e:
            print(f"❌ Error getting cache stats: {e}")

def main():
    """Main entry point"""
    print("\n" + "="*50)
    print(f"{'📞 DID MANAGER v2.5':^50}")
    print("="*50)
    
    try:
        manager = DIDManager(Config())
        manager.display_menu()
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        print("Please check your configuration and try again.")

if __name__ == "__main__":
    main()