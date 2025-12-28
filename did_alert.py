"""
DID Manager - Google Sheets Automation Tool
Version: 2.3
Author: Business Automation System
Description: Sync, search, and manage DID data from Google Sheets
"""

import pandas as pd
import os
import time
import gspread
import re
import logging
import shutil
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from google.oauth2.service_account import Credentials

# ===== LOGGING SETUP =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('did_manager.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===== CONFIGURATION =====
class Config:
    """Configuration manager for DID Manager"""
    
    def __init__(self):
        # File paths
        self.SERVICE_ACCOUNT_KEY_FILE = 'service_account_key.json'
        self.CACHE_DB_PATH = 'cache/did_manager.db'
        self.EXPORT_DIR = 'exports'
        
        # Google Sheets
        self.SPREADSHEET_ID = "1bY8_Oq-s2yskrWZL2NQF_JESMfOXcrmCVcKyRyNfYXc"
        self.SCOPES = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # Performance
        self.API_DELAY = 1.2  # seconds between API calls
        self.BATCH_SIZE = 100  # rows per batch insert
        
        # Data format
        self.SHEET_DATE_FORMAT = '%m/%d/%Y'
        self.DATE_COLUMN_INDEX = 1  # Column B
        
        # Validation
        self.MIN_ROWS_FOR_SYNC = 1
        self.MAX_COLUMNS = 20
        
    def validate(self):
        """Validate configuration on startup"""
        if not os.path.exists(self.SERVICE_ACCOUNT_KEY_FILE):
            raise FileNotFoundError(f"❌ Service account key not found: {self.SERVICE_ACCOUNT_KEY_FILE}")
        
        # Create necessary directories
        os.makedirs(os.path.dirname(self.CACHE_DB_PATH), exist_ok=True)
        os.makedirs(self.EXPORT_DIR, exist_ok=True)
        
        logger.info("Configuration validated successfully")
        return True

# ===== MAIN MANAGER CLASS =====
class DIDManager:
    """Main manager class for DID operations"""
    
    def __init__(self, config: Config):
        self.config = config
        self.gs_client = None
        self.db_engine = None
        self.spreadsheet = None
        self._total_synced_rows = 0
        
        if self.config.validate():
            self._initialize()
    
    def _initialize(self):
        """Initialize all components"""
        try:
            # Initialize Google Sheets client
            creds = Credentials.from_service_account_file(
                self.config.SERVICE_ACCOUNT_KEY_FILE,
                scopes=self.config.SCOPES
            )
            self.gs_client = gspread.authorize(creds)
            self.spreadsheet = self.gs_client.open_by_key(self.config.SPREADSHEET_ID)
            
            # Initialize database with connection pooling
            self.db_engine = create_engine(
                f'sqlite:///{self.config.CACHE_DB_PATH}',
                pool_pre_ping=True,
                connect_args={'timeout': 30}
            )
            
            logger.info("✅ DID Manager initialized successfully")
        except Exception as e:
            logger.error(f"❌ Initialization failed: {e}")
            raise

    def sync_all_sheets(self) -> bool:
        """Sync all worksheets from Google Sheets to local cache"""
        try:
            # Create backup of existing database
            if os.path.exists(self.config.CACHE_DB_PATH):
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_path = f"{self.config.CACHE_DB_PATH}.backup_{timestamp}"
                shutil.copy2(self.config.CACHE_DB_PATH, backup_path)
                print(f"📦 Backup created: {os.path.basename(backup_path)}")
            
            worksheets = self.spreadsheet.worksheets()
            total_worksheets = len(worksheets)
            
            if total_worksheets == 0:
                print("❌ No worksheets found in the spreadsheet")
                return False
            
            stats = {
                "synced": 0,
                "skipped": 0,
                "failed": 0,
                "total_rows": 0
            }
            
            print(f"\n🔄 Syncing {total_worksheets} worksheets...")
            
            for i, sheet in enumerate(worksheets, 1):
                sheet_title = sheet.title.strip()
                print(f"\n{i:3d}/{total_worksheets}: Processing '{sheet_title}'...", end="")
                
                status, row_count = self._sync_single_sheet(sheet)
                stats[status] += 1
                stats["total_rows"] += row_count
                
                status_symbol = {
                    "synced": "✅",
                    "skipped": "⚠️ ",
                    "failed": "❌"
                }[status]
                
                print(f" {status_symbol} ({row_count} rows)")
                
                if i % 10 == 0:
                    print(f"   📊 Progress: {i}/{total_worksheets} worksheets")
                
                time.sleep(self.config.API_DELAY)
            
            # Update cache timestamp
            if os.path.exists(self.config.CACHE_DB_PATH):
                os.utime(self.config.CACHE_DB_PATH, None)
            
            # Display summary
            self._display_sync_summary(stats, total_worksheets)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Sync failed: {e}")
            print(f"\n❌ Sync failed with error: {e}")
            return False

    def _sync_single_sheet(self, sheet) -> tuple:
        """Sync a single worksheet to database"""
        sheet_title = sheet.title.strip()
        safe_table_name = self._sanitize_table_name(sheet_title)
        
        try:
            # Get data from Google Sheets
            data = sheet.get_all_values()
            
            # Check if sheet has meaningful data
            if not data or not any(any(cell.strip() for cell in row) for row in data):
                logger.warning(f"Sheet '{sheet_title}' is empty")
                return "skipped", 0
            
            # Create DataFrame
            df = pd.DataFrame(data)
            
            # Detect if first row contains headers
            first_row = df.iloc[0] if len(df) > 0 else pd.Series()
            has_header = self._detect_header_row(first_row)
            
            if has_header:
                # Use first row as headers
                df.columns = [self._clean_column_name(str(col)) for col in data[0]]
                df = df.iloc[1:].reset_index(drop=True)
            else:
                # Use numeric column names
                df.columns = [str(i) for i in range(len(df.columns))]
            
            # Skip if no data after header removal
            if df.empty:
                return "skipped", 0
            
            # Clean and standardize data
            df = self._clean_dataframe(df)
            
            # Apply column name standardization
            df = self._standardize_column_names(df)
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            if df.empty:
                return "skipped", 0
            
            # Save to database
            with self.db_engine.begin() as conn:
                df.to_sql(
                    safe_table_name,
                    conn,
                    if_exists='replace',
                    index=False,
                    chunksize=self.config.BATCH_SIZE
                )
            
            logger.info(f"Synced: {sheet_title} -> {safe_table_name} ({len(df)} rows)")
            return "synced", len(df)
            
        except Exception as e:
            logger.error(f"Failed to sync '{sheet_title}': {e}")
            return "failed", 0

    def _detect_header_row(self, first_row: pd.Series) -> bool:
        """Detect if first row contains column headers"""
        if len(first_row) == 0:
            return False
        
        # Check for common header patterns
        header_keywords = ['did', 'date', 'phone', 'number', 'price', 'vendor', 'cost', 'rate']
        first_row_str = first_row.astype(str).str.lower().str.strip()
        
        # Count how many cells contain header keywords
        header_score = sum(1 for cell in first_row_str 
                          if any(keyword in cell for keyword in header_keywords))
        
        # If at least 2 cells look like headers, assume it's a header row
        return header_score >= 2

    def _clean_column_name(self, col_name: str) -> str:
        """Clean column names for SQL compatibility"""
        # Remove special characters, keep underscores
        cleaned = re.sub(r'[^\w\s]', ' ', col_name)
        # Replace multiple spaces with single underscore
        cleaned = re.sub(r'\s+', '_', cleaned.strip())
        # Ensure it starts with a letter or underscore
        if cleaned and not cleaned[0].isalpha() and cleaned[0] != '_':
            cleaned = '_' + cleaned
        return cleaned.lower()

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize dataframe data"""
        df = df.copy()
        
        # Clean each column
        for col in df.columns:
            # Convert to string and strip whitespace
            df[col] = df[col].astype(str).str.strip()
            
            # Handle empty strings
            df[col] = df[col].replace(['', 'nan', 'None', 'null'], pd.NA)
        
        return df

    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names to consistent format"""
        column_mapping = {}
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            
            # Map to standard column names
            if 'did' in col_lower and 'plus' not in col_lower:
                column_mapping[col] = 'DID'
            elif 'date' in col_lower:
                column_mapping[col] = 'Date'
            elif 'plus' in col_lower or 'did_plus' in col_lower:
                column_mapping[col] = 'DID_Plus'
            elif any(keyword in col_lower for keyword in ['price', 'cost', 'rate', 'amount']):
                column_mapping[col] = 'Price'
            elif any(keyword in col_lower for keyword in ['vendor', 'provider', 'carrier']):
                column_mapping[col] = 'Vendor'
        
        if column_mapping:
            df = df.rename(columns=column_mapping)
        
        return df

    @staticmethod
    def _sanitize_table_name(name: str) -> str:
        """Sanitize table name for SQLite"""
        # Remove non-alphanumeric characters
        name = re.sub(r'[^\w]', '_', name)
        # Remove multiple underscores
        name = re.sub(r'_+', '_', name)
        # Remove leading/trailing underscores
        name = name.strip('_')
        # Ensure it starts with a letter
        if name and not name[0].isalpha():
            name = 'table_' + name
        return name.lower()[:63]  # SQLite table name limit

    def _display_sync_summary(self, stats: dict, total_worksheets: int):
        """Display sync summary report"""
        print(f"\n" + "="*50)
        print(f"{'📊 SYNC SUMMARY':^50}")
        print("="*50)
        print(f"📁 Total Worksheets:     {total_worksheets:>10}")
        print(f"✅ Successfully Synced:  {stats['synced']:>10}")
        print(f"⚠️  Skipped (Empty):      {stats['skipped']:>10}")
        print(f"❌ Failed (Errors):      {stats['failed']:>10}")
        print(f"📈 Total Rows Synced:    {stats['total_rows']:>10}")
        print("="*50)
        
        if stats['synced'] == 0:
            print("\n⚠️  Warning: No worksheets were synced successfully")
        elif stats['failed'] > 0:
            print(f"\n⚠️  Warning: {stats['failed']} worksheets failed to sync. Check logs.")

    def list_clients(self):
        """Display all cached clients with statistics"""
        try:
            with self.db_engine.connect() as conn:
                # Get all table names
                tables_result = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                )
                tables = [row[0] for row in tables_result.fetchall()]
                
                if not tables:
                    print("❌ No data found. Please run Sync first.")
                    return
                
                print(f"\n{'CLIENT NAME':<35} | {'ROWS':<8} | {'SAMPLE DATA':<30}")
                print("-" * 80)
                
                for table_name in tables:
                    try:
                        # Get row count
                        count_result = conn.execute(
                            text(f'SELECT COUNT(*) FROM "{table_name}"')
                        ).scalar()
                        
                        # Get a sample row
                        sample_result = conn.execute(
                            text(f'SELECT * FROM "{table_name}" LIMIT 1')
                        ).fetchone()
                        
                        # Format status indicator
                        if count_result == 0:
                            status = "⚪"
                            preview = "Empty"
                        elif count_result < 10:
                            status = "🟡"
                            preview = self._format_sample_preview(sample_result)
                        else:
                            status = "🟢"
                            preview = self._format_sample_preview(sample_result)
                        
                        print(f"{status} {table_name:<33} | {count_result:<8} | {preview}")
                        
                    except Exception as e:
                        print(f"❌ {table_name:<33} | ERROR: {str(e)[:20]}")
                
                print(f"\n📊 Total: {len(tables)} clients")
                
        except Exception as e:
            print(f"❌ Error listing clients: {e}")

    @staticmethod
    def _format_sample_preview(sample_row) -> str:
        """Format sample row for display"""
        if not sample_row:
            return "No data"
        
        try:
            # Take first 3 non-empty values
            preview_parts = []
            for val in sample_row[:5]:  # Check first 5 columns
                if val and str(val).strip() and str(val).lower() not in ['nan', 'none', 'null']:
                    preview_parts.append(str(val)[:15])
                    if len(preview_parts) >= 3:
                        break
            
            if preview_parts:
                return " | ".join(preview_parts)
            else:
                return "Has data (empty preview)"
        except:
            return "Error reading"

    def search_by_did(self, search_term: str):
        """Search for phone number across all clients"""
        if not search_term.strip():
            print("❌ Please enter a search term")
            return
        
        print(f"\n🔍 Searching for '{search_term}' across all clients...")
        start_time = time.time()
        
        results = []
        client_hits = 0
        
        try:
            with self.db_engine.connect() as conn:
                # Get all table names
                tables_result = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table'")
                )
                tables = [row[0] for row in tables_result.fetchall()]
                
                if not tables:
                    print("❌ No data available. Run Sync first.")
                    return
                
                for table_name in tables:
                    try:
                        # Read table data
                        df = pd.read_sql_query(
                            text(f'SELECT * FROM "{table_name}"'),
                            conn
                        )
                        
                        if df.empty:
                            continue
                        
                        # Search across all string columns
                        mask = pd.Series(False, index=df.index)
                        for col in df.columns:
                            mask = mask | df[col].astype(str).str.contains(
                                search_term, case=False, na=False
                            )
                        
                        matches = df[mask].copy()
                        
                        if not matches.empty:
                            matches.insert(0, 'Client', table_name)
                            results.append(matches)
                            client_hits += 1
                            
                    except Exception as e:
                        logger.debug(f"Search in '{table_name}' failed: {e}")
                        continue
            
            # Combine and display results
            if results:
                combined_df = pd.concat(results, ignore_index=True)
                elapsed_time = time.time() - start_time
                
                print(f"\n✅ Found {len(combined_df)} match(es) in {client_hits} client(s)")
                print(f"⏱️  Search completed in {elapsed_time:.2f} seconds")
                print("=" * 100)
                
                # Clean display - remove empty columns
                combined_df = combined_df.loc[:, (combined_df != '').any(axis=0)]
                combined_df = combined_df.dropna(axis=1, how='all')
                
                # Display results
                print(combined_df.to_string(index=False, max_rows=50))
                
                # Summary
                if len(combined_df) > 50:
                    print(f"\n📝 Showing 50 of {len(combined_df)} results. Use export for full data.")
                
            else:
                print(f"\n❌ No matches found for '{search_term}'")
                
        except Exception as e:
            print(f"❌ Search failed: {e}")

    def filter_by_day(self, day: int):
        """Filter records by day of the month"""
        if not 1 <= day <= 31:
            print("❌ Day must be between 1 and 31")
            return
        
        print(f"\n📅 Filtering records for day {day}...")
        start_time = time.time()
        
        results = []
        client_hits = 0
        
        try:
            with self.db_engine.connect() as conn:
                tables_result = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table'")
                )
                tables = [row[0] for row in tables_result.fetchall()]
                
                for table_name in tables:
                    try:
                        df = pd.read_sql_query(
                            text(f'SELECT * FROM "{table_name}"'),
                            conn
                        )
                        
                        if df.empty:
                            continue
                        
                        # Find date column
                        date_column = self._find_date_column(df)
                        if not date_column:
                            continue
                        
                        # Parse dates with multiple format attempts
                        parsed_dates = self._parse_dates(df[date_column])
                        
                        if parsed_dates is None:
                            continue
                        
                        # Filter by day
                        mask = parsed_dates.dt.day == day
                        matches = df[mask].copy()
                        
                        if not matches.empty:
                            matches.insert(0, 'Client', table_name)
                            # Add parsed date for display
                            matches['Parsed_Date'] = parsed_dates[mask].dt.strftime('%Y-%m-%d')
                            results.append(matches)
                            client_hits += 1
                            
                    except Exception as e:
                        logger.debug(f"Filtering '{table_name}' failed: {e}")
                        continue
            
            # Display results
            if results:
                combined_df = pd.concat(results, ignore_index=True)
                elapsed_time = time.time() - start_time
                
                print(f"\n✅ Found {len(combined_df)} record(s) in {client_hits} client(s)")
                print(f"⏱️  Filter completed in {elapsed_time:.2f} seconds")
                print("=" * 100)
                
                # Sort by client and date
                if 'Parsed_Date' in combined_df.columns:
                    combined_df = combined_df.sort_values(['Client', 'Parsed_Date'])
                
                # Display
                display_cols = [col for col in combined_df.columns if col != 'Parsed_Date']
                print(combined_df[display_cols].to_string(index=False, max_rows=100))
                
                # Summary
                print(f"\n📊 Summary: Day {day} has {len(combined_df)} records across {client_hits} clients")
                
            else:
                print(f"\n❌ No records found for day {day}")
                
        except Exception as e:
            print(f"❌ Filter failed: {e}")

    def _find_date_column(self, df: pd.DataFrame) -> str:
        """Find date column in dataframe"""
        # Check for standardized column names first
        if 'Date' in df.columns:
            return 'Date'
        
        # Check for date-like column names
        for col in df.columns:
            col_lower = str(col).lower()
            if any(keyword in col_lower for keyword in ['date', 'time', 'day']):
                return col
        
        # Check column index 1 (Column B in sheets)
        if str(self.config.DATE_COLUMN_INDEX) in df.columns:
            return str(self.config.DATE_COLUMN_INDEX)
        
        # Try to find by data pattern
        for col in df.columns:
            sample = df[col].dropna().head(5)
            if len(sample) > 0 and any('/' in str(val) or '-' in str(val) for val in sample):
                return col
        
        return None

    def _parse_dates(self, date_series: pd.Series) -> pd.Series:
        """Parse dates with multiple format attempts"""
        if date_series.empty:
            return None
        
        # Try configured format first
        try:
            dates = pd.to_datetime(
                date_series.astype(str).str.strip(),
                format=self.config.SHEET_DATE_FORMAT,
                errors='coerce'
            )
            if not dates.isna().all():
                return dates
        except:
            pass
        
        # Try common formats
        formats_to_try = [
            '%m/%d/%Y', '%m-%d-%Y', '%Y/%m/%d', '%Y-%m-%d',
            '%d/%m/%Y', '%d-%m-%Y', '%m/%d/%y', '%m-%d-%y'
        ]
        
        for fmt in formats_to_try:
            try:
                dates = pd.to_datetime(date_series, format=fmt, errors='coerce')
                if not dates.isna().all():
                    return dates
            except:
                continue
        
        # Final attempt with pandas auto-detection
        return pd.to_datetime(date_series, errors='coerce')

    def export_client_data(self, client_name: str, format: str = 'csv'):
        """Export client data to file"""
        if not client_name.strip():
            print("❌ Please enter a client name")
            return
        
        safe_name = self._sanitize_table_name(client_name)
        
        try:
            with self.db_engine.connect() as conn:
                # Check if table exists
                exists_result = conn.execute(
                    text("SELECT name FROM sqlite_master WHERE type='table' AND name=:name"),
                    {'name': safe_name}
                ).fetchone()
                
                if not exists_result:
                    print(f"❌ Client '{client_name}' not found in cache")
                    return
                
                # Read data
                df = pd.read_sql_query(
                    text(f'SELECT * FROM "{safe_name}"'),
                    conn
                )
                
                if df.empty:
                    print(f"❌ Client '{client_name}' has no data")
                    return
                
                # Clean column names for export
                df.columns = [str(col).replace('_', ' ').title() for col in df.columns]
                
                # Generate filename
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"export_{safe_name}_{timestamp}.{format}"
                filepath = os.path.join(self.config.EXPORT_DIR, filename)
                
                # Export based on format
                if format.lower() == 'csv':
                    df.to_csv(filepath, index=False, encoding='utf-8')
                elif format.lower() == 'excel':
                    df.to_excel(filepath, index=False)
                else:
                    print(f"❌ Unsupported format: {format}")
                    return
                
                # Display success message
                file_size = os.path.getsize(filepath) / 1024  # KB
                
                print(f"\n✅ Export successful!")
                print(f"📁 File: {filename}")
                print(f"📊 Records: {len(df)} rows, {len(df.columns)} columns")
                print(f"💾 Size: {file_size:.1f} KB")
                print(f"📍 Location: {os.path.abspath(filepath)}")
                
                # Show preview
                print(f"\n📋 Preview (first 3 rows):")
                print("=" * 80)
                print(df.head(3).to_string(index=False))
                
        except Exception as e:
            print(f"❌ Export failed: {e}")

    def get_cache_status(self) -> dict:
        """Get cache status information"""
        status = {
            'exists': False,
            'age_minutes': None,
            'table_count': 0,
            'total_rows': 0,
            'last_modified': None
        }
        
        if not os.path.exists(self.config.CACHE_DB_PATH):
            return status
        
        status['exists'] = True
        status['last_modified'] = datetime.fromtimestamp(
            os.path.getmtime(self.config.CACHE_DB_PATH)
        )
        
        age_seconds = time.time() - os.path.getmtime(self.config.CACHE_DB_PATH)
        status['age_minutes'] = int(age_seconds / 60)
        
        try:
            with self.db_engine.connect() as conn:
                # Count tables
                tables_result = conn.execute(
                    text("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                )
                status['table_count'] = tables_result.scalar() or 0
                
                # Count total rows (approximate)
                if status['table_count'] > 0:
                    tables = conn.execute(
                        text("SELECT name FROM sqlite_master WHERE type='table'")
                    ).fetchall()
                    
                    total_rows = 0
                    for table in tables:
                        try:
                            count_result = conn.execute(
                                text(f'SELECT COUNT(*) FROM "{table[0]}"')
                            )
                            total_rows += count_result.scalar() or 0
                        except:
                            continue
                    
                    status['total_rows'] = total_rows
                    
        except Exception as e:
            logger.warning(f"Could not get cache stats: {e}")
        
        return status

    def display_menu(self):
        """Display interactive menu system"""
        while True:
            # Get cache status
            cache_status = self.get_cache_status()
            
            # Build status line
            if not cache_status['exists']:
                status_line = "⚪ NO CACHE - Run Sync first"
            else:
                age = cache_status['age_minutes']
                if age < 30:
                    status_line = f"🟢 FRESH ({age}m ago, {cache_status['table_count']} clients)"
                elif age < 1440:  # 24 hours
                    status_line = f"🟡 STALE ({age}m ago, {cache_status['table_count']} clients)"
                else:
                    status_line = f"🔴 OUTDATED ({age//60}h ago, {cache_status['table_count']} clients)"
            
            print("\n" + "="*60)
            print(f"{'📞 DID MANAGER v2.3':^60}")
            print(f"{status_line:^60}")
            print("="*60)
            print("1. 📋  List All Clients")
            print("2. 🔄  Sync from Google Sheets")
            print("3. 🔍  Search by DID/Phone Number")
            print("4. 📅  Today's Records")
            print("5. 🗓️  Filter by Day (1-31)")
            print("6. 💾  Export Client Data to CSV")
            print("7. 📊  Cache Statistics")
            print("8. 🗑️  Clear Cache")
            print("q. 🚪  Quit")
            print("="*60)
            
            choice = input("\nSelect option: ").strip().lower()
            
            if choice == '1':
                self.list_clients()
            elif choice == '2':
                self.sync_all_sheets()
            elif choice == '3':
                search_term = input("Enter DID/Phone to search: ").strip()
                if search_term:
                    self.search_by_did(search_term)
                else:
                    print("❌ Please enter a search term")
            elif choice == '4':
                today = datetime.now().day
                print(f"\n📅 Today is day {today} of the month")
                self.filter_by_day(today)
            elif choice == '5':
                try:
                    day = int(input("Enter day number (1-31): ").strip())
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
                self.display_cache_statistics()
            elif choice == '8':
                self.clear_cache()
            elif choice == 'q':
                print("\n👋 Goodbye! Thank you for using DID Manager.")
                break
            else:
                print("❌ Invalid option. Please try again.")
            
            input("\nPress Enter to continue...")

    def display_cache_statistics(self):
        """Display detailed cache statistics"""
        cache_status = self.get_cache_status()
        
        print("\n" + "="*50)
        print(f"{'📊 CACHE STATISTICS':^50}")
        print("="*50)
        
        if not cache_status['exists']:
            print("❌ No cache database found")
            print("   Run 'Sync from Google Sheets' to create cache")
            return
        
        print(f"Last Modified:    {cache_status['last_modified'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Age:              {cache_status['age_minutes']} minutes")
        print(f"Total Clients:    {cache_status['table_count']}")
        print(f"Total Records:    {cache_status['total_rows']:,}")
        print(f"Database Size:    {os.path.getsize(self.config.CACHE_DB_PATH)/1024:.1f} KB")
        
        # Show oldest backup if exists
        backup_files = []
        db_dir = os.path.dirname(self.config.CACHE_DB_PATH)
        if os.path.exists(db_dir):
            for file in os.listdir(db_dir):
                if file.startswith('did_manager.db.backup_'):
                    backup_files.append(os.path.join(db_dir, file))
        
        if backup_files:
            latest_backup = max(backup_files, key=os.path.getmtime)
            backup_age = (time.time() - os.path.getmtime(latest_backup)) / 3600
            print(f"Latest Backup:    {os.path.basename(latest_backup)}")
            print(f"Backup Age:       {backup_age:.1f} hours")
            print(f"Total Backups:    {len(backup_files)}")
        
        print("="*50)

    def clear_cache(self):
        """Clear the cache database with confirmation"""
        if not os.path.exists(self.config.CACHE_DB_PATH):
            print("❌ Cache database doesn't exist")
            return
        
        # Get cache stats for warning
        cache_status = self.get_cache_status()
        
        print(f"\n⚠️  WARNING: You are about to clear the cache!")
        print(f"   This will delete: {cache_status['table_count']} clients")
        print(f"   Total records: {cache_status['total_rows']:,}")
        print("\nThis action cannot be undone!")
        
        confirmation = input("\nType 'DELETE' to confirm: ").strip()
        
        if confirmation == 'DELETE':
            try:
                os.remove(self.config.CACHE_DB_PATH)
                print("✅ Cache cleared successfully")
                print("⚠️  You must run 'Sync from Google Sheets' to rebuild the cache")
            except Exception as e:
                print(f"❌ Failed to clear cache: {e}")
        else:
            print("❌ Cache clearance cancelled")

def main():
    """Main entry point"""
    print("\n" + "="*60)
    print(f"{'📞 DID MANAGER - Business Automation Tool':^60}")
    print(f"{'Version 2.3 - Production Ready':^60}")
    print("="*60)
    
    try:
        # Initialize configuration
        config = Config()
        
        # Create manager instance
        manager = DIDManager(config)
        
        # Display main menu
        manager.display_menu()
        
    except FileNotFoundError as e:
        print(f"\n❌ Configuration Error: {e}")
        print("Please ensure 'service_account_key.json' exists in the same directory.")
        print("Follow Google Sheets API setup instructions to create this file.")
    except KeyboardInterrupt:
        print("\n\n👋 Program interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal Error: {e}")
        logger.exception("Fatal error occurred")
        input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()