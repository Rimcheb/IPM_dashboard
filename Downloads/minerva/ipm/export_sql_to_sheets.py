#!/usr/bin/env python3
"""
Export all TiDB SQL data to Google Sheets (respecting 1M row limit per sheet).
Splits large tables across multiple sheets automatically.

Usage:
  python export_sql_to_sheets.py
"""

import os
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector
from gspread.utils import rowcol_to_a1

# ============================================================================
# CONFIG
# ============================================================================

DB_HOST = os.getenv("DB_HOST", "gateway01.eu-central-1.prod.aws.tidbcloud.com")
DB_PORT = int(os.getenv("DB_PORT", "4000"))
DB_NAME = os.getenv("DB_NAME", "oasis")
DB_USER = os.getenv("DB_USER", "2XztaJ7WFkYB7Sv.root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

GOOGLE_CREDS_FILE = "ipm-dashboard.json"
GOOGLE_SHEET_ID = "1POZpWu4tgYSGW7o5DUN4kBbL0vP0vhD9Skeq3FLYHSk"

BATCH_SIZE = 2000  # Rows per batch write
MAX_ROWS_PER_SHEET = 900000  # Stay under 1M limit with buffer
MAX_COLS = 50  # Fallback if sheet has too many columns

# ============================================================================
# HELPERS
# ============================================================================

def connect_tidb():
    """Connect to TiDB"""
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        ssl_disabled=False
    )

def connect_sheets():
    """Connect to Google Sheets"""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)
    client = gspread.authorize(creds)
    client.session.timeout = 60
    return client

def clean_value(val):
    """Convert complex types to strings for Google Sheets"""
    if isinstance(val, (dict, list)):
        import json
        return json.dumps(val)
    elif val is None:
        return ""
    elif isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    return str(val)

def get_all_tables(conn):
    """Get list of all tables in the database"""
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return sorted(tables)

def get_table_count(conn, table_name):
    """Get row count for a table"""
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
    count = cursor.fetchone()[0]
    cursor.close()
    return count

def export_table_to_sheets(sheet, table_name, conn, batch_size=BATCH_SIZE):
    """
    Export a single table to Sheets, splitting across multiple sheets if needed.
    Returns number of rows exported.
    """
    total_rows = get_table_count(conn, table_name)
    
    if total_rows == 0:
        print(f"  ⏭️  {table_name}: Empty (0 rows)")
        return 0
    
    print(f"\n📥 Exporting {table_name} ({total_rows:,} rows)...")
    
    # Fetch column names
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE `{table_name}`")
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    num_cols = len(columns)
    print(f"  Columns: {num_cols}")
    
    # Determine how many sheets needed
    sheets_needed = (total_rows // MAX_ROWS_PER_SHEET) + 1
    rows_per_sheet = total_rows // sheets_needed if sheets_needed > 1 else total_rows
    
    if sheets_needed > 1:
        print(f"  ⚠️  Large table: splitting across {sheets_needed} sheets (~{rows_per_sheet:,} rows each)")
    
    # Export in chunks to separate sheets
    exported = 0
    for sheet_idx in range(sheets_needed):
        sheet_suffix = f"_{sheet_idx + 1}" if sheets_needed > 1 else ""
        sheet_name = f"{table_name}{sheet_suffix}"[:100]  # Sheets have 100-char limit
        
        print(f"  📄 Sheet: {sheet_name}")
        
        # Create or clear sheet
        try:
            ws = sheet.worksheet(sheet_name)
            ws.clear()
            print(f"    ✓ Cleared existing sheet")
        except gspread.WorksheetNotFound:
            ws = sheet.add_worksheet(title=sheet_name, rows=MAX_ROWS_PER_SHEET + 100, cols=num_cols + 5)
            print(f"    ✓ Created new sheet")
        
        # Write headers
        ws.batch_update([{"range": "A1", "values": [columns]}])
        rows_written = 1
        
        # Calculate offset for this sheet
        offset = sheet_idx * rows_per_sheet
        limit = rows_per_sheet if sheet_idx < sheets_needed - 1 else total_rows - offset
        
        # Fetch and write in batches
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM `{table_name}` LIMIT {limit} OFFSET {offset}")
        
        batch = []
        for row_dict in cursor:
            row = [clean_value(row_dict.get(col, "")) for col in columns]
            batch.append(row)
            
            if len(batch) >= batch_size:
                start_row = rows_written + 1
                end_row = start_row + len(batch) - 1
                end_col = num_cols
                cell_range = f"{rowcol_to_a1(start_row, 1)}:{rowcol_to_a1(end_row, end_col)}"
                
                # Ensure sheet has enough rows
                if end_row > ws.row_count:
                    rows_to_add = end_row - ws.row_count + 10000
                    ws.add_rows(rows_to_add)
                
                ws.batch_update([{"range": cell_range, "values": batch}])
                print(f"    ✓ Wrote rows {start_row}-{end_row}")
                
                rows_written += len(batch)
                batch = []
                time.sleep(0.05)
        
        # Write remaining batch
        if batch:
            start_row = rows_written + 1
            end_row = start_row + len(batch) - 1
            end_col = num_cols
            cell_range = f"{rowcol_to_a1(start_row, 1)}:{rowcol_to_a1(end_row, end_col)}"
            
            if end_row > ws.row_count:
                rows_to_add = end_row - ws.row_count + 1000
                ws.add_rows(rows_to_add)
            
            ws.batch_update([{"range": cell_range, "values": batch}])
            print(f"    ✓ Wrote final rows {start_row}-{end_row}")
            rows_written += len(batch)
        
        cursor.close()
        exported += rows_written - 1  # Subtract 1 for header row
    
    print(f"  ✅ Exported {exported:,} rows")
    return exported

def main():
    print("=" * 70)
    print("  TiDB SQL → Google Sheets Bulk Export")
    print("=" * 70)
    
    try:
        # Connect
        conn = connect_tidb()
        print("✅ Connected to TiDB")
        
        client = connect_sheets()
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        print("✅ Connected to Google Sheets")
        
        # Get all tables
        tables = get_all_tables(conn)
        print(f"\n📊 Found {len(tables)} tables:\n")
        
        total_exported = 0
        
        # Export each table
        for table_name in tables:
            try:
                exported = export_table_to_sheets(sheet, table_name, conn)
                total_exported += exported
            except Exception as e:
                print(f"  ❌ Failed: {e}")
        
        conn.close()
        
        print("\n" + "=" * 70)
        print(f"✅ EXPORT COMPLETE! ({total_exported:,} rows total)")
        print("=" * 70)
        print("\n📊 All tables exported to Google Sheets:")
        print(f"   • Sheet ID: {GOOGLE_SHEET_ID}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
