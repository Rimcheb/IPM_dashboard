#!/usr/bin/env python3
"""
Export new/updated TiDB rows to Excel (incremental — only adds new data).
Tracks last exported ID/timestamp for each table to avoid duplicates.

Usage:
  python export_sql_to_excel.py
"""

import os
import json
import mysql.connector
import pandas as pd
from openpyxl import load_workbook

DB_HOST = os.getenv("DB_HOST", "gateway01.eu-central-1.prod.aws.tidbcloud.com")
DB_PORT = int(os.getenv("DB_PORT", "4000"))
DB_NAME = os.getenv("DB_NAME", "oasis")
DB_USER = os.getenv("DB_USER", "2XztaJ7WFkYB7Sv.root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

OUTPUT_FILE = "oasis_data.xlsx"
PROGRESS_FILE = ".export_progress.json"

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

def get_all_tables(conn):
    """Get list of all tables"""
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return sorted(tables)

def load_progress():
    """Load progress from file"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_progress(progress):
    """Save progress to file"""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)

def get_max_id(conn, table):
    """Get max ID from table (assumes 'id' column exists)"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(id) FROM `{table}`")
        result = cursor.fetchone()[0]
        cursor.close()
        return result if result else 0
    except:
        return None

def main():
    print("=" * 70)
    print("  TiDB SQL → Excel (Incremental)")
    print("=" * 70)
    
    try:
        conn = connect_tidb()
        print("✅ Connected to TiDB")
        
        tables = get_all_tables(conn)
        print(f"📊 Found {len(tables)} tables\n")
        
        progress = load_progress()
        new_rows = 0
        
        # Ensure Excel file exists
        if not os.path.exists(OUTPUT_FILE):
            print(f"📝 Creating new Excel file: {OUTPUT_FILE}\n")
            wb = load_workbook()
            wb.save(OUTPUT_FILE)
        
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
            for idx, table in enumerate(tables, 1):
                try:
                    # Get last exported ID for this table
                    last_id = progress.get(table, {}).get("last_id", 0)
                    
                    # Fetch only new rows (id > last_id)
                    query = f"SELECT * FROM `{table}` WHERE id > {last_id} ORDER BY id" if get_max_id(conn, table) is not None else f"SELECT * FROM `{table}`"
                    df = pd.read_sql(query, conn)
                    rows = len(df)
                    
                    if rows > 0:
                        # Get max ID from this batch
                        if 'id' in df.columns:
                            current_max_id = df['id'].max()
                        else:
                            current_max_id = last_id
                        
                        # Sheet name: max 31 chars (Excel limit)
                        sheet_name = table[:31]
                        
                        # Write to sheet (append if exists, create if new)
                        df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0 if pd.isna(writer.sheets.get(sheet_name)) else len(pd.read_excel(OUTPUT_FILE, sheet_name=sheet_name)) + 1)
                        
                        # Update progress
                        progress[table] = {"last_id": int(current_max_id) if isinstance(current_max_id, (int, float)) else current_max_id}
                        new_rows += rows
                        
                        print(f"  [{idx:3d}/{len(tables)}] {table:50s} ✓ {rows:6d} new rows")
                    else:
                        print(f"  [{idx:3d}/{len(tables)}] {table:50s}   (no new rows)")
                    
                except Exception as e:
                    print(f"  [{idx:3d}/{len(tables)}] {table:50s} ❌ {str(e)[:50]}")
        
        conn.close()
        save_progress(progress)
        
        print("\n" + "=" * 70)
        print(f"✅ EXPORT COMPLETE! ({new_rows:,} new rows added)")
        print(f"   File: {OUTPUT_FILE}")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
