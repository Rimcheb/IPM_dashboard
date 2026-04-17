#!/usr/bin/env python3
"""
Import all Oasis data from API into Google Sheets (streaming with resume).
Writes data in real-time as it's fetched (500 records at a time).
Can resume from interruption.

Usage:
  python import_api_to_sheets.py
"""

import os
import time
import json
import requests
import gspread
from gspread.utils import rowcol_to_a1
from oauth2client.service_account import ServiceAccountCredentials
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

API_TOKEN = os.getenv("OASIS_API_TOKEN", "6df8fe289b263ef4e221123e5f21bd83f27eca44")
NETWORK_URL = os.getenv("OASIS_NETWORK_URL", "https://ipm.oasisinsight.net")

GOOGLE_CREDS_FILE = "ipm-dashboard.json"
GOOGLE_SHEET_ID = "1POZpWu4tgYSGW7o5DUN4kBbL0vP0vhD9Skeq3FLYHSk"
PROGRESS_FILE = ".import_progress.json"

def api_session():
    """Create authenticated API session"""
    s = requests.Session()
    s.headers.update({"Authorization": f"Token {API_TOKEN}"})
    return s

def connect_sheets():
    """Connect to Google Sheets with timeout handling"""
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)
    client = gspread.authorize(creds)
    client.session.timeout = 60  # Set explicit timeout to 60 seconds
    return client

def retry_with_backoff(func, max_retries=5, base_delay=2):
    """Retry a function with exponential backoff"""
    for attempt in range(max_retries):
        try:
            return func()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, Exception) as e:
            if attempt == max_retries - 1:
                raise
            # Longer delay for network errors
            delay = base_delay * (2 ** attempt) + random.uniform(0, 2)
            print(f"    ⏳ Retry {attempt + 1}/{max_retries} after {delay:.1f}s... ({type(e).__name__})")
            time.sleep(delay)

def load_progress():
    """Load progress from file"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"categories": {"count": 0}, "cases": {"count": 0}, "assistances": {"count": 0}}

def save_progress(progress):
    """Save progress to file"""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f, indent=2)

def get_last_id_from_sheet(ws):
    """Get the last ID from existing sheet data"""
    try:
        rows = ws.get_all_values()
        if len(rows) > 1:  # Has header + at least 1 data row
            # Find the 'id' column (case-insensitive)
            headers = rows[0]
            id_col = None
            for i, h in enumerate(headers):
                if str(h).lower() == "id":
                    id_col = i
                    break
            
            if id_col is not None:
                # Return last row's ID
                last_row = rows[-1]
                if id_col < len(last_row):
                    last_id = last_row[id_col]
                    if last_id:  # Make sure it's not empty
                        return last_id
    except:
        pass
    return None

def clean_value(val):
    """Convert complex types to JSON strings for Google Sheets"""
    if isinstance(val, (dict, list)):
        return json.dumps(val)
    elif val is None:
        return ""
    return val

def fetch_and_write_streaming(endpoint, sheet_name, session, sheet, progress_key, batch_size=500):
    """
    Fetch from API page by page and write to sheet in batches.
    Resumes from last written position if interrupted.
    SAFE: Never deletes existing data - always resumes from actual sheet row count.
    """
    progress = load_progress()
    progress_count = progress.get(progress_key, {}).get("count", 0)
    
    print(f"\n📥 Fetching {endpoint}...")
    
    # Get or create sheet
    try:
        ws = retry_with_backoff(lambda: sheet.worksheet(sheet_name))
        # Check actual row count in sheet
        actual_rows = len(ws.get_all_values())
        
        # Use the ACTUAL sheet row count, not progress file
        # (safer: if progress file was lost, we still know where we are)
        if actual_rows > 0:
            # Sheet already has data, resume from there
            last_count = actual_rows  # Include header row in count
            print(f"  ✓ Found sheet with {actual_rows} rows (header + {actual_rows - 1} data rows)")
            if progress_count != last_count:
                print(f"  💾 Updating progress from {progress_count} to {last_count} (based on actual sheet)")
                progress[progress_key] = {"count": last_count}
                save_progress(progress)
        else:
            # Sheet exists but is empty
            last_count = 0
            print(f"  ✓ Found empty sheet, starting fresh")
            
    except gspread.WorksheetNotFound:
        # Sheet doesn't exist, create it
        print(f"  📄 Creating '{sheet_name}' sheet...")
        ws = retry_with_backoff(lambda: sheet.add_worksheet(title=sheet_name, rows=50000, cols=50))
        last_count = 0
    
    # Fetch and write in streaming fashion
    url = f"{NETWORK_URL}/api/v1/{endpoint}/"
    page = 1
    batch = []
    headers = None
    records_fetched = 0
    records_written = last_count
    
    # Get last ID from sheet to resume from (more robust than page-based)
    last_id_in_sheet = get_last_id_from_sheet(ws) if records_written > 1 else None
    if last_id_in_sheet:
        print(f"  🔍 Last ID in sheet: {last_id_in_sheet}")
        print(f"  ⏩ Skipping records until ID > {last_id_in_sheet}...")
    else:
        print(f"  ℹ️  Starting fresh (no existing data)")
        try:
            existing_rows = ws.get_all_values()
            if existing_rows:
                headers = existing_rows[0]
                print(f"  📖 Using existing headers from sheet")
        except:
            pass
    
    skip_mode = True if last_id_in_sheet else False  # Skip until we find new data
    skipped_count = 0  # Track how many we've skipped
    
    while url:
        print(f"  Page {page}: {endpoint}")
        
        # Fetch page with retry
        page_data = retry_with_backoff(
            lambda u=url: session.get(u, timeout=30),
            max_retries=5,
            base_delay=3
        )
        page_data.raise_for_status()
        data = page_data.json()
        
        # Extract records
        if isinstance(data, dict):
            results = data.get("results", [])
            url = data.get("next")
        else:
            results = data
            url = None
        
        # Process each record
        for record in results:
            # Extract headers on first record (if not already loaded from existing sheet)
            if headers is None:
                headers = list(record.keys())
                # Only write header if this is a fresh sheet (records_written == 0 means no rows yet)
                if records_written == 0:
                    ws.batch_update([{"range": "A1", "values": [headers]}])
                    records_written = 1
                    # Save progress after writing header
                    progress_local = load_progress()
                    progress_local[progress_key] = {"count": records_written, "last_id": None}
                    save_progress(progress_local)
                else:
                    # Resuming: headers already exist, just use them
                    print(f"  ✓ Headers loaded, resuming from ID > {last_id_in_sheet}")
            
            # Skip records until we find one past the last ID we have
            current_id = record.get("id")
            if skip_mode and last_id_in_sheet:
                try:
                    # Numeric comparison, not string!
                    last_id_num = int(last_id_in_sheet)
                    current_id_num = int(current_id)
                    
                    if current_id_num <= last_id_num:
                        # Skip this record, we already have it
                        skipped_count += 1
                        # Only log every 100 skips to reduce noise
                        if skipped_count % 100 == 0:
                            print(f"    ⏭️  Skipped {skipped_count} old records (up to ID {current_id})...")
                        continue
                    else:
                        # Found new records!
                        skip_mode = False
                        print(f"  ✅ Resuming from ID {current_id} (skipped {skipped_count} old records)")
                except (ValueError, TypeError):
                    # If IDs aren't numeric, fall back to string comparison
                    if str(current_id) <= str(last_id_in_sheet):
                        skipped_count += 1
                        if skipped_count % 100 == 0:
                            print(f"    ⏭️  Skipped {skipped_count} old records (up to ID {current_id})...")
                        continue
                    else:
                        skip_mode = False
                        print(f"  ✅ Resuming from ID {current_id} (skipped {skipped_count} old records)")
            
            # Add to batch, converting complex types to strings
            row = [clean_value(record.get(k, "")) for k in headers]
            batch.append(row)
            records_fetched += 1
            
            # Write batch when it reaches size
            if len(batch) >= batch_size:
                start_row = records_written + 1
                end_row = start_row + len(batch) - 1
                end_col = max(1, len(headers) if headers else 1)
                cell_range = f"{rowcol_to_a1(start_row, 1)}:{rowcol_to_a1(end_row, end_col)}"

                # Ensure the sheet has enough rows for this batch
                if end_row > ws.row_count:
                    rows_to_add = end_row - ws.row_count
                    retry_with_backoff(lambda r=rows_to_add: ws.add_rows(r), max_retries=3)
                
                retry_with_backoff(
                    lambda b=batch, r=cell_range: ws.batch_update([{"range": r, "values": b}]),
                    max_retries=3
                )
                print(f"    ✓ Wrote rows {start_row}-{end_row} ({len(batch)} records)")
                
                records_written += len(batch)
                
                # CRITICAL: Save progress after each batch, including last ID
                # Get last ID from the last row in batch (ID column is first)
                last_batch_id = batch[-1][0] if batch and len(batch[-1]) > 0 else None
                progress = load_progress()
                progress[progress_key] = {"count": records_written, "last_id": last_batch_id}
                save_progress(progress)
                
                batch = []
                time.sleep(0.1)
        
        page += 1
        time.sleep(0.4)  # Respect API rate limit: 5 req/sec, 2 parallel threads = 0.4s delay
    
    # Write remaining batch
    if batch:
        start_row = records_written + 1
        end_row = start_row + len(batch) - 1
        end_col = max(1, len(headers) if headers else 1)
        cell_range = f"{rowcol_to_a1(start_row, 1)}:{rowcol_to_a1(end_row, end_col)}"

        # Ensure the sheet has enough rows for the final batch
        if end_row > ws.row_count:
            rows_to_add = end_row - ws.row_count
            retry_with_backoff(lambda r=rows_to_add: ws.add_rows(r), max_retries=3)
        
        retry_with_backoff(
            lambda b=batch, r=cell_range: ws.batch_update([{"range": r, "values": b}]),
            max_retries=3
        )
        print(f"    ✓ Wrote final rows {start_row}-{end_row} ({len(batch)} records)")
        records_written += len(batch)
        
        # Save final batch progress with last ID
        last_batch_id = batch[-1][0] if batch and len(batch[-1]) > 0 else None
        progress = load_progress()
        progress[progress_key] = {"count": records_written, "last_id": last_batch_id}
        save_progress(progress)
    
    print(f"  ✅ Got {records_fetched} new records, total: {records_written}")
    
    # Final progress save
    progress = load_progress()
    progress[progress_key] = {"count": records_written}
    save_progress(progress)
    
    return records_written

def fetch_and_write_categories(session, sheet, progress):
    """Fetch categories (small dataset, write all at once)"""
    last_count = progress.get("categories", {}).get("count", 0)
    
    if last_count > 0:
        print(f"  ✓ Categories already imported ({last_count} records)")
        return last_count
    
    print(f"\n📥 Fetching categories...")
    
    # Create or clear sheet
    try:
        ws = retry_with_backoff(lambda: sheet.worksheet("Categories"))
        retry_with_backoff(lambda: ws.clear())
    except gspread.WorksheetNotFound:
        print(f"  📄 Creating 'Categories' sheet...")
        ws = retry_with_backoff(lambda: sheet.add_worksheet(title="Categories", rows=500, cols=10))
    
    # Fetch all categories
    url = f"{NETWORK_URL}/api/v1/categories/"
    categories = []
    
    while url:
        page_data = retry_with_backoff(
            lambda u=url: session.get(u, timeout=30),
            max_retries=5,
            base_delay=2
        )
        page_data.raise_for_status()
        data = page_data.json()
        
        if isinstance(data, dict):
            categories.extend(data.get("results", []))
            url = data.get("next")
        else:
            categories.extend(data)
            url = None
        
        time.sleep(0.05)
    
    # Write all categories
    if categories:
        headers = ["ID", "Name", "Description"]
        rows = [headers]
        for cat in categories:
            rows.append([
                clean_value(cat.get("id")),
                clean_value(cat.get("name")),
                clean_value(cat.get("description", ""))
            ])
        
        ws.batch_update([{"range": "A1", "values": rows}])
        print(f"  ✅ Wrote {len(categories)} categories")
    
    # Update progress
    progress["categories"] = {"count": len(categories)}
    save_progress(progress)
    
    return len(categories)

def main():
    print("=" * 70)
    print("  IPM API → Google Sheets Import (Parallel + Resume)")
    print("=" * 70)
    
    try:
        # Create shared sheet connection (reading metadata is thread-safe)
        client = connect_sheets()
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        progress = load_progress()
        
        print("\n🚀 Starting import (respecting 5 req/sec API limit)...\n")
        print("   📋 Categories (fast)  → Cases + Assistances (parallel, 2 threads, 0.4s delay)")
        print("   ⏱️  Each thread: 1 req every 0.4s = 2.5 req/sec × 2 threads = ~5 req/sec total\n")
        
        # Step 1: Categories first (quick, sequential)
        fetch_and_write_categories(api_session(), sheet, progress)
        
        # Step 2: Cases + Assistances in parallel (2 threads, 0.4s delay each = 5 req/sec total)
        print("\n📥 Now fetching cases + assistances in parallel...")
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(
                    fetch_and_write_streaming,
                    "cases", "Case Data", api_session(), sheet, "cases", 2000
                ): "Cases",
                
                executor.submit(
                    fetch_and_write_streaming,
                    "assistances", "Assistance Data", api_session(), sheet, "assistances", 2000
                ): "Assistances",
            }
            
            completed = 0
            for future in as_completed(futures):
                endpoint_name = futures[future]
                completed += 1
                try:
                    result = future.result()
                    print(f"  ✅ [{completed}/2] {endpoint_name} completed")
                except Exception as e:
                    print(f"  ❌ [{completed}/2] {endpoint_name} failed: {e}")
                    raise
        
        # Done
        print("\n" + "=" * 70)
        print("✅ IMPORT COMPLETE!")
        print("=" * 70)
        print("\n📊 All data imported to Google Sheets:")
        print("   • Case Data sheet")
        print("   • Assistance Data sheet")
        print("   • Categories sheet")
        
        # Clean up progress file
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
        
    except KeyboardInterrupt:
        print("\n\n⏸️  Interrupted by user. Progress saved.")
        print("   Run again to resume: python import_api_to_sheets.py")
        print("   Run again to resume: python import_api_to_sheets.py")

if __name__ == "__main__":
    main()

