#!/usr/bin/env python3
"""
Import all Oasis data from API into MySQL once.
After this runs, you never need the API again — just use SQL.

Usage:
  python import_from_api.py
"""

import os
import time
import requests
import mysql.connector
from datetime import datetime

API_TOKEN = os.getenv("OASIS_API_TOKEN", "6df8fe289b263ef4e221123e5f21bd83f27eca44")
NETWORK_URL = os.getenv("OASIS_NETWORK_URL", "https://ipm.oasisinsight.net")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "oasis")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

def connect_db():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME, 
        user=DB_USER, password=DB_PASSWORD
    )

def api_session():
    s = requests.Session()
    s.headers.update({"Authorization": f"Token {API_TOKEN}"})
    return s

def fetch_all_from_api(endpoint, session):
    """Paginate through API and fetch ALL records"""
    records = []
    url = f"{NETWORK_URL}/api/v1/{endpoint}/"
    while url:
        print(f"  Fetching: {url}")
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        
        if isinstance(data, dict):
            records.extend(data.get("results", []))
            url = data.get("next")
        else:
            records.extend(data)
            url = None
        
        time.sleep(0.2)  # Rate limit
    
    return records

def insert_assistances(conn, assistances):
    """Insert assistance records into DB"""
    cur = conn.cursor()
    
    # Get existing categories
    cur.execute("SELECT id, name FROM assistance_category")
    categories = {r[1]: r[0] for r in cur.fetchall()}
    
    inserted = 0
    skipped = 0
    
    for asst in assistances:
        try:
            cat_id = asst.get("category")
            case_id = asst.get("case")
            entry_date = asst.get("entry_date", datetime.now().date())
            
            # Parse case ID if it's a URL
            if isinstance(case_id, str) and case_id:
                case_id = int(case_id.rstrip("/").split("/")[-1])
            
            cur.execute("""
                INSERT INTO assistance_assistance 
                (category_id, case_id, date, created_date)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE date=VALUES(date)
            """, (cat_id, case_id, entry_date, datetime.now()))
            
            inserted += 1
        except Exception as e:
            skipped += 1
            print(f"    ⚠️  Skipped record: {e}")
    
    conn.commit()
    print(f"  ✅ Inserted: {inserted}, Skipped: {skipped}")

def main():
    print("=" * 60)
    print("  IPM API → SQL Import")
    print("=" * 60)
    
    session = api_session()
    conn = connect_db()
    
    print("\n📥 Fetching assistances from API...")
    print("   (This may take 5-10 minutes for large datasets)")
    assisted = fetch_all_from_api("assistances", session)
    print(f"  ✅ Got {len(assisted)} assistance records")
    
    print("\n📥 Fetching all cases...")
    cases = fetch_all_from_api("cases", session)
    print(f"  ✅ Got {len(cases)} case records")
    
    print("\n💾 Importing assistances into SQL...")
    insert_assistances(conn, assisted)
    
    print("\n✅ Import complete!")
    print("   You can now use: python ipm.py --source sql")
    print("   (API will no longer be needed)")
    
    conn.close()

if __name__ == "__main__":
    main()
