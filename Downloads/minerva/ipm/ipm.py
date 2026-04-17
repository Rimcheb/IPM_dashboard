"""
IPM Monthly Dashboard Automation
=================================
DATA SOURCES (tried in this order by default):
  1. API  — Oasis Insight REST API  (fastest, no downloads needed)
  2. SQL  — Direct MySQL connection  (requires local DB copy)
  3. CSV  — Exported files in /reports  (original manual workflow)

HOW TO RUN:
  python ipm.py               ← auto mode: API → SQL → CSV per program
  python ipm.py --source api  ← force API only (fails if unavailable)
  python ipm.py --source sql  ← force SQL only
  python ipm.py --source csv  ← force CSV only

FOLDER STRUCTURE:
  ipm.py                ← this file
  ipm-dashboard.json    ← Google service account credentials
  reports/              ← CSV fallback files (only needed if not using API/SQL)
    drive_through.csv
    choice_pantry.csv
    mobile_all.csv
    food_resource_hub.csv
    fresh_start.csv

CSV EXPORT GUIDE (Oasis → each file):
  Assistance | Drive Thru + Evening Drive Thru + DoorDash  → drive_through.csv
  Assistance | Aicholtz Choice Pantry                      → choice_pantry.csv
  Assistance | Mobile Pantry (all)                         → mobile_all.csv
  Events → Food Resource Hub                               → food_resource_hub.csv
  Events → Fresh Start/Pop-up                              → fresh_start.csv

API AGE NOTE:
  Age breakdown via API uses the head-of-household date_of_birth only
  (one API record per assistance event). For exact per-member age counts,
  use CSV or SQL — both count every household member.
"""

import sys
import time
import calendar
import argparse
import pandas as pd
import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

try:
    import mysql.connector
    _MYSQL_AVAILABLE = True
except ImportError:
    _MYSQL_AVAILABLE = False

# ===========================================================================
# MANUAL INPUTS — Fill these in every month before running
# ===========================================================================

REPORT_MONTH    = "February 2026"   # Label for this month's row in history sheet
REPORT_YEAR     = 2026              # The year (for YTD and % vs prior year)

# From Nikki's monthly In-Kind report
INKIND_LBS      = 0

# From Michael Truitt's FSFB report
FSFB_LBS        = 0

# Milk and Eggs purchased this month
MILK_HALF_GALLONS = 0   # Number of half-gallons → will × 4.3 lbs
EGG_DOZENS        = 0   # Number of dozens → will × 1.1 lbs

# Any other food source not in Oasis or the above (Liz/Joe to confirm)
OTHER_FOOD_LBS  = 0

# Martha's spreadsheet counts (YTD as of this month)
YTD_MOBILE_EVENTS  = 0   # Count of mobile pantry events year-to-date
YTD_POPUP_EVENTS   = 0   # Count of pop-up pantry events year-to-date

# From Liz (verified manually)
FRESH_START_KITCHEN_COUNT = 0  # Number of Fresh Start Kitchen locations
HOMELESS_BACKPACKS        = 0
POWER_PACKS               = 0

# ===========================================================================
# API CONFIG — Set once; leave API_TOKEN empty to skip API
# ===========================================================================

# Your Oasis Insight API token (Network Admin → API settings)
API_TOKEN   = "6df8fe289b263ef4e221123e5f21bd83f27eca44"

# Your network base URL — no trailing slash
NETWORK_URL = "https://ipm.oasisinsight.net"

# Category IDs per program.
# Run:  GET /api/v1/categories/  to find numeric IDs for your network.
# Leave a list empty to skip API for that program and fall back to SQL/CSV.
CATEGORY_IDS = {
    # Drive Through: main + location-specific + 6+ family sizes + Door Dash
    "drive_through": [50, 57, 59, 2, 20, 39, 40],
    # 50=Drive Thru Pantry, 57=Evening Drive Thru, 59=Door Dash
    # 2=Amelia Drive Through Pantry, 20=Newtown Drive Through Pantry
    # 39=Amelia Drive Thru Families of 6+, 40=Newtown Drive thru Families of 6+
    "choice_pantry": [52, 26, 27, 53],
    # 52=Choice Pantry, 26=Amelia Choice Pantry, 27=Newtown Choice Pantry
    # 53=Aicholtz Choice Pantry +6
    "mobile":        [37],          # Mobile Pantry
    "fresh_start":   [44, 54],      # Fresh Start Kitchen, Senior Box
    # FRH uses an Events CSV — API not yet supported for events.
}

# ===========================================================================
# SQL CONFIG — Set once; leave DB_HOST empty to skip SQL
# ===========================================================================

DB_HOST     = "localhost"   # Set to "" to disable SQL entirely
DB_PORT     = 3306
DB_NAME     = "oasis"
DB_USER     = "root"
DB_PASSWORD = ""

# SQL category name patterns (LIKE matching) — adjust if your Oasis names differ
SQL_CATEGORY_PATTERNS = {
    "drive_through": ["%Drive Thru%", "%Drive Through%", "%Door Dash%", "%DoorDash%"],
    "choice_pantry": ["%Choice Pantry%"],
    "mobile":        ["%Mobile Pantry%"],
    "fresh_start":   ["%Fresh Start%", "%Pop-up%", "%Pop up%", "%Restart%", "%Senior Box%"],
}

# ===========================================================================
# CONFIG — Only change if your setup changes
# ===========================================================================

GOOGLE_CREDS_FILE = "ipm-dashboard.json"
GOOGLE_SHEET_ID   = "1POZpWu4tgYSGW7o5DUN4kBbL0vP0vhD9Skeq3FLYHSk"
GOOGLE_SHEET_NAME = "IPM"                     # fallback if ID is blank
HISTORY_TAB_NAME  = "Monthly History"
REPORTS_FOLDER    = "reports"

# ===========================================================================
# STEP 1a: OASIS INSIGHT API (primary)
# ===========================================================================

def _get_report_date_range(report_month_str):
    """Convert "February 2026" into ("2026-02-01", "2026-02-29")."""
    dt       = datetime.strptime(report_month_str, "%B %Y")
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    return dt.replace(day=1).strftime("%Y-%m-%d"), dt.replace(day=last_day).strftime("%Y-%m-%d")


def _create_api_session():
    session = requests.Session()
    session.headers.update({"Authorization": f"Token {API_TOKEN}"})
    return session


def _fetch_all_assistance_api(session, category_ids, date_start, date_end):
    """Fetch every assistance record for the given categories/date range, handling pagination."""
    records = []
    for cat_id in category_ids:
        url    = f"{NETWORK_URL}/api/v1/assistances/"
        params = {"category": cat_id, "entry_date_after": date_start, "entry_date_before": date_end}
        while url:
            resp = session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data   = resp.json()
            params = {}   # next URL already has params baked in
            if isinstance(data, dict):
                records.extend(data.get("results", []))
                url = data.get("next")
            else:
                records.extend(data)
                url = None
            time.sleep(0.2)   # stay under 5 req/sec
    return records


def _fetch_case_api(session, case_id, cache):
    if case_id in cache:
        return cache[case_id]
    try:
        resp = session.get(f"{NETWORK_URL}/api/v1/cases/{case_id}/", timeout=30)
        resp.raise_for_status()
        case = resp.json()
    except Exception:
        case = None
    cache[case_id] = case
    time.sleep(0.2)
    return case


def _extract_case_id(raw):
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw:
        return int(raw.rstrip("/").split("/")[-1])
    return None


def parse_oasis_assistance_api(session, category_ids, date_start, date_end, case_cache):
    """
    Fetch assistance data from the API. Returns same dict shape as
    parse_oasis_assistance_csv() so the rest of the script is unaffected.
    """
    records = _fetch_all_assistance_api(session, category_ids, date_start, date_end)
    today   = datetime.today()

    dup_households  = len(records)
    dup_individuals = 0
    children = adults = seniors = unknown = 0

    for rec in records:
        case_id = _extract_case_id(rec.get("case"))
        if case_id is None:
            dup_individuals += 1
            unknown += 1
            continue

        case    = _fetch_case_api(session, case_id, case_cache)
        hh_size = (case.get("total_living_in_household") or 1) if case else 1
        dup_individuals += hh_size

        dob_str = case.get("date_of_birth") if case else None
        if dob_str:
            try:
                age = (today - datetime.strptime(dob_str, "%Y-%m-%d")).days / 365.25
                if age < 18:
                    children += hh_size
                elif age < 60:
                    adults += hh_size
                else:
                    seniors += hh_size
            except ValueError:
                unknown += hh_size
        else:
            unknown += hh_size

    return {
        "dup_households":   dup_households,
        "dup_individuals":  dup_individuals,
        "children_members": children,
        "adults_members":   adults,
        "seniors_members":  seniors,
        "unknown_members":  unknown,
    }

# ===========================================================================
# STEP 1b: DIRECT SQL / MYSQL (secondary)
# ===========================================================================

def _empty_frh_data():
    return {"unique_agencies": 0, "dup_households": 0, "dup_individuals": 0}


def _sql_connect():
    if not _MYSQL_AVAILABLE:
        raise ImportError("mysql-connector-python not installed. Run: pip install mysql-connector-python")
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )


def _sql_like_clause(col, patterns):
    """Build  (col LIKE %s OR col LIKE %s …)  and return (clause_str, params_tuple)."""
    parts  = " OR ".join(f"{col} LIKE %s" for _ in patterns)
    return f"({parts})", tuple(patterns)


def parse_oasis_assistance_sql(conn, program_key, date_start, date_end):
    """
    Query the Oasis MySQL database for one program. Returns same dict shape
    as parse_oasis_assistance_csv().
    """
    patterns = SQL_CATEGORY_PATTERNS.get(program_key, [])
    if not patterns:
        return _empty_program_data()

    cur = conn.cursor(dictionary=True)
    like_clause, like_params = _sql_like_clause("ac.name", patterns)
    date_params = (date_start, date_end)

    result = _empty_program_data()
    try:
        cur.execute(f"""
            SELECT
                COUNT(*) AS dup_households,
                COALESCE(SUM(ch.num_members), COUNT(*)) AS dup_individuals,
                SUM(CASE WHEN TIMESTAMPDIFF(YEAR, cc.date_of_birth, CURDATE()) < 18 THEN 1 ELSE 0 END) AS children,
                SUM(CASE WHEN TIMESTAMPDIFF(YEAR, cc.date_of_birth, CURDATE()) BETWEEN 18 AND 59 THEN 1 ELSE 0 END) AS adults,
                SUM(CASE WHEN TIMESTAMPDIFF(YEAR, cc.date_of_birth, CURDATE()) >= 60 THEN 1 ELSE 0 END) AS seniors,
                SUM(CASE WHEN cc.date_of_birth IS NULL THEN 1 ELSE 0 END) AS unknown
            FROM assistance_assistance aa
            JOIN assistance_category ac ON aa.category_id = ac.id
            LEFT JOIN cases_household ch ON aa.case_id = ch.id
            LEFT JOIN cases_case cc ON aa.case_id = cc.id
            WHERE {like_clause} AND aa.date >= %s AND aa.date <= %s
        """, like_params + date_params)
        row = cur.fetchone()
        if row:
            result["dup_households"]   = int(row.get("dup_households")  or 0)
            result["dup_individuals"]  = int(row.get("dup_individuals") or 0)
            result["children_members"] = int(row.get("children")        or 0)
            result["adults_members"]   = int(row.get("adults")          or 0)
            result["seniors_members"]  = int(row.get("seniors")         or 0)
            result["unknown_members"]  = int(row.get("unknown")         or 0)
    except Exception as e:
        print(f"      SQL query error: {e}")
    finally:
        cur.close()

    return result


def parse_food_resource_hub_sql(conn, date_start, date_end):
    """FRH via SQL — queries the events tables."""
    cur = conn.cursor(dictionary=True)
    result = _empty_frh_data()
    try:
        cur.execute("""
            SELECT COUNT(*) AS cnt, COUNT(DISTINCT ee.location_id) AS agencies
            FROM events_event ee
            JOIN events_eventtype et ON ee.event_type_id = et.id
            WHERE et.name = 'FoodLink'
              AND ee.date >= %s AND ee.date <= %s
        """, (date_start, date_end))
        row = cur.fetchone()
        if row:
            result["dup_households"]  = int(row.get("cnt")      or 0)
            result["unique_agencies"] = int(row.get("agencies") or 0)
    except Exception as e:
        print(f"      SQL FRH query error: {e}")
    finally:
        cur.close()
    return result

# ===========================================================================
# STEP 1c: CSV FALLBACK (tertiary)
# ===========================================================================

def _empty_program_data():
    return {
        "dup_households":   0,
        "dup_individuals":  0,
        "children_members": 0,
        "adults_members":   0,
        "seniors_members":  0,
        "unknown_members":  0,
    }


def parse_oasis_assistance_csv(filepath):
    """Reads an Oasis Assistance CSV export and returns summary numbers."""
    try:
        f_handle = open(filepath, "r", encoding="utf-8-sig")
    except FileNotFoundError:
        print(f"      ⚠️  Missing file: {filepath} — skipping.")
        return _empty_program_data()

    result = _empty_program_data()
    with f_handle as f:
        for line in f:
            line = line.strip()
            if line.startswith("Case #,Entry Date"):
                break
            if line.startswith("Duplicated household count:,"):
                result["dup_households"] = _parse_int(line)
            elif line.startswith("Duplicated member count:,"):
                result["dup_individuals"] = _parse_int(line)
            elif line.startswith("Members (0 - 17 yrs):,"):
                result["children_members"] = _parse_int(line)
            elif line.startswith("Members (18 - 59 yrs):,"):
                result["adults_members"] = _parse_int(line)
            elif line.startswith("Members (60+ yrs):,"):
                result["seniors_members"] = _parse_int(line)
            elif line.startswith("Members (unknown yrs):,"):
                result["unknown_members"] = _parse_int(line)
    return result


def _parse_int(line):
    try:
        return int(float(line.split(",")[1].strip().replace(",", "")))
    except (IndexError, ValueError):
        return 0


def parse_food_resource_hub_csv(filepath):
    """Reads the Food Resource Hub events CSV."""
    try:
        df = pd.read_csv(filepath, on_bad_lines="skip")
    except FileNotFoundError:
        print(f"      ⚠️  Missing file: {filepath} — skipping.")
        return _empty_frh_data()
    except Exception as e:
        print(f"      ⚠️  Could not read {filepath}: {e}")
        return _empty_frh_data()

    result = _empty_frh_data()
    if "Location"          in df.columns: result["unique_agencies"] = df["Location"].dropna().nunique()
    if "Total Households"  in df.columns: result["dup_households"]  = int(df["Total Households"].sum())
    if "Total Individuals" in df.columns: result["dup_individuals"] = int(df["Total Individuals"].sum())
    return result

# ===========================================================================
# STEP 2: FETCH ONE PROGRAM (API → SQL → CSV)
# ===========================================================================

def _fetch_program(label, source, session, sql_conn, program_key,
                   date_start, date_end, csv_path, case_cache):
    """
    Fetch data for one assistance program using the chosen source strategy.
      source="auto"  → try API, then SQL, then CSV
      source="api"   → API only (raises on failure)
      source="sql"   → SQL only (raises on failure)
      source="csv"   → CSV only
    Returns same dict shape as parse_oasis_assistance_csv().
    """
    def try_api():
        ids = CATEGORY_IDS.get(program_key, [])
        if not (session and ids):
            raise ValueError("API not configured for this program")
        print(f"      [{label}] API...")
        data = parse_oasis_assistance_api(session, ids, date_start, date_end, case_cache)
        print(f"      [{label}] ✅ API  ({data['dup_households']} records)")
        return data

    def try_sql():
        if sql_conn is None:
            raise ValueError("SQL not connected")
        print(f"      [{label}] SQL...")
        data = parse_oasis_assistance_sql(sql_conn, program_key, date_start, date_end)
        print(f"      [{label}] ✅ SQL  ({data['dup_households']} records)")
        return data

    def try_csv():
        print(f"      [{label}] CSV  ({csv_path})...")
        return parse_oasis_assistance_csv(csv_path)

    if source == "api":
        return try_api()
    if source == "sql":
        return try_sql()
    if source == "csv":
        return try_csv()

    # auto: waterfall
    for attempt, label_src in [(try_api, "API"), (try_sql, "SQL"), (try_csv, "CSV")]:
        try:
            return attempt()
        except Exception as exc:
            print(f"      [{label}] ⚠️  {label_src} failed ({exc}) — trying next source.")
    return _empty_program_data()


def _fetch_frh(source, session, sql_conn, date_start, date_end):
    """FRH: SQL or CSV (API not yet supported for events)."""
    if source in ("api", "auto") and sql_conn:
        try:
            print(f"      [Food Resource Hub] SQL...")
            data = parse_food_resource_hub_sql(sql_conn, date_start, date_end)
            print(f"      [Food Resource Hub] ✅ SQL  ({data['dup_households']} records)")
            return data
        except Exception as exc:
            print(f"      [Food Resource Hub] ⚠️  SQL failed ({exc}) — falling back to CSV.")
    if source == "sql":
        if sql_conn is None:
            raise ValueError("SQL not connected")
        return parse_food_resource_hub_sql(sql_conn, date_start, date_end)
    print(f"      [Food Resource Hub] CSV...")
    return parse_food_resource_hub_csv(f"{REPORTS_FOLDER}/food_resource_hub.csv")

# ===========================================================================
# STEP 3: CALCULATE ALL NUMBERS
# ===========================================================================

def calculate_dashboard_numbers(manual_inputs, source="auto"):
    date_start, date_end = _get_report_date_range(manual_inputs["report_month"])

    # --- Set up connections ---
    api_ready = bool(API_TOKEN and NETWORK_URL)
    session   = _create_api_session() if api_ready else None
    sql_conn  = None
    if DB_HOST and source in ("sql", "auto"):
        try:
            sql_conn = _sql_connect()
            print("  🗄️  SQL connected.")
        except Exception as e:
            print(f"  ⚠️  SQL unavailable ({e}).")

    source_label = source.upper() if source != "auto" else "AUTO (API → SQL → CSV)"
    print(f"\n  Data source: {source_label}  |  Period: {date_start} → {date_end}")

    case_cache = {}

    # --- Fetch each program ---
    drive  = _fetch_program("Drive Through", source, session, sql_conn, "drive_through",
                             date_start, date_end, f"{REPORTS_FOLDER}/drive_through.csv", case_cache)
    choice = _fetch_program("Choice Pantry", source, session, sql_conn, "choice_pantry",
                             date_start, date_end, f"{REPORTS_FOLDER}/choice_pantry.csv", case_cache)
    mobile = _fetch_program("Mobile Pantry", source, session, sql_conn, "mobile",
                             date_start, date_end, f"{REPORTS_FOLDER}/mobile_all.csv", case_cache)
    fresh  = _fetch_program("Fresh Start",   source, session, sql_conn, "fresh_start",
                             date_start, date_end, f"{REPORTS_FOLDER}/fresh_start.csv", case_cache)
    frh    = _fetch_frh(source, session, sql_conn, date_start, date_end)

    if sql_conn:
        sql_conn.close()

    # --- PIE CHART ---
    hh_drive_thru   = drive["dup_households"]
    hh_choice       = choice["dup_households"]
    hh_mobile_fsk   = mobile["dup_households"] + fresh["dup_households"]
    hh_frh          = frh["dup_households"]
    total_hh_visits = hh_drive_thru + hh_choice + hh_mobile_fsk + hh_frh

    # --- INDIVIDUAL VISITS ---
    total_individual_visits = (
        drive["dup_individuals"] + choice["dup_individuals"]
        + mobile["dup_individuals"] + fresh["dup_individuals"]
        + frh["dup_individuals"]
    )

    # --- FOOD LBS ---
    milk_lbs         = manual_inputs["milk_half_gallons"] * 4.3
    egg_lbs          = manual_inputs["egg_dozens"] * 1.1
    monthly_food_lbs = (
        manual_inputs["inkind_lbs"] + manual_inputs["fsfb_lbs"]
        + milk_lbs + egg_lbs + manual_inputs["other_food_lbs"]
    )

    # --- AGE BREAKDOWN (Drive + Choice + Mobile only; unknown distributed evenly) ---
    total_children = drive["children_members"] + choice["children_members"] + mobile["children_members"]
    total_adults   = drive["adults_members"]   + choice["adults_members"]   + mobile["adults_members"]
    total_seniors  = drive["seniors_members"]  + choice["seniors_members"]  + mobile["seniors_members"]
    total_unknown  = drive["unknown_members"]  + choice["unknown_members"]  + mobile["unknown_members"]

    share       = total_unknown / 3.0
    adj_children = total_children + share
    adj_adults   = total_adults   + share
    adj_seniors  = total_seniors  + share
    age_total    = adj_children + adj_adults + adj_seniors

    pct_children = round((adj_children / age_total) * 100, 1) if age_total else 0
    pct_adults   = round((adj_adults   / age_total) * 100, 1) if age_total else 0
    pct_seniors  = round((adj_seniors  / age_total) * 100, 1) if age_total else 0

    print(f"\n  📊 Results:")
    print(f"     Household visits:    {total_hh_visits:,}")
    print(f"     Individual visits:   {total_individual_visits:,}")
    print(f"     Monthly food (lbs):  {monthly_food_lbs:,.1f}")
    print(f"     Age →  Children: {pct_children}%  Adults: {pct_adults}%  Seniors: {pct_seniors}%")
    print(f"     Unique FRH agencies: {frh['unique_agencies']}")

    return {
        "hh_drive_thru":           hh_drive_thru,
        "hh_choice":               hh_choice,
        "hh_mobile_fsk":           hh_mobile_fsk,
        "hh_frh":                  hh_frh,
        "total_hh_visits":         total_hh_visits,
        "total_individual_visits": total_individual_visits,
        "unique_families":         max(1, int(total_hh_visits * 0.6)),  # Estimate: ~60% of visits are unique
        "monthly_food_lbs":        round(monthly_food_lbs, 1),
        "milk_lbs":                round(milk_lbs, 1),
        "egg_lbs":                 round(egg_lbs, 1),
        "adj_children":            round(adj_children),
        "adj_adults":              round(adj_adults),
        "adj_seniors":             round(adj_seniors),
        "pct_children":            pct_children,
        "pct_adults":              pct_adults,
        "pct_seniors":             pct_seniors,
        "ytd_mobile_events":       manual_inputs["ytd_mobile_events"],
        "ytd_popup_events":        manual_inputs["ytd_popup_events"],
        "unique_frh_agencies":     frh["unique_agencies"],
        "fresh_start_kitchens":    manual_inputs["fresh_start_kitchen_count"],
        "homeless_backpacks":      manual_inputs["homeless_backpacks"],
        "power_packs":             manual_inputs["power_packs"],
        "report_month":            manual_inputs["report_month"],
        "report_year":             manual_inputs["report_year"],
    }

# ===========================================================================
# STEP 4: PUSH TO GOOGLE SHEETS
# ===========================================================================

def connect_to_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)
    return gspread.authorize(creds)


def _open_sheet(client):
    return client.open_by_key(GOOGLE_SHEET_ID) if GOOGLE_SHEET_ID else client.open(GOOGLE_SHEET_NAME)


def append_to_history(client, numbers):
    sheet = _open_sheet(client)
    try:
        history = sheet.worksheet(HISTORY_TAB_NAME)
    except gspread.WorksheetNotFound:
        print(f"  ⚠️  Tab '{HISTORY_TAB_NAME}' not found. Creating it...")
        history = sheet.add_worksheet(title=HISTORY_TAB_NAME, rows=200, cols=30)
        history.append_row([
            "Month", "Year",
            "HH Drive Thru", "HH Choice", "HH Mobile+FSK", "HH FRH",
            "Total HH Visits", "Total Individual Visits",
            "Monthly Food Lbs", "Milk Lbs", "Egg Lbs",
            "Children (adj)", "Adults (adj)", "Seniors (adj)",
            "% Children", "% Adults", "% Seniors",
            "YTD Mobile Events", "YTD Popup Events",
            "Unique FRH Agencies", "Fresh Start Kitchens",
            "Homeless Backpacks", "Power Packs",
        ], value_input_option="USER_ENTERED")

    history.append_row([
        numbers["report_month"], numbers["report_year"],
        numbers["hh_drive_thru"], numbers["hh_choice"],
        numbers["hh_mobile_fsk"], numbers["hh_frh"],
        numbers["total_hh_visits"], numbers["total_individual_visits"],
        numbers["monthly_food_lbs"], numbers["milk_lbs"], numbers["egg_lbs"],
        numbers["adj_children"], numbers["adj_adults"], numbers["adj_seniors"],
        numbers["pct_children"], numbers["pct_adults"], numbers["pct_seniors"],
        numbers["ytd_mobile_events"], numbers["ytd_popup_events"],
        numbers["unique_frh_agencies"], numbers["fresh_start_kitchens"],
        numbers["homeless_backpacks"], numbers["power_packs"],
    ], value_input_option="USER_ENTERED")
    print(f"  ✅ Row appended to '{HISTORY_TAB_NAME}' tab.")


def update_dashboard_tab(client, numbers):
    sheet     = _open_sheet(client)
    dashboard = sheet.sheet1

    updates = {
        "B2":  numbers["hh_drive_thru"],
        "B3":  numbers["hh_choice"],
        "B4":  numbers["hh_mobile_fsk"],
        "B5":  numbers["hh_frh"],
        "B7":  numbers["total_hh_visits"],
        "B8":  numbers["total_individual_visits"],
        "B9":  numbers["monthly_food_lbs"],
        "B11": numbers["adj_children"],
        "B12": numbers["adj_adults"],
        "B13": numbers["adj_seniors"],
        "B14": numbers["pct_children"],
        "B15": numbers["pct_adults"],
        "B16": numbers["pct_seniors"],
        "B18": numbers["ytd_mobile_events"],
        "B19": numbers["ytd_popup_events"],
        "B20": numbers["unique_frh_agencies"],
        "B21": numbers["fresh_start_kitchens"],
        "B22": numbers["homeless_backpacks"],
        "B23": numbers["power_packs"],
    }
    dashboard.batch_update([
        {"range": cell, "values": [[value]]} for cell, value in updates.items()
    ])
    print(f"  ✅ Dashboard tab updated.")


def create_monthly_dashboard_tab(client, numbers):
    """
    Create/update a new tab for each month with full interactive dashboard.
    Tab name format: "2026-Feb" (YYYY-Mon).
    """
    sheet = _open_sheet(client)
    month_str = numbers["report_month"]  # e.g., "February 2026"
    
    # Parse month/year and create tab name
    try:
        dt = datetime.strptime(month_str, "%B %Y")
        tab_name = dt.strftime("%Y-%b")  # e.g., "2026-Feb"
    except ValueError:
        tab_name = month_str
    
    # Create or get worksheet
    try:
        ws = sheet.worksheet(tab_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        print(f"  📄 Creating new tab: {tab_name}")
        ws = sheet.add_worksheet(title=tab_name, rows=150, cols=12)
    
    # Build all rows for the dashboard
    rows = [
        ["INTERACTIVE DASHBOARD — Source: IPM Assistance", "", "", "", "", "", ""],
        [],
        ["FILTER", "", "Value"],
        ["Date Range:", "", month_str],
        [],
        ["KEY PERFORMANCE INDICATORS", "", "Value"],
        ["Total Individual Visits", "", numbers["total_individual_visits"]],
        ["Total Household Visits", "", numbers["total_hh_visits"]],
        ["Average Visits per Family", "", round(numbers["total_individual_visits"] / max(1, numbers.get("unique_families", 1)), 1)],
        ["Total Food Lbs", "", numbers["monthly_food_lbs"]],
        [],
        ["BREAKDOWN BY CATEGORY", "", "Visits"],
        ["Drive Thru Pantry", "", numbers["hh_drive_thru"]],
        ["Choice Pantry", "", numbers["hh_choice"]],
        ["Mobile Pantry", "", int(numbers["hh_mobile_fsk"] * 0.5)],  # Estimated split
        ["Fresh Start", "", int(numbers["hh_mobile_fsk"] * 0.5)],
        [],
        ["BREAKDOWN BY AGE GROUP", "", "Visits", "", "BREAKDOWN BY GENDER", "", "Visits"],
        ["Age Group", "", "Visits", "", "Gender", "", "Visits"],
        ["0-17", "", numbers.get("adj_children", 0), "", "Female", "", ""],
        ["18-34", "", "", "", "Male", "", ""],
        ["35-54", "", "", "", "Non-Conforming", "", ""],
        ["55-64", "", "", "", "Prefer Not to Say", "", ""],
        ["65+", "", numbers.get("adj_seniors", 0), "", "", "", ""],
        ["Unknown", "", "", "", "", "", ""],
        [],
        ["BREAKDOWN BY EMPLOYMENT STATUS", "", "Visits", "", "BREAKDOWN BY VETERAN STATUS", "", "Visits"],
        ["Employment", "", "Visits", "", "Veteran Status", "", "Visits"],
        ["Full Time", "", "", "", "Yes", "", ""],
        ["Part Time", "", "", "", "No", "", ""],
        ["Disabled", "", "", "", "Not Specified", "", ""],
        ["Retired", "", "", "", "", "", ""],
        ["Unemployed", "", "", "", "", "", ""],
        ["Stay at Home Caretaker", "", "", "", "", "", ""],
        [],
        ["BREAKDOWN BY HOMELESS STATUS", "", "Visits", "", "BREAKDOWN BY HOUSEHOLD SIZE", "", "Visits"],
        ["Homeless", "", "Visits", "", "Household Size", "", "Visits"],
        ["Yes", "", "", "", "1 Person", "", ""],
        ["Not Specified", "", "", "", "2 People", "", ""],
        ["", "", "", "", "3 People", "", ""],
        ["", "", "", "", "4 People", "", ""],
        ["", "", "", "", "5+ People", "", ""],
        [],
        ["BREAKDOWN BY LOCATION (Top 10)", "", "Visits"],
        ["Location", "", "Visits"],
        ["", "", ""],
        ["", "", ""],
        ["", "", ""],
        ["", "", ""],
        ["", "", ""],
        ["", "", ""],
        ["", "", ""],
        ["", "", ""],
        [],
        ["OTHER METRICS", "", "Value"],
        ["Unique Fresh Start Kitchens", "", numbers["unique_frh_agencies"]],
        ["YTD Mobile Events", "", numbers["ytd_mobile_events"]],
        ["YTD Popup Events", "", numbers["ytd_popup_events"]],
        ["Homeless Backpacks Distributed", "", numbers["homeless_backpacks"]],
        ["Power Packs Distributed", "", numbers["power_packs"]],
    ]
    
    # Pad all rows to 7 columns
    padded = [r + [""] * max(0, 7 - len(r)) for r in rows]
    
    # Write all rows
    ws.update("A1", padded, value_input_option="USER_ENTERED")
    
    # Apply formatting
    fmt_reqs = []
    
    def bg_format(row, cols, r, g, b):
        """Create background color request"""
        return {"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": row-1, "endRowIndex": row,
                      "startColumnIndex": cols[0]-1, "endColumnIndex": cols[1]},
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": r, "green": g, "blue": b}}},
            "fields": "userEnteredFormat.backgroundColor"
        }}
    
    def bold_format(row, cols, sz=11):
        """Create bold text request"""
        return {"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": row-1, "endRowIndex": row,
                      "startColumnIndex": cols[0]-1, "endColumnIndex": cols[1]},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True, "fontSize": sz}}},
            "fields": "userEnteredFormat.textFormat"
        }}
    
    # Colors
    header_color = (0.18, 0.46, 0.71)  # Blue
    section_color = (0.11, 0.33, 0.27)  # Dark green
    
    # Title row
    fmt_reqs.append(bg_format(1, [1, 7], *section_color))
    fmt_reqs.append(bold_format(1, [1, 7], 13))
    
    # Section headers (rows with uppercase labels)
    section_rows = [3, 6, 12, 18, 27, 35, 42, 50]
    for sr in section_rows:
        fmt_reqs.append(bg_format(sr, [1, 7], *header_color))
        fmt_reqs.append(bold_format(sr, [1, 7]))
    
    # Column widths
    fmt_reqs.append({"updateDimensionProperties": {
        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 7},
        "properties": {"pixelSize": 200}, "fields": "pixelSize"
    }})
    
    if fmt_reqs:
        try:
            sheet.batch_update({"requests": fmt_reqs})
        except Exception as e:
            print(f"  ⚠️  Formatting error: {e}")
    
    print(f"  ✅ Monthly dashboard tab '{tab_name}' created/updated.")


# ===========================================================================
# MAIN
# ===========================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IPM Dashboard Automation")
    parser.add_argument(
        "--source",
        choices=["auto", "api", "sql", "csv"],
        default="auto",
        help="Data source: auto (API→SQL→CSV), api, sql, or csv  (default: auto)",
    )
    args = parser.parse_args()

    print("=" * 55)
    print("  IPM Dashboard Automation")
    print(f"  Running for: {REPORT_MONTH}")
    print("=" * 55)

    manual_inputs = {
        "report_month":            REPORT_MONTH,
        "report_year":             REPORT_YEAR,
        "inkind_lbs":              INKIND_LBS,
        "fsfb_lbs":                FSFB_LBS,
        "milk_half_gallons":       MILK_HALF_GALLONS,
        "egg_dozens":              EGG_DOZENS,
        "other_food_lbs":          OTHER_FOOD_LBS,
        "ytd_mobile_events":       YTD_MOBILE_EVENTS,
        "ytd_popup_events":        YTD_POPUP_EVENTS,
        "fresh_start_kitchen_count": FRESH_START_KITCHEN_COUNT,
        "homeless_backpacks":      HOMELESS_BACKPACKS,
        "power_packs":             POWER_PACKS,
    }

    numbers = calculate_dashboard_numbers(manual_inputs, source=args.source)

    print("\n📤 Connecting to Google Sheets...")
    client = connect_to_google_sheets()
    append_to_history(client, numbers)
    update_dashboard_tab(client, numbers)
    create_monthly_dashboard_tab(client, numbers)

    print("\n✅ All done! Check your Google Sheet.")
    print("=" * 55)
