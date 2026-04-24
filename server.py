"""
IPM Live Web Dashboard — Flask server
======================================
Serves dashboard.html at http://localhost:5002

    python server.py              ← SQL only

DATA SOURCE NOTES
    SQL  — full dashboard: YTD, lifetime, trends, year-over-year, county, age
"""

import sys
import os
import calendar
import argparse
import time
import re
from threading import Lock
from pathlib import Path
import requests
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify, request, send_file, make_response
from flask_cors import CORS  # Add this

from env_loader import load_local_env

app = Flask(__name__)
CORS(app)
load_local_env()
# Import Google Sheets helpers from ipm.py (same codebase)
try:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from ipm import connect_to_google_sheets, append_to_history, update_dashboard_tab
    _IPM_IMPORT_OK = True
except Exception:
    _IPM_IMPORT_OK = False

# ===========================================================================
# CONFIG — keep in sync with ipm.py
# ===========================================================================

API_TOKEN   = os.getenv("OASIS_API_TOKEN", "")
NETWORK_URL = os.getenv("OASIS_NETWORK_URL", "https://ipm.oasisinsight.net")

CATEGORY_IDS = {
    "drive_through": [50, 57, 59, 2, 20, 39, 40],
    "choice_pantry": [52, 26, 27, 53],
    "mobile":        [37],
    "fresh_start":   [44, 54],
}

DB_HOST     = os.getenv("DB_HOST", "gateway01.eu-central-1.prod.aws.tidbcloud.com")
DB_PORT     = int(os.getenv("DB_PORT", "4000"))
DB_NAME     = os.getenv("DB_NAME", "oasis")
DB_USER     = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSL_CA   = os.getenv("DB_SSL_CA", "/etc/ssl/cert.pem")  # macOS default; Linux: /etc/ssl/certs/ca-certificates.crt

GOOGLE_CREDS_FILE  = os.getenv("GOOGLE_CREDS_FILE", str(Path(__file__).resolve().parent / "ipm-dashboard.json"))
GOOGLE_SHEET_NAME  = os.getenv("GOOGLE_SHEET_NAME", "IPM Monthly Dashboard")
GOOGLE_SHEET_ID    = os.getenv("GOOGLE_SHEET_ID", "1POZpWu4tgYSGW7o5DUN4kBbL0vP0vhD9Skeq3FLYHSk")
HISTORY_TAB_NAME   = os.getenv("HISTORY_TAB_NAME", "Monthly History")
DASHBOARD_TAB_NAME = os.getenv("DASHBOARD_TAB_NAME", "Dashboard")

# Small in-memory cache for repeated dashboard requests.
# This speeds up back-to-back page loads without requiring paid warm instances.
# Longer TTL lowers repeat SQL load and usually lowers Cloud Run CPU time/cost.
DASHBOARD_CACHE_TTL_SECONDS = int(os.getenv("DASHBOARD_CACHE_TTL_SECONDS", "900"))
_DASHBOARD_CACHE = {}
_DASHBOARD_CACHE_LOCK = Lock()
SQL_POOL_SIZE = int(os.getenv("SQL_POOL_SIZE", "16"))
_SQL_POOL = None
_SQL_POOL_LOCK = Lock()
_PANTRY_PDF_INDEX = None
_PANTRY_PDF_INDEX_LOCK = Lock()
_PANTRY_PDF_EXTRACT_CACHE = {}
_PANTRY_PDF_EXTRACT_CACHE_LOCK = Lock()

# ===========================================================================
# SQL HELPERS
# ===========================================================================

def _sql_connect():
    try:
        import mysql.connector as _mc
    except ImportError:
        raise ImportError("mysql-connector-python not installed. Run: pip install mysql-connector-python")

    kwargs = dict(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        connection_timeout=15,
    )
    if DB_SSL_CA and os.path.exists(DB_SSL_CA):
        kwargs.update(ssl_ca=DB_SSL_CA, ssl_verify_cert=True, ssl_verify_identity=True)

    global _SQL_POOL
    try:
        if _SQL_POOL is None:
            with _SQL_POOL_LOCK:
                if _SQL_POOL is None:
                    _SQL_POOL = _mc.pooling.MySQLConnectionPool(
                        pool_name="ipm_dashboard_pool",
                        pool_size=max(1, SQL_POOL_SIZE),
                        pool_reset_session=True,
                        **kwargs,
                    )
        return _SQL_POOL.get_connection()
    except Exception:
        # Fallback: if pool initialization fails for any reason, still allow direct connection.
        return _mc.connect(**kwargs)


def _run(conn, sql, params=()):
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        cur.close()


def _run_pool(sql, params=()):
    """Get a connection from the shared pool, run one query, release the connection."""
    conn = _sql_connect()
    try:
        return _run(conn, sql, params)
    finally:
        conn.close()


def _ytd_range(year, month):
    # Returns (start_inclusive, end_exclusive) so queries use < end, not <= end.
    # This avoids missing same-day records on datetime columns (BETWEEN truncates to 00:00:00).
    next_month = month + 1
    next_year  = year
    if next_month > 12:
        next_month = 1
        next_year += 1
    return f"{year}-01-01", f"{next_year}-{next_month:02d}-01"


def _month_report_exists(year, month):
    reports_dir = Path(__file__).resolve().parent / "reports"
    month_prefix = f"{year}_{month:02d}_"
    return any(path.is_file() for path in reports_dir.glob(f"{month_prefix}*.csv"))


def _month_window(year, month):
    month_start = f"{year}-{month:02d}-01"
    next_month = month + 1
    next_year = year
    if next_month > 12:
        next_month = 1
        next_year += 1
    month_end = f"{next_year}-{next_month:02d}-01"
    return month_start, month_end


def _monthly_report_has_data(data):
    if not data or not isinstance(data, dict):
        return False

    base_total = sum(int(data.get(key) or 0) for key in ("monthHH", "monthInd", "monthLbs", "uniqueFamiliesMonth"))
    if base_total > 0:
        return True

    prog = data.get("monthProg") or {}
    prog_total = sum(int(prog.get(key) or 0) for key in ("D", "C", "M", "F", "other"))
    if prog_total > 0:
        return True

    events = data.get("monthEvents") or {}
    event_total = sum(int(events.get(key) or 0) for key in ("agencies", "popup", "fsk"))
    return event_total > 0


def _attach_monthly_report_source(data, year, month, trend_months):
    if _monthly_report_has_data(data):
        return data

    search_year = year
    search_month = month - 1
    while search_year >= 2021:
        if search_month < 1:
            search_month = 12
            search_year -= 1
            if search_year < 2021:
                break

        fallback = _get_dashboard_sql(search_year, search_month, trend_months)
        if _monthly_report_has_data(fallback):
            data["reportSourceKey"] = f"{calendar.month_name[search_month]} {search_year}"
            data["reportSourceData"] = fallback
            data["reportSourceUsedFallback"] = True
            return data

        search_month -= 1

    return data


def _get_dashboard_sql(year, month, trend_months):
    start, end       = _ytd_range(year, month)
    ly_start, ly_end = _ytd_range(year - 1, month)
    month_start, month_end     = _month_window(year, month)
    ly_month_start, ly_month_end = _month_window(year - 1, month)
    month_report_available = _month_report_exists(year, month)

    TL = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    # Build trend SQL before entering executor so we can submit it along with everything else.
    if trend_months == "all":
        trend_sql    = "SELECT YEAR(aa.date) AS yr, MONTH(aa.date) AS mo, COUNT(*) AS cnt FROM assistance_assistance aa GROUP BY yr, mo ORDER BY yr, mo"
        trend_params = ()
    else:
        try:
            _mb = int(trend_months)
        except (ValueError, TypeError):
            _mb = 12
        trend_sql    = "SELECT YEAR(aa.date) AS yr, MONTH(aa.date) AS mo, COUNT(*) AS cnt FROM assistance_assistance aa WHERE aa.date >= DATE_SUB(%s, INTERVAL %s MONTH) AND aa.date < %s GROUP BY yr, mo ORDER BY yr, mo"
        trend_params = (end, _mb, end)

    # ── Dispatch all independent queries in parallel ──────────────
    with ThreadPoolExecutor(max_workers=SQL_POOL_SIZE) as ex:
        fmc  = ex.submit(_run_pool, "SELECT COUNT(*) AS cnt FROM assistance_assistance aa WHERE aa.date >= %s AND aa.date < %s", (start, end))
        fytd = ex.submit(_run_pool, """
            SELECT
                SUM(CASE WHEN ac.name LIKE %s OR ac.name LIKE %s OR ac.name LIKE %s OR ac.name LIKE %s THEN 1 ELSE 0 END) AS d_hh,
                SUM(CASE WHEN ac.name LIKE %s THEN 1 ELSE 0 END) AS c_hh,
                SUM(CASE WHEN ac.name = %s THEN 1 ELSE 0 END) AS m_hh,
                SUM(CASE WHEN ac.name LIKE %s OR ac.name LIKE %s OR ac.name LIKE %s THEN 1 ELSE 0 END) AS f_hh,
                COUNT(*) AS total_hh
            FROM assistance_assistance aa
            JOIN assistance_category ac ON aa.category_id = ac.id
            WHERE aa.date >= %s AND aa.date < %s
        """, ('%Drive Thru%', '%Drive Through%', '%Door Dash%', '%DoorDash%',
              '%Choice Pantry%', 'Mobile Pantry',
              '%Fresh Start%', '%Pop-up%', '%Senior Box%',
              start, end))
        fmyt = ex.submit(_run_pool, """
            SELECT
                SUM(CASE WHEN ac.name LIKE %s OR ac.name LIKE %s OR ac.name LIKE %s OR ac.name LIKE %s THEN 1 ELSE 0 END) AS d_hh,
                SUM(CASE WHEN ac.name LIKE %s THEN 1 ELSE 0 END) AS c_hh,
                SUM(CASE WHEN ac.name = %s THEN 1 ELSE 0 END) AS m_hh,
                SUM(CASE WHEN ac.name LIKE %s OR ac.name LIKE %s OR ac.name LIKE %s THEN 1 ELSE 0 END) AS f_hh,
                COUNT(*) AS total_hh
            FROM assistance_assistance aa
            JOIN assistance_category ac ON aa.category_id = ac.id
            WHERE aa.date >= %s AND aa.date < %s
        """, ('%Drive Thru%', '%Drive Through%', '%Door Dash%', '%DoorDash%',
              '%Choice Pantry%', 'Mobile Pantry',
              '%Fresh Start%', '%Pop-up%', '%Senior Box%',
              month_start, month_end))
        fmi  = ex.submit(_run_pool, "SELECT SUM(COALESCE(ch.case_count, 1)) AS cnt FROM assistance_assistance aa JOIN cases_case hoh ON hoh.id = aa.case_id LEFT JOIN cases_household ch ON ch.id = hoh.household_id WHERE aa.date >= %s AND aa.date < %s", (month_start, month_end))
        fml  = ex.submit(_run_pool, "SELECT ROUND(SUM(aa.amount)) AS lbs  FROM assistance_assistance aa WHERE aa.unit_id = 2 AND aa.date >= %s AND aa.date < %s", (month_start, month_end))
        fmb  = ex.submit(_run_pool, "SELECT ROUND(SUM(aa.amount)) AS bags FROM assistance_assistance aa WHERE aa.unit_id = 3 AND aa.date >= %s AND aa.date < %s", (month_start, month_end))
        find = ex.submit(_run_pool, "SELECT SUM(COALESCE(ch.case_count, 1)) AS cnt FROM assistance_assistance aa JOIN cases_case hoh ON hoh.id = aa.case_id LEFT JOIN cases_household ch ON ch.id = hoh.household_id WHERE aa.date >= %s AND aa.date < %s", (start, end))
        flbs = ex.submit(_run_pool, "SELECT ROUND(SUM(aa.amount)) AS lbs  FROM assistance_assistance aa WHERE aa.unit_id = 2 AND aa.date >= %s AND aa.date < %s", (start, end))
        fbag = ex.submit(_run_pool, "SELECT ROUND(SUM(aa.amount)) AS bags FROM assistance_assistance aa WHERE aa.unit_id = 3 AND aa.date >= %s AND aa.date < %s", (start, end))
        fly  = ex.submit(_run_pool, """
            SELECT COUNT(*) AS cnt FROM assistance_assistance aa
            JOIN assistance_category ac ON aa.category_id = ac.id
            WHERE aa.date >= %s AND aa.date < %s
              AND (ac.name LIKE '%Drive Thru%' OR ac.name LIKE '%Drive Through%'
                   OR ac.name LIKE '%Door Dash%' OR ac.name LIKE '%DoorDash%'
                   OR ac.name LIKE '%Choice Pantry%' OR ac.name = 'Mobile Pantry'
                   OR ac.name LIKE '%Fresh Start%' OR ac.name LIKE '%Senior Box%')
        """, (ly_start, ly_end))
        flt  = ex.submit(_run_pool, """
            SELECT
                SUM(CASE WHEN ac.name LIKE '%Drive Thru%' OR ac.name LIKE '%Drive Through%'
                           OR ac.name LIKE '%Door Dash%' OR ac.name LIKE '%DoorDash%' THEN 1 ELSE 0 END) AS d,
                SUM(CASE WHEN ac.name LIKE '%Choice Pantry%' THEN 1 ELSE 0 END) AS c,
                SUM(CASE WHEN ac.name = 'Mobile Pantry' THEN 1 ELSE 0 END) AS m,
                SUM(CASE WHEN ac.name LIKE '%Fresh Start%' OR ac.name LIKE '%Pop-up%'
                           OR ac.name LIKE '%Senior Box%' THEN 1 ELSE 0 END) AS f,
                COUNT(*) AS total
            FROM assistance_assistance aa
            JOIN assistance_category ac ON aa.category_id = ac.id
        """)
        filt = ex.submit(_run_pool, "SELECT SUM(COALESCE(ch.case_count, 1)) AS cnt FROM assistance_assistance aa JOIN cases_case hoh ON hoh.id = aa.case_id LEFT JOIN cases_household ch ON ch.id = hoh.household_id")
        fage = ex.submit(_run_pool, """
            SELECT
                prog,
                SUM(CASE WHEN c2.date_of_birth IS NULL THEN 1 ELSE 0 END) AS uk,
                SUM(CASE WHEN c2.date_of_birth IS NOT NULL AND TIMESTAMPDIFF(YEAR, c2.date_of_birth, CURDATE()) < 18 THEN 1 ELSE 0 END) AS ch,
                SUM(CASE WHEN c2.date_of_birth IS NOT NULL AND TIMESTAMPDIFF(YEAR, c2.date_of_birth, CURDATE()) BETWEEN 18 AND 59 THEN 1 ELSE 0 END) AS ad,
                SUM(CASE WHEN c2.date_of_birth IS NOT NULL AND TIMESTAMPDIFF(YEAR, c2.date_of_birth, CURDATE()) >= 60 THEN 1 ELSE 0 END) AS sr
            FROM (
                SELECT aa.id AS visit_id, aa.case_id,
                    CASE
                        WHEN ac.name LIKE '%Drive Thru%' OR ac.name LIKE '%Drive Through%'
                          OR ac.name LIKE '%Door Dash%' OR ac.name LIKE '%DoorDash%' THEN 'D'
                        WHEN ac.name LIKE '%Choice Pantry%' THEN 'C'
                        WHEN ac.name = 'Mobile Pantry' THEN 'M'
                        ELSE NULL
                    END AS prog
                FROM assistance_assistance aa
                JOIN assistance_category ac ON ac.id = aa.category_id
                WHERE aa.date >= %s AND aa.date < %s
            ) v
            JOIN cases_case hoh ON hoh.id = v.case_id
            JOIN cases_case c2  ON c2.household_id = hoh.household_id AND c2.deceased = 0
            WHERE v.prog IS NOT NULL
            GROUP BY prog
        """, (start, end))
        fcty = ex.submit(_run_pool, """
            SELECT
                COALESCE(dc.name, 'Other') AS county,
                SUM(CASE WHEN ch.max_dob > DATE_SUB(CURDATE(), INTERVAL 18 YEAR) THEN 1 ELSE 0 END) AS w,
                SUM(CASE WHEN ch.max_dob IS NULL OR ch.max_dob <= DATE_SUB(CURDATE(), INTERVAL 18 YEAR) THEN 1 ELSE 0 END) AS n,
                COUNT(*) AS total
            FROM assistance_assistance aa
            JOIN assistance_category ac ON aa.category_id = ac.id
            JOIN cases_case hoh ON hoh.id = aa.case_id
            JOIN cases_household ch ON ch.id = hoh.household_id
            LEFT JOIN details_county dc ON ch.county_id = dc.id
            WHERE ac.name = 'Mobile Pantry' AND aa.date >= %s AND aa.date < %s
            GROUP BY county ORDER BY total DESC
        """, (start, end))
        fctf = ex.submit(_run_pool, """
            SELECT
                COALESCE(dc.name, 'Other') AS county,
                SUM(CASE WHEN ac.name LIKE %s OR ac.name LIKE %s OR ac.name LIKE %s OR ac.name LIKE %s THEN 1 ELSE 0 END) AS drive,
                SUM(CASE WHEN ac.name LIKE %s THEN 1 ELSE 0 END) AS choice,
                SUM(CASE WHEN ac.name = %s THEN 1 ELSE 0 END) AS mobile,
                SUM(CASE WHEN ac.name LIKE %s OR ac.name LIKE %s OR ac.name LIKE %s THEN 1 ELSE 0 END) AS fresh,
                COUNT(*) AS total,
                COUNT(DISTINCT aa.case_id) AS unique_hh,
                SUM(COALESCE(ch.case_count, 1)) AS total_members
            FROM assistance_assistance aa
            JOIN assistance_category ac ON aa.category_id = ac.id
            JOIN cases_case hoh ON hoh.id = aa.case_id
            JOIN cases_household ch ON ch.id = hoh.household_id
            LEFT JOIN details_county dc ON dc.id = ch.county_id
            WHERE aa.date >= %s AND aa.date < %s
            GROUP BY county ORDER BY total DESC
        """, ('%Drive Thru%', '%Drive Through%', '%Door Dash%', '%DoorDash%',
              '%Choice Pantry%', 'Mobile Pantry',
              '%Fresh Start%', '%Pop-up%', '%Senior Box%',
              start, end))
        fmob = ex.submit(_run_pool, "SELECT COUNT(DISTINCT DATE(aa.date)) AS cnt FROM assistance_assistance aa JOIN assistance_category ac ON aa.category_id = ac.id WHERE ac.name = 'Mobile Pantry' AND aa.date >= %s AND aa.date < %s", (start, end))
        ffrh = ex.submit(_run_pool, "SELECT COUNT(*) AS cnt, COUNT(DISTINCT ee.location_id) AS agencies FROM events_event ee JOIN events_eventtype et ON ee.event_type_id = et.id WHERE et.name = 'FoodLink' AND ee.date >= %s AND ee.date < %s", (start, end))
        fpop = ex.submit(_run_pool, "SELECT COUNT(*) AS cnt FROM events_event ee JOIN events_eventtype et ON ee.event_type_id = et.id WHERE et.name IN ('Pop up Pantry', 'Senior Pop Up') AND ee.date >= %s AND ee.date < %s", (start, end))
        ffsk = ex.submit(_run_pool, "SELECT COUNT(*) AS cnt FROM events_event ee JOIN events_eventtype et ON ee.event_type_id = et.id WHERE et.name = 'Fresh Start/Re-Start Kitchen' AND ee.date >= %s AND ee.date < %s", (start, end))
        fbp  = ex.submit(_run_pool, "SELECT COALESCE(SUM(ea.value), 0) AS total FROM events_eventactivity ea JOIN events_activity a ON a.id = ea.activity_id JOIN events_event ee ON ee.id = ea.event_id WHERE a.name = 'Homeless Backpacks' AND ee.date >= %s AND ee.date < %s", (start, end))
        fpp  = ex.submit(_run_pool, "SELECT COALESCE(SUM(ea.value), 0) AS total FROM events_eventactivity ea JOIN events_activity a ON a.id = ea.activity_id JOIN events_event ee ON ee.id = ea.event_id WHERE a.name IN ('Total Power Packs', 'Total Elevated Power Packs') AND ee.date >= %s AND ee.date < %s", (start, end))
        fzip = ex.submit(_run_pool, "SELECT ch.zip_code AS zip, COUNT(DISTINCT aa.case_id) AS cnt FROM assistance_assistance aa JOIN cases_case hoh ON hoh.id = aa.case_id JOIN cases_household ch ON ch.id = hoh.household_id WHERE aa.date >= %s AND aa.date < %s AND ch.zip_code IS NOT NULL AND ch.zip_code != '' GROUP BY ch.zip_code ORDER BY cnt DESC", (start, end))
        ftrd = ex.submit(_run_pool, trend_sql, trend_params)
        fmly = ex.submit(_run_pool, """
            SELECT MONTH(aa.date) AS mo, COUNT(*) AS hh, SUM(COALESCE(ch.case_count, 1)) AS ind
            FROM assistance_assistance aa
            JOIN cases_case hoh ON hoh.id = aa.case_id
            LEFT JOIN cases_household ch ON ch.id = hoh.household_id
            WHERE aa.date >= %s AND aa.date < %s
            GROUP BY mo ORDER BY mo
        """, (f"{year}-01-01", f"{year + 1}-01-01"))
        fdem = ex.submit(_run_pool, """
            SELECT dg.name AS grp, dd.name AS val, COUNT(DISTINCT aa.case_id) AS cnt
            FROM assistance_assistance aa
            JOIN cases_casedetail dc ON aa.case_id = dc.case_id
            JOIN details_detail dd ON dc.detail_id = dd.id
            JOIN details_group dg ON dd.group_id = dg.id
            WHERE dg.name IN ('Gender','Ethnicity','Veteran','Employment',
                              'Government Benefits','Education','Language')
              AND aa.date >= %s AND aa.date < %s
            GROUP BY dg.name, dd.name
            ORDER BY dg.name, cnt DESC
        """, (start, end))
        fhhs = ex.submit(_run_pool, "SELECT ch.case_count AS size, COUNT(*) AS households FROM assistance_assistance aa JOIN cases_case hoh ON hoh.id = aa.case_id JOIN cases_household ch ON ch.id = hoh.household_id WHERE aa.date >= %s AND aa.date < %s GROUP BY ch.case_count ORDER BY ch.case_count", (start, end))
        fnf  = ex.submit(_run_pool, "SELECT COUNT(*) AS cnt FROM (SELECT aa.case_id, MIN(aa.date) AS first_visit FROM assistance_assistance aa GROUP BY aa.case_id) t WHERE t.first_visit >= %s AND t.first_visit < %s", (start, end))
        frf  = ex.submit(_run_pool, "SELECT COUNT(DISTINCT aa.case_id) AS cnt FROM assistance_assistance aa WHERE aa.date >= %s AND aa.date < %s AND aa.case_id IN (SELECT DISTINCT case_id FROM assistance_assistance WHERE date < %s)", (start, end, start))
        # ── Monthly Report extra queries ───────────────────────────
        funiqm    = ex.submit(_run_pool, "SELECT COUNT(DISTINCT case_id) AS cnt FROM assistance_assistance WHERE date >= %s AND date < %s", (month_start, month_end))
        flymonkpi = ex.submit(_run_pool, """
            SELECT COUNT(*) AS hh, COUNT(DISTINCT aa.case_id) AS uniq,
                   SUM(COALESCE(ch.case_count, 1)) AS ind,
                   ROUND(SUM(CASE WHEN aa.unit_id = 2 THEN aa.amount ELSE 0 END)) AS lbs,
                   SUM(CASE WHEN ac.name LIKE %s OR ac.name LIKE %s OR ac.name LIKE %s OR ac.name LIKE %s THEN 1 ELSE 0 END) AS d,
                   SUM(CASE WHEN ac.name LIKE %s THEN 1 ELSE 0 END) AS c,
                   SUM(CASE WHEN ac.name = %s THEN 1 ELSE 0 END) AS m,
                   SUM(CASE WHEN ac.name LIKE %s OR ac.name LIKE %s OR ac.name LIKE %s THEN 1 ELSE 0 END) AS f
            FROM assistance_assistance aa
            JOIN assistance_category ac ON aa.category_id = ac.id
            JOIN cases_case hoh ON hoh.id = aa.case_id
            LEFT JOIN cases_household ch ON ch.id = hoh.household_id
            WHERE aa.date >= %s AND aa.date < %s
        """, ('%Drive Thru%', '%Drive Through%', '%Door Dash%', '%DoorDash%',
              '%Choice Pantry%', 'Mobile Pantry',
              '%Fresh Start%', '%Pop-up%', '%Senior Box%',
              ly_month_start, ly_month_end))
        fsamoyoy  = ex.submit(_run_pool, f"SELECT YEAR(date) AS yr, COUNT(*) AS hh, COUNT(DISTINCT case_id) AS uniq FROM assistance_assistance WHERE MONTH(date) = {month} AND YEAR(date) >= {year - 6} GROUP BY yr ORDER BY yr")
        fmon3yr   = ex.submit(_run_pool, f"SELECT YEAR(date) AS yr, MONTH(date) AS mo, COUNT(*) AS cnt FROM assistance_assistance WHERE date >= '{year-2}-01-01' AND date < '{year+1}-01-01' GROUP BY yr, mo ORDER BY yr, mo")
        fmoncity  = ex.submit(_run_pool, """
            SELECT ch.city, COUNT(*) AS cnt
            FROM assistance_assistance aa
            JOIN cases_case hoh ON hoh.id = aa.case_id
            JOIN cases_household ch ON ch.id = hoh.household_id
            WHERE aa.date >= %s AND aa.date < %s AND ch.city IS NOT NULL AND ch.city != ''
            GROUP BY ch.city ORDER BY cnt DESC LIMIT 10
        """, (month_start, month_end))
        fmoncty   = ex.submit(_run_pool, """
            SELECT COALESCE(TRIM(dc.name), 'Other') AS county, COUNT(*) AS total
            FROM assistance_assistance aa
            JOIN cases_case hoh ON hoh.id = aa.case_id
            JOIN cases_household ch ON ch.id = hoh.household_id
            LEFT JOIN details_county dc ON dc.id = ch.county_id
            WHERE aa.date >= %s AND aa.date < %s
            GROUP BY county ORDER BY total DESC
        """, (month_start, month_end))
        fmondem   = ex.submit(_run_pool, """
            SELECT dg.name AS grp, dd.name AS val, COUNT(DISTINCT aa.case_id) AS cnt
            FROM assistance_assistance aa
            JOIN cases_casedetail dc ON aa.case_id = dc.case_id
            JOIN details_detail dd ON dc.detail_id = dd.id
            JOIN details_group dg ON dd.group_id = dg.id
            WHERE dg.name IN ('Veteran','Employment','Government Benefits','Housing')
              AND aa.date >= %s AND aa.date < %s
            GROUP BY dg.name, dd.name ORDER BY dg.name, cnt DESC
        """, (month_start, month_end))
        fmondemly = ex.submit(_run_pool, """
            SELECT dg.name AS grp, dd.name AS val, COUNT(DISTINCT aa.case_id) AS cnt
            FROM assistance_assistance aa
            JOIN cases_casedetail dc ON aa.case_id = dc.case_id
            JOIN details_detail dd ON dc.detail_id = dd.id
            JOIN details_group dg ON dd.group_id = dg.id
            WHERE dg.name IN ('Veteran','Employment','Government Benefits','Housing')
              AND aa.date >= %s AND aa.date < %s
            GROUP BY dg.name, dd.name ORDER BY dg.name, cnt DESC
        """, (ly_month_start, ly_month_end))
        fmonagehh = ex.submit(_run_pool, """
            SELECT
                SUM(CASE WHEN c2.date_of_birth IS NOT NULL AND TIMESTAMPDIFF(YEAR, c2.date_of_birth, CURDATE()) >= 60 THEN 1 ELSE 0 END) AS seniors,
                SUM(CASE WHEN c2.date_of_birth IS NOT NULL AND TIMESTAMPDIFF(YEAR, c2.date_of_birth, CURDATE()) < 18 THEN 1 ELSE 0 END) AS children,
                SUM(CASE WHEN c2.date_of_birth IS NOT NULL AND TIMESTAMPDIFF(YEAR, c2.date_of_birth, CURDATE()) BETWEEN 18 AND 59 THEN 1 ELSE 0 END) AS adults
            FROM (SELECT DISTINCT case_id FROM assistance_assistance WHERE date >= %s AND date < %s) v
            JOIN cases_case hoh ON hoh.id = v.case_id
            JOIN cases_case c2 ON c2.household_id = hoh.household_id AND c2.deceased = 0
        """, (month_start, month_end))
        fmonagehhly = ex.submit(_run_pool, """
            SELECT
                SUM(CASE WHEN c2.date_of_birth IS NOT NULL AND TIMESTAMPDIFF(YEAR, c2.date_of_birth, CURDATE()) >= 60 THEN 1 ELSE 0 END) AS seniors,
                SUM(CASE WHEN c2.date_of_birth IS NOT NULL AND TIMESTAMPDIFF(YEAR, c2.date_of_birth, CURDATE()) < 18 THEN 1 ELSE 0 END) AS children
            FROM (SELECT DISTINCT case_id FROM assistance_assistance WHERE date >= %s AND date < %s) v
            JOIN cases_case hoh ON hoh.id = v.case_id
            JOIN cases_case c2 ON c2.household_id = hoh.household_id AND c2.deceased = 0
        """, (ly_month_start, ly_month_end))
        fmonevt   = ex.submit(_run_pool, """
            SELECT et.name AS evt, COUNT(*) AS cnt, COUNT(DISTINCT ee.location_id) AS locs
            FROM events_event ee
            JOIN events_eventtype et ON ee.event_type_id = et.id
            WHERE et.name IN ('FoodLink', 'Pop up Pantry', 'Senior Pop Up', 'Fresh Start/Re-Start Kitchen')
              AND ee.date >= %s AND ee.date < %s
            GROUP BY et.name
        """, (month_start, month_end))
        fmonevtly = ex.submit(_run_pool, """
            SELECT et.name AS evt, COUNT(*) AS cnt, COUNT(DISTINCT ee.location_id) AS locs
            FROM events_event ee
            JOIN events_eventtype et ON ee.event_type_id = et.id
            WHERE et.name IN ('FoodLink', 'Pop up Pantry', 'Senior Pop Up', 'Fresh Start/Re-Start Kitchen')
              AND ee.date >= %s AND ee.date < %s
            GROUP BY et.name
        """, (ly_month_start, ly_month_end))

    # ── Collect results ───────────────────────────────────────────
    month_record_count = int((fmc.result() or [{}])[0].get("cnt") or 0)

    ytd_row  = (fytd.result() or [{}])[0]
    d_hh     = int(ytd_row.get("d_hh")    or 0)
    c_hh     = int(ytd_row.get("c_hh")    or 0)
    m_hh     = int(ytd_row.get("m_hh")    or 0)
    f_hh     = int(ytd_row.get("f_hh")    or 0)
    total_hh = int(ytd_row.get("total_hh") or 0)

    myt_row    = (fmyt.result() or [{}])[0]
    month_hh   = int(myt_row.get("total_hh") or 0)
    month_d_hh = int(myt_row.get("d_hh")     or 0)
    month_c_hh = int(myt_row.get("c_hh")     or 0)
    month_m_hh = int(myt_row.get("m_hh")     or 0)
    month_f_hh = int(myt_row.get("f_hh")     or 0)
    month_other_hh = max(0, month_hh - month_d_hh - month_c_hh - month_m_hh - month_f_hh)
    month_ind = int((fmi.result()  or [{}])[0].get("cnt")      or 0)
    month_lbs = int((fml.result()  or [{}])[0].get("lbs")      or 0)
    month_bags= int((fmb.result()  or [{}])[0].get("bags")     or 0)
    total_ind = int((find.result() or [{}])[0].get("cnt")      or 0)
    total_lbs = int((flbs.result() or [{}])[0].get("lbs")      or 0)
    total_bags= int((fbag.result() or [{}])[0].get("bags")     or 0)

    ly_total  = int((fly.result()  or [{}])[0].get("cnt") or 0)
    vs_ly     = round((total_hh - ly_total) / ly_total * 100, 1) if ly_total else 0

    lt_row       = (flt.result()  or [{}])[0]
    total_ind_lt = int((filt.result() or [{}])[0].get("cnt") or 0)

    # ── Age breakdown ─────────────────────────────────────────────
    age_by_prog = {"D": {"ch":0,"ad":0,"sr":0,"uk":0}, "C": {"ch":0,"ad":0,"sr":0,"uk":0}, "M": {"ch":0,"ad":0,"sr":0,"uk":0}}
    for r in fage.result() or []:
        p = r.get("prog")
        if p in age_by_prog:
            age_by_prog[p] = {"ch": int(r.get("ch") or 0), "ad": int(r.get("ad") or 0), "sr": int(r.get("sr") or 0), "uk": int(r.get("uk") or 0)}
    base_ch     = age_by_prog["D"]["ch"] + age_by_prog["C"]["ch"] + age_by_prog["M"]["ch"]
    base_ad     = age_by_prog["D"]["ad"] + age_by_prog["C"]["ad"] + age_by_prog["M"]["ad"]
    base_sr     = age_by_prog["D"]["sr"] + age_by_prog["C"]["sr"] + age_by_prog["M"]["sr"]
    other_avg   = (age_by_prog["D"]["uk"] + age_by_prog["C"]["uk"] + age_by_prog["M"]["uk"]) / 3.0
    adj_ch, adj_ad, adj_sr = base_ch + other_avg, base_ad + other_avg, base_sr + other_avg
    adj_total   = (adj_ch + adj_ad + adj_sr) or 1
    age = {"ch": round(adj_ch/adj_total*100,1), "ad": round(adj_ad/adj_total*100,1), "sr": round(adj_sr/adj_total*100,1)}

    county = {r["county"]: {"w": int(r["w"] or 0), "n": int(r["n"] or 0)} for r in (fcty.result() or []) if r["county"] != "Other"}

    county_full = [
        {"county": str(r["county"]).strip(), "drive": int(r["drive"] or 0), "choice": int(r["choice"] or 0),
         "mobile": int(r["mobile"] or 0), "fresh": int(r["fresh"] or 0), "total": int(r["total"] or 0),
         "unique_hh": int(r["unique_hh"] or 0), "total_members": int(r["total_members"] or 0)}
        for r in (fctf.result() or [])
    ]

    mobile_event_cnt = int((fmob.result() or [{}])[0].get("cnt")      or 0)
    unique_agencies  = int((ffrh.result() or [{}])[0].get("agencies") or 0)
    popup_event_cnt  = int((fpop.result() or [{}])[0].get("cnt")      or 0)
    fsk_event_cnt    = int((ffsk.result() or [{}])[0].get("cnt")      or 0)
    total_backpacks  = int((fbp.result()  or [{}])[0].get("total")    or 0)
    total_powerpacks = int((fpp.result()  or [{}])[0].get("total")    or 0)
    zip_counts       = {r["zip"].strip(): int(r["cnt"]) for r in (fzip.result() or [])}

    # ── Trend ─────────────────────────────────────────────────────
    trend_rows = ftrd.result()
    trend_map  = {f"{int(r['yr'])}-{int(r['mo']):02d}": int(r["cnt"]) for r in trend_rows}
    if trend_months == "all":
        if trend_map:
            first = min(trend_map)
            fy, fm = int(first[:4]), int(first[5:])
        else:
            fy, fm = year, month
    else:
        try:
            mb = int(trend_months)
        except (ValueError, TypeError):
            mb = 12
        fy, fm = year, month
        for _ in range(mb - 1):
            fm -= 1
            if fm < 1:
                fm = 12
                fy -= 1
    trend, trend_labels = [], []
    cy, cm = fy, fm
    while (cy, cm) <= (year, month):
        key_mo = f"{cy}-{cm:02d}"
        trend.append(trend_map.get(key_mo, 0))
        trend_labels.append(f"{TL[cm-1]} {str(cy)[2:]}")
        cm += 1
        if cm > 12:
            cm = 1
            cy += 1

    # ── Monthly YTD ───────────────────────────────────────────────
    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    monthly_map = {int(r["mo"]): {"hh": int(r.get("hh") or 0), "ind": int(r.get("ind") or 0)} for r in (fmly.result() or [])}
    monthly_ytd, cum_hh, cum_ind = [], 0, 0
    for m in range(1, month + 1):
        row = monthly_map.get(m, {"hh": 0, "ind": 0})
        cum_hh  += row["hh"]
        cum_ind += row["ind"]
        monthly_ytd.append({"month": month_names[m - 1], "hh": cum_hh, "ind": cum_ind})

    # ── Demographics ──────────────────────────────────────────────
    demo_rows  = fdem.result() or []
    def _demo(grp): return {r["val"]: int(r["cnt"]) for r in demo_rows if r["grp"] == grp}
    gender     = _demo("Gender")
    ethnicity  = _demo("Ethnicity")
    veteran    = _demo("Veteran")
    employment = _demo("Employment")
    benefits   = _demo("Government Benefits")
    education  = _demo("Education")
    language   = _demo("Language")

    hh_size_dist    = {int(r["size"]): int(r["households"]) for r in (fhhs.result() or [])}
    new_families    = int((fnf.result() or [{}])[0].get("cnt") or 0)
    returning_families = int((frf.result() or [{}])[0].get("cnt") or 0)

    # ── Monthly Report data ───────────────────────────────────────
    unique_families_month = int((funiqm.result() or [{}])[0].get("cnt") or 0)

    ly_mon_row = (flymonkpi.result() or [{}])[0]
    report_ly  = {
        "hh":   int(ly_mon_row.get("hh")   or 0),
        "uniq": int(ly_mon_row.get("uniq") or 0),
        "ind":  int(ly_mon_row.get("ind")  or 0),
        "lbs":  int(ly_mon_row.get("lbs")  or 0),
        "d":    int(ly_mon_row.get("d")    or 0),
        "c":    int(ly_mon_row.get("c")    or 0),
        "m":    int(ly_mon_row.get("m")    or 0),
        "f":    int(ly_mon_row.get("f")    or 0),
    }
    report_ly["other"] = max(0, report_ly["hh"] - report_ly["d"] - report_ly["c"] - report_ly["m"] - report_ly["f"])

    same_month_yoy = [{"yr": int(r["yr"]), "hh": int(r["hh"]), "uniq": int(r["uniq"])} for r in (fsamoyoy.result() or [])]

    mon3yr_rows = fmon3yr.result() or []
    report_3yr: dict = {}
    for r in mon3yr_rows:
        yr_key = str(int(r["yr"]))
        mo_key = int(r["mo"])
        if yr_key not in report_3yr:
            report_3yr[yr_key] = {}
        report_3yr[yr_key][mo_key] = int(r["cnt"])

    top_cities = [{"city": r["city"], "cnt": int(r["cnt"])} for r in (fmoncity.result() or [])]
    month_county = [{"county": str(r["county"]).strip(), "total": int(r["total"] or 0)} for r in (fmoncty.result() or [])]

    mon_dem_rows   = fmondem.result()   or []
    mon_dem_ly_rows = fmondemly.result() or []
    def _mdem(rows, grp): return {r["val"]: int(r["cnt"]) for r in rows if r["grp"] == grp}
    month_demo = {
        "veteran":    _mdem(mon_dem_rows, "Veteran"),
        "employment": _mdem(mon_dem_rows, "Employment"),
        "benefits":   _mdem(mon_dem_rows, "Government Benefits"),
        "housing":    _mdem(mon_dem_rows, "Housing"),
    }
    month_demo_ly = {
        "veteran":    _mdem(mon_dem_ly_rows, "Veteran"),
        "employment": _mdem(mon_dem_ly_rows, "Employment"),
        "benefits":   _mdem(mon_dem_ly_rows, "Government Benefits"),
        "housing":    _mdem(mon_dem_ly_rows, "Housing"),
    }
    age_hh_row    = (fmonagehh.result()   or [{}])[0]
    age_hh_ly_row = (fmonagehhly.result() or [{}])[0]
    month_age_counts = {
        "seniors":  int(age_hh_row.get("seniors")  or 0),
        "children": int(age_hh_row.get("children") or 0),
        "adults":   int(age_hh_row.get("adults")   or 0),
    }
    month_age_counts_ly = {
        "seniors":  int(age_hh_ly_row.get("seniors")  or 0),
        "children": int(age_hh_ly_row.get("children") or 0),
    }
    unhoused_month = month_demo["housing"].get("Unhoused", 0) + month_demo["housing"].get("Yes", 0)

    def _evt_map(rows):
        m = {}
        for r in rows:
            m[r["evt"]] = {"cnt": int(r["cnt"]), "locs": int(r.get("locs") or 0)}
        return m
    mon_evt    = _evt_map(fmonevt.result()   or [])
    mon_evt_ly = _evt_map(fmonevtly.result() or [])
    month_events = {
        "agencies": mon_evt.get("FoodLink", {}).get("locs", 0),
        "popup":    (mon_evt.get("Pop up Pantry", {}).get("cnt", 0) + mon_evt.get("Senior Pop Up", {}).get("cnt", 0)),
        "fsk":      mon_evt.get("Fresh Start/Re-Start Kitchen", {}).get("cnt", 0),
    }
    month_events_ly = {
        "agencies": mon_evt_ly.get("FoodLink", {}).get("locs", 0),
        "popup":    (mon_evt_ly.get("Pop up Pantry", {}).get("cnt", 0) + mon_evt_ly.get("Senior Pop Up", {}).get("cnt", 0)),
        "fsk":      mon_evt_ly.get("Fresh Start/Re-Start Kitchen", {}).get("cnt", 0),
    }

    return {
        "hhYTD":    total_hh,
        "indYTD":   total_ind,
        "lbsYTD":   total_lbs,
        "bagsYTD":  total_bags,
        "monthHH":  month_hh,
        "monthInd": month_ind,
        "monthLbs": month_lbs,
        "monthBags": month_bags,
        "vsLY":     vs_ly,
        "progYTD":  {"D": d_hh, "C": c_hh, "M": m_hh, "F": f_hh},
        "hhLifetime":   int(lt_row.get("total") or 0),
        "indLifetime":  total_ind_lt,
        "progLifetime": {
            "D": int(lt_row.get("d") or 0),
            "C": int(lt_row.get("c") or 0),
            "M": int(lt_row.get("m") or 0),
            "F": int(lt_row.get("f") or 0),
        },
        "age":        age,
        "county":     county,
        "countyFull": county_full,
        "monthlyYTD": monthly_ytd,
        "demographics": {
            "gender": gender, "ethnicity": ethnicity,
            "veteran": veteran, "employment": employment,
            "benefits": benefits, "education": education,
            "language": language,
        },
        "hhSizeDist":    hh_size_dist,
        "newFamilies":   new_families,
        "returningFamilies": returning_families,
        "stats": {
            "mob": mobile_event_cnt,
            "mob_hh": m_hh,
            "pop": popup_event_cnt,
            "frh": unique_agencies,
            "fsk": fsk_event_cnt,
            "bp": total_backpacks,
            "pp": total_powerpacks,
        },
        "trend":       trend,
        "trendLabels": trend_labels,
        "trendSpan":   trend_months,
        "monthHasData": month_record_count > 0,
        "updatedReportAvailable": month_report_available,
        "monthRecordCount": month_record_count,
        "zipCounts":  zip_counts,
        "source":     "sql",
        # ── Monthly Report fields ──────────────────────────────────
        "monthProg": {"D": month_d_hh, "C": month_c_hh, "M": month_m_hh, "F": month_f_hh, "other": month_other_hh},
        "uniqueFamiliesMonth": unique_families_month,
        "reportLY":        report_ly,
        "sameMonthYOY":    same_month_yoy,
        "report3yr":       report_3yr,
        "topCities":       top_cities,
        "monthCounty":     month_county,
        "monthDemographics": month_demo,
        "monthDemographicsLY": month_demo_ly,
        "monthAgeCounts":   month_age_counts,
        "monthAgeCountsLY": month_age_counts_ly,
        "unhousingMonth":  unhoused_month,
        "monthEvents":     month_events,
        "monthEventsLY":   month_events_ly,
    }

# ===========================================================================
# FULL BREAKDOWNS — for Google Sheet push
# ===========================================================================

def _get_full_breakdowns(conn, start, end):
    """Return all breakdowns needed for the full Google Sheet dashboard."""

    def detail_group(group_id):
        rows = _run(conn, """
            SELECT dd.name, COUNT(DISTINCT aa.case_id) cnt
            FROM assistance_assistance aa
            JOIN cases_casedetail ccd ON aa.case_id = ccd.case_id
            JOIN details_detail dd ON ccd.detail_id = dd.id
            WHERE dd.group_id = %s AND aa.date >= %s AND aa.date < %s
            GROUP BY dd.name ORDER BY cnt DESC
        """, (group_id, start, end))
        return {r["name"]: int(r["cnt"]) for r in rows}

    # KPI extras
    kpi = _run(conn, """
        SELECT
            COUNT(*) AS dup_visits,
            COUNT(DISTINCT case_id) AS uniq_hh
        FROM assistance_assistance
        WHERE date >= %s AND date < %s
    """, (start, end))[0]

    avg_row = _run(conn, """
        SELECT AVG(cnt) AS avg_v FROM (
            SELECT case_id, COUNT(*) cnt FROM assistance_assistance
            WHERE date >= %s AND date < %s GROUP BY case_id
        ) t
    """, (start, end))
    avg_visits = round(float(avg_row[0]["avg_v"] or 0), 1) if avg_row else 0

    # Median: fetch all per-family counts sorted
    visit_counts = _run(conn, """
        SELECT COUNT(*) cnt FROM assistance_assistance
        WHERE date >= %s AND date < %s GROUP BY case_id ORDER BY cnt
    """, (start, end))
    vc = [int(r["cnt"]) for r in visit_counts]
    n = len(vc)
    median_visits = round((vc[n//2-1] + vc[n//2]) / 2, 1) if n >= 2 else (vc[0] if n == 1 else 0)

    new_fam = _run(conn, """
        SELECT COUNT(*) cnt FROM (
            SELECT case_id, MIN(date) first_v FROM assistance_assistance GROUP BY case_id
        ) t WHERE first_v >= %s AND first_v <= %s
    """, (start, end))
    new_families = int(new_fam[0]["cnt"] or 0) if new_fam else 0

    old_fam = _run(conn, """
        SELECT COUNT(*) cnt FROM (
            SELECT case_id, MIN(date) first_v FROM assistance_assistance GROUP BY case_id
        ) t WHERE first_v < %s
        AND case_id IN (SELECT DISTINCT case_id FROM assistance_assistance WHERE date >= %s AND date < %s)
    """, (start, start, end))
    old_families = int(old_fam[0]["cnt"] or 0) if old_fam else 0

    # Granular age (based on head-of-household DOB in cases_case)
    age_rows = _run(conn, """
        SELECT
            SUM(CASE WHEN TIMESTAMPDIFF(YEAR, cc.date_of_birth, CURDATE()) < 18 THEN 1 ELSE 0 END) a0,
            SUM(CASE WHEN TIMESTAMPDIFF(YEAR, cc.date_of_birth, CURDATE()) BETWEEN 18 AND 34 THEN 1 ELSE 0 END) a18,
            SUM(CASE WHEN TIMESTAMPDIFF(YEAR, cc.date_of_birth, CURDATE()) BETWEEN 35 AND 54 THEN 1 ELSE 0 END) a35,
            SUM(CASE WHEN TIMESTAMPDIFF(YEAR, cc.date_of_birth, CURDATE()) BETWEEN 55 AND 64 THEN 1 ELSE 0 END) a55,
            SUM(CASE WHEN TIMESTAMPDIFF(YEAR, cc.date_of_birth, CURDATE()) >= 65 THEN 1 ELSE 0 END) a65,
            SUM(CASE WHEN cc.date_of_birth IS NULL THEN 1 ELSE 0 END) unk
        FROM assistance_assistance aa
        JOIN cases_case cc ON aa.case_id = cc.household_id
        WHERE aa.date >= %s AND aa.date < %s
    """, (start, end))
    ar = age_rows[0] if age_rows else {}
    age_detail = {
        "0-17":    int(ar.get("a0")  or 0),
        "18-34":   int(ar.get("a18") or 0),
        "35-54":   int(ar.get("a35") or 0),
        "55-64":   int(ar.get("a55") or 0),
        "65+":     int(ar.get("a65") or 0),
        "Unknown": int(ar.get("unk") or 0),
    }

    # Homeless (detail_id=27)
    hml = _run(conn, """
        SELECT COUNT(DISTINCT aa.case_id) cnt FROM assistance_assistance aa
        JOIN cases_casedetail ccd ON aa.case_id = ccd.case_id
        WHERE ccd.detail_id = 27 AND aa.date >= %s AND aa.date < %s
    """, (start, end))
    homeless_yes = int(hml[0]["cnt"] or 0) if hml else 0
    homeless_no  = int(kpi["uniq_hh"]) - homeless_yes

    # Top 20 cities
    city_rows = _run(conn, """
        SELECT ch.city, COUNT(*) cnt
        FROM assistance_assistance aa
        JOIN cases_household ch ON aa.case_id = ch.id
        WHERE aa.date >= %s AND aa.date < %s AND ch.city IS NOT NULL AND ch.city != ''
        GROUP BY ch.city ORDER BY cnt DESC LIMIT 20
    """, (start, end))
    cities = [(r["city"], int(r["cnt"])) for r in city_rows]

    # Household size (count members per household via cases_case)
    hhsize_rows = _run(conn, """
        SELECT hh_size, COUNT(*) cnt FROM (
            SELECT aa.case_id, COUNT(cc.id) hh_size
            FROM assistance_assistance aa
            JOIN cases_case cc ON cc.household_id = aa.case_id
            WHERE aa.date >= %s AND aa.date < %s
            GROUP BY aa.case_id
        ) t GROUP BY hh_size ORDER BY hh_size LIMIT 14
    """, (start, end))
    hh_size = {int(r["hh_size"]): int(r["cnt"]) for r in hhsize_rows}

    return {
        "kpi": {
            "dup_visits":   int(kpi["dup_visits"]),
            "uniq_hh":      int(kpi["uniq_hh"]),
            "avg_visits":   avg_visits,
            "median_visits": median_visits,
            "new_families": new_families,
            "old_families": old_families,
        },
        "age_detail":  age_detail,
        "gender":      detail_group(1),
        "ethnicity":   detail_group(2),
        "employment":  detail_group(4),
        "veteran":     detail_group(5),
        "benefits":    detail_group(6),
        "homeless":    {"Yes": homeless_yes, "Not Specified": homeless_no},
        "cities":      cities,
        "hh_size":     hh_size,
    }


def _write_full_sheet_dashboard(sheet, start_label, end_label, bd):
    """Write the full interactive-style dashboard to the DASHBOARD_TAB_NAME worksheet."""
    import gspread

    try:
        ws = sheet.worksheet(DASHBOARD_TAB_NAME)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=DASHBOARD_TAB_NAME, rows=120, cols=12)

    kpi = bd["kpi"]
    total_hh  = bd.get("total_hh",  kpi["uniq_hh"])
    total_ind = bd.get("total_ind", kpi["dup_visits"])

    # ── Build all rows ────────────────────────────────────────────
    rows = [
        ["INTERACTIVE DASHBOARD — Source: IPM Oasis", "", "", "", "", "", ""],
        [],
        ["FILTER", "", "Value"],
        ["Assistance Category:", "", "All"],
        ["Start Month:", "", start_label],
        ["End Month:", "", end_label],
        [],
        ["KEY PERFORMANCE INDICATORS", "", "Value"],
        ["Total Individual Visits",     "", total_ind],
        ["Total Household Visits",      "", total_hh],
        ["Unique Household Visits",     "", kpi["uniq_hh"]],
        ["Avg Visits per Family",       "", kpi["avg_visits"]],
        ["Median Visits per Family",    "", kpi["median_visits"]],
        ["New Families Entering IPM",   "", kpi["new_families"]],
        ["Old Families Still Coming",   "", kpi["old_families"]],
        [],
        ["BREAKDOWN BY GENDER", "", "Visits", "", "BREAKDOWN BY ETHNICITY", "", "Visits"],
        ["Gender", "", "Visits", "", "Ethnicity", "", "Visits"],
    ]

    # Gender + Ethnicity side by side
    gender_items    = list(bd["gender"].items())
    ethnicity_items = list(bd["ethnicity"].items())
    max_de = max(len(gender_items), len(ethnicity_items))
    for i in range(max_de):
        g = list(gender_items[i])   + [""] if i < len(gender_items)    else ["", "", ""]
        e = list(ethnicity_items[i])      if i < len(ethnicity_items)  else ["", ""]
        rows.append([g[0], "", g[1], "", e[0], "", e[1]])
    rows.append([])

    rows.append(["BREAKDOWN BY AGE GROUP", "", "Visits", "", "BREAKDOWN BY VETERAN STATUS", "", "Visits"])
    rows.append(["Age Group", "", "Visits", "", "Veteran", "", "Visits"])
    age_items = list(bd["age_detail"].items())
    vet_items = list(bd["veteran"].items())
    # Not Specified = total uniq - yes - no
    vet_specified = sum(bd["veteran"].values())
    vet_items_full = list(bd["veteran"].items()) + [("Not Specified", max(0, kpi["uniq_hh"] - vet_specified))]
    max_av = max(len(age_items), len(vet_items_full))
    for i in range(max_av):
        a = list(age_items[i])        if i < len(age_items)       else ["", ""]
        v = list(vet_items_full[i])   if i < len(vet_items_full)  else ["", ""]
        rows.append([a[0], "", a[1], "", v[0], "", v[1]])
    rows.append([])

    rows.append(["BREAKDOWN BY EMPLOYMENT STATUS", "", "Visits", "", "BREAKDOWN BY HOMELESS STATUS", "", "Visits"])
    rows.append(["Employment", "", "Visits", "", "Homeless", "", "Visits"])
    emp_items = list(bd["employment"].items())
    hml_items = list(bd["homeless"].items())
    max_eh = max(len(emp_items), len(hml_items))
    for i in range(max_eh):
        em = list(emp_items[i])  if i < len(emp_items)  else ["", ""]
        hl = list(hml_items[i])  if i < len(hml_items)  else ["", ""]
        rows.append([em[0], "", em[1], "", hl[0], "", hl[1]])
    rows.append([])

    rows.append(["BREAKDOWN BY GOVERNMENT BENEFITS", "", "Visits", "", "BREAKDOWN BY HOUSEHOLD SIZE", "", "Visits"])
    rows.append(["Benefit", "", "Visits w/ Benefit", "", "Household Size", "", "Visits"])
    ben_items  = list(bd["benefits"].items())
    size_items = [(str(k) if k <= 13 else "14+", v) for k, v in sorted(bd["hh_size"].items())]
    # Merge 14+ entries
    size_merged = {}
    for k, v in bd["hh_size"].items():
        key = str(k) if k <= 13 else "14+"
        size_merged[key] = size_merged.get(key, 0) + v
    size_items = list(size_merged.items())
    max_bs = max(len(ben_items), len(size_items))
    for i in range(max_bs):
        b = list(ben_items[i])   if i < len(ben_items)   else ["", ""]
        s = list(size_items[i])  if i < len(size_items)  else ["", ""]
        rows.append([b[0], "", b[1], "", s[0], "", s[1]])
    rows.append([])

    rows.append(["BREAKDOWN BY LOCATION (Top 20)", "", "Visits"])
    rows.append(["Location", "", "Visits"])
    for city, cnt in bd["cities"]:
        rows.append([city, "", cnt])

    # ── Write all rows at once ────────────────────────────────────
    # Pad all rows to 7 columns
    padded = [r + [""] * max(0, 7 - len(r)) for r in rows]
    ws.update("A1", padded, value_input_option="USER_ENTERED")

    # ── Formatting ────────────────────────────────────────────────
    fmt_reqs = []

    def bg(row, col_start, col_end, r, g, b):
        return {"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": row-1, "endRowIndex": row,
                      "startColumnIndex": col_start-1, "endColumnIndex": col_end},
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": r, "green": g, "blue": b}}},
            "fields": "userEnteredFormat.backgroundColor"
        }}

    def bold(row, col_start, col_end, sz=11):
        return {"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": row-1, "endRowIndex": row,
                      "startColumnIndex": col_start-1, "endColumnIndex": col_end},
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True, "fontSize": sz}}},
            "fields": "userEnteredFormat.textFormat"
        }}

    teal   = (0.11, 0.33, 0.27)   # dark green
    header = (0.18, 0.46, 0.71)   # blue-ish header
    sub    = (0.68, 0.85, 0.90)   # light blue subheader

    # Title row
    fmt_reqs.append(bg(1, 1, 7, *teal))
    fmt_reqs.append(bold(1, 1, 7, 13))
    fmt_reqs.append({"repeatCell": {
        "range": {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": 1,
                  "startColumnIndex": 0, "endColumnIndex": 7},
        "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red":1,"green":1,"blue":1},"bold":True,"fontSize":13}}},
        "fields": "userEnteredFormat.textFormat"
    }})

    # Section headers — find their row numbers
    section_rows = []
    for i, r in enumerate(rows):
        if r and isinstance(r[0], str) and r[0].startswith("BREAKDOWN"):
            section_rows.append(i + 1)
        if r and isinstance(r[0], str) and r[0] in ("FILTER", "KEY PERFORMANCE INDICATORS"):
            section_rows.append(i + 1)

    for sr in section_rows:
        fmt_reqs.append(bg(sr, 1, 7, *header))
        fmt_reqs.append({"repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": sr-1, "endRowIndex": sr,
                      "startColumnIndex": 0, "endColumnIndex": 7},
            "cell": {"userEnteredFormat": {"textFormat": {"foregroundColor": {"red":1,"green":1,"blue":1},"bold":True}}},
            "fields": "userEnteredFormat.textFormat"
        }})

    # Column sub-headers (row after section header)
    for sr in section_rows:
        sub_r = sr + 1
        if sub_r <= len(rows):
            fmt_reqs.append(bg(sub_r, 1, 7, *sub))
            fmt_reqs.append(bold(sub_r, 1, 7))

    # Column widths: A=220, B=8, C=80, D=8, E=220, F=8, G=80
    fmt_reqs.append({"updateDimensionProperties": {
        "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
        "properties": {"pixelSize": 220}, "fields": "pixelSize"
    }})
    for col, px in [(1,8),(2,90),(3,8),(4,220),(5,8),(6,90)]:
        fmt_reqs.append({"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": col, "endIndex": col+1},
            "properties": {"pixelSize": px}, "fields": "pixelSize"
        }})

    sheet.batch_update({"requests": fmt_reqs})


# ===========================================================================
# API HELPERS — full dashboard support
# ===========================================================================

def _api_session():
    s = requests.Session()
    s.headers.update({"Authorization": f"Token {API_TOKEN}"})
    return s


def _api_count(session, cat_ids=None, date_start=None, date_end=None):
    """
    Read the `count` field from the first page — true total without fetching all records.
    One call per category ID; results summed.
    cat_ids=None → no category filter (all categories).
    """
    if cat_ids and len(cat_ids) > 1:
        return sum(_api_count(session, [c], date_start, date_end) for c in cat_ids)
    params = {}
    if cat_ids:
        params["category"] = cat_ids[0]
    if date_start:
        params["entry_date_after"]  = date_start
    if date_end:
        params["entry_date_before"] = date_end
    try:
        resp = session.get(f"{NETWORK_URL}/api/v1/assistances/", params=params, timeout=15)
        resp.raise_for_status()
        time.sleep(0.05)   # Rate limiting
        return resp.json().get("count", 0)
    except Exception as e:
        print(f"    ⚠️  API count error: {e}")
        return 0


def _api_collect_case_ids(session, cat_ids, date_start, date_end):
    """Paginate through assistance records and return a list of case IDs (with duplicates)."""
    case_ids = []
    for cat_id in cat_ids:
        url    = f"{NETWORK_URL}/api/v1/assistances/"
        params = {"category": cat_id, "entry_date_after": date_start, "entry_date_before": date_end}
        while url:
            resp   = session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data   = resp.json()
            params = {}
            results = data.get("results", data) if isinstance(data, dict) else data
            for rec in results:
                c = rec.get("case")
                if isinstance(c, int):
                    case_ids.append(c)
                elif isinstance(c, str) and c:
                    case_ids.append(int(c.rstrip("/").split("/")[-1]))
            url = data.get("next") if isinstance(data, dict) else None
            time.sleep(0.2)
    return case_ids


def _api_fetch_case(session, case_id, cache):
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


def _api_fetch_household(session, hh_id, cache):
    key = f"hh_{hh_id}"
    if key in cache:
        return cache[key]
    try:
        resp = session.get(f"{NETWORK_URL}/api/v1/households/{hh_id}/", timeout=30)
        resp.raise_for_status()
        hh = resp.json()
    except Exception:
        hh = None
    cache[key] = hh
    time.sleep(0.2)
    return hh


def _month_range(year, month):
    last = calendar.monthrange(year, month)[1]
    return f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last:02d}"


def _months_back(end_str, n):
    """Return list of (start, end) strings for n months ending at end_str."""
    ref = datetime.strptime(end_str, "%Y-%m-%d")
    ranges = []
    for i in range(n - 1, -1, -1):
        y, m = ref.year, ref.month - i
        while m <= 0:
            m += 12
            y -= 1
        ranges.append(_month_range(y, m))
    return ranges


def _get_dashboard_api(year, month, trend_months="12"):
    """
    Full dashboard via API.
    Call budget:
      7  — YTD per category (3 drive + 1 choice + 1 mobile + 2 fresh)
      7  — same for last year (YoY)
      7  — lifetime (no date filter)
      1  — counties lookup
      1  — age sample (one cases page)
    = ~23 calls × 0.1s = ~2–3 seconds.
    Trend and monthly-YTD are omitted in API mode (would need 12–24 extra calls each).
    """
    start, end = _ytd_range(year, month)
    session    = _api_session()
    today      = datetime.today()

    # ── Counties lookup (8 rows, 1 call) ─────────────────────────
    try:
        c_resp = session.get(f"{NETWORK_URL}/api/v1/counties/", timeout=15).json()
        c_list = c_resp.get("results", c_resp) if isinstance(c_resp, dict) else c_resp
        counties_map = {r["id"]: r["name"] for r in c_list}
    except Exception:
        counties_map = {}
    time.sleep(0.05)

    # ── YTD per program — sequential calls (simpler, more reliable) ──────────────
    d_hh = _api_count(session, CATEGORY_IDS["drive_through"], start, end)
    c_hh = _api_count(session, CATEGORY_IDS["choice_pantry"], start, end)
    m_hh = _api_count(session, CATEGORY_IDS["mobile"], start, end)
    f_hh = _api_count(session, CATEGORY_IDS["fresh_start"], start, end)
    total_hh = d_hh + c_hh + m_hh + f_hh
    total_ind = total_hh

    # ── Last year YTD ────────────────────────────────────────────
    ly_start, ly_end = _ytd_range(year - 1, month)
    d_ly = _api_count(session, CATEGORY_IDS["drive_through"], ly_start, ly_end)
    c_ly = _api_count(session, CATEGORY_IDS["choice_pantry"], ly_start, ly_end)
    m_ly = _api_count(session, CATEGORY_IDS["mobile"], ly_start, ly_end)
    f_ly = _api_count(session, CATEGORY_IDS["fresh_start"], ly_start, ly_end)
    ly_total = d_ly + c_ly + m_ly + f_ly
    vs_ly = round((total_hh - ly_total) / ly_total * 100, 1) if ly_total else 0

    # ── Lifetime ─────────────────────────────────────────────────
    d_lt = _api_count(session, CATEGORY_IDS["drive_through"])
    c_lt = _api_count(session, CATEGORY_IDS["choice_pantry"])
    m_lt = _api_count(session, CATEGORY_IDS["mobile"])
    f_lt = _api_count(session, CATEGORY_IDS["fresh_start"])
    total_lt = d_lt + c_lt + m_lt + f_lt

    # ── Age — 1 call, first page of cases modified this period ───
    age_counts = {"ch": 0, "ad": 0, "sr": 0}
    county: dict = {}
    try:
        resp = session.get(f"{NETWORK_URL}/api/v1/cases/",
                           params={"mod_date_after": start, "mod_date_before": end},
                           timeout=15)
        resp.raise_for_status()
        time.sleep(0.05)
        cases_sample = resp.json().get("results", [])
        for case in cases_sample:
            # Age bucket
            dob = case.get("date_of_birth")
            if dob:
                try:
                    age_yrs = (today - datetime.strptime(dob[:10], "%Y-%m-%d")).days / 365.25
                    if age_yrs < 18:   age_counts["ch"] += 1
                    elif age_yrs < 60: age_counts["ad"] += 1
                    else:              age_counts["sr"] += 1
                except ValueError:
                    pass
            # County (same page of cases — no extra call needed)
            cid  = case.get("street_county")
            name = counties_map.get(cid)
            if name:
                county.setdefault(name, {"w": 0, "n": 0})["n"] += 1
    except Exception:
        pass

    age_total = sum(age_counts.values()) or 1
    age = {k: round(v / age_total * 100, 1) for k, v in age_counts.items()}

    # ── Demographics via case_details (lifetime totals — date filter unsupported) ──
    # Group 1 = Gender, Group 2 = Ethnicity
    GENDER_IDS    = {1: "Male", 2: "Female", 29: "Transgender", 30: "Non-Conforming"}
    ETHNICITY_IDS = {3: "White/Caucasian", 4: "Asian", 5: "Hispanic",
                     6: "Black/African-American", 7: "Middle Eastern",
                     8: "Pacific Islander", 9: "Native American", 41: "Multi-Racial"}

    def _demo_count(detail_id):
        r = session.get(f"{NETWORK_URL}/api/v1/case_details/",
                        params={"detail": detail_id}, timeout=30)
        r.raise_for_status()
        time.sleep(0.1)
        return r.json().get("count", 0)

    gender    = {}
    ethnicity = {}
    try:
        for did, name in GENDER_IDS.items():
            cnt = _demo_count(did)
            if cnt: gender[name] = cnt
        for did, name in ETHNICITY_IDS.items():
            cnt = _demo_count(did)
            if cnt: ethnicity[name] = cnt
    except Exception:
        pass

    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    return {
        "hhYTD":        total_hh,
        "indYTD":       total_ind,
        "lbsYTD":       0,
        "vsLY":         vs_ly,
        "progYTD":      {"D": d_hh, "C": c_hh, "M": m_hh, "F": f_hh},
        "hhLifetime":   total_lt,
        "indLifetime":  total_lt,
        "progLifetime": {"D": d_lt, "C": c_lt, "M": m_lt, "F": f_lt},
        "age":          age,
        "county":       county,
        "monthlyYTD":   [{"month": month_names[month - 1], "hh": total_hh, "ind": total_ind}],
        "demographics": {"gender": gender, "ethnicity": ethnicity},
        "stats":        {"mob": 0, "pop": 0, "frh": 0, "fsk": 0, "bp": 0, "pp": 0},
        "trend":        [],
        "trendSpan":    trend_months,
        "source":       "sql",
        "api_note":     "Demographics show lifetime totals (API date filtering unavailable). Trend and monthly breakdown require SQL.",
    }

# ===========================================================================
# FLASK ROUTES
# ===========================================================================

@app.route("/")
def serve_dashboard():
    resp = make_response(send_file(Path(__file__).resolve().parent / "dashboard.html", mimetype="text/html"))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.route("/logo.png")
def serve_logo():
    return send_file(Path(__file__).resolve().parent / "logo.png", mimetype="image/png")


def _list_dashboard_months(start_year=2021):
    today = date.today()
    months = []
    for y in range(start_year, today.year + 1):
        max_month = today.month if y == today.year else 12
        for m in range(1, max_month + 1):
            months.append((y, m, f"{calendar.month_name[m]} {y}"))
    return months


@app.route("/api/dashboard")
def dashboard():
    try:
        year         = int(request.args.get("year",  date.today().year))
        month        = int(request.args.get("month", date.today().month))
        trend_months = request.args.get("trend_months", "12")
    except ValueError:
        return jsonify({"error": "Invalid year or month"}), 400

    cache_key = (year, month, str(trend_months))
    now = time.time()

    # Fast path: serve recent result from memory.
    with _DASHBOARD_CACHE_LOCK:
        cached = _DASHBOARD_CACHE.get(cache_key)
        if cached and (now - cached["ts"] < DASHBOARD_CACHE_TTL_SECONDS):
            return jsonify(cached["data"])

    try:
        data = _get_dashboard_sql(year, month, trend_months)
        data = _attach_monthly_report_source(data, year, month, trend_months)

        with _DASHBOARD_CACHE_LOCK:
            _DASHBOARD_CACHE[cache_key] = {"ts": now, "data": data}

        return jsonify(data)
    except Exception as e:
        return jsonify({"error": f"SQL failed: {e}"}), 500


@app.route("/api/dashboard-bulk")
def dashboard_bulk():
    trend_months = request.args.get("trend_months", "12")
    now = time.time()
    months = _list_dashboard_months(2021)

    out = {}
    missing = []

    with _DASHBOARD_CACHE_LOCK:
        for y, m, key in months:
            cache_key = (y, m, str(trend_months))
            cached = _DASHBOARD_CACHE.get(cache_key)
            if cached and (now - cached["ts"] < DASHBOARD_CACHE_TTL_SECONDS):
                out[key] = cached["data"]
            else:
                missing.append((y, m, key))

    if missing:
        try:
            for y, m, key in missing:
                data = _get_dashboard_sql(y, m, trend_months)
                out[key] = data
                with _DASHBOARD_CACHE_LOCK:
                    _DASHBOARD_CACHE[(y, m, str(trend_months))] = {"ts": now, "data": data}
        except Exception as e:
            return jsonify({"error": f"Bulk SQL failed: {e}"}), 500

    return jsonify({"months": out, "trendSpan": str(trend_months), "count": len(out)})


@app.route("/api/push-to-sheets", methods=["POST"])
def push_to_sheets():
    body = request.get_json(force=True, silent=True) or {}
    try:
        year  = int(body.get("year",  date.today().year))
        month = int(body.get("month", date.today().month))
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid year or month"}), 400

    try:
        numbers = _get_dashboard_sql(year, month, "1")
    except Exception as e:
        return jsonify({"error": f"SQL failed: {e}"}), 500

    month_name = datetime(year, month, 1).strftime("%B %Y")

    if not _IPM_IMPORT_OK:
        return jsonify({"error": "Could not import Google Sheets helpers from ipm.py"}), 500

    # _get_dashboard_sql returns keys like progYTD, hhYTD, indYTD, age, stats
    # Map to the flat shape ipm.py's sheets functions expect
    prog  = numbers.get("progYTD") or {}
    age   = numbers.get("age")     or {}
    stats = numbers.get("stats")   or {}
    total_hh  = numbers.get("hhYTD",  0)
    total_ind = numbers.get("indYTD", 0)
    age_total = total_hh or 1
    ipm_numbers = {
        "report_month":            month_name,
        "report_year":             year,
        "hh_drive_thru":           prog.get("D", 0),
        "hh_choice":               prog.get("C", 0),
        "hh_mobile_fsk":           prog.get("M", 0) + prog.get("F", 0),
        "hh_frh":                  prog.get("FRH", numbers.get("hh_frh", 0)),
        "total_hh_visits":         total_hh,
        "total_individual_visits": total_ind,
        "monthly_food_lbs":        numbers.get("lbsYTD", 0),
        "milk_lbs":                0,
        "egg_lbs":                 0,
        # age values are percentages (0-100); convert back to counts
        "adj_children":            round(age.get("ch", 0) / 100 * age_total),
        "adj_adults":              round(age.get("ad", 0) / 100 * age_total),
        "adj_seniors":             round(age.get("sr", 0) / 100 * age_total),
        "pct_children":            age.get("ch", 0),
        "pct_adults":              age.get("ad", 0),
        "pct_seniors":             age.get("sr", 0),
        "ytd_mobile_events":       stats.get("mob", 0),
        "ytd_popup_events":        stats.get("pop", 0),
        "unique_frh_agencies":     stats.get("frh", 0),
        "fresh_start_kitchens":    stats.get("fsk", 0),
        "homeless_backpacks":      stats.get("bp",  0),
        "power_packs":             stats.get("pp",  0),
    }

    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        scope  = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds  = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)  # type: ignore[arg-type]
        gc     = gspread.authorize(creds)  # type: ignore[arg-type]
        sheet  = gc.open_by_key(GOOGLE_SHEET_ID) if GOOGLE_SHEET_ID else gc.open(GOOGLE_SHEET_NAME)

        # History tab
        try:
            history = sheet.worksheet(HISTORY_TAB_NAME)
        except gspread.WorksheetNotFound:
            history = sheet.add_worksheet(title=HISTORY_TAB_NAME, rows=200, cols=30)
            history.append_row([
                "Month","Year","HH Drive Thru","HH Choice","HH Mobile+FSK","HH FRH",
                "Total HH Visits","Total Individual Visits","Monthly Food Lbs","Milk Lbs","Egg Lbs",
                "Children (adj)","Adults (adj)","Seniors (adj)",
                "% Children","% Adults","% Seniors",
                "YTD Mobile Events","YTD Popup Events","Unique FRH Agencies",
                "Fresh Start Kitchens","Homeless Backpacks","Power Packs",
            ], value_input_option="USER_ENTERED")  # type: ignore[arg-type]
        n = ipm_numbers
        history.append_row([
            n["report_month"], n["report_year"],
            n["hh_drive_thru"], n["hh_choice"], n["hh_mobile_fsk"], n["hh_frh"],
            n["total_hh_visits"], n["total_individual_visits"],
            n["monthly_food_lbs"], n["milk_lbs"], n["egg_lbs"],
            n["adj_children"], n["adj_adults"], n["adj_seniors"],
            n["pct_children"], n["pct_adults"], n["pct_seniors"],
            n["ytd_mobile_events"], n["ytd_popup_events"],
            n["unique_frh_agencies"], n["fresh_start_kitchens"],
            n["homeless_backpacks"], n["power_packs"],
        ], value_input_option="USER_ENTERED")  # type: ignore[arg-type]

        # Dashboard tab — update key cells
        sheet.sheet1.batch_update([
            {"range": cell, "values": [[val]]} for cell, val in [
                ("B2",  n["hh_drive_thru"]),  ("B3",  n["hh_choice"]),
                ("B4",  n["hh_mobile_fsk"]),  ("B5",  n["hh_frh"]),
                ("B7",  n["total_hh_visits"]),("B8",  n["total_individual_visits"]),
                ("B11", n["adj_children"]),   ("B12", n["adj_adults"]),
                ("B13", n["adj_seniors"]),    ("B14", n["pct_children"]),
                ("B15", n["pct_adults"]),     ("B16", n["pct_seniors"]),
                ("B20", n["unique_frh_agencies"]),
            ]
        ], value_input_option="USER_ENTERED")  # type: ignore[arg-type]

        return jsonify({"ok": True, "month": month_name})
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"error": f"Google Sheets error: {type(e).__name__}: {e}"}), 500


@app.route("/api/list-sheets")
def list_sheets():
    """Debug endpoint — lists all spreadsheets the service account can see."""
    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_FILE, scope)  # type: ignore[arg-type]
        gc    = gspread.authorize(creds)  # type: ignore[arg-type]
        sheets = [{"name": s["name"], "id": s["id"]} for s in gc.list_spreadsheet_files()]
        return jsonify({"sheets": sheets})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/health")
def health():
    status = {"sql": "unconfigured"}
    if DB_HOST:
        try:
            conn = _sql_connect()
            conn.close()
            status["sql"] = "ok"
        except Exception as e:
            status["sql"] = f"error: {e}"
    return jsonify(status)

# ===========================================================================
# MANUAL INPUTS — GET/POST per month
# ===========================================================================

def _manual_input_defaults(year, month):
    return {
        "year": year,
        "month": month,
        "inkind_lbs": 0,
        "fsfb_lbs": 0,
        "milk_half_gallons": 0,
        "egg_dozens": 0,
        "other_food_lbs": 0,
        "ytd_mobile_events": 0,
        "ytd_popup_events": 0,
        "fsk_count": 0,
        "homeless_backpacks": 0,
        "power_packs": 0,
        "notes": "",
    }


def _build_pantry_pdf_index():
    root = Path(__file__).resolve().parent / "Pantry Dashboards"
    idx = {}
    if not root.exists():
        return idx

    month_names = list(calendar.month_name)
    for p in root.rglob("*.pdf"):
        stem = p.stem.replace("_", " ").replace("(1)", " ").strip()
        m = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d\d)", stem, re.IGNORECASE)
        if m:
            mon_name = m.group(1).title()
            year = int(m.group(2))
            month = month_names.index(mon_name)
            idx[(year, month)] = p
            continue

        m2 = re.search(r"(20\d\d)\.(0[1-9]|1[0-2])", stem)
        if m2:
            year = int(m2.group(1))
            month = int(m2.group(2))
            idx[(year, month)] = p
    return idx


def _get_pantry_pdf_for_month(year, month):
    global _PANTRY_PDF_INDEX
    if _PANTRY_PDF_INDEX is None:
        with _PANTRY_PDF_INDEX_LOCK:
            if _PANTRY_PDF_INDEX is None:
                _PANTRY_PDF_INDEX = _build_pantry_pdf_index()
    return (_PANTRY_PDF_INDEX or {}).get((year, month))


def _normalize_pdf_text(text):
    # Some exports split words into single-letter tokens (e.g., "M o b i l e").
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\b([A-Za-z])(?:\s+([A-Za-z]))+\b", lambda m: m.group(0).replace(" ", ""), text)
    return text


def _extract_int(text, pattern):
    m = re.search(pattern, text, re.IGNORECASE)
    if not m:
        return None
    raw = m.group(1).replace(",", "").strip()
    try:
        return int(raw)
    except Exception:
        return None


def _extract_manual_inputs_from_pantry_pdf(pdf_path):
    key = str(pdf_path)
    with _PANTRY_PDF_EXTRACT_CACHE_LOCK:
        if key in _PANTRY_PDF_EXTRACT_CACHE:
            return _PANTRY_PDF_EXTRACT_CACHE[key]

    out = {
        "ytd_mobile_events": None,
        "ytd_popup_events": None,
        "fsk_count": None,
        "homeless_backpacks": None,
        "power_packs": None,
        "source_file": str(pdf_path.relative_to(Path(__file__).resolve().parent)),
    }
    try:
        from pypdf import PdfReader  # type: ignore
        reader = PdfReader(str(pdf_path))
        text = "\n".join((pg.extract_text() or "") for pg in reader.pages)
        text = _normalize_pdf_text(text)

        out["ytd_mobile_events"] = _extract_int(text, r"Mobile\s*Pantries\s*:?\s*([0-9,]+)")
        out["ytd_popup_events"] = _extract_int(text, r"Pop\s*[- ]?Up\s*Pantries\s*:?\s*([0-9,]+)")
        out["fsk_count"] = _extract_int(text, r"Fresh\s*Start\s*Kitchens\s*:?\s*([0-9,]+)")
        out["homeless_backpacks"] = _extract_int(text, r"Homeless\s*Backpacks\s*:?\s*([0-9,]+)")
        out["power_packs"] = _extract_int(text, r"Power\s*Packs\s*:?\s*([0-9,]+)")
    except Exception:
        pass

    with _PANTRY_PDF_EXTRACT_CACHE_LOCK:
        _PANTRY_PDF_EXTRACT_CACHE[key] = out
    return out


def _historical_manual_inputs_from_pantry(year, month):
    pdf = _get_pantry_pdf_for_month(year, month)
    if not pdf:
        return None
    parsed = _extract_manual_inputs_from_pantry_pdf(pdf)
    data = _manual_input_defaults(year, month)
    any_value = False
    for f in ["ytd_mobile_events", "ytd_popup_events", "fsk_count", "homeless_backpacks", "power_packs"]:
        if parsed.get(f) is not None:
            data[f] = parsed[f]
            any_value = True
    if not any_value:
        return None
    data["notes"] = f"Auto-loaded from {parsed['source_file']}"
    return {"found": True, "data": data, "source": "pantry_dashboard_pdf", "source_file": parsed["source_file"]}

def _ensure_manual_inputs_table(conn):
    conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS manual_inputs (
            year              SMALLINT NOT NULL,
            month             TINYINT  NOT NULL,
            inkind_lbs        FLOAT    DEFAULT 0,
            fsfb_lbs          FLOAT    DEFAULT 0,
            milk_half_gallons INT      DEFAULT 0,
            egg_dozens        INT      DEFAULT 0,
            other_food_lbs    FLOAT    DEFAULT 0,
            ytd_mobile_events INT      DEFAULT 0,
            ytd_popup_events  INT      DEFAULT 0,
            fsk_count         INT      DEFAULT 0,
            homeless_backpacks INT     DEFAULT 0,
            power_packs       INT      DEFAULT 0,
            notes             TEXT,
            updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP
                          ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (year, month)
        )
    """)
    conn.commit()


@app.route("/api/manual-inputs/<int:year>/<int:month>", methods=["GET"])
def get_manual_inputs(year, month):
    defaults = _manual_input_defaults(year, month)
    try:
        conn = _sql_connect()
        _ensure_manual_inputs_table(conn)
        rows = _run(conn, "SELECT * FROM manual_inputs WHERE year=%s AND month=%s", (year, month))
        conn.close()
        if rows:
            row = {k: v for k, v in rows[0].items() if k != "updated_at"}  # type: ignore[union-attr]
            return jsonify({"found": True, "data": row, "source": "manual_inputs_table"})

        hist = _historical_manual_inputs_from_pantry(year, month)
        if hist:
            return jsonify(hist)

        return jsonify({"found": False, "data": defaults, "source": "empty"})
    except Exception as e:
        hist = _historical_manual_inputs_from_pantry(year, month)
        if hist:
            return jsonify(hist)
        return jsonify({"error": str(e), "found": False, "data": defaults, "source": "empty"}), 500


@app.route("/api/manual-inputs/<int:year>/<int:month>", methods=["POST"])
def save_manual_inputs(year, month):
    body = request.get_json(silent=True) or {}
    fields = ["inkind_lbs", "fsfb_lbs", "milk_half_gallons", "egg_dozens",
              "other_food_lbs", "ytd_mobile_events", "ytd_popup_events",
              "fsk_count", "homeless_backpacks", "power_packs", "notes"]
    vals = [body.get(f, 0) for f in fields]
    try:
        conn = _sql_connect()
        _ensure_manual_inputs_table(conn)
        conn.cursor().execute(f"""
            INSERT INTO manual_inputs (year, month, {', '.join(fields)})
            VALUES (%s, %s, {', '.join(['%s']*len(fields))})
            ON DUPLICATE KEY UPDATE
            {', '.join(f'{f}=VALUES({f})' for f in fields)}
        """, [year, month] + vals)
        conn.commit()
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ===========================================================================
# MAIN
# ===========================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IPM Live Dashboard Server")
    parser.add_argument("--port", type=int, default=5002)
    args = parser.parse_args()

    print("=" * 55)
    print("  IPM Live Dashboard Server")
    print("  Source : SQL")
    print(f"  Open   : http://localhost:{args.port}")
    print("=" * 55)
    app.run(host="0.0.0.0", port=args.port, debug=True, threaded=True)
