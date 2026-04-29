import os
"""
GradeView — Standard School Data Upload Script
Uses local SQLite staging → Turso CLI push for maximum speed.

Usage:
    python3 upload_school.py --school <school_id> --file <path_to_csv_or_xlsx> [--map <mapping_name>]

Examples:
    python3 upload_school.py --school ferrisstate --file ~/Downloads/ferris_grades.csv
    python3 upload_school.py --school cincinnati --file ~/Downloads/cincinnati.xlsx --map xlsx_generic

Column mappings (--map):
    utaustin    — Semester, Course Prefix, Course Number, Letter Grade, Count of letter grade
    tamu        — (existing TAMU format)
    xlsx_generic — auto-detect common column names
    uh          — UH Houston format (IR11215.xlsx)
"""

import csv, json, os, sys, sqlite3, subprocess, argparse, glob
from pathlib import Path
import openpyxl

TURSO_URL = os.environ["TURSO_URL"]
TURSO_TOKEN = os.environ["TURSO_TOKEN"]
STAGING_DB = "/tmp/gradeview_staging.db"

# ── Column mappings ────────────────────────────────────────────────────────────
MAPPINGS = {
    "utaustin": {
        "semester":  "Semester",
        "dept":      "Course Prefix",
        "course_number": "Course Number",
        "grade":     "Letter Grade",
        "count":     "Count of letter grade",
        "instructor": None,
    },
    "tamu": {
        "semester":  "Term",
        "dept":      "Dept",
        "course_number": "Course",
        "grade":     "Grade",
        "count":     "Count",
        "instructor": "Instructor",
    },
    "xlsx_generic": {
        # Auto-detect — tries common names
        "semester":  ["Semester", "Term", "Academic Term", "Acad Term"],
        "dept":      ["Dept", "Department", "Course Prefix", "Subject"],
        "course_number": ["Course", "Course Number", "Course No", "CourseNo"],
        "grade":     ["Grade", "Letter Grade", "Final Grade"],
        "count":     ["Count", "Count of letter grade", "N", "Enrollment"],
        "instructor": ["Instructor", "Professor", "Faculty", "Instructor Name"],
    },
    "unlv": {
        "format":    "wide",
        "sheet":     "PRR-2026-102_ODS-grade-distribu",
        "year":      "CALENDAR_YEAR",
        "semester":  "TERM",
        "dept":      "SUBJECT",
        "course_number": "CATALOG_NBR",
        "instructor_first": "INSTRUCTORS",
        "instructor_last":  None,
        "grade_cols": {
            "A_GRADE": "A", "AMINUS_GRADE": "A-",
            "BPLUS_GRADE": "B+", "B_GRADE": "B", "BMINUS_GRADE": "B-",
            "CPLUS_GRADE": "C+", "C_GRADE": "C", "CMINUS_GRADE": "C-",
            "DPLUS_GRADE": "D+", "D_GRADE": "D", "DMINUS_GRADE": "D-",
            "F_GRADE": "F"
        },
    },
    # Wide format: one row per course section with separate columns per grade letter
    "neiu": {
        "format":    "wide",
        "sheet":     "Data",
        "year":      "ACADEMIC_YEAR",
        "semester":  "SEMESTER",
        "dept":      "COURSE_SUBJECT",
        "course_number": "COURSE_NUMBER",
        "instructor_first": "PRIMARY_INSTRUCTOR_FIRST_NAME",
        "instructor_last":  "PRIMARY_INSTRUCTOR_LAST_NAME",
        "grade_cols": {
            "A_GRADE": "A", "B_GRADE": "B", "C_GRADE": "C",
            "D_GRADE": "D", "F_GRADE": "F", "W_GRADE": "W",
            "I_GRADE": "I", "PASS_GRADE": "P"
        },
    },
}

def resolve_col(row, mapping_val):
    """Resolve a column value from a row dict, supporting list of fallbacks."""
    if mapping_val is None:
        return None
    if isinstance(mapping_val, list):
        for candidate in mapping_val:
            if candidate in row:
                return row[candidate]
        return None
    return row.get(mapping_val)

def read_csv(filepath, mapping):
    rows = []
    with open(filepath, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows

def read_xlsx(filepath, mapping):
    rows = []
    wb = openpyxl.load_workbook(filepath, read_only=True)
    ws = wb.active
    headers = None
    for excel_row in ws.iter_rows(values_only=True):
        if headers is None:
            headers = [str(c).strip() if c else '' for c in excel_row]
            continue
        row = dict(zip(headers, [str(c).strip() if c is not None else '' for c in excel_row]))
        rows.append(row)
    wb.close()
    return rows

def parse_semester(semester_str):
    """Extract term and year from semester string."""
    parts = (semester_str or '').strip().split()
    term = parts[0].upper() if parts else 'UNKNOWN'
    year = parts[1] if len(parts) > 1 else '0000'
    return term, year

def build_staging_db_wide(school_id, data_rows, mapping):
    """Build staging DB for wide-format data (one row per section, multiple grade columns)."""
    if os.path.exists(STAGING_DB):
        os.remove(STAGING_DB)
    conn = sqlite3.connect(STAGING_DB)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS grades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            school_id TEXT NOT NULL,
            year TEXT, semester TEXT, dept TEXT,
            course_number TEXT, instructor TEXT, grade TEXT, count INTEGER
        )
    """)
    inserted = 0
    batch = []
    for row in data_rows:
        year = str(row.get(mapping['year'], '') or '').strip()
        term = str(row.get(mapping['semester'], '') or '').strip().upper()
        dept = str(row.get(mapping['dept'], '') or '').strip()
        course = str(row.get(mapping['course_number'], '') or '').strip()
        fname = str(row.get(mapping.get('instructor_first', ''), '') or '').strip()
        lname = str(row.get(mapping.get('instructor_last', ''), '') or '').strip()
        instructor = f'{fname} {lname}'.strip() or 'N/A'
        for col, letter in mapping['grade_cols'].items():
            try:
                count = int(row.get(col) or 0)
            except:
                count = 0
            if count > 0:
                batch.append((school_id, year, term, dept, course, instructor, letter, count))
        if len(batch) >= 1000:
            cur.executemany("INSERT INTO grades (school_id, year, semester, dept, course_number, instructor, grade, count) VALUES (?,?,?,?,?,?,?,?)", batch)
            inserted += len(batch)
            batch = []
    if batch:
        cur.executemany("INSERT INTO grades (school_id, year, semester, dept, course_number, instructor, grade, count) VALUES (?,?,?,?,?,?,?,?)", batch)
        inserted += len(batch)
    conn.commit()
    conn.close()
    print(f"✅ Staging DB built: {inserted:,} rows")
    return inserted


def build_staging_db(school_id, data_rows, mapping):
    """Build a local SQLite DB with the data."""
    if os.path.exists(STAGING_DB):
        os.remove(STAGING_DB)

    conn = sqlite3.connect(STAGING_DB)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS grades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            school_id TEXT NOT NULL,
            year TEXT,
            semester TEXT,
            dept TEXT,
            course_number TEXT,
            instructor TEXT,
            grade TEXT,
            count INTEGER
        )
    """)

    inserted = 0
    skipped = 0
    batch = []

    for row in data_rows:
        semester_str = resolve_col(row, mapping.get("semester")) or ''
        dept = (resolve_col(row, mapping.get("dept")) or '').strip()
        course_number = (resolve_col(row, mapping.get("course_number")) or '').strip()
        grade = (resolve_col(row, mapping.get("grade")) or '').strip()
        instructor = (resolve_col(row, mapping.get("instructor")) or 'N/A').strip()
        try:
            count = int((resolve_col(row, mapping.get("count")) or '0').strip())
        except:
            count = 0

        if not grade:
            skipped += 1
            continue

        term, year = parse_semester(semester_str)
        semester = f"{term} {year}".strip()

        batch.append((school_id, year, term, dept, course_number, instructor or 'N/A', grade, count))

        if len(batch) >= 1000:
            cur.executemany("INSERT INTO grades (school_id, year, semester, dept, course_number, instructor, grade, count) VALUES (?,?,?,?,?,?,?,?)", batch)
            inserted += len(batch)
            batch = []

    if batch:
        cur.executemany("INSERT INTO grades (school_id, year, semester, dept, course_number, instructor, grade, count) VALUES (?,?,?,?,?,?,?,?)", batch)
        inserted += len(batch)

    conn.commit()
    conn.close()
    print(f"✅ Staging DB built: {inserted:,} rows ({skipped:,} skipped)")
    return inserted

def turso_http(stmts, retries=3):
    import urllib.request as urlreq
    import time
    api_url = f"{TURSO_URL}/v2/pipeline"
    payload = json.dumps({"requests": [{"type": "execute", "stmt": s} for s in stmts]}).encode()
    for attempt in range(retries):
        try:
            req = urlreq.Request(api_url, data=payload,
                  headers={"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"})
            with urlreq.urlopen(req, timeout=120) as r:
                return json.loads(r.read())
        except Exception as e:
            if attempt < retries - 1:
                print(f"  ⚠️ Network error (attempt {attempt+1}/{retries}): {e} — retrying in 5s...")
                time.sleep(5)
            else:
                raise

def get_turso_count(school_id):
    result = turso_http([{"sql": f"SELECT COUNT(*) FROM grades WHERE school_id='{school_id}'"}])
    return int(result['results'][0]['response']['result']['rows'][0][0]['value'])

def push_to_turso(school_id, local_count, resume=False, append=False):
    """Push staging DB to Turso via HTTP batch API. Supports --resume to skip already-uploaded rows."""
    BATCH = 500

    # Check how many rows already exist
    existing = get_turso_count(school_id)

    if resume and existing > 0:
        print(f"⏩ Resume mode: {existing:,} rows already in Turso — skipping first {existing:,} rows")
        skip = existing
    elif append:
        print(f"➕ Append mode: {existing:,} rows already in Turso — adding new rows on top")
        skip = 0  # Upload all staging rows (new semester data only, no delete)
    else:
        # Fresh upload — delete existing rows first
        print(f"Deleting existing {school_id} rows from Turso...")
        turso_http([{"sql": f"DELETE FROM grades WHERE school_id='{school_id}'"}])
        skip = 0

    # Read staging DB and upload in batches
    remaining = local_count - skip
    print(f"Pushing {remaining:,} rows to Turso via HTTP API...")
    conn = sqlite3.connect(STAGING_DB)
    cur = conn.cursor()
    cur.execute("SELECT school_id, year, semester, dept, course_number, instructor, grade, count FROM grades LIMIT -1 OFFSET ?", (skip,))

    total = 0
    errors = 0
    batch = []
    for row in cur.fetchall():
        sid, yr, sem, dept, course, instr, grade, cnt = row
        dept = str(dept or '').replace("'", "''")
        course = str(course or '').replace("'", "''")
        instr = str(instr or 'N/A').replace("'", "''")
        grade = str(grade or '').replace("'", "''")
        yr = str(yr or '').replace("'", "''")
        sem = str(sem or '').replace("'", "''")
        batch.append({"sql": f"INSERT INTO grades (school_id, year, semester, dept, course_number, instructor, grade, count) VALUES ('{sid}', '{yr}', '{sem}', '{dept}', '{course}', '{instr}', '{grade}', {cnt})"})

        if len(batch) >= BATCH:
            result = turso_http(batch)
            errs = sum(1 for r in result['results'] if r['type'] == 'error')
            if errs:
                for r in result['results']:
                    if r['type'] == 'error':
                        print(f"  ERR: {r.get('error',{}).get('message','?')[:80]}")
            errors += errs
            total += len(batch) - errs
            batch = []
            if (total + skip) % 50000 == 0:
                print(f"  {total + skip:,} rows uploaded ({total:,} this session)...")

    if batch:
        result = turso_http(batch)
        errs = sum(1 for r in result['results'] if r['type'] == 'error')
        errors += errs
        total += len(batch) - errs

    conn.close()
    print(f"✅ Uploaded {total:,} rows this session ({errors:,} errors)")
    return True

def verify(school_id, expected):
    """Verify row count in Turso matches expected."""
    import urllib.request
    payload = json.dumps({"requests": [{"type": "execute", "stmt": {"sql": f"SELECT COUNT(*) FROM grades WHERE school_id='{school_id}'"}}]}).encode()
    req = urllib.request.Request(f"{TURSO_URL}/v2/pipeline", data=payload,
        headers={"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        data = json.loads(r.read())
    actual = int(data['results'][0]['response']['result']['rows'][0][0]['value'])
    if actual == expected:
        print(f"✅ VERIFIED: {actual:,} rows in Turso for {school_id}")
    else:
        print(f"⚠️ MISMATCH: Expected {expected:,}, got {actual:,} rows for {school_id}")
    return actual

def main():
    parser = argparse.ArgumentParser(description='Upload school grade data to Turso')
    parser.add_argument('--school', required=True, help='School ID (e.g. ferrisstate)')
    parser.add_argument('--file', required=True, help='Path to CSV or XLSX file (supports glob)')
    parser.add_argument('--map', default='xlsx_generic', help='Column mapping name')
    parser.add_argument('--resume', action='store_true', help='Resume interrupted upload (skip already-uploaded rows)')
    parser.add_argument('--append', action='store_true', help='Append new rows only — do not delete existing data (use for new semester updates)')
    args = parser.parse_args()

    mapping = MAPPINGS.get(args.map, MAPPINGS['xlsx_generic'])
    filepath = os.path.expanduser(args.file)
    files = glob.glob(filepath) if '*' in filepath else [filepath]
    files = sorted([f for f in files if "(1)" not in f])

    if not files:
        print(f"❌ No files found: {filepath}")
        sys.exit(1)

    print(f"\n📤 Uploading {len(files)} file(s) for school: {args.school}")
    print(f"   Mapping: {args.map}\n")

    all_rows = []
    for f in files:
        print(f"Reading {os.path.basename(f)}...")
        if f.endswith('.xlsx') or f.endswith('.xls'):
            rows = read_xlsx(f, mapping)
        else:
            rows = read_csv(f, mapping)
        all_rows.extend(rows)
        print(f"  → {len(rows):,} rows")

    print(f"\nTotal: {len(all_rows):,} rows read")

    if mapping.get('format') == 'wide':
        local_count = build_staging_db_wide(args.school, all_rows, mapping)
    else:
        local_count = build_staging_db(args.school, all_rows, mapping)
    push_to_turso(args.school, local_count, resume=args.resume, append=args.append)
    final = verify(args.school, local_count)

    print(f"\n🎉 Done! {final:,} rows live in Turso for {args.school}")
    print(f"   Don't forget to update the iCloud tracker and MEMORY.md!")

if __name__ == '__main__':
    main()
