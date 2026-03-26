#!/usr/bin/env python3
"""
GradeView — Turso migration via HTTP API
Loads TAMU, UT Austin, UW-Madison, UH into Turso cloud DB.
"""

import csv, json, os, requests, warnings
warnings.filterwarnings('ignore')

TURSO_URL = "https://gradeview-agentsolomon.aws-us-east-2.turso.io/v2/pipeline"
TURSO_TOKEN = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3NzQzMTc5NTQsImlkIjoiMDE5ZDFkOTMtZTgwMS03ODU2LThlNjYtNWY1NTgwN2I0Y2E2IiwicmlkIjoiYjBlOTI1YzAtYzM4My00ODcxLTg0NjAtYjg4OGM2NGRhNWQ3In0.uBwikoGHc1YqWDWu8HX9LIIMZ9blRUozr04x0SXBYXtyecQdcWy3RKIxXuDAAlxSHOD5R2F5k2xaQrsPk38cBA"
HEADERS = {'Authorization': f'Bearer {TURSO_TOKEN}', 'Content-Type': 'application/json'}
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

GRADE_MAP = {
    'A+':'A','A':'A','A-':'A',
    'B+':'B','B':'B','B-':'B',
    'C+':'C','C':'C','C-':'C',
    'D+':'D','D':'D','D-':'D',
    'F':'F',
}

def sql(statements):
    """Execute a list of SQL statements via Turso HTTP API."""
    reqs = [{"type": "execute", "stmt": {"sql": s["sql"], "args": [{"type": "text", "value": str(a)} if not isinstance(a, int) else {"type": "integer", "value": a} for a in s.get("args", [])]}} for s in statements]
    reqs.append({"type": "close"})
    r = requests.post(TURSO_URL, headers=HEADERS, json={"requests": reqs})
    results = r.json().get("results", [])
    for i, res in enumerate(results[:-1]):
        if res.get("type") == "error":
            print(f"  ⚠️  SQL error: {res}")
    return results

def sql_one(query, args=None):
    reqs = [{"type": "execute", "stmt": {"sql": query, "args": [{"type": "text", "value": str(a)} if not isinstance(a, int) else {"type": "integer", "value": a} for a in (args or [])]}}, {"type": "close"}]
    r = requests.post(TURSO_URL, headers=HEADERS, json={"requests": reqs})
    return r.json()

def normalize_grade(g):
    if not g: return None
    return GRADE_MAP.get(str(g).strip().upper(), None)

def batch_insert(rows, batch_size=200):
    """Insert rows in batches."""
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i+batch_size]
        stmts = [{"sql": "INSERT INTO grades (school_id,year,semester,dept,course_number,instructor,grade,count) VALUES (?,?,?,?,?,?,?,?)", "args": r} for r in chunk]
        sql(stmts)

# ── Schema ───────────────────────────────────────────────
print("Creating schema...")
sql([{"sql": "CREATE TABLE IF NOT EXISTS schools (id TEXT PRIMARY KEY, name TEXT NOT NULL, short_name TEXT NOT NULL, color TEXT NOT NULL)"}])
sql([{"sql": """CREATE TABLE IF NOT EXISTS grades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    school_id TEXT NOT NULL,
    year TEXT, semester TEXT, dept TEXT,
    course_number TEXT, instructor TEXT, grade TEXT, count INTEGER,
    FOREIGN KEY (school_id) REFERENCES schools(id))"""}])
sql([{"sql": "CREATE INDEX IF NOT EXISTS idx_school ON grades(school_id)"}])
sql([{"sql": "CREATE INDEX IF NOT EXISTS idx_course ON grades(school_id, dept, course_number)"}])
sql([{"sql": "CREATE INDEX IF NOT EXISTS idx_instructor ON grades(school_id, instructor)"}])
print("Schema ✅")

# ── Schools ──────────────────────────────────────────────
SCHOOLS = [
    ("tamu",     "Texas A&M University",                "TAMU",     "#500000"),
    ("utaustin", "University of Texas at Austin",       "UT Austin","#BF5700"),
    ("uwmadison","University of Wisconsin-Madison",     "UW-Madison","#C5050C"),
    ("uh",       "University of Houston",               "UH",       "#C8102E"),
]
for s in SCHOOLS:
    sql([{"sql": "INSERT OR REPLACE INTO schools (id,name,short_name,color) VALUES (?,?,?,?)", "args": list(s)}])
print("Schools ✅")

# ── TAMU ─────────────────────────────────────────────────
print("\nMigrating TAMU...")
rows, skipped = [], 0
with open(os.path.join(BASE_DIR, 'tamu_grades.csv'), encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        g = normalize_grade(row.get('Lettergrade',''))
        if not g: skipped += 1; continue
        try: count = int(float(row.get('Count', 0)))
        except: skipped += 1; continue
        rows.append(['tamu', row.get('Year','').strip(), row.get('Semester','').strip(),
                     row.get('Dept','').strip().upper(), row.get('Coursenbr','').strip(),
                     row.get('Instructor','').strip(), g, count])
batch_insert(rows)
print(f"TAMU ✅ {len(rows):,} rows ({skipped} skipped)")

# ── UT Austin ────────────────────────────────────────────
print("\nMigrating UT Austin...")
rows, skipped = [], 0
with open(os.path.join(BASE_DIR, 'utaustin_grades.csv'), encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        g = normalize_grade(row.get('grade',''))
        if not g: skipped += 1; continue
        try: count = int(float(row.get('count', 0)))
        except: skipped += 1; continue
        rows.append(['utaustin', str(row.get('year','')).strip(), str(row.get('semester','')).strip(),
                     str(row.get('dept','')).strip().upper(), str(row.get('course_number','')).strip(),
                     str(row.get('professor_name','')).strip(), g, count])
batch_insert(rows)
print(f"UT Austin ✅ {len(rows):,} rows ({skipped} skipped)")

# ── UW-Madison ───────────────────────────────────────────
print("\nMigrating UW-Madison...")
rows, skipped = [], 0
with open(os.path.join(BASE_DIR, 'uwmadison_grades.csv'), encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        g = normalize_grade(row.get('grade',''))
        if not g: skipped += 1; continue
        try: count = int(float(row.get('count', 0)))
        except: skipped += 1; continue
        rows.append(['uwmadison', str(row.get('year','')).strip(), str(row.get('term','')).strip(),
                     str(row.get('subject','')).strip().upper(), str(row.get('course_number','')).strip(),
                     str(row.get('instructor','')).strip(), g, count])
batch_insert(rows)
print(f"UW-Madison ✅ {len(rows):,} rows ({skipped} skipped)")

# ── University of Houston ────────────────────────────────
print("\nMigrating University of Houston...")
import openpyxl
wb = openpyxl.load_workbook(os.path.join(BASE_DIR, 'IR11215.xlsx'), read_only=True)
ws = wb.active
headers = None
rows, skipped = [], 0
for row in ws.iter_rows(values_only=True):
    if headers is None:
        headers = [str(h).strip() for h in row]
        continue
    r = dict(zip(headers, row))
    g = normalize_grade(r.get('COURSE_GRADE_OFFICIAL',''))
    if not g: skipped += 1; continue
    try: count = int(r.get('HEADCOUNT', 0))
    except: skipped += 1; continue
    rows.append(['uh', str(r.get('ACADEMIC_YEAR','')).strip(), str(r.get('SEMESTER','')).strip(),
                 str(r.get('COURSE_DEPT','')).strip().upper(), str(r.get('COURSE_NUMBER','')).strip(),
                 str(r.get('INSTRUCTOR_NAME','')).strip(), g, count])
batch_insert(rows)
print(f"UH ✅ {len(rows):,} rows ({skipped} skipped)")

# ── Verify ───────────────────────────────────────────────
print("\n📊 Final verification:")
result = sql_one("SELECT school_id, COUNT(*) as cnt FROM grades GROUP BY school_id")
rows_data = result['results'][0]['response']['result']['rows']
total = 0
for row in rows_data:
    sid = row[0]['value']
    cnt = int(row[1]['value'])
    total += cnt
    print(f"  {sid}: {cnt:,} rows")
print(f"  TOTAL: {total:,} rows")
print("\n✅ Migration complete!")
