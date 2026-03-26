#!/usr/bin/env python3
"""
GradeView — Turso schema creation + data migration
Loads TAMU, UT Austin, UW-Madison, and UH into Turso cloud DB.
"""

import asyncio
import csv
import os
import libsql_client

TURSO_URL = "libsql://gradeview-agentsolomon.aws-us-east-2.turso.io"
TURSO_TOKEN = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3NzQzMTc5NTQsImlkIjoiMDE5ZDFkOTMtZTgwMS03ODU2LThlNjYtNWY1NTgwN2I0Y2E2IiwicmlkIjoiYjBlOTI1YzAtYzM4My00ODcxLTg0NjAtYjg4OGM2NGRhNWQ3In0.uBwikoGHc1YqWDWu8HX9LIIMZ9blRUozr04x0SXBYXtyecQdcWy3RKIxXuDAAlxSHOD5R2F5k2xaQrsPk38cBA"

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Grade normalization — map +/- variants to base letter grades
GRADE_MAP = {
    'A+': 'A', 'A': 'A', 'A-': 'A',
    'B+': 'B', 'B': 'B', 'B-': 'B',
    'C+': 'C', 'C': 'C', 'C-': 'C',
    'D+': 'D', 'D': 'D', 'D-': 'D',
    'F': 'F',
}

SCHOOLS = {
    'tamu': {
        'name': 'Texas A&M University',
        'short': 'TAMU',
        'color': '#500000',
        'file': 'tamu_grades.csv',
        'col_map': {
            'year': 'Year', 'semester': 'Semester',
            'dept': 'Dept', 'course': 'Coursenbr',
            'instructor': 'Instructor', 'grade': 'Lettergrade',
            'count': 'Count'
        }
    },
    'utaustin': {
        'name': 'University of Texas at Austin',
        'short': 'UT Austin',
        'color': '#BF5700',
        'file': 'utaustin_grades.csv',
        'col_map': {
            'year': 'year', 'semester': 'semester',
            'dept': 'dept', 'course': 'course_number',
            'instructor': 'professor_name', 'grade': 'grade',
            'count': 'count'
        }
    },
    'uwmadison': {
        'name': 'University of Wisconsin-Madison',
        'short': 'UW-Madison',
        'color': '#C5050C',
        'file': 'uwmadison_grades.csv',
        'col_map': {
            'year': 'year', 'semester': 'term',
            'dept': 'subject', 'course': 'course_number',
            'instructor': 'instructor', 'grade': 'grade',
            'count': 'count'
        }
    },
}

def normalize_grade(g):
    if not g: return None
    g = str(g).strip().upper()
    return GRADE_MAP.get(g, None)

async def main():
    async with libsql_client.create_client(url=TURSO_URL, auth_token=TURSO_TOKEN) as client:

        print("Creating schema...")
        await client.execute("""
            CREATE TABLE IF NOT EXISTS schools (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                short_name TEXT NOT NULL,
                color TEXT NOT NULL
            )
        """)

        await client.execute("""
            CREATE TABLE IF NOT EXISTS grades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                school_id TEXT NOT NULL,
                year TEXT,
                semester TEXT,
                dept TEXT,
                course_number TEXT,
                instructor TEXT,
                grade TEXT,
                count INTEGER,
                FOREIGN KEY (school_id) REFERENCES schools(id)
            )
        """)

        await client.execute("CREATE INDEX IF NOT EXISTS idx_school ON grades(school_id)")
        await client.execute("CREATE INDEX IF NOT EXISTS idx_dept ON grades(school_id, dept)")
        await client.execute("CREATE INDEX IF NOT EXISTS idx_course ON grades(school_id, dept, course_number)")
        await client.execute("CREATE INDEX IF NOT EXISTS idx_instructor ON grades(school_id, instructor)")

        print("Schema created ✅")

        # Add UH to schools dict
        SCHOOLS['uh'] = {
            'name': 'University of Houston',
            'short': 'UH',
            'color': '#C8102E',
            'file': None,  # xlsx handled separately
        }

        # Insert schools
        for sid, s in SCHOOLS.items():
            await client.execute(
                "INSERT OR REPLACE INTO schools (id, name, short_name, color) VALUES (?, ?, ?, ?)",
                [sid, s['name'], s['short'], s['color']]
            )
        print("Schools inserted ✅")

        # Migrate CSV schools
        for sid, s in SCHOOLS.items():
            if not s.get('file'):
                continue
            filepath = os.path.join(BASE_DIR, s['file'])
            if not os.path.exists(filepath):
                print(f"  ⚠️  {sid}: file not found — {filepath}")
                continue

            col = s['col_map']
            print(f"\nMigrating {s['name']}...")

            batch = []
            total = 0
            skipped = 0

            with open(filepath, encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    grade = normalize_grade(row.get(col['grade'], ''))
                    if not grade:
                        skipped += 1
                        continue
                    try:
                        count = int(float(row.get(col['count'], 0)))
                    except:
                        skipped += 1
                        continue

                    batch.append([
                        sid,
                        str(row.get(col['year'], '')).strip(),
                        str(row.get(col['semester'], '')).strip(),
                        str(row.get(col['dept'], '')).strip().upper(),
                        str(row.get(col['course'], '')).strip(),
                        str(row.get(col['instructor'], '')).strip(),
                        grade,
                        count
                    ])

                    if len(batch) >= 500:
                        stmts = [libsql_client.Statement(
                            "INSERT INTO grades (school_id, year, semester, dept, course_number, instructor, grade, count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            row
                        ) for row in batch]
                        await client.batch(stmts)
                        total += len(batch)
                        batch = []
                        print(f"  {total:,} rows...", end='\r')

            if batch:
                stmts = [libsql_client.Statement(
                    "INSERT INTO grades (school_id, year, semester, dept, course_number, instructor, grade, count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    row
                ) for row in batch]
                await client.batch(stmts)
                total += len(batch)

            print(f"  ✅ {s['name']}: {total:,} rows inserted ({skipped} skipped)")

        # Migrate UH xlsx
        print("\nMigrating University of Houston (xlsx)...")
        try:
            import openpyxl
            wb = openpyxl.load_workbook(os.path.join(BASE_DIR, 'IR11215.xlsx'), read_only=True)
            ws = wb.active
            headers = None
            batch = []
            total = 0
            skipped = 0

            for row in ws.iter_rows(values_only=True):
                if headers is None:
                    headers = [str(h).strip() for h in row]
                    print(f"  Columns: {headers}")
                    continue

                r = dict(zip(headers, row))
                grade = normalize_grade(r.get('COURSE_GRADE_OFFICIAL', ''))
                if not grade:
                    skipped += 1
                    continue
                try:
                    count = int(r.get('HEADCOUNT', 0))
                except:
                    skipped += 1
                    continue

                batch.append([
                    'uh',
                    str(r.get('ACADEMIC_YEAR', '')).strip(),
                    str(r.get('SEMESTER', '')).strip(),
                    str(r.get('COURSE_DEPT', '')).strip().upper(),
                    str(r.get('COURSE_NUMBER', '')).strip(),
                    str(r.get('INSTRUCTOR_NAME', '')).strip(),
                    grade,
                    count
                ])

                if len(batch) >= 500:
                    stmts = [libsql_client.Statement(
                        "INSERT INTO grades (school_id, year, semester, dept, course_number, instructor, grade, count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        r
                    ) for r in batch]
                    await client.batch(stmts)
                    total += len(batch)
                    batch = []
                    print(f"  {total:,} rows...", end='\r')

            if batch:
                stmts = [libsql_client.Statement(
                    "INSERT INTO grades (school_id, year, semester, dept, course_number, instructor, grade, count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    r
                ) for r in batch]
                await client.batch(stmts)
                total += len(batch)

            print(f"  ✅ University of Houston: {total:,} rows inserted ({skipped} skipped)")
        except Exception as e:
            print(f"  ❌ UH migration failed: {e}")

        # Verify
        print("\n📊 Verification:")
        result = await client.execute("SELECT school_id, COUNT(*) as cnt FROM grades GROUP BY school_id")
        for row in result.rows:
            print(f"  {row[0]}: {row[1]:,} rows")

        result = await client.execute("SELECT COUNT(*) FROM grades")
        print(f"  TOTAL: {result.rows[0][0]:,} rows")

asyncio.run(main())
