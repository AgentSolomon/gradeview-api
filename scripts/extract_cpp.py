#!/usr/bin/env python3.11
"""
CPP (Cal Poly Pomona) grade distribution extractor.
Source: PRA_20260323_Aikin_Ryan.xlsx
Headers at row 9: Course Title, Subject, Catalog Nbr, Section, Term,
                  Instructor Alias ID, A, A-, B+, B, B-, C+, C, C-, D+, D, D-, F
"""
import openpyxl
import csv
import re
import sys
import os

INPUT = os.path.expanduser('~/Documents/GradeView/raw_data/cpp/PRA_20260323_Aikin_Ryan.xlsx')
OUTPUT = os.path.expanduser('~/Documents/GradeView/raw_data/cpp/cpp_extracted.csv')

GRADE_COLS = ['A','A-','B+','B','B-','C+','C','C-','D+','D','D-','F']

def parse_term(term_str):
    """'Fall Semester 2020' -> ('Fall', 2020)"""
    if not term_str:
        return None, None
    term_str = str(term_str).strip()
    m = re.match(r'(Fall|Spring|Summer|Winter)\s+Semester\s+(\d{4})', term_str, re.IGNORECASE)
    if m:
        return m.group(1).capitalize(), int(m.group(2))
    return None, None

def safe_int(val):
    try:
        return int(val) if val is not None and str(val).strip() not in ('', 'None') else 0
    except:
        return 0

rows_out = []
skipped = 0

wb = openpyxl.load_workbook(INPUT, read_only=True)
ws = wb['Public Data Request']

for i, row in enumerate(ws.iter_rows(values_only=True)):
    if i < 9:  # skip header rows 1-9
        continue

    (course_title, subject, catalog_nbr, section, term_str,
     instructor_id, A, Am, Bp, B, Bm, Cp, C, Cm, Dp, D, Dm, F) = row[:18]

    semester, year = parse_term(term_str)
    if not semester or not year:
        skipped += 1
        continue

    # Skip if no meaningful grade data
    grades = [A, Am, Bp, B, Bm, Cp, C, Cm, Dp, D, Dm, F]
    total = sum(safe_int(g) for g in grades)
    if total == 0:
        skipped += 1
        continue

    rows_out.append({
        'school_id': 'cpp',
        'semester': semester,
        'year': year,
        'department': str(subject).strip() if subject else '',
        'course_number': str(catalog_nbr).strip() if catalog_nbr else '',
        'section': str(section).strip() if section else '',
        'course_name': str(course_title).strip() if course_title else '',
        'instructor': f'Instructor_{instructor_id}' if instructor_id else '',
        'A': safe_int(A),
        'A-': safe_int(Am),
        'B+': safe_int(Bp),
        'B': safe_int(B),
        'B-': safe_int(Bm),
        'C+': safe_int(Cp),
        'C': safe_int(C),
        'C-': safe_int(Cm),
        'D+': safe_int(Dp),
        'D': safe_int(D),
        'D-': safe_int(Dm),
        'F': safe_int(F),
        'total_students': total,
    })

wb.close()

fieldnames = ['school_id','semester','year','department','course_number','section',
              'course_name','instructor','A','A-','B+','B','B-','C+','C','C-',
              'D+','D','D-','F','total_students']

with open(OUTPUT, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows_out)

print(f"✅ CPP extracted: {len(rows_out):,} rows → {OUTPUT}")
print(f"   Skipped: {skipped:,}")

# Show semester breakdown
from collections import Counter
sems = Counter(f"{r['semester']} {r['year']}" for r in rows_out)
for k, v in sorted(sems.items()):
    print(f"   {k}: {v:,} rows")
