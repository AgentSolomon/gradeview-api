#!/usr/bin/env python3.11
"""
University of Michigan grade distribution extractor.
Source: 261324 nonexempt data.xlsx
Columns: Term, Term Descrshort, Subject, Catalog Nbr, Crse Descr,
         Class Section, Crse Grade Off, Count of Emplid
Notes:
  - No instructor names (withheld by UMICH, cost estimate requested)
  - Privacy threshold: 30 students (stricter than most)
  - Semester codes: FA=Fall, WN=Winter, SS=Spring, SP=Spring
  - Grade E = F (Michigan convention)
  - Filter out incomplete grades (I*, IA, IB, etc.)
"""
import openpyxl
import csv
import re
import os

INPUT = os.path.expanduser('~/Documents/GradeView/raw_data/umich/261324 nonexempt data.xlsx')
OUTPUT = os.path.expanduser('~/Documents/GradeView/raw_data/umich/umich_extracted.csv')

SEMESTER_MAP = {
    'FA': 'Fall',
    'WN': 'Winter',
    'SS': 'Spring',
    'SP': 'Spring',
    'SU': 'Summer',
}

VALID_GRADES = {'A+','A','A-','B+','B','B-','C+','C','C-','D+','D','D-','F'}
GRADE_REMAP = {'E': 'F'}

def parse_term(term_str):
    """'FA 2020' -> ('Fall', 2020)"""
    if not term_str:
        return None, None
    parts = str(term_str).strip().split()
    if len(parts) == 2:
        code, year = parts[0].upper(), parts[1]
        sem = SEMESTER_MAP.get(code)
        if sem and year.isdigit():
            return sem, int(year)
    return None, None

def safe_int(val):
    try:
        return int(val) if val is not None else 0
    except:
        return 0

rows_out = []
skipped_grade = 0
skipped_term = 0

wb = openpyxl.load_workbook(INPUT, read_only=True)
ws = wb['261324']

for i, row in enumerate(ws.iter_rows(values_only=True)):
    if i == 0:  # header
        continue

    term_code, term_str, subject, catalog_nbr, crse_descr, section, grade, count = row[:8]

    # Normalize grade
    grade = str(grade).strip() if grade else ''
    grade = GRADE_REMAP.get(grade, grade)

    if grade not in VALID_GRADES:
        skipped_grade += 1
        continue

    semester, year = parse_term(term_str)
    if not semester:
        skipped_term += 1
        continue

    count_val = safe_int(count)
    if count_val == 0:
        continue

    rows_out.append({
        'school_id': 'umich',
        'semester': semester,
        'year': year,
        'department': str(subject).strip() if subject else '',
        'course_number': str(catalog_nbr).strip() if catalog_nbr else '',
        'course_name': str(crse_descr).strip() if crse_descr else '',
        'instructor': '',
        'grade': grade,
        'count': count_val,
    })

wb.close()

fieldnames = ['school_id','semester','year','department','course_number',
              'course_name','instructor','grade','count']

with open(OUTPUT, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows_out)

print(f"✅ UMich extracted: {len(rows_out):,} rows → {OUTPUT}")
print(f"   Skipped (non-grade): {skipped_grade:,}")
print(f"   Skipped (bad term):  {skipped_term:,}")

from collections import Counter
sems = Counter(f"{r['semester']} {r['year']}" for r in rows_out)
for k, v in sorted(sems.items()):
    print(f"   {k}: {v:,} rows")
