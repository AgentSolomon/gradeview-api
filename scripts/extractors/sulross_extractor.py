#!/usr/bin/env python3.11
"""
Sul Ross State University grade distribution extractor.
Sheet: 'Data'
Columns: Subject, Subject_desc, Course_number, Section_number, Course,
         Instructor_name, Term Desc, A_Grades, B_Grades, C_Grades, D_Grades,
         F_Grades, F0_Grades, FX_Grades, ... Total_Students
Note: rows with '<10' are privacy-masked — skip grade columns, still count row.
      Aggregate sections by (term, subject, course_number).
"""
import openpyxl
from collections import defaultdict
import os

FILE = os.path.expanduser(
    "~/Documents/GradeView/raw_data/sulross/04062026_Grade Dist Request Report_PIO.xlsx"
)

GRADE_COLS = {
    7:  'A',
    8:  'B',
    9:  'C',
    10: 'D',
    11: 'F',
    12: 'F',  # F0 -> F
    13: 'F',  # FX -> F
}

def parse_term(term_desc):
    parts = term_desc.strip().split()
    if len(parts) == 2:
        return parts[0], parts[1]
    return None, None

def extract_all():
    wb = openpyxl.load_workbook(FILE, read_only=True, data_only=True)
    ws = wb['Data']
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    agg = defaultdict(lambda: defaultdict(int))
    meta = {}

    for row in rows[1:]:
        if not row or row[6] is None:
            continue

        term_desc = str(row[6]).strip()
        season, year = parse_term(term_desc)
        if not season or not year:
            continue

        subject   = str(row[0]).strip() if row[0] else ''
        course_no = str(row[2]).strip() if row[2] else ''
        title     = str(row[4]).strip() if row[4] else ''
        instructor = str(row[5]).strip() if row[5] else 'N/A'

        key = (season, year, subject, course_no)
        if key not in meta:
            meta[key] = (title, instructor)

        for col_idx, grade in GRADE_COLS.items():
            val = row[col_idx]
            if isinstance(val, (int, float)) and val > 0:
                agg[key][grade] += int(val)

    records = []
    for key, grades in agg.items():
        season, year, subject, course_no = key
        title, instructor = meta[key]
        for grade, count in grades.items():
            records.append({
                'semester': season,
                'year': year,
                'subject': subject,
                'catalog_number': course_no,
                'course_title': title,
                'instructor': instructor,
                'grade': grade,
                'count': count,
            })
    return records

if __name__ == '__main__':
    records = extract_all()
    print(f'Total records: {len(records)}')
    semesters = sorted(set(f"{r['semester']} {r['year']}" for r in records))
    print(f'Semesters: {semesters}')
    for r in records[:5]:
        print(r)
