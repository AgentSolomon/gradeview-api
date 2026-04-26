#!/usr/bin/env python3.11
"""
TAMU-San Antonio grade distribution extractor.
Source: PIR_L001472-040226_FOR_RELEASE.numbers (converted to xlsx)

Columns: ACADEMIC_YEAR, ACADEMIC_PERIOD_DESC, SUBJECT, COURSE_NUMBER,
         COURSE_REFERENCE_NUMBER, INSTRUCTOR_FIRST_NAME, INSTRUCTOR_MIDDLE_NAME,
         INSTRUCTOR_LAST_NAME, A, AU, B, C, CR, D, F, FN, I, IC, IP, NC, W, WS, WX, NG

Grade mapping: A→A, B→B, C→C, D→D, F→F, W→W (keep standard)
Skip: AU, CR, FN, I, IC, IP, NC, WS, WX, NG (non-grade or admin codes)
Privacy: rows where all grade counts are None/0 are skipped

Output: school_id=tamusa, standard upload_v2.py CSV format
"""
import openpyxl
import csv
import sys
import os
from pathlib import Path

INPUT = Path.home() / "Documents/GradeView/raw_data/tamusa/tamusa_grades.xlsx"
OUTPUT = Path.home() / "Documents/GradeView/raw_data/tamusa/tamusa_extracted.csv"

# Columns to treat as valid grade counts
GRADE_COLS = ['A', 'B', 'C', 'D', 'F', 'W']
# Column indices in the sheet (0-based, after header)
COL_MAP = {
    'year': 0, 'semester_desc': 1, 'subject': 2, 'course_number': 3,
    'crn': 4, 'fname': 5, 'mname': 6, 'lname': 7,
    'A': 8, 'AU': 9, 'B': 10, 'C': 11, 'CR': 12, 'D': 13, 'F': 14,
    'FN': 15, 'I': 16, 'IC': 17, 'IP': 18, 'NC': 19, 'W': 20,
    'WS': 21, 'WX': 22, 'NG': 23
}

def parse_semester(desc):
    """'Fall 2020' → ('Fall', 2020)"""
    parts = desc.strip().split()
    if len(parts) == 2:
        term, year = parts
        return term, int(year)
    return desc, 0

def val(v):
    if v is None: return 0
    try: return int(v)
    except: return 0

def main():
    wb = openpyxl.load_workbook(str(INPUT))
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    header = rows[0]
    data = rows[1:]

    records = []
    skipped_privacy = 0
    skipped_empty = 0

    for row in data:
        semester_desc = row[COL_MAP['semester_desc']]
        subject = row[COL_MAP['subject']]
        course_num = row[COL_MAP['course_number']]

        if not semester_desc or not subject or not course_num:
            skipped_empty += 1
            continue

        term, year = parse_semester(str(semester_desc))

        # Grade counts
        grades = {g: val(row[COL_MAP[g]]) for g in GRADE_COLS}
        total = sum(grades.values())

        # Privacy: skip if all zeros (masked or truly empty)
        if total == 0:
            skipped_privacy += 1
            continue

        # Instructor name
        fname = (row[COL_MAP['fname']] or '').strip()
        mname = (row[COL_MAP['mname']] or '').strip().replace(' ', '')
        lname = (row[COL_MAP['lname']] or '').strip()
        instructor = ' '.join(filter(None, [fname, lname])) or 'Unknown'

        records.append({
            'school_id': 'tamusa',
            'year': year,
            'semester': term,
            'department': str(subject).strip(),
            'course_number': str(course_num).strip(),
            'instructor': instructor,
            'grade_a': grades['A'],
            'grade_b': grades['B'],
            'grade_c': grades['C'],
            'grade_d': grades['D'],
            'grade_f': grades['F'],
            'grade_w': grades['W'],
            'grade_other': 0,
        })

    print(f"Total source rows:    {len(data)}")
    print(f"Skipped (empty):      {skipped_empty}")
    print(f"Skipped (all zeros):  {skipped_privacy}")
    print(f"Records extracted:    {len(records)}")

    # Write CSV in long format (one row per grade letter) to match upload_v2.py schema
    # Schema: school_id, year, semester, dept, course_number, instructor, grade, count
    with open(OUTPUT, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['school_id','Semester','Department','Course Number','Instructor','Grade','Count'])
        for r in records:
            semester_str = f"{r['semester']} {r['year']}"  # e.g. "Fall 2020"
            for grade in GRADE_COLS:
                count = r[f'grade_{grade.lower()}']
                if count > 0:
                    writer.writerow([
                        r['school_id'], semester_str,
                        r['department'], r['course_number'], r['instructor'],
                        grade, count
                    ])

    print(f"Output: {OUTPUT}")

    # Semester breakdown
    from collections import Counter
    sem_counts = Counter(f"{r['semester']} {r['year']}" for r in records)
    print(f"\nSemesters ({len(sem_counts)}):")
    for s in sorted(sem_counts): print(f"  {s}: {sem_counts[s]} rows")
    print(f"\nDepartments: {len(set(r['department'] for r in records))}")

if __name__ == '__main__':
    main()
