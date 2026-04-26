#!/usr/bin/env python3.11
"""
UC Berkeley Fall 2025 append extractor.
Source: Grade Distribution Report F25.xlsx
Headers at row 10: Semester Year Name Concat, Crs Academic Dept Short Nm,
  Course Subject Cd, Course Subject Short Nm, Course Number, Section Nbr,
  Course Title Nm, Instr Name Concat, Course Control Nbr,
  A+, A, A-, B+, B, B-, C+, C, C-, D+, D, D-, F,
  Pass, Satisfactory, Not Pass, Unsatisfactory, Total Enrollment
"""
import openpyxl
import csv
import os

INPUT = os.path.expanduser('~/Documents/GradeView/raw_data/ucberkeley/Grade Distribution Report F25.xlsx')
OUTPUT = os.path.expanduser('~/Documents/GradeView/raw_data/ucberkeley/berkeley_f25_extracted.csv')

def safe_int(val):
    try:
        return int(val) if val is not None and str(val).strip() not in ('', 'None') else 0
    except:
        return 0

rows_out = []
skipped = 0

wb = openpyxl.load_workbook(INPUT, read_only=True)
ws = wb['2025 Fall']

for i, row in enumerate(ws.iter_rows(values_only=True)):
    if i < 10:  # skip to row 11 (data starts)
        continue

    if len(row) < 27:
        skipped += 1
        continue

    (sem_year, dept_short, subject_cd, subject_nm, course_num, section_nbr,
     course_title, instructor, control_nbr,
     Ap, A, Am, Bp, B, Bm, Cp, C, Cm, Dp, D, Dm, F,
     Pass, Satisfactory, Not_Pass, Unsatisfactory, total_enroll) = row[:27]

    # Parse semester/year from "2025 Fall"
    semester = 'Fall'
    year = 2025

    grades = [Ap, A, Am, Bp, B, Bm, Cp, C, Cm, Dp, D, Dm, F]
    total = sum(safe_int(g) for g in grades)
    if total == 0:
        skipped += 1
        continue

    rows_out.append({
        'school_id': 'ucberkeley',
        'semester': semester,
        'year': year,
        'department': str(subject_cd).strip() if subject_cd else '',
        'course_number': str(course_num).strip() if course_num else '',
        'section': str(section_nbr).strip() if section_nbr else '',
        'course_name': str(course_title).strip() if course_title else '',
        'instructor': str(instructor).strip() if instructor else '',
        'A+': safe_int(Ap),
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
              'course_name','instructor','A+','A','A-','B+','B','B-','C+','C','C-',
              'D+','D','D-','F','total_students']

with open(OUTPUT, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows_out)

print(f"✅ Berkeley F25 extracted: {len(rows_out):,} rows → {OUTPUT}")
print(f"   Skipped: {skipped:,}")
