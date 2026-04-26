#!/usr/bin/env python3
"""Valdosta extractor — long format, one row per student grade. Header on row 2. Aggregate counts."""
import csv, glob
from pathlib import Path
from collections import defaultdict
import openpyxl, re

RAW_DIR = Path.home() / "Documents/GradeView/raw_data/valdosta"
OUTPUT  = RAW_DIR / "valdosta_grades_extracted.csv"

VALID_GRADES = {'A','A+','A-','B','B+','B-','C','C+','C-','D','D+','D-','F','P','W','WF'}
SEMESTER_MAP = {'spring':'SPRING','fall':'FALL','summer':'SUMMER','winter':'WINTER'}

def parse_semester(sem):
    """'Spring 2020' → (2020, SPRING)"""
    m = re.match(r'(\w+)\s+(\d{4})', str(sem).strip())
    if m:
        s = SEMESTER_MAP.get(m.group(1).lower(), m.group(1).upper())
        return m.group(2), s
    return '0000', 'UNKNOWN'

def main():
    files = sorted(glob.glob(str(RAW_DIR / "*.xlsx")))
    # Aggregate: (school, year, sem, dept, course, instr, grade) → count
    counts = defaultdict(int)
    for f in files:
        print(f"  {Path(f).name}...")
        wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        # Header on row 2 (index 1)
        headers = [str(h).strip() if h else '' for h in rows[1]]
        try:
            i_sem    = headers.index('SEMESTER')
            i_subj   = headers.index('Course Subject')
            i_course = headers.index('Course Number')
            i_instr  = headers.index('Course Instructor')
            i_grade  = headers.index('Course Grade')
        except ValueError as e:
            print(f"  ⚠️  Missing column: {e}")
            continue
        for row in rows[2:]:
            if not row or not row[i_sem]: continue
            grade = str(row[i_grade]).strip().upper() if row[i_grade] else ''
            if grade not in VALID_GRADES: continue
            year, semester = parse_semester(row[i_sem])
            dept   = str(row[i_subj]).strip() if row[i_subj] else ''
            course = str(row[i_course]).strip() if row[i_course] else ''
            instr  = str(row[i_instr]).strip() if row[i_instr] else 'N/A'
            key = ('valdosta', year, semester, dept, course, instr, grade)
            counts[key] += 1
        wb.close()
        print(f"    → {len(counts):,} unique combos so far")

    all_rows = []
    for (school,year,sem,dept,course,instr,grade), count in counts.items():
        all_rows.append({'school_id':school,'year':year,'semester':sem,
            'dept':dept,'course_number':course,'instructor':instr,'grade':grade,'count':count})

    print(f"\nTotal rows: {len(all_rows):,}")
    fieldnames = ['school_id','year','semester','dept','course_number','instructor','grade','count']
    with open(OUTPUT, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Saved → {OUTPUT}")

if __name__ == '__main__':
    main()
