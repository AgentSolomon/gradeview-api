#!/usr/bin/env python3
"""UNG (North Georgia) extractor — wide format, grades as columns (A_Grade, B_Grade etc), * = masked small counts (skip)."""
import csv, glob
from pathlib import Path
import openpyxl, re

RAW_DIR = Path.home() / "Documents/GradeView/raw_data/ung"
OUTPUT  = RAW_DIR / "ung_grades_extracted.csv"

GRADE_COL_MAP = {'A_Grade':'A','B_Grade':'B','C_Grade':'C','D_Grade':'D','F_Grade':'F'}

def parse_term(term_code):
    """202002 → (2020, SPRING), 202009 → (2020, FALL)"""
    t = str(term_code).strip()
    if len(t) == 6:
        year = t[:4]
        month = int(t[4:])
        if month <= 2:   sem = 'SPRING'
        elif month <= 6: sem = 'SUMMER'
        elif month <= 8: sem = 'FALL'
        else:            sem = 'FALL'
        return year, sem
    return '0000', 'UNKNOWN'

def main():
    files = sorted(glob.glob(str(RAW_DIR / "*.xlsx")))
    all_rows = []
    for f in files:
        print(f"  {Path(f).name}...")
        wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        headers = [str(h).strip() if h else '' for h in rows[0]]
        try:
            i_term  = headers.index('TERM_CODE')
            i_dept  = headers.index('DEPARTMENT_DESC')
            i_course= headers.index('COURSE')
            i_name  = headers.index('Name')
        except ValueError as e:
            print(f"  ⚠️  {e}"); continue
        grade_idx = {GRADE_COL_MAP[h]: i for i,h in enumerate(headers) if h in GRADE_COL_MAP}
        for row in rows[1:]:
            if not row or not row[i_term]: continue
            year, semester = parse_term(row[i_term])
            # Parse course: 'CHEM-4901L' → dept=CHEM, course=4901L
            course_raw = str(row[i_course]).strip() if row[i_course] else ''
            if '-' in course_raw:
                parts = course_raw.split('-', 1)
                dept   = parts[0]
                course = parts[1]
            else:
                dept   = str(row[i_dept]).strip() if row[i_dept] else ''
                course = course_raw
            instr = str(row[i_name]).strip() if row[i_name] and str(row[i_name]).strip() not in ('NULL','') else 'N/A'
            for grade, col_i in grade_idx.items():
                val = row[col_i]
                if val == '*' or val is None: continue  # masked or empty
                try: count = int(val)
                except: count = 0
                if count <= 0: continue
                all_rows.append({'school_id':'ung','year':year,'semester':semester,
                    'dept':dept,'course_number':course,'instructor':instr,'grade':grade,'count':count})
        wb.close()
    print(f"\nTotal: {len(all_rows):,}")
    fieldnames = ['school_id','year','semester','dept','course_number','instructor','grade','count']
    with open(OUTPUT, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Saved → {OUTPUT}")

if __name__ == '__main__':
    main()
