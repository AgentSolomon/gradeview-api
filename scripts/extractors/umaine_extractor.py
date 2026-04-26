#!/usr/bin/env python3
"""UMaine extractor — wide format, grades as _sum columns."""
import csv, re
from pathlib import Path
import openpyxl

RAW_DIR = Path.home() / "Documents/GradeView/raw_data/umaine"
OUTPUT  = RAW_DIR / "umaine_grades_extracted.csv"

GRADE_COL_MAP = {
    'A_sum':'A', 'A.minus_sum':'A-', 'A.plus_sum':'A+',
    'B_sum':'B', 'B.minus_sum':'B-', 'B.plus_sum':'B+',
    'C_sum':'C', 'C.minus_sum':'C-', 'C.plus_sum':'C+',
    'D_sum':'D', 'D.minus_sum':'D-', 'D.plus_sum':'D+',
    'F_sum':'F', 'W_sum':'W', 'P_sum':'P',
}

SEMESTER_MAP = {'SPRING':'SPRING','FALL':'FALL','SUMMER':'SUMMER','WINTER':'WINTER'}

def parse_term(term_including_winter, term_year):
    sem = str(term_including_winter).strip().upper()
    year = str(term_year).strip()
    return year, SEMESTER_MAP.get(sem, sem)

def main():
    wb = openpyxl.load_workbook(RAW_DIR / 'umaine_grades.xlsx', read_only=True, data_only=True)
    ws = wb['FOAA Request Grades Sp20 to F25']
    rows = list(ws.iter_rows(values_only=True))
    headers = [str(h).strip() if h else '' for h in rows[0]]

    i_term    = headers.index('Term')
    i_sem     = headers.index('TermIncludingWINTER')
    i_subj    = headers.index('Subject')
    i_catalog = headers.index('Catalog')
    i_instr   = headers.index('All_Instructors')
    grade_idx = {GRADE_COL_MAP[h]: i for i,h in enumerate(headers) if h in GRADE_COL_MAP}

    all_rows = []
    for row in rows[1:]:
        if not row or not row[i_subj]: continue
        year, semester = parse_term(row[i_sem], row[i_term])
        dept   = str(row[i_subj]).strip()
        course = str(row[i_catalog]).strip()
        instr  = str(row[i_instr]).strip() if row[i_instr] else 'N/A'
        for grade, col_i in grade_idx.items():
            val = row[col_i]
            try: count = int(val)
            except: count = 0
            if count <= 0: continue
            all_rows.append({'school_id':'umaine','year':year,'semester':semester,
                'dept':dept,'course_number':course,'instructor':instr,'grade':grade,'count':count})

    print(f"Total: {len(all_rows):,}")
    fieldnames = ['school_id','year','semester','dept','course_number','instructor','grade','count']
    with open(OUTPUT, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Saved → {OUTPUT}")

if __name__ == '__main__':
    main()
