#!/usr/bin/env python3
"""UTRGV extractor — long format, header on row 2, semester in col A."""
import csv, re
from pathlib import Path
import openpyxl

RAW_DIR = Path.home() / "Documents/GradeView/raw_data/utrgv"
OUTPUT  = RAW_DIR / "utrgv_grades_extracted.csv"

VALID_GRADES = {'A','A+','A-','B','B+','B-','C','C+','C-','D','D+','D-','F','P','W'}
SEMESTER_MAP = {'fall':'FALL','spring':'SPRING','summer':'SUMMER','winter':'WINTER'}

def parse_semester(sem):
    sem = str(sem).strip().lstrip('\xa0')
    # Format: '2020/FA', '2021/SP', '2022/SU'
    m = re.match(r'(\d{4})/(\w+)', sem)
    if m:
        year = m.group(1)
        code = m.group(2).upper()
        code_map = {'FA':'FALL','SP':'SPRING','SU':'SUMMER','WI':'WINTER','S1':'SUMMER','S2':'SUMMER'}
        return year, code_map.get(code, code)
    # Format: 'Fall 2020'
    m = re.match(r'(\w+)\s+(\d{4})', sem)
    if m:
        s = SEMESTER_MAP.get(m.group(1).lower(), m.group(1).upper())
        return m.group(2), s
    return '0000', 'UNKNOWN'

def main():
    files = list(RAW_DIR.glob("*.xlsx"))
    all_rows = []
    for f in files:
        print(f"  {f.name}...")
        wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        # Header on row 2 (index 1)
        headers = [str(h).strip().lstrip('\xa0') if h else '' for h in rows[1]]
        try:
            i_sem    = headers.index('Semester')
            i_subj   = headers.index('Course Subject')
            i_course = headers.index('Course Number')
            i_instr  = headers.index('Instructors')
            i_grade  = headers.index('Grade')
            i_count  = headers.index('Grade Count')
        except ValueError as e:
            print(f"  ⚠️ {e}"); continue

        for row in rows[2:]:
            if not row or not row[i_grade]: continue
            grade = str(row[i_grade]).strip().upper()
            if grade not in VALID_GRADES: continue
            year, semester = parse_semester(row[i_sem])
            dept   = str(row[i_subj]).strip() if row[i_subj] else ''
            course = str(row[i_course]).strip() if row[i_course] else ''
            instr  = str(row[i_instr]).strip() if row[i_instr] else 'N/A'
            try: count = int(row[i_count])
            except: count = 0
            if count <= 0: continue
            all_rows.append({'school_id':'utrgv','year':year,'semester':semester,
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
