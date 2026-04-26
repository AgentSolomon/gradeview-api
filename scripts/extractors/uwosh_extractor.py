"""
UW Oshkosh grade distribution extractor.
Format: Wide (one row = one class). Row 1 blank, Row 2 = headers.
Grade 'E' = F. Course# embedded in subject string (e.g. AAFS287).
"""
import os, re, csv
from pathlib import Path

SCHOOL_ID = 'uwosh'
INPUT  = Path.home() / 'Downloads/grades_by_class_with_Instructors.xlsx'
OUTPUT = Path.home() / 'Documents/GradeView/raw_data/uwosh/uwosh_extracted.csv'

GRADE_COLS = ['A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D+', 'D', 'D-', 'E']
GRADE_MAP  = {
    'A': 'A', 'A-': 'A',
    'B+': 'B', 'B': 'B', 'B-': 'B',
    'C+': 'C', 'C': 'C', 'C-': 'C',
    'D+': 'D', 'D': 'D', 'D-': 'D',
    'E': 'F',
}

def parse_semester(raw):
    """'Fall 2022' or 'Winter 2020' → (sem, year)"""
    raw = str(raw).strip()
    parts = raw.rsplit(' ', 1)
    if len(parts) == 2:
        try:
            return parts[0].strip(), int(parts[1])
        except:
            pass
    return None, None

def split_course(code):
    """'AAFS287' → ('AAFS', '287'); 'ALCS269' → ('ALCS', '269')"""
    m = re.match(r'^([A-Z]+)(\d+.*)$', str(code).strip())
    if m:
        return m.group(1), m.group(2)
    return code, ''

import openpyxl

def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.load_workbook(INPUT, read_only=True, data_only=True)
    ws = wb['FOIL']

    rows_iter = ws.iter_rows(values_only=True)
    next(rows_iter)  # skip blank row 1
    headers = [str(h).strip() if h else '' for h in next(rows_iter)]  # row 2 = headers

    idx_sem   = headers.index('Semester and Year')
    idx_instr = headers.index('Instructor Name(s)')
    idx_code  = headers.index('Class with course (catalog) number')
    grade_indices = {g: headers.index(g) for g in GRADE_COLS if g in headers}

    out_rows = 0
    with open(OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['school_id', 'semester', 'year', 'department', 'course_number', 'instructor', 'grade', 'count'])

        for row in rows_iter:
            sem_raw = row[idx_sem]
            if not sem_raw:
                continue
            sem, yr = parse_semester(sem_raw)
            if not sem or not yr:
                continue
            # Only Fall and Spring
            if sem not in ('Fall', 'Spring'):
                continue

            instr = str(row[idx_instr]).strip() if row[idx_instr] else 'Unknown'
            course_code = str(row[idx_code]).strip() if row[idx_code] else ''
            dept, course_num = split_course(course_code)

            # Collapse +/- grades to A/B/C/D/F and sum
            grade_totals = {}
            for raw_grade, col_idx in grade_indices.items():
                count = row[col_idx]
                try:
                    count = int(count) if count else 0
                except:
                    count = 0
                mapped = GRADE_MAP.get(raw_grade)
                if mapped and count > 0:
                    grade_totals[mapped] = grade_totals.get(mapped, 0) + count

            for grade, count in grade_totals.items():
                if count > 0:
                    writer.writerow([SCHOOL_ID, sem, yr, dept, course_num, instr, grade, count])
                    out_rows += 1

    print(f"UW Oshkosh: {out_rows:,} output rows saved to {OUTPUT}")

if __name__ == '__main__':
    main()
