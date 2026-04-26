"""
University at Albany (SUNY Albany) grade distribution extractor.
Format: Wide, row 1 = merged header, row 2 = actual headers.
Already grouped (no section numbers). Instructor: 'Last, First' format.
"""
import os, csv
from pathlib import Path

SCHOOL_ID = 'ualbany'
INPUT  = Path.home() / 'Downloads/Courses Grades  Instr_Public Records Request.xlsx'
OUTPUT = Path.home() / 'Documents/GradeView/raw_data/ualbany/ualbany_extracted.csv'

GRADE_MAP = {
    'A': 'A', 'A-': 'A', 'A+': 'A',
    'B+': 'B', 'B': 'B', 'B-': 'B',
    'C+': 'C', 'C': 'C', 'C-': 'C',
    'D+': 'D', 'D': 'D', 'D-': 'D',
    'F': 'F',
}

def parse_semester(raw):
    """'Fall 2020' → ('Fall', 2020)"""
    parts = str(raw).strip().rsplit(' ', 1)
    if len(parts) == 2:
        try:
            return parts[0].strip(), int(parts[1])
        except:
            pass
    return None, None

def clean_instructor(raw):
    """'Jaeckel-Rodriguez, K' → 'K Jaeckel-Rodriguez'"""
    if not raw or str(raw).strip() in ('', 'nan', 'None'):
        return 'Unknown'
    raw = str(raw).strip()
    if ',' in raw:
        parts = raw.split(',', 1)
        return f"{parts[1].strip()} {parts[0].strip()}"
    return raw

import openpyxl

def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    wb = openpyxl.load_workbook(INPUT, read_only=True, data_only=True)
    ws = wb.active

    rows_iter = ws.iter_rows(values_only=True)
    next(rows_iter)  # skip merged header row 1
    headers = [str(h).strip() if h else '' for h in next(rows_iter)]  # row 2 = actual headers

    idx_term  = headers.index('TERM')
    idx_subj  = headers.index('SUBJECT')
    idx_cat   = headers.index('CATALOG_NBR')
    idx_instr = headers.index('Instructor Name')

    # Find all grade columns we care about
    grade_cols = {h: i for i, h in enumerate(headers) if h in GRADE_MAP}

    out_rows = 0
    with open(OUTPUT, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['school_id', 'semester', 'year', 'department', 'course_number', 'instructor', 'grade', 'count'])

        for row in rows_iter:
            term_raw = row[idx_term]
            if not term_raw:
                continue
            sem, yr = parse_semester(term_raw)
            if not sem or not yr:
                continue
            if sem not in ('Fall', 'Spring'):
                continue

            subj  = str(row[idx_subj]).strip() if row[idx_subj] else ''
            cat   = str(row[idx_cat]).strip() if row[idx_cat] else ''
            instr = clean_instructor(row[idx_instr])

            # Collapse +/- grades and sum
            grade_totals = {}
            for raw_grade, col_idx in grade_cols.items():
                count = row[col_idx]
                try:
                    count = int(count) if count else 0
                except:
                    count = 0
                mapped = GRADE_MAP[raw_grade]
                if count > 0:
                    grade_totals[mapped] = grade_totals.get(mapped, 0) + count

            for grade, count in grade_totals.items():
                if count > 0:
                    writer.writerow([SCHOOL_ID, sem, yr, subj, cat, instr, grade, count])
                    out_rows += 1

    print(f"U Albany: {out_rows:,} output rows saved to {OUTPUT}")

if __name__ == '__main__':
    main()
