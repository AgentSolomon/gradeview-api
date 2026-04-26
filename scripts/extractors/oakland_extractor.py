#!/usr/bin/env python3
"""Oakland University extractor — wide format, grades as columns, semester from filename/row1."""
import csv, glob, re
from pathlib import Path
from collections import defaultdict
import openpyxl

RAW_DIR = Path.home() / "Documents/GradeView/raw_data/oakland/Record"
OUTPUT  = Path.home() / "Documents/GradeView/raw_data/oakland/oakland_grades_extracted.csv"

GRADE_COLS = ['A','A-','B+','B','B-','C+','C','C-','D+','D','F','P','W']
SEMESTER_MAP = {'fall':'FALL','spring':'SPRING','summer':'SUMMER','winter':'WINTER'}

def parse_filename(name):
    """'Fall_2023.xlsx' → (2023, FALL)"""
    m = re.match(r'(\w+)_(\d{4})', name)
    if m:
        sem = SEMESTER_MAP.get(m.group(1).lower(), m.group(1).upper())
        return m.group(2), sem
    return '0000', 'UNKNOWN'

def main():
    files = sorted(glob.glob(str(RAW_DIR / "*.xlsx")))
    all_rows = []

    for f in files:
        fname = Path(f).name
        year, semester = parse_filename(fname)
        print(f"  {fname} → {semester} {year}")

        wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))

        # Row 1 = title, Row 2 = grade headers (with N/% pairs), Row 3 = col headers
        # Find header row with Dept/Subject/Instructor
        header_idx = None
        for i, row in enumerate(rows[:5]):
            if row and any(str(c) in ('Dept.', 'Subject', 'Instructor Last') for c in row if c):
                header_idx = i
                break
        if header_idx is None:
            print(f"  ⚠️  Could not find header row"); continue

        headers = [str(h).strip() if h else '' for h in rows[header_idx]]
        grade_row = rows[header_idx - 1]  # Row above has grade letters

        # Map grade letter → column index (the 'N' count column)
        grade_idx = {}
        for i, val in enumerate(grade_row):
            if val in GRADE_COLS:
                # Next column (i) is N count, skip % at i+1
                grade_idx[str(val)] = i

        try:
            i_subj  = next(i for i,h in enumerate(headers) if h == 'Subject')
            i_instr = next(i for i,h in enumerate(headers) if 'Instructor' in h)
        except StopIteration:
            print(f"  ⚠️  Missing Subject/Instructor columns"); continue

        for row in rows[header_idx+1:]:
            if not row or not row[i_subj]: continue
            subj_raw = str(row[i_subj]).strip()
            if 'Total' in subj_raw or not subj_raw: continue
            # Parse dept and course from 'AH 1001'
            parts = subj_raw.split()
            dept   = parts[0] if parts else ''
            course = parts[1] if len(parts) > 1 else ''
            instr  = str(row[i_instr]).strip() if row[i_instr] else 'N/A'

            for grade, col_i in grade_idx.items():
                if col_i >= len(row): continue
                val = row[col_i]
                try: count = int(float(val)) if val is not None else 0
                except: count = 0
                if count <= 0: continue
                all_rows.append({'school_id':'oakland','year':year,'semester':semester,
                    'dept':dept,'course_number':course,'instructor':instr,'grade':grade,'count':count})
        wb.close()
        print(f"    → {len(all_rows):,} rows so far")

    print(f"\nTotal: {len(all_rows):,}")
    fieldnames = ['school_id','year','semester','dept','course_number','instructor','grade','count']
    with open(OUTPUT, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Saved → {OUTPUT}")

if __name__ == '__main__':
    main()
