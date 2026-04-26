#!/usr/bin/env python3
"""MSU extractor — numeric grades (0.0-4.0), TERM like 'US20' = Summer 2020."""
import csv
from pathlib import Path

RAW_DIR = Path.home() / "Documents/GradeView/raw_data/msu"
OUTPUT  = RAW_DIR / "msu_grades_extracted.csv"

# Map numeric grades to letter grades
def numeric_to_letter(g):
    try:
        v = float(g)
    except (ValueError, TypeError):
        return None
    if v >= 3.5:  return 'A'
    if v >= 3.0:  return 'B+'  # 3.0-3.4
    if v >= 2.5:  return 'B'   # 2.5-2.9 — actually MSU uses 4.0=A, 3.5=A-, 3.0=B+, 2.5=B, 2.0=C, 1.5=C-, 1.0=D, 0.0=F
    if v >= 2.0:  return 'C'
    if v >= 1.5:  return 'C-'
    if v >= 1.0:  return 'D'
    if v == 0.0:  return 'F'
    return None

# Exact MSU grade point map
MSU_GRADE_MAP = {
    '4.0': 'A', '3.5': 'A-', '3.0': 'B+', '2.5': 'B', '2.0': 'C+',
    '1.5': 'C', '1.0': 'D', '0.0': 'F'
}

TERM_MAP = {
    'US': 'SUMMER', 'SS': 'SPRING', 'FS': 'FALL', 'WS': 'WINTER',
    'SP': 'SPRING', 'FA': 'FALL', 'SU': 'SUMMER', 'WI': 'WINTER'
}

def parse_term(term):
    """'US20' → (2020, SUMMER), 'FS21' → (2021, FALL)"""
    term = str(term).strip()
    if len(term) >= 4:
        prefix = term[:2].upper()
        year_suffix = term[2:4]
        year = '20' + year_suffix if int(year_suffix) < 50 else '19' + year_suffix
        sem = TERM_MAP.get(prefix, f'TERM_{prefix}')
        return year, sem
    return '0000', 'UNKNOWN'

def main():
    files = list(RAW_DIR.glob('*.csv'))
    all_rows = []
    for f in files:
        print(f"  {f.name}...")
        with open(f, newline='', encoding='utf-8-sig') as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                term     = row.get('TERM','').strip()
                subject  = row.get('SUBJECT','').strip()
                crse     = row.get('CRSE_CODE','').strip()
                instr    = row.get('INSTRUCTOR','').strip()
                grade_raw= row.get('GRADE','').strip()
                count_raw= row.get('GRADE_COUNT','').strip()
                # Map grade
                grade = MSU_GRADE_MAP.get(grade_raw)
                if not grade:
                    grade = numeric_to_letter(grade_raw)
                if not grade:
                    continue
                try:
                    count = int(count_raw)
                except (ValueError, TypeError):
                    count = 0
                if count <= 0:
                    continue
                year, semester = parse_term(term)
                # Format instructor: "WHIMS,JOHN F" → "Whims, John F"
                if ',' in instr:
                    parts = instr.split(',', 1)
                    instr = f"{parts[0].strip().title()}, {parts[1].strip().title()}"
                all_rows.append({
                    'school_id': 'msu', 'year': year, 'semester': semester,
                    'dept': subject, 'course_number': crse, 'instructor': instr,
                    'grade': grade, 'count': count
                })
        print(f"    → {len(all_rows):,} rows")

    print(f"\nTotal: {len(all_rows):,}")
    fieldnames = ['school_id','year','semester','dept','course_number','instructor','grade','count']
    with open(OUTPUT, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Saved → {OUTPUT}")

if __name__ == '__main__':
    main()
