#!/usr/bin/env python3.11
"""
Ohio State University grade distribution extractor.
Format: Single sheet 'CLASS', one row per class section.
Columns: Term Description, Term Code, Course Campus, Subject, Catalog Number,
         Class Number, Course Title, Total, A, A-, B+, B, B-, C+, C, C-, D+, D, E
Note: OSU uses 'E' for failing grade (not F).
"""
import sys
import os
import openpyxl
from collections import defaultdict
import glob

RAW_DIR = os.path.expanduser("~/Documents/GradeView/raw_data/osu/")

GRADE_MAP = {
    'A': 'A', 'A-': 'A-',
    'B+': 'B+', 'B': 'B', 'B-': 'B-',
    'C+': 'C+', 'C': 'C', 'C-': 'C-',
    'D+': 'D+', 'D': 'D',
    'E': 'F'  # OSU uses E for fail
}

def parse_semester(term_desc):
    """Convert '2020 Spr' -> ('Spring', '2020')"""
    parts = term_desc.strip().split()
    if len(parts) != 2:
        return None, None
    year = parts[0]
    term_map = {
        'Spr': 'Spring', 'Sum': 'Summer', 'Aut': 'Fall', 'Autmn': 'Fall',
        'Win': 'Winter', 'Fall': 'Fall', 'Spring': 'Spring',
        'Summer': 'Summer'
    }
    season = term_map.get(parts[1], parts[1])
    return season, year

def extract_file(filepath):
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb['CLASS']
    
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []
    
    # Get headers from row 0
    headers = [str(h).strip() if h else '' for h in rows[0]]
    
    # Find grade column indices
    grade_cols = {}
    for i, h in enumerate(headers):
        if h in GRADE_MAP:
            grade_cols[i] = GRADE_MAP[h]
    
    # Find other column indices
    def col(name):
        try: return headers.index(name)
        except ValueError: return None
    
    term_col = col('Term Description')
    subj_col = col('Subject')
    cat_col = col('Catalog Number')
    title_col = col('Course Title')
    
    # Aggregate by (semester, year, subject, catalog_number, title)
    agg = defaultdict(lambda: defaultdict(int))
    meta = {}
    
    for row in rows[1:]:
        if not row or row[term_col] is None:
            continue
        
        term_desc = str(row[term_col]).strip()
        season, year = parse_semester(term_desc)
        if not season or not year:
            continue
        
        subject = str(row[subj_col]).strip() if row[subj_col] else ''
        catalog = str(row[cat_col]).strip() if row[cat_col] else ''
        title = str(row[title_col]).strip() if row[title_col] else ''
        
        key = (season, year, subject, catalog)
        if key not in meta:
            meta[key] = title
        
        for col_idx, grade_letter in grade_cols.items():
            val = row[col_idx]
            if val and isinstance(val, (int, float)) and val > 0:
                agg[key][grade_letter] += int(val)
    
    records = []
    for key, grades in agg.items():
        season, year, subject, catalog = key
        title = meta.get(key, '')
        for grade, count in grades.items():
            records.append({
                'semester': season,
                'year': year,
                'subject': subject,
                'catalog_number': catalog,
                'course_title': title,
                'grade': grade,
                'count': count
            })
    
    wb.close()
    return records

def extract_all():
    files = sorted(glob.glob(os.path.join(RAW_DIR, "osu_*.xlsx")))
    all_records = []
    for f in files:
        print(f"  Processing {os.path.basename(f)}...")
        records = extract_file(f)
        print(f"    -> {len(records)} grade records")
        all_records.extend(records)
    return all_records

if __name__ == '__main__':
    print("Extracting Ohio State University grade data...")
    records = extract_all()
    print(f"\nTotal records: {len(records)}")
    
    # Show sample
    print("\nSample records:")
    for r in records[:5]:
        print(f"  {r['semester']} {r['year']} | {r['subject']} {r['catalog_number']} | {r['grade']}: {r['count']}")
    
    # Show semester range
    semesters = sorted(set(f"{r['semester']} {r['year']}" for r in records))
    print(f"\nSemesters covered ({len(semesters)}):")
    for s in semesters:
        print(f"  {s}")
