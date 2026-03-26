#!/usr/bin/env python3
"""
GradeView - UT Austin Grade Data Fetcher
Uses the DereC4 GitHub dataset (Fall 2022) — long format, one row per grade letter.
"""

import csv
import io
import os
import re
import urllib.request
from collections import defaultdict

OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "utaustin_grades.csv")

FIELDNAMES = ['year', 'semester', 'college', 'department', 'course', 'section',
              'professor', 'A', 'B', 'C', 'D', 'F', 'I', 'S', 'U', 'Q', 'X', 'avg_gpa']

# Map UT letter grades to A/B/C/D/F buckets
GRADE_MAP = {
    'A+': 'A', 'A': 'A', 'A-': 'A',
    'B+': 'B', 'B': 'B', 'B-': 'B',
    'C+': 'C', 'C': 'C', 'C-': 'C',
    'D+': 'D', 'D': 'D', 'D-': 'D',
    'F':  'F',
}

SEMESTER_MAP = {
    'fall':   'FALL',
    'spring': 'SPRING',
    'summer': 'SUMMER',
}

def calc_gpa(a, b, c, d, f):
    total = a + b + c + d + f
    if total == 0:
        return 0.0
    return round((a * 4.0 + b * 3.0 + c * 2.0 + d * 1.0) / total, 3)

def parse_semester(sem_str):
    """'Fall 2022' -> ('FALL', 2022)"""
    sem_str = sem_str.strip()
    year_match = re.search(r'(20\d\d)', sem_str)
    year = int(year_match.group(1)) if year_match else 0
    semester = 'UNKNOWN'
    for k, v in SEMESTER_MAP.items():
        if k in sem_str.lower():
            semester = v
            break
    return semester, year

def fetch_and_convert():
    url = "https://raw.githubusercontent.com/DereC4/ut-grade-distribution-viewer/master/data/Fall2022.csv"
    print(f"Downloading UT Austin data from GitHub...")
    with urllib.request.urlopen(url, timeout=60) as resp:
        content = resp.read().decode('utf-8-sig')
    print(f"Downloaded {len(content):,} bytes")

    reader = csv.DictReader(io.StringIO(content))

    # Group by (year, semester, dept, course, section)
    # Key: (year, semester, dept, course, section)
    sections = defaultdict(lambda: {'A': 0, 'B': 0, 'C': 0, 'D': 0, 'F': 0,
                                     'college': '', 'professor': ''})

    skipped = 0
    for row in reader:
        letter = row.get('Letter Grade', '').strip()
        bucket = GRADE_MAP.get(letter)
        if bucket is None:
            skipped += 1
            continue

        sem_str = row.get('Semester', '')
        semester, year = parse_semester(sem_str)

        dept = row.get('Course Prefix', '').strip().upper()
        course = row.get('Course Number', '').strip().lstrip('0 ').strip()
        section = row.get('Section Number', '').strip()
        college = row.get('Department/Program', '').strip()

        count = int(row.get('Count of letter grade', 0) or 0)
        key = (year, semester, dept, course, section)
        sections[key][bucket] += count
        sections[key]['college'] = college

    print(f"Skipped {skipped:,} non-ABCDF rows (Other, CR, W, etc.)")
    print(f"Found {len(sections):,} unique sections")

    rows = []
    for (year, semester, dept, course, section), grades in sections.items():
        A, B, C, D, F = grades['A'], grades['B'], grades['C'], grades['D'], grades['F']
        if A + B + C + D + F == 0:
            continue
        rows.append({
            'year': year, 'semester': semester,
            'college': grades['college'],
            'department': dept, 'course': course, 'section': section,
            'professor': '',
            'A': A, 'B': B, 'C': C, 'D': D, 'F': F,
            'I': 0, 'S': 0, 'U': 0, 'Q': 0, 'X': 0,
            'avg_gpa': calc_gpa(A, B, C, D, F),
        })

    rows.sort(key=lambda r: (r['department'], r['course']))

    with open(OUTPUT_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Saved {len(rows):,} sections to {OUTPUT_CSV}")
    size_mb = os.path.getsize(OUTPUT_CSV) / 1024 / 1024
    print(f"   File size: {size_mb:.1f} MB")

if __name__ == '__main__':
    fetch_and_convert()
