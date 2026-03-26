#!/usr/bin/env python3
"""
GradeView - TAMU Grade Data Fetcher
Downloads and parses TAMU Registrar grade distribution PDFs.
Outputs clean CSV and JSON — no MySQL required.
"""

import os
import re
import csv
import json
import requests
import bs4
import PyPDF2

PDF_ROOT_LINK = 'https://web-as.tamu.edu/GradeReports/'
PDF_BASE_LINK = 'https://web-as.tamu.edu/GradeReports/PDFReports/{0}/grd{0}{1}.pdf'
PDF_SAVE_DIR  = 'tamu_pdfs/'
CSV_OUTPUT    = 'tamu_grades.csv'
JSON_OUTPUT   = 'tamu_grades.json'

BLACKLIST = {'AE','AP','GV','QT','UT','DN_PROF','SL_PROF','MD_PROF','CP_PROF','VM_PROF',
             'DT_PROF','MN_PROF','PM_PROF','VT_PROF'}
SEMESTER_MAP = {'1': 'SPRING', '2': 'SUMMER', '3': 'FALL'}

FIELDNAMES = [
    'year','semester','college','department','course','section',
    'professor','A','B','C','D','F','I','S','U','Q','X','avg_gpa'
]

# Pattern: DEPT-COURSE-SECTION followed by grade counts on subsequent lines
# Line format example: "AERO-201-500    12"
COURSE_LINE = re.compile(r'^([A-Z]{2,6})-(\d{3,4})-(\S+)\s+(\d+)$')


def scrape_metadata():
    print('Fetching available years and colleges from TAMU Registrar...')
    soup = bs4.BeautifulSoup(requests.get(PDF_ROOT_LINK, timeout=30).text, 'html.parser')
    years = [int(o['value']) for o in soup.select('#ctl00_plcMain_lstGradYear > option')]
    colleges = [o['value'] for o in soup.select('#ctl00_plcMain_lstGradCollege > option')]
    colleges = list(set(colleges) - BLACKLIST)
    return sorted(years), colleges


def download_pdf(year, semester, college):
    key = f'{year}{semester}'
    url = PDF_BASE_LINK.format(key, college)
    filename = f'grd{key}{college}.pdf'
    filepath = os.path.join(PDF_SAVE_DIR, filename)

    if os.path.exists(filepath):
        return filepath

    os.makedirs(PDF_SAVE_DIR, exist_ok=True)
    resp = requests.get(url, timeout=60)
    if resp.status_code == 200 and len(resp.content) > 1000:
        with open(filepath, 'wb') as f:
            f.write(resp.content)
        return filepath
    return None


def last_int(line):
    """Return the last integer found in a line, or None."""
    nums = re.findall(r'\d+', line)
    return int(nums[-1]) if nums else None

def last_float(line):
    """Return the last float found in a line, or None."""
    nums = re.findall(r'\d+\.\d+', line)
    return float(nums[-1]) if nums else None

def parse_pdf(filepath, year, semester_num, college):
    """
    PDF line format (one course per 6-line block):
      Line 0: DEPT-COURSE-SECTION    <A count>
      Line 1: <A pct>%   <B count>
      Line 2: <B pct>%   <C count>
      Line 3: <C pct>%   <D count>
      Line 4: <D pct>%      <F count>
      Line 5: <F pct>%   <total>  <GPA>  <I> <S> <U> <Q> <X>  <total>  <PROF NAME>
    """
    semester = SEMESTER_MAP[str(semester_num)]
    rows = []
    try:
        with open(filepath, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            for page in reader.pages:
                lines = [l.strip() for l in page.extract_text().split('\n')]
                i = 0
                while i < len(lines) - 5:
                    m = COURSE_LINE.match(lines[i])
                    if m:
                        dept, course, section, _ = m.groups()
                        try:
                            A   = last_int(lines[i])
                            B   = last_int(lines[i+1])
                            C   = last_int(lines[i+2])
                            D   = last_int(lines[i+3])
                            F   = last_int(lines[i+4])
                            # line5: "  0.00%   422.880    0    0    0    0    0     42 PROF"
                            # "422.880" = total(42) + GPA(2.880) concatenated — split via GPA pattern
                            line5 = lines[i+5]
                            gpa_match = re.search(r'([0-4]\.\d{3})', line5)
                            gpa = float(gpa_match.group(1)) if gpa_match else 0.0

                            # Everything after the GPA: I S U Q X total PROF
                            after_gpa = line5[gpa_match.end():] if gpa_match else ''
                            after_ints = [int(x) for x in re.findall(r'\b(\d+)\b', after_gpa)]
                            I = after_ints[0] if len(after_ints) > 0 else 0
                            S = after_ints[1] if len(after_ints) > 1 else 0
                            U = after_ints[2] if len(after_ints) > 2 else 0
                            Q = after_ints[3] if len(after_ints) > 3 else 0
                            X = after_ints[4] if len(after_ints) > 4 else 0

                            # Professor name: trailing uppercase words on line 5
                            prof_match = re.search(r'([A-Z][A-Z ,\.\']+)$', lines[i+5])
                            prof = prof_match.group(1).strip() if prof_match else ''

                            if None not in (A, B, C, D, F):
                                rows.append({
                                    'year': year, 'semester': semester, 'college': college,
                                    'department': dept, 'course': course, 'section': section,
                                    'professor': prof,
                                    'A': A, 'B': B, 'C': C, 'D': D, 'F': F,
                                    'I': I, 'S': S, 'U': U, 'Q': Q, 'X': X,
                                    'avg_gpa': gpa
                                })
                        except (IndexError, ValueError):
                            pass
                        i += 6
                    else:
                        i += 1
    except Exception as e:
        print(f'\n  Error parsing {os.path.basename(filepath)}: {e}')
    return rows


def main(start_year=None, end_year=None):
    years, colleges = scrape_metadata()
    print(f'Found {len(years)} years, {len(colleges)} colleges')

    if start_year:
        years = [y for y in years if y >= start_year]
    if end_year:
        years = [y for y in years if y <= end_year]

    print(f'Processing years: {years}')

    all_rows = []
    total = len(years) * 3 * len(colleges)
    done = 0

    for year in years:
        for sem_num in [1, 2, 3]:
            for college in sorted(colleges):
                done += 1
                sem_name = SEMESTER_MAP[str(sem_num)]
                print(f'[{done}/{total}] {year} {sem_name} {college}        ', end='\r')

                filepath = download_pdf(str(year), str(sem_num), college)
                if filepath is None:
                    continue

                rows = parse_pdf(filepath, year, sem_num, college)
                all_rows.extend(rows)

    print(f'\nDone. Total records: {len(all_rows)}')

    with open(CSV_OUTPUT, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(all_rows)
    print(f'CSV written to: {CSV_OUTPUT}')

    with open(JSON_OUTPUT, 'w') as f:
        json.dump(all_rows, f)
    print(f'JSON written to: {JSON_OUTPUT}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--start-year', type=int, default=None)
    parser.add_argument('--end-year', type=int, default=None)
    args = parser.parse_args()
    main(args.start_year, args.end_year)
