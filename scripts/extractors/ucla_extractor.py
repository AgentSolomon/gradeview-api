"""
UCLA Extractor — University of California, Los Angeles
Input:  ~/Documents/GradeView/raw_data/ucla/*.xlsx  (one file per semester or combined)
        Files named: "Spring 2025.xlsx", "Fall 2024.xlsx", "Spring 2022 and Fall 2022.xlsx", etc.
        Note: Fall 2020.PDF is skipped (PDF format, no xlsx available yet).

Source columns (per UCLA FOIA response):
    enrl_term_cd  | subj_area_name | disp_catlg_no | disp_sect_no | grd_cd | num_grd |
    enrl_tot      | instr_nm       | instr_func_cd  | crs_long_ttl | subj_area_cd | crs_catlg_no | sect_no

Output: ucla_extracted.csv
    school_id, Semester, Department, Course Number, Instructor, Grade, Count
"""

import openpyxl, csv, os, re, glob

RAW_DIR  = os.path.expanduser("~/Documents/GradeView/raw_data/ucla/")
OUTPUT   = os.path.join(RAW_DIR, "ucla_extracted.csv")

# ── Grade filter ─────────────────────────────────────────────────────────────
# Keep standard letter grades + pass/fail/withdraw equivalents.
# Drop: DR (dropped), IP (in progress), NR (not reported), R (repeat), LI, blank
KEEP_GRADES = {
    'A+', 'A', 'A-',
    'B+', 'B', 'B-',
    'C+', 'C', 'C-',
    'D+', 'D', 'D-',
    'F',
    'P', 'NP',
    'S', 'U',
    'W', 'I', 'NC',
}

# ── Semester code → human label ───────────────────────────────────────────────
# UCLA uses codes like "25S" = Spring 2025, "24F" = Fall 2024
# Combined files (Spring 2022 and Fall 2022) are split by enrl_term_cd in the data.
TERM_CODE_MAP = {
    '20S': 'Spring 2020', '20F': 'Fall 2020',
    '21W': 'Winter 2021', '21S': 'Spring 2021', '21F': 'Fall 2021',
    '22W': 'Winter 2022', '22S': 'Spring 2022', '22F': 'Fall 2022',
    '23W': 'Winter 2023', '23S': 'Spring 2023', '23F': 'Fall 2023',
    '24W': 'Winter 2024', '24S': 'Spring 2024', '24F': 'Fall 2024',
    '25W': 'Winter 2025', '25S': 'Spring 2025', '25F': 'Fall 2025',
}

def decode_term(code):
    """'25S' → 'Spring 2025'"""
    if not code:
        return 'Unknown'
    code = str(code).strip().upper()
    if code in TERM_CODE_MAP:
        return TERM_CODE_MAP[code]
    # Fallback: try to parse manually
    m = re.match(r'(\d{2})([WSFQ])', code)
    if m:
        yr, s = m.groups()
        season = {'W': 'Winter', 'S': 'Spring', 'F': 'Fall', 'Q': 'Summer'}.get(s, s)
        return f"{season} 20{yr}"
    return code

def flip_name(name):
    """'FU, RONG' → 'Rong Fu'"""
    if not name or str(name).strip() in ('', 'STAFF', 'Staff', 'TBA'):
        return 'Unknown'
    name = str(name).strip()
    if ',' in name:
        last, first = name.split(',', 1)
        full = f"{first.strip()} {last.strip()}"
    else:
        full = name
    # Title case
    return ' '.join(w.capitalize() for w in full.split())

def clean_course(catalog_no):
    return str(catalog_no).strip() if catalog_no else ''

def clean_dept(subj_area_cd):
    return str(subj_area_cd).strip() if subj_area_cd else ''

# ── Find all xlsx files (skip the _extracted.csv stub copies) ────────────────
xlsx_files = sorted([
    f for f in glob.glob(os.path.join(RAW_DIR, "*.xlsx"))
    if ' ' in os.path.basename(f)   # use files with spaces (originals, not our curl stubs)
])

print(f"Found {len(xlsx_files)} xlsx files:")
for f in xlsx_files:
    print(f"  {os.path.basename(f)}")
print()

rows_written = 0
rows_skipped = 0
semesters_seen = set()

with open(OUTPUT, 'w', newline='', encoding='utf-8') as out_f:
    writer = csv.writer(out_f)
    writer.writerow(['school_id', 'Semester', 'Department', 'Course Number', 'Instructor', 'Grade', 'Count'])

    for xlsx_path in xlsx_files:
        fname = os.path.basename(xlsx_path)
        print(f"Processing: {fname} ...", end=' ', flush=True)

        wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
        ws = wb.active
        file_rows = 0
        file_skipped = 0

        for row in ws.iter_rows(min_row=2, values_only=True):
            # Unpack columns
            (enrl_term_cd, subj_area_name, disp_catlg_no, disp_sect_no,
             grd_cd, num_grd, enrl_tot, instr_nm, instr_func_cd,
             crs_long_ttl, subj_area_cd, crs_catlg_no, sect_no) = (list(row) + [None]*13)[:13]

            # Skip rows without essential fields
            if not all([enrl_term_cd, grd_cd, num_grd]):
                file_skipped += 1
                continue

            grade = str(grd_cd).strip().upper()
            if grade not in KEEP_GRADES:
                file_skipped += 1
                continue

            try:
                count = int(float(str(num_grd).strip()))
            except (ValueError, TypeError):
                file_skipped += 1
                continue

            if count < 1:
                file_skipped += 1
                continue

            semester  = decode_term(enrl_term_cd)
            dept      = clean_dept(subj_area_cd) or clean_dept(subj_area_name)
            course_no = clean_course(crs_catlg_no) or clean_course(disp_catlg_no)
            instructor = flip_name(instr_nm)

            semesters_seen.add(semester)
            writer.writerow(['ucla', semester, dept, course_no, instructor, grade, count])
            file_rows += 1

        wb.close()
        print(f"{file_rows:,} rows written, {file_skipped:,} skipped")
        rows_written += file_rows
        rows_skipped += file_skipped

print()
print("=" * 55)
print(f"TOTAL ROWS WRITTEN : {rows_written:,}")
print(f"TOTAL ROWS SKIPPED : {rows_skipped:,}")
print(f"SEMESTERS          : {sorted(semesters_seen)}")
print(f"OUTPUT             : {OUTPUT}")
print("=" * 55)
