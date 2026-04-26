"""
UWEC Extractor — UW-Eau Claire Grade Distribution
Input: REC - AD HOC - Grade Distribution Report.xlsx
Columns: Term (Short Descr), Class Subject Code, Class Catalog Number, Section,
          Description, Instructor Name, total students per course, Official Grade, Student Count
Output: uwec_extracted.csv — school_id, Semester, Department, Course Number, Instructor, Grade, Count
"""
import openpyxl, csv, os, sys

INPUT = os.path.expanduser("~/Documents/GradeView/raw_data/uwec/REC - AD HOC - Grade Distribution Report.xlsx")
OUTPUT = os.path.expanduser("~/Documents/GradeView/raw_data/uwec/uwec_extracted.csv")

# Grade normalization — map plus/minus to base letters
GRADE_MAP = {
    'A+': 'A', 'A': 'A', 'A-': 'A',
    'B+': 'B', 'B': 'B', 'B-': 'B',
    'C+': 'C', 'C': 'C', 'C-': 'C',
    'D+': 'D', 'D': 'D', 'D-': 'D',
    'F': 'F', 'W': 'W', 'I': 'I',
    'S': 'S', 'U': 'U', 'P': 'P',
    'CR': 'CR', 'NC': 'NC',
}

def normalize_semester(term):
    """'Fall 2021' -> 'Fall 2021'  (already clean)"""
    return str(term).strip()

def normalize_instructor(name):
    """'Gilberstadt,Sheril L.' -> 'Sheril L. Gilberstadt'"""
    if not name or str(name).strip() == '':
        return 'Unknown'
    name = str(name).strip()
    if ',' in name:
        parts = name.split(',', 1)
        return f"{parts[1].strip()} {parts[0].strip()}"
    return name

wb = openpyxl.load_workbook(INPUT, read_only=True, data_only=True)
ws = wb.active

rows_written = 0
skipped = 0

with open(OUTPUT, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['school_id', 'Semester', 'Department', 'Course Number', 'Instructor', 'Grade', 'Count'])

    for row in ws.iter_rows(min_row=2, values_only=True):
        term, subj, catalog, section, desc, instructor, total, grade, count = row[:9]

        if not all([term, subj, catalog, grade, count]):
            skipped += 1
            continue

        count = int(float(count)) if count else 0
        if count < 1:
            skipped += 1
            continue

        grade_str = str(grade).strip().upper()
        normalized_grade = GRADE_MAP.get(grade_str, grade_str)

        writer.writerow([
            'uwec',
            normalize_semester(term),
            str(subj).strip().upper(),
            str(catalog).strip(),
            normalize_instructor(instructor),
            normalized_grade,
            count
        ])
        rows_written += 1

wb.close()
print(f"✅ UWEC extraction complete")
print(f"   Rows written: {rows_written:,}")
print(f"   Rows skipped: {skipped:,}")
print(f"   Output: {OUTPUT}")
