"""
Lamar University Extractor
Input: Fall2019-Fall2025.csv
Columns: Faculty_First_Name, Faculty_Last_Name, Department, Term, Subject_Abbreviation,
         Course_Number, Section_Number, Title, Total_Enrollment,
         A_Count, B_Count, C_Count, D_Count, F_Count, I_Count, S_Count, U_Count,
         Q_Count, QL_Count, W_Count, Audit_Count
Output: lamar_extracted.csv — school_id, Semester, Department, Course Number, Instructor, Grade, Count
"""
import csv, os

INPUT = os.path.expanduser("~/Documents/GradeView/raw_data/lamar/Fall2019-Fall2025.csv")
OUTPUT = os.path.expanduser("~/Documents/GradeView/raw_data/lamar/lamar_extracted.csv")

GRADE_COLS = ['A_Count','B_Count','C_Count','D_Count','F_Count','W_Count','I_Count','S_Count','U_Count']
GRADE_LABELS = ['A','B','C','D','F','W','I','S','U']

def normalize_semester(term):
    """'Fall 2024' -> 'Fall 2024'  (already clean)"""
    return term.strip()

rows_written = 0
skipped = 0

with open(INPUT, newline='', encoding='utf-8-sig') as fin, \
     open(OUTPUT, 'w', newline='') as fout:

    reader = csv.DictReader(fin)
    writer = csv.writer(fout)
    writer.writerow(['school_id', 'Semester', 'Department', 'Course Number', 'Instructor', 'Grade', 'Count'])

    for row in reader:
        first = row.get('Faculty_First_Name','').strip()
        last  = row.get('Faculty_Last_Name','').strip()
        instructor = f"{first} {last}".strip() or 'Unknown'

        term    = normalize_semester(row.get('Term',''))
        subj    = row.get('Subject_Abbreviation','').strip().upper()
        course  = row.get('Course_Number','').strip()
        dept    = row.get('Department','').strip()

        if not all([term, subj, course]):
            skipped += 1
            continue

        any_written = False
        for col, label in zip(GRADE_COLS, GRADE_LABELS):
            try:
                count = int(float(row.get(col, 0) or 0))
            except ValueError:
                count = 0
            if count < 1:
                continue
            writer.writerow(['lamar', term, subj, course, instructor, label, count])
            rows_written += 1
            any_written = True

        if not any_written:
            skipped += 1

print(f"✅ Lamar extraction complete")
print(f"   Rows written: {rows_written:,}")
print(f"   Rows skipped: {skipped:,}")
print(f"   Output: {OUTPUT}")
