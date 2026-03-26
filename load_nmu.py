#!/usr/bin/env python3
import openpyxl
import json
import urllib.request
import urllib.error

TURSO_URL = "https://gradeview-agentsolomon.aws-us-east-2.turso.io/v2/pipeline"
TURSO_TOKEN = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3NzQzMTc5NTQsImlkIjoiMDE5ZDFkOTMtZTgwMS03ODU2LThlNjYtNWY1NTgwN2I0Y2E2IiwicmlkIjoiYjBlOTI1YzAtYzM4My00ODcxLTg0NjAtYjg4OGM2NGRhNWQ3In0.uBwikoGHc1YqWDWu8HX9LIIMZ9blRUozr04x0SXBYXtyecQdcWy3RKIxXuDAAlxSHOD5R2F5k2xaQrsPk38cBA"
VALID_GRADES = {'A', 'B', 'C', 'D', 'F'}
SEMESTER_MAP = {'10': 'SPRING', '20': 'SUMMER', '30': 'FALL'}

def turso_request(requests_list):
    payload = json.dumps({"requests": requests_list}).encode()
    req = urllib.request.Request(TURSO_URL, data=payload, method='POST',
        headers={"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def esc(s):
    return str(s).replace("'", "''") if s is not None else ''

# Step 1: DELETE existing NMU data
print("Deleting existing NMU data...")
resp = turso_request([
    {"type": "execute", "stmt": {"sql": "DELETE FROM grades WHERE school_id='nmu'"}},
    {"type": "close"}
])
print("Delete response:", resp)

# Step 2: Parse Excel
print("Loading Excel...")
wb = openpyxl.load_workbook('/Users/solomon/Downloads/NMU/Aikin Ryan FOIA Response Doc Grade Distribution by Term (2020-2025).xlsx')
ws = wb.active

rows = []
cur_term = None
cur_course = None
cur_instructor = None

for i, row in enumerate(ws.iter_rows(values_only=True)):
    if i < 2:  # skip title + header
        continue
    term_code, course, instructor, grade, count = row
    
    # Subtotal rows: course set but grade is None and instructor is None — skip
    # They look like: (None, 'ABA 100', None, None, 46)
    if course is not None and grade is None:
        continue
    
    # Update carry-forward values
    if term_code is not None:
        cur_term = str(term_code).strip()
    if course is not None:
        cur_course = str(course).strip()
        cur_instructor = str(instructor).strip() if instructor else ''
    elif instructor is not None:
        cur_instructor = str(instructor).strip()
    
    if grade is None or count is None:
        continue
    
    grade = str(grade).strip()
    if grade not in VALID_GRADES:
        continue
    
    if cur_term is None or cur_course is None:
        continue
    
    # Parse term
    year = cur_term[:4]
    sem_code = cur_term[4:]
    semester = SEMESTER_MAP.get(sem_code)
    if semester is None:
        continue
    
    # Parse course
    parts = cur_course.split(' ', 1)
    if len(parts) != 2:
        continue
    dept, course_number = parts[0], parts[1]
    
    rows.append((year, semester, dept, course_number, cur_instructor, grade, int(count)))

print(f"Total valid rows: {len(rows)}")

# Step 3: Batch insert
BATCH = 500
total_inserted = 0
for batch_start in range(0, len(rows), BATCH):
    batch = rows[batch_start:batch_start+BATCH]
    requests_list = []
    for (year, semester, dept, course_number, instructor, grade, count) in batch:
        sql = (f"INSERT INTO grades (school_id,year,semester,dept,course_number,instructor,grade,count) "
               f"VALUES ('nmu','{esc(year)}','{esc(semester)}','{esc(dept)}','{esc(course_number)}',"
               f"'{esc(instructor)}','{esc(grade)}',{count})")
        requests_list.append({"type": "execute", "stmt": {"sql": sql}})
    requests_list.append({"type": "close"})
    resp = turso_request(requests_list)
    # Check for errors
    errors = [r for r in resp.get('results', []) if r.get('type') == 'error']
    if errors:
        print(f"ERRORS in batch {batch_start}: {errors[:3]}")
        break
    total_inserted += len(batch)
    if batch_start % 5000 == 0:
        print(f"  Inserted {total_inserted}/{len(rows)}...")

print(f"Done. Inserted {total_inserted} rows.")

# Step 4: Count
resp = turso_request([
    {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM grades WHERE school_id='nmu'"}},
    {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM grades"}},
    {"type": "close"}
])
results = resp.get('results', [])
nmu_count = results[0]['response']['result']['rows'][0][0]['value'] if results else '?'
total_count = results[1]['response']['result']['rows'][0][0]['value'] if len(results) > 1 else '?'
print(f"NMU count: {nmu_count}, Total count: {total_count}")

# Write result
import os
os.makedirs('/Users/solomon/.openclaw/workspace/projects/gradeview', exist_ok=True)
with open('/Users/solomon/.openclaw/workspace/projects/gradeview/nmu_load_result.md', 'w') as f:
    f.write(f"""# NMU Grade Data Load Result

**Date:** 2026-03-25  
**School:** Northern Michigan University (nmu)  
**Rows inserted:** {total_inserted}  
**NMU count in DB:** {nmu_count}  
**Total grades in DB:** {total_count}  
""")
print("Result written.")
