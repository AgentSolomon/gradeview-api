#!/usr/bin/env python3
import csv
import glob
import json
import urllib.request
import urllib.error
import os

TURSO_URL = os.environ["TURSO_URL"]
TOKEN = os.environ["TURSO_TOKEN"]

GRADE_MAP = {
    'A+': 'A', 'A': 'A', 'A-': 'A',
    'B+': 'B', 'B': 'B', 'B-': 'B',
    'C+': 'C', 'C': 'C', 'C-': 'C',
    'D+': 'D', 'D': 'D', 'D-': 'D',
    'F': 'F', 'WF': 'F',
}

def turso_request(requests_list):
    payload = json.dumps({"requests": requests_list}).encode('utf-8')
    req = urllib.request.Request(
        TURSO_URL,
        data=payload,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json"
        },
        method="POST"
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())

def execute_sql(sql, params=None):
    stmt = {"sql": sql}
    if params:
        stmt["args"] = params
    result = turso_request([{"type": "execute", "stmt": stmt}, {"type": "close"}])
    return result

def batch_insert(rows):
    requests = []
    for row in rows:
        requests.append({
            "type": "execute",
            "stmt": {
                "sql": "INSERT INTO grades (school_id, year, semester, dept, course_number, instructor, grade, count) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                "args": [
                    {"type": "text", "value": row[0]},
                    {"type": "text", "value": row[1]},
                    {"type": "text", "value": row[2]},
                    {"type": "text", "value": row[3]},
                    {"type": "text", "value": row[4]},
                    {"type": "text", "value": row[5]},
                    {"type": "text", "value": row[6]},
                    {"type": "integer", "value": str(row[7])},
                ]
            }
        })
    requests.append({"type": "close"})
    return turso_request(requests)

# Delete existing rows
print("Deleting existing utaustin rows...")
execute_sql("DELETE FROM grades WHERE school_id='utaustin'")
print("Deleted.")

files = sorted(glob.glob(os.path.expanduser("~/Downloads/UTAUSTIN/*.csv")))
files = [f for f in files if "(1)" not in f]
print(f"Found {len(files)} files")

all_rows = []
total_skipped = 0

for fpath in files:
    fname = os.path.basename(fpath)
    file_rows = 0
    file_skipped = 0
    with open(fpath, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            semester_full = row['Semester'].strip()
            parts = semester_full.split()
            if len(parts) < 2:
                continue
            semester = parts[0].upper()
            year = parts[1]

            letter = row['Letter Grade'].strip()
            mapped = GRADE_MAP.get(letter)
            if not mapped:
                file_skipped += 1
                total_skipped += 1
                continue

            dept = row['Course Prefix'].strip().replace(' ', '')
            course_number = row['Course Number'].strip().replace(' ', '')
            try:
                count = int(row['Count of letter grade'].strip())
            except:
                file_skipped += 1
                total_skipped += 1
                continue

            all_rows.append(('utaustin', year, semester, dept, course_number, 'N/A', mapped, count))
            file_rows += 1
    print(f"  {fname}: {file_rows} rows, {file_skipped} skipped")

print(f"\nTotal rows to insert: {len(all_rows)}, total skipped: {total_skipped}")

# Batch insert 500 at a time
batch_size = 200
total_inserted = 0
for i in range(0, len(all_rows), batch_size):
    batch = all_rows[i:i+batch_size]
    result = batch_insert(batch)
    # Check for errors
    errors = [r for r in result.get('results', []) if r.get('type') == 'error']
    if errors:
        print(f"ERROR in batch {i//batch_size}: {errors[:2]}")
        break
    else:
        total_inserted += len(batch)
    if (i // batch_size) % 50 == 0:
        print(f"  Inserted {total_inserted}/{len(all_rows)}...")

print(f"\nDone! Inserted {total_inserted} rows.")

# Verify
print("\nVerifying...")
r1 = execute_sql("SELECT COUNT(*) FROM grades WHERE school_id='utaustin'")
utaustin_count = r1['results'][0]['response']['result']['rows'][0][0]['value']
print(f"utaustin count: {utaustin_count}")

r2 = execute_sql("SELECT COUNT(*) FROM grades")
total_count = r2['results'][0]['response']['result']['rows'][0][0]['value']
print(f"Total DB count: {total_count}")

# Write results
result_md = f"""# UT Austin Load Results

## Summary
- Files processed: {len(files)}
- Rows inserted: {total_inserted}
- Rows skipped (unmapped grades): {total_skipped}

## Verification
- `SELECT COUNT(*) FROM grades WHERE school_id='utaustin'`: **{utaustin_count}**
- `SELECT COUNT(*) FROM grades` (entire DB): **{total_count}**
"""

os.makedirs("/Users/solomon/.openclaw/workspace/projects/gradeview", exist_ok=True)
with open("/Users/solomon/.openclaw/workspace/projects/gradeview/utaustin_load_result.md", "w") as f:
    f.write(result_md)

print("Results written to utaustin_load_result.md")
