#!/usr/bin/env python3
import openpyxl
import requests
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import ssl

TURSO_URL = os.environ["TURSO_URL"]
TURSO_TOKEN = os.environ["TURSO_TOKEN"]

GRADE_COLS = {3: 'A', 5: 'B', 6: 'C', 7: 'D', 8: 'F', 10: 'F'}  # col_idx -> grade (WF->F)

FILES = [
    os.path.expanduser("~/Downloads/WESTGA/GRADE-DIST-AY2021 by semester.xlsx"),
    os.path.expanduser("~/Downloads/WESTGA/GRADE-DIST-AY2022 by semester.xlsx"),
    os.path.expanduser("~/Downloads/WESTGA/GRADE-DIST-AY2023 by semester.xlsx"),
    os.path.expanduser("~/Downloads/WESTGA/GRADE-DIST-AY2024 by semester.xlsx"),
    os.path.expanduser("~/Downloads/WESTGA/GRADE-DIST-AY2025 by semester.xlsx"),
]

def esc(s):
    return str(s).replace("'", "''")

def parse_semester(sem_str):
    parts = sem_str.strip().split()
    season = parts[0].upper()
    year = parts[1]
    return season, year

def send_batch(sqls, retries=3):
    requests_list = [{"type": "execute", "stmt": {"sql": s}} for s in sqls]
    requests_list.append({"type": "close"})
    payload = {"requests": requests_list}
    headers = {"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"}
    for attempt in range(retries):
        try:
            r = requests.post(TURSO_URL, headers=headers, json=payload, timeout=120)
            if r.status_code != 200:
                print(f"ERROR {r.status_code}: {r.text[:500]}")
                if attempt < retries - 1:
                    import time; time.sleep(5)
                    continue
                return False
            resp = r.json()
            for item in resp.get("results", []):
                if item.get("type") == "error":
                    print(f"SQL ERROR: {item}")
                    return False
            return True
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {e}")
            if attempt < retries - 1:
                import time; time.sleep(10)
    return False

def query(sql):
    payload = {"requests": [{"type": "execute", "stmt": {"sql": sql}}, {"type": "close"}]}
    headers = {"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"}
    r = requests.post(TURSO_URL, headers=headers, json=payload, timeout=30)
    return r.json()

total_inserts = 0
all_sqls = []

for fpath in FILES:
    print(f"Processing {os.path.basename(fpath)}...")
    wb = openpyxl.load_workbook(fpath, read_only=True, data_only=True)
    ws = wb.active
    file_count = 0
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0:
            continue  # skip header
        sem_raw = row[0]
        course_raw = row[1]
        instructor_raw = row[2]
        if not sem_raw or not course_raw or not instructor_raw:
            continue
        try:
            semester, year = parse_semester(str(sem_raw))
        except:
            continue
        course_parts = str(course_raw).strip().split()
        if len(course_parts) < 2:
            continue
        dept = course_parts[0]
        course_number = course_parts[1]
        instructor = esc(str(instructor_raw).strip())
        for col_idx, grade in GRADE_COLS.items():
            val = row[col_idx] if col_idx < len(row) else None
            if val is None or val == 0:
                continue
            try:
                count = int(val)
            except:
                continue
            if count == 0:
                continue
            sql = f"INSERT INTO grades (school_id,year,semester,dept,course_number,instructor,grade,count) VALUES ('westga','{year}','{semester}','{esc(dept)}','{esc(course_number)}','{instructor}','{grade}',{count})"
            all_sqls.append(sql)
            file_count += 1
    print(f"  Rows collected: {file_count}")
    wb.close()

print(f"\nTotal SQL statements: {len(all_sqls)}")
print("Sending in batches of 500...")

BATCH_SIZE = 100
errors = 0
for i in range(0, len(all_sqls), BATCH_SIZE):
    batch = all_sqls[i:i+BATCH_SIZE]
    ok = send_batch(batch)
    if ok:
        total_inserts += len(batch)
    else:
        errors += 1
    if (i // BATCH_SIZE) % 20 == 0:
        print(f"  Sent {i + len(batch)}/{len(all_sqls)}...")

print(f"\nDone. Inserted: {total_inserts}, Batch errors: {errors}")

# Verify
print("\nVerifying...")
r1 = query("SELECT COUNT(*) FROM grades WHERE school_id='westga'")
r2 = query("SELECT COUNT(*) FROM grades")

westga_count = r1["results"][0]["response"]["result"]["rows"][0][0]["value"]
total_count = r2["results"][0]["response"]["result"]["rows"][0][0]["value"]

print(f"WestGA rows: {westga_count}")
print(f"Total DB rows: {total_count}")

# Write result file
os.makedirs("/Users/solomon/.openclaw/workspace/projects/gradeview", exist_ok=True)
with open("/Users/solomon/.openclaw/workspace/projects/gradeview/westga_load_result.md", "w") as f:
    f.write(f"# UWG Grade Data Load Results\n\n")
    f.write(f"- **Rows inserted:** {total_inserts}\n")
    f.write(f"- **Batch errors:** {errors}\n")
    f.write(f"- **WestGA rows in DB:** {westga_count}\n")
    f.write(f"- **Total rows in DB:** {total_count}\n")
    f.write(f"- **Files processed:** {len(FILES)}\n")

print("\nResult file written.")

# Send thank-you email
print("\nSending thank-you email...")
try:
    msg = MIMEMultipart()
    msg['From'] = 'admin@gradeview.app'
    msg['To'] = 'tpearson@westga.edu'
    msg['Subject'] = 'Thank You — Grade Distribution Data'
    body = """Dear Tara,

Thank you so much for fulfilling our public records request and providing the grade distribution data for the University of West Georgia. This information will be used in GradeView, a mobile app that helps students make informed course selection decisions. We truly appreciate your prompt response and look forward to including UWG in our app. We will reach out again each semester for updated data.

Best regards,
Ryan Aikin
GradeView
admin@gradeview.app"""
    msg.attach(MIMEText(body, 'plain'))
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as server:
        server.login('admin@gradeview.app', os.environ["GMAIL_PASSWORD"])
        server.sendmail('admin@gradeview.app', 'tpearson@westga.edu', msg.as_string())
    print("Email sent successfully!")
except Exception as e:
    print(f"Email error: {e}")

print("\nAll done!")
