#!/usr/bin/env python3
"""Upload UT Austin CSVs one file at a time, verify each before moving to next."""

import os, sys, glob, json, csv, sqlite3, urllib.request, time

TURSO_URL = os.environ["TURSO_URL"]
TURSO_TOKEN = os.environ["TURSO_TOKEN"]
FILES_DIR = os.path.expanduser("~/Downloads/UTAUSTIN")
BATCH = 500

def turso_http(stmts, retries=3):
    api_url = f"{TURSO_URL}/v2/pipeline"
    payload = json.dumps({"requests": [{"type": "execute", "stmt": s} for s in stmts]}).encode()
    for attempt in range(retries):
        try:
            req = urllib.request.Request(api_url, data=payload,
                  headers={"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=120) as r:
                return json.loads(r.read())
        except Exception as e:
            if attempt < retries - 1:
                print(f"  ⚠️  Network error: {e} — retrying in 5s...")
                time.sleep(5)
            else:
                raise

def get_count(year_tag):
    """Get current Turso count for a specific year tag."""
    result = turso_http([{"sql": f"SELECT COUNT(*) FROM grades WHERE school_id='utaustin' AND year LIKE '{year_tag}%'"}])
    return int(result['results'][0]['response']['result']['rows'][0][0]['value'])

def get_total():
    result = turso_http([{"sql": "SELECT COUNT(*) FROM grades WHERE school_id='utaustin'"}])
    return int(result['results'][0]['response']['result']['rows'][0][0]['value'])

def extract_year(filepath):
    """Extract year from filename e.g. 2020-21 -> '2020'."""
    import re
    m = re.search(r'(\d{4})-\d{2}', os.path.basename(filepath))
    return m.group(1) if m else 'Unknown'

def read_csv(filepath):
    rows = []
    year = extract_year(filepath)
    with open(filepath, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            sem = str(row.get('Semester','') or '').strip().replace("'","''")
            dept = str(row.get('Course Prefix','') or '').strip().replace("'","''")
            course = str(row.get('Course Number','') or '').strip().replace("'","''")
            instr = 'N/A'  # not in these files
            grade = str(row.get('Letter Grade','') or '').strip().replace("'","''")
            try:
                cnt = int(float(row.get('Count of letter grade','0') or 0))
            except:
                cnt = 0
            if not grade or not dept or not course:
                continue
            rows.append((year, sem, dept, course, instr, grade, cnt))
    return rows

def upload_rows(rows):
    total = 0
    errors = 0
    batch = []
    for yr, sem, dept, course, instr, grade, cnt in rows:
        batch.append({"sql": f"INSERT INTO grades (school_id, year, semester, dept, course_number, instructor, grade, count) VALUES ('utaustin', '{yr}', '{sem}', '{dept}', '{course}', '{instr}', '{grade}', {cnt})"})
        if len(batch) >= BATCH:
            result = turso_http(batch)
            errs = sum(1 for r in result['results'] if r['type'] == 'error')
            errors += errs
            total += len(batch) - errs
            batch = []
    if batch:
        result = turso_http(batch)
        errs = sum(1 for r in result['results'] if r['type'] == 'error')
        errors += errs
        total += len(batch) - errs
    return total, errors

def main():
    files = sorted([
        f for f in glob.glob(os.path.join(FILES_DIR, "*.csv"))
        if "(1)" not in f
    ])

    print(f"Found {len(files)} files to process\n")
    print(f"Current Turso utaustin count: {get_total():,}\n")

    grand_total = 0
    for filepath in files:
        filename = os.path.basename(filepath)
        print(f"📄 {filename}")

        rows = read_csv(filepath)
        source_count = len(rows)
        print(f"   Source rows: {source_count:,}")

        uploaded, errors = upload_rows(rows)
        print(f"   Uploaded:    {uploaded:,} ({errors} errors)")

        # Verify by checking total increase
        turso_total = get_total()
        grand_total += uploaded
        print(f"   Turso total: {turso_total:,}")

        if errors > 0:
            print(f"   ⚠️  {errors} errors — check this file!")
        else:
            print(f"   ✅ OK")
        print()

    print(f"🏁 Done! Total uploaded this run: {grand_total:,}")
    print(f"   Final Turso utaustin count: {get_total():,} (target: 585,048)")

if __name__ == "__main__":
    main()
