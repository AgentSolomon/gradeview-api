#!/usr/bin/env python3
"""
GradeView API v0.3.0 — FastAPI + Turso cloud database
"""

import os
import requests
from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="GradeView API", version="0.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

TURSO_URL = os.environ.get("TURSO_URL", "https://gradeview-agentsolomon.aws-us-east-2.turso.io/v2/pipeline")
TURSO_TOKEN = os.environ.get("TURSO_TOKEN", "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicnciLCJpYXQiOjE3NzQzMTc5NTQsImlkIjoiMDE5ZDFkOTMtZTgwMS03ODU2LThlNjYtNWY1NTgwN2I0Y2E2IiwicmlkIjoiYjBlOTI1YzAtYzM4My00ODcxLTg0NjAtYjg4OGM2NGRhNWQ3In0.uBwikoGHc1YqWDWu8HX9LIIMZ9blRUozr04x0SXBYXtyecQdcWy3RKIxXuDAAlxSHOD5R2F5k2xaQrsPk38cBA")

def db(sql: str, args: list = None):
    """Execute a single SQL query against Turso and return rows as dicts."""
    stmt = {"sql": sql}
    if args:
        stmt["args"] = [
            {"type": "integer", "value": str(a)} if isinstance(a, int)
            else {"type": "text", "value": str(a) if a is not None else ""}
            for a in args
        ]
    r = requests.post(
        TURSO_URL,
        headers={"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"},
        json={"requests": [{"type": "execute", "stmt": stmt}, {"type": "close"}]},
        timeout=15
    )
    result = r.json()["results"][0]
    if result.get("type") == "error":
        raise HTTPException(status_code=500, detail=result.get("error", "DB error"))
    res = result["response"]["result"]
    cols = [c["name"] for c in res["cols"]]
    return [dict(zip(cols, [v["value"] for v in row])) for row in res["rows"]]


def grade_summary(rows: list) -> dict:
    """Compute grade summary from a list of DB rows."""
    A = sum(int(r["count"]) for r in rows if r["grade"] == "A")
    B = sum(int(r["count"]) for r in rows if r["grade"] == "B")
    C = sum(int(r["count"]) for r in rows if r["grade"] == "C")
    D = sum(int(r["count"]) for r in rows if r["grade"] == "D")
    F = sum(int(r["count"]) for r in rows if r["grade"] == "F")
    total = A + B + C + D + F
    avg_gpa = round(
        (A * 4.0 + B * 3.0 + C * 2.0 + D * 1.0) / total, 3
    ) if total else 0.0
    return {
        "A": A, "B": B, "C": C, "D": D, "F": F, "Q": 0,
        "total": total,
        "avg_gpa": avg_gpa,
        "pct_A": round(A / total * 100, 1) if total else 0,
        "pct_B": round(B / total * 100, 1) if total else 0,
        "pct_C": round(C / total * 100, 1) if total else 0,
        "pct_D": round(D / total * 100, 1) if total else 0,
        "pct_F": round(F / total * 100, 1) if total else 0,
    }


# ── Routes ──────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    schools = db("SELECT id FROM schools")
    return {"status": "ok", "version": "0.3.0", "schools": [s["id"] for s in schools]}


@app.get("/schools")
def list_schools():
    schools = db("SELECT id, name, short_name, color, state FROM schools")
    return {"schools": [{"id": s["id"], "name": s["name"], "short": s["short_name"], "color": s["color"], "state": s["state"] or ""} for s in schools]}

@app.get("/states")
def list_states():
    schools = db("SELECT id, name, short_name, color, state FROM schools")
    states: dict = {}
    for s in schools:
        st = s["state"] or "Other"
        if st not in states:
            states[st] = []
        states[st].append({"id": s["id"], "name": s["name"], "short": s["short_name"], "color": s["color"], "state": st})
    result = [{"state": k, "schools": v} for k, v in sorted(states.items())]
    return {"states": result}


@app.get("/departments")
def get_departments(school: str = Query("tamu")):
    rows = db(
        "SELECT DISTINCT dept FROM grades WHERE school_id = ? AND dept != '' ORDER BY dept",
        [school]
    )
    return {"school": school, "departments": [r["dept"] for r in rows]}


@app.get("/courses")
def get_courses(department: str = Query(...), school: str = Query("tamu")):
    dept = department.upper()
    rows = db(
        "SELECT DISTINCT course_number FROM grades WHERE school_id = ? AND dept = ? ORDER BY course_number",
        [school, dept]
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Department '{dept}' not found.")
    return {"school": school, "department": dept, "courses": [r["course_number"] for r in rows]}


@app.get("/course")
def get_course(
    department: str = Query(...),
    course: str = Query(...),
    school: str = Query("tamu"),
    semester: Optional[str] = Query(None),
    year: Optional[str] = Query(None),
):
    dept = department.upper()
    sql = "SELECT instructor, year, semester, grade, count FROM grades WHERE school_id = ? AND dept = ? AND course_number = ?"
    args = [school, dept, course]
    if semester:
        sql += " AND semester = ?"
        args.append(semester.upper())
    if year:
        sql += " AND year = ?"
        args.append(str(year))

    rows = db(sql, args)
    if not rows:
        raise HTTPException(status_code=404, detail="No data found for that course.")

    # Group by professor
    profs: dict = {}
    for r in rows:
        p = r["instructor"] or "UNKNOWN"
        profs.setdefault(p, []).append(r)

    prof_summaries = []
    for prof, prof_rows in sorted(profs.items()):
        summary = grade_summary(prof_rows)
        summary["professor"] = prof
        summary["sections"] = len(set((r["year"], r["semester"]) for r in prof_rows))
        prof_summaries.append(summary)
    prof_summaries.sort(key=lambda x: x["total"], reverse=True)

    years = sorted(set(int(r["year"]) for r in rows if r["year"] and r["year"].isdigit()))

    return {
        "school": school,
        "department": dept,
        "course": course,
        "overall": grade_summary(rows),
        "by_professor": prof_summaries,
        "semesters_available": sorted(set(r["semester"] for r in rows)),
        "years_available": years,
    }


@app.get("/professor")
def get_professor(
    name: str = Query(...),
    school: str = Query("tamu"),
    department: Optional[str] = Query(None),
):
    sql = "SELECT instructor, dept, course_number, year, semester, grade, count FROM grades WHERE school_id = ? AND instructor LIKE ?"
    args = [school, f"%{name.upper()}%"]
    if department:
        sql += " AND dept = ?"
        args.append(department.upper())

    rows = db(sql, args)
    if not rows:
        raise HTTPException(status_code=404, detail="Professor not found.")

    # Group by course
    courses: dict = {}
    for r in rows:
        key = f"{r['dept']}-{r['course_number']}"
        courses.setdefault(key, []).append(r)

    course_summaries = []
    for key, course_rows in sorted(courses.items()):
        summary = grade_summary(course_rows)
        summary["course"] = key
        summary["sections"] = len(set((r["year"], r["semester"]) for r in course_rows))
        course_summaries.append(summary)

    prof_name = rows[0]["instructor"]
    return {
        "school": school,
        "professor": prof_name,
        "overall": grade_summary(rows),
        "by_course": course_summaries,
        "departments": sorted(set(r["dept"] for r in rows)),
    }


@app.get("/search")
def search(
    q: str = Query(..., min_length=2),
    school: str = Query("tamu"),
):
    import re
    q_upper = q.upper().replace("-", " ").strip()
    # Handle "BIOL111" or "BIOL 111" or "BIOL-111"
    match = re.match(r'^([A-Z]+)\s*(\d+)?$', q_upper.replace(" ", ""))
    if match:
        dept = match.group(1)
        course_num = match.group(2) if match.group(2) else None
    else:
        parts = q_upper.split()
        dept = parts[0]
        course_num = parts[1] if len(parts) > 1 else None

    if course_num:
        rows = db(
            "SELECT instructor, dept, course_number, year, semester, grade, count FROM grades WHERE school_id = ? AND dept = ? AND course_number = ?",
            [school, dept, course_num]
        )
    else:
        rows = db(
            "SELECT instructor, dept, course_number, year, semester, grade, count FROM grades WHERE school_id = ? AND dept = ?",
            [school, dept]
        )

    if not rows:
        raise HTTPException(status_code=404, detail="No results found.")

    # Group by course
    courses: dict = {}
    for r in rows:
        key = r["course_number"]
        courses.setdefault(key, []).append(r)

    results = []
    for cn, course_rows in sorted(courses.items()):
        summary = grade_summary(course_rows)
        summary["department"] = dept
        summary["course"] = cn
        summary["sections"] = len(set((r["year"], r["semester"]) for r in course_rows))
        results.append(summary)

    return {"school": school, "query": q, "department": dept, "results": results}


@app.get("/health")
def health():
    """Health check — also verifies DB connectivity."""
    try:
        rows = db("SELECT COUNT(*) as cnt FROM grades")
        return {"status": "ok", "total_rows": int(rows[0]["cnt"])}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ── LectureKey Autocomplete Endpoints ──────────────────────────────────────

@app.get("/professors")
def search_professors(school: str = "tamu", q: str = ""):
    if not q or len(q) < 2:
        return []
    try:
        results = db(
            "SELECT DISTINCT instructor, dept, course_number FROM grades WHERE school_id = ? AND LOWER(instructor) LIKE ? LIMIT 10",
            [school, f"%{q.lower()}%"]
        )
        return [{"instructor": r["instructor"], "dept": r["dept"], "course_number": r["course_number"]} for r in results]
    except Exception as e:
        return {"error": str(e)}

@app.get("/courses")
def search_courses(school: str = "tamu", dept: str = ""):
    if not dept or len(dept) < 2:
        return []
    try:
        results = db(
            "SELECT DISTINCT dept, course_number FROM grades WHERE school_id = ? AND UPPER(dept) LIKE ? LIMIT 10",
            [school, f"{dept.upper()}%"]
        )
        return [{"dept": r["dept"], "course_number": r["course_number"]} for r in results]
    except Exception as e:
        return {"error": str(e)}
