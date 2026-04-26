#!/usr/bin/env python3
"""
TPIA Email Gate — GradeView Outreach Safeguard
==============================================
Prevents duplicate emails, contacts to denied schools, and cooldown violations.
Solomon MUST run --check before sending ANY TPIA email.

Usage:
    # Check if we can email a school
    python3 tpia_gate.py --check <school_id>

    # Log an action (after sending email, receiving response, etc.)
    python3 tpia_gate.py --log <school_id> <action> [--details "..."] [--email-subject "..."] [--channel email]

    # Schedule a follow-up
    python3 tpia_gate.py --follow-up <school_id> --due <YYYY-MM-DD>

    # Show status of a school
    python3 tpia_gate.py --status <school_id>

    # Show all schools needing follow-up
    python3 tpia_gate.py --pending

    # Show full report
    python3 tpia_gate.py --report

    # Regenerate tracker.md from database
    python3 tpia_gate.py --export-tracker

Actions:
    initial_request    — first TPIA/FOIA request sent
    follow_up          — follow-up email sent
    response_received  — school acknowledged/replied
    data_received      — grade data received and confirmed
    denied             — request denied by school
    bounced            — email bounced / delivery failure
    portal_submitted   — portal form submitted
    portal_ready       — data ready for download on portal
    withdrawn          — we withdrew the request
"""

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta

DB_PATH = os.path.expanduser(
    "~/.openclaw/workspace/projects/gradeview/scripts/tpia/tpia_outreach.db"
)

COOLDOWN_BUSINESS_DAYS = 10
TERMINAL_ACTIONS = {"denied", "data_received", "withdrawn"}
BLOCKING_ACTIONS = {"denied", "withdrawn"}

# ── Database Setup ────────────────────────────────────────────────────────────

def get_db():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    return db

def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS schools (
            school_id TEXT PRIMARY KEY,
            school_name TEXT NOT NULL,
            contact_email TEXT,
            state TEXT,
            law_type TEXT DEFAULT 'FOIA',
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS outreach (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            school_id TEXT NOT NULL REFERENCES schools(school_id),
            action TEXT NOT NULL,
            date TEXT NOT NULL,
            channel TEXT DEFAULT 'email',
            details TEXT,
            email_subject TEXT,
            email_hash TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS follow_ups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            school_id TEXT NOT NULL REFERENCES schools(school_id),
            due_date TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_outreach_school ON outreach(school_id);
        CREATE INDEX IF NOT EXISTS idx_outreach_action ON outreach(action);
        CREATE INDEX IF NOT EXISTS idx_followups_due ON follow_ups(due_date);
        CREATE INDEX IF NOT EXISTS idx_followups_status ON follow_ups(status);
    """)
    db.close()

# ── Business Day Calculation ──────────────────────────────────────────────────

def business_days_between(start_str, end_str):
    """Count business days between two ISO date strings."""
    start = datetime.fromisoformat(start_str).date()
    end = datetime.fromisoformat(end_str).date()
    if end <= start:
        return 0
    days = 0
    current = start
    while current < end:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Mon-Fri
            days += 1
    return days

def add_business_days(start_str, n):
    """Add N business days to an ISO date string."""
    current = datetime.fromisoformat(start_str).date()
    added = 0
    while added < n:
        current += timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current.isoformat()

# ── Core Gate Logic ───────────────────────────────────────────────────────────

def check_can_send(school_id):
    """
    Returns (allowed: bool, reason: str)
    This is THE gate. If it returns False, Solomon MUST NOT send.
    """
    db = get_db()

    # 1. School exists?
    school = db.execute(
        "SELECT * FROM schools WHERE school_id = ?", (school_id,)
    ).fetchone()
    if not school:
        db.close()
        return False, f"❌ BLOCKED: Unknown school '{school_id}'. Add to database first with --add-school."

    # 2. Check for terminal/blocking actions
    blocking = db.execute("""
        SELECT action, date, details FROM outreach
        WHERE school_id = ? AND action IN ('denied', 'withdrawn')
        ORDER BY date DESC LIMIT 1
    """, (school_id,)).fetchone()
    if blocking:
        db.close()
        return False, (
            f"❌ BLOCKED: School was {blocking['action']} on {blocking['date']}. "
            f"Details: {blocking['details'] or 'none'}. Do NOT contact again."
        )

    # 3. Already received data?
    received = db.execute("""
        SELECT date, details FROM outreach
        WHERE school_id = ? AND action = 'data_received'
        ORDER BY date DESC LIMIT 1
    """, (school_id,)).fetchone()
    if received:
        db.close()
        return False, (
            f"❌ BLOCKED: Data already received on {received['date']}. "
            f"Details: {received['details'] or 'none'}. No further outreach needed."
        )

    # 4. School responded and we haven't followed up?
    last_response = db.execute("""
        SELECT date, details FROM outreach
        WHERE school_id = ? AND action = 'response_received'
        ORDER BY date DESC LIMIT 1
    """, (school_id,)).fetchone()
    if last_response:
        # Check if we sent anything AFTER their response
        sent_after = db.execute("""
            SELECT date FROM outreach
            WHERE school_id = ? AND action IN ('follow_up', 'initial_request')
            AND date > ?
            ORDER BY date DESC LIMIT 1
        """, (school_id, last_response['date'])).fetchone()
        if not sent_after:
            db.close()
            return False, (
                f"⚠️ BLOCKED: School responded on {last_response['date']} "
                f"({last_response['details'] or 'no details'}). "
                f"They are waiting on US — do not send another request. "
                f"Review their response and take appropriate action."
            )

    # 5. Portal data ready? Download instead of emailing.
    portal_ready = db.execute("""
        SELECT date, details FROM outreach
        WHERE school_id = ? AND action = 'portal_ready'
        AND school_id NOT IN (
            SELECT school_id FROM outreach WHERE action = 'data_received'
        )
        ORDER BY date DESC LIMIT 1
    """, (school_id,)).fetchone()
    if portal_ready:
        db.close()
        return False, (
            f"⚠️ BLOCKED: Data is ready for download (since {portal_ready['date']}). "
            f"Download it instead of emailing. Details: {portal_ready['details'] or 'none'}"
        )

    # 6. Cooldown check — last contact within N business days?
    last_contact = db.execute("""
        SELECT date, action FROM outreach
        WHERE school_id = ?
        AND action IN ('initial_request', 'follow_up', 'portal_submitted')
        ORDER BY date DESC LIMIT 1
    """, (school_id,)).fetchone()
    if last_contact:
        today = datetime.now().strftime("%Y-%m-%d")
        bdays = business_days_between(last_contact['date'], today)
        if bdays < COOLDOWN_BUSINESS_DAYS:
            next_ok = add_business_days(last_contact['date'], COOLDOWN_BUSINESS_DAYS)
            db.close()
            return False, (
                f"⏳ BLOCKED: Last {last_contact['action']} sent on {last_contact['date']} "
                f"({bdays} business days ago). Cooldown: {COOLDOWN_BUSINESS_DAYS} business days. "
                f"Next contact allowed: {next_ok}."
            )

    db.close()

    context = f"Last contact: {last_contact['date']} ({last_contact['action']})" if last_contact else "No previous contact"
    return True, f"✅ CLEAR TO SEND. {context}."

# ── Logging ───────────────────────────────────────────────────────────────────

def log_action(school_id, action, details=None, email_subject=None, channel="email"):
    """Log an outreach action. Returns confirmation message."""
    valid_actions = {
        "initial_request", "follow_up", "response_received",
        "data_received", "denied", "bounced", "portal_submitted",
        "portal_ready", "withdrawn"
    }
    if action not in valid_actions:
        return f"❌ Invalid action '{action}'. Valid: {', '.join(sorted(valid_actions))}"

    db = get_db()

    # Verify school exists
    school = db.execute(
        "SELECT school_name FROM schools WHERE school_id = ?", (school_id,)
    ).fetchone()
    if not school:
        db.close()
        return f"❌ Unknown school '{school_id}'. Add with --add-school first."

    today = datetime.now().strftime("%Y-%m-%d")

    # Compute email hash if subject provided (for duplicate detection)
    email_hash = None
    if email_subject:
        email_hash = hashlib.sha256(
            f"{school_id}:{email_subject}:{today}".encode()
        ).hexdigest()[:16]

        # Check for duplicate email same day
        existing = db.execute(
            "SELECT id FROM outreach WHERE school_id = ? AND email_hash = ?",
            (school_id, email_hash)
        ).fetchone()
        if existing:
            db.close()
            return f"⚠️ DUPLICATE: An email with this subject was already logged for {school_id} today."

    db.execute("""
        INSERT INTO outreach (school_id, action, date, channel, details, email_subject, email_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (school_id, action, today, channel, details, email_subject, email_hash))

    # Auto-cancel pending follow-ups if terminal action
    if action in TERMINAL_ACTIONS:
        db.execute("""
            UPDATE follow_ups SET status = 'cancelled'
            WHERE school_id = ? AND status = 'pending'
        """, (school_id,))

    db.commit()
    db.close()
    return f"✅ Logged: {school_id} — {action} on {today}" + (f" ({details})" if details else "")

# ── Follow-ups ────────────────────────────────────────────────────────────────

def schedule_follow_up(school_id, due_date):
    db = get_db()
    school = db.execute(
        "SELECT school_name FROM schools WHERE school_id = ?", (school_id,)
    ).fetchone()
    if not school:
        db.close()
        return f"❌ Unknown school '{school_id}'."

    db.execute("""
        INSERT INTO follow_ups (school_id, due_date, status) VALUES (?, ?, 'pending')
    """, (school_id, due_date))
    db.commit()
    db.close()
    return f"✅ Follow-up scheduled: {school_id} — due {due_date}"

def get_pending_followups():
    db = get_db()
    today = datetime.now().strftime("%Y-%m-%d")
    rows = db.execute("""
        SELECT f.school_id, s.school_name, s.contact_email, f.due_date
        FROM follow_ups f
        JOIN schools s ON f.school_id = s.school_id
        WHERE f.status = 'pending' AND f.due_date <= ?
        ORDER BY f.due_date
    """, (today,)).fetchall()
    db.close()
    return rows

# ── School Management ─────────────────────────────────────────────────────────

def add_school(school_id, school_name, contact_email=None, state=None, law_type="FOIA", notes=None):
    db = get_db()
    try:
        db.execute("""
            INSERT INTO schools (school_id, school_name, contact_email, state, law_type, notes)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (school_id, school_name, contact_email, state, law_type, notes))
        db.commit()
        result = f"✅ Added school: {school_id} ({school_name})"
    except sqlite3.IntegrityError:
        result = f"⚠️ School '{school_id}' already exists. Use --update-school to modify."
    db.close()
    return result

# ── Status & Reporting ────────────────────────────────────────────────────────

def get_school_status(school_id):
    db = get_db()
    school = db.execute(
        "SELECT * FROM schools WHERE school_id = ?", (school_id,)
    ).fetchone()
    if not school:
        db.close()
        return f"❌ Unknown school '{school_id}'."

    history = db.execute("""
        SELECT action, date, channel, details, email_subject
        FROM outreach WHERE school_id = ?
        ORDER BY date ASC
    """, (school_id,)).fetchall()

    followups = db.execute("""
        SELECT due_date, status FROM follow_ups
        WHERE school_id = ? ORDER BY due_date
    """, (school_id,)).fetchall()

    db.close()

    lines = [
        f"\n{'='*50}",
        f"School: {school['school_name']} ({school_id})",
        f"Email: {school['contact_email'] or 'N/A'}",
        f"State: {school['state'] or 'N/A'} | Law: {school['law_type'] or 'N/A'}",
        f"Notes: {school['notes'] or 'none'}",
        f"{'='*50}",
        f"\nHistory ({len(history)} actions):"
    ]
    for h in history:
        lines.append(f"  {h['date']} | {h['action']:20s} | {h['details'] or ''}")

    if followups:
        lines.append(f"\nFollow-ups:")
        for f in followups:
            lines.append(f"  {f['due_date']} | {f['status']}")

    # Run gate check
    allowed, reason = check_can_send(school_id)
    lines.append(f"\nGate status: {reason}")

    return "\n".join(lines)

def get_full_report():
    db = get_db()
    schools = db.execute("""
        SELECT s.school_id, s.school_name, s.contact_email, s.state,
               (SELECT action FROM outreach WHERE school_id = s.school_id ORDER BY date DESC LIMIT 1) as last_action,
               (SELECT date FROM outreach WHERE school_id = s.school_id ORDER BY date DESC LIMIT 1) as last_date,
               (SELECT COUNT(*) FROM outreach WHERE school_id = s.school_id) as total_actions
        FROM schools s
        ORDER BY s.school_name
    """).fetchall()

    pending = get_pending_followups()
    db.close()

    lines = [
        f"\n{'='*60}",
        f"TPIA OUTREACH REPORT — {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"{'='*60}",
        f"\nTotal schools: {len(schools)}",
        f"Pending follow-ups due today or earlier: {len(pending)}",
        f"\n{'─'*60}"
    ]

    # Group by status
    received = []
    denied = []
    active = []
    bounced = []

    for s in schools:
        if s['last_action'] == 'data_received':
            received.append(s)
        elif s['last_action'] == 'denied':
            denied.append(s)
        elif s['last_action'] == 'bounced':
            bounced.append(s)
        else:
            active.append(s)

    lines.append(f"\n✅ DATA RECEIVED ({len(received)}):")
    for s in received:
        lines.append(f"  {s['school_name']:35s} | {s['last_date']}")

    lines.append(f"\n📬 ACTIVE REQUESTS ({len(active)}):")
    for s in active:
        lines.append(f"  {s['school_name']:35s} | {s['last_action'] or 'N/A':20s} | {s['last_date'] or 'N/A'}")

    lines.append(f"\n❌ DENIED ({len(denied)}):")
    for s in denied:
        lines.append(f"  {s['school_name']:35s} | {s['last_date']}")

    lines.append(f"\n💀 BOUNCED ({len(bounced)}):")
    for s in bounced:
        lines.append(f"  {s['school_name']:35s} | {s['last_date']}")

    if pending:
        lines.append(f"\n⏰ FOLLOW-UPS DUE:")
        for p in pending:
            lines.append(f"  {p['school_name']:35s} | due {p['due_date']} | {p['contact_email'] or 'N/A'}")

    return "\n".join(lines)

# ── Tracker Export ────────────────────────────────────────────────────────────

def export_tracker_md():
    """Regenerate tracker.md from database — single source of truth."""
    db = get_db()
    schools = db.execute("""
        SELECT s.*,
               (SELECT action FROM outreach WHERE school_id = s.school_id ORDER BY date DESC LIMIT 1) as last_action,
               (SELECT date FROM outreach WHERE school_id = s.school_id ORDER BY date DESC LIMIT 1) as last_date,
               (SELECT details FROM outreach WHERE school_id = s.school_id ORDER BY date DESC LIMIT 1) as last_details
        FROM schools s
        ORDER BY s.school_name
    """).fetchall()
    db.close()

    status_map = {
        "data_received": "✅",
        "denied": "❌",
        "bounced": "💀",
        "response_received": "🔄",
        "portal_submitted": "🌐",
        "portal_ready": "📝",
        "initial_request": "📬",
        "follow_up": "📬",
    }

    lines = [
        "# TPIA Tracker — GradeView University Data Requests",
        "",
        "> **AUTO-GENERATED from tpia_outreach.db — DO NOT EDIT MANUALLY**",
        f"> Last generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"> To update: `python3 tpia_gate.py --export-tracker`",
        "",
        "---",
        "",
        "| School | Contact | Last Action | Date | Status |",
        "|--------|---------|-------------|------|--------|",
    ]

    for s in schools:
        emoji = status_map.get(s['last_action'], "❓")
        lines.append(
            f"| {s['school_name']} | {s['contact_email'] or 'N/A'} "
            f"| {s['last_action'] or 'none'} | {s['last_date'] or 'N/A'} | {emoji} |"
        )

    tracker_path = os.path.expanduser(
        "~/.openclaw/workspace/projects/gradeview/tpia/tracker.md"
    )
    with open(tracker_path, 'w') as f:
        f.write("\n".join(lines) + "\n")

    return f"✅ Exported tracker.md with {len(schools)} schools → {tracker_path}"

# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="TPIA Email Gate — Outreach Safeguard")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", metavar="SCHOOL_ID", help="Check if we can email this school")
    group.add_argument("--log", nargs=2, metavar=("SCHOOL_ID", "ACTION"), help="Log an outreach action")
    group.add_argument("--follow-up", metavar="SCHOOL_ID", help="Schedule a follow-up")
    group.add_argument("--status", metavar="SCHOOL_ID", help="Show full status for a school")
    group.add_argument("--pending", action="store_true", help="Show all pending follow-ups")
    group.add_argument("--report", action="store_true", help="Full outreach report")
    group.add_argument("--export-tracker", action="store_true", help="Regenerate tracker.md from DB")
    group.add_argument("--add-school", nargs=2, metavar=("SCHOOL_ID", "SCHOOL_NAME"),
                       help="Add a new school to the database")

    parser.add_argument("--details", help="Details for --log action")
    parser.add_argument("--email-subject", help="Email subject for --log (used for duplicate detection)")
    parser.add_argument("--channel", default="email", help="Channel for --log (email, portal, phone)")
    parser.add_argument("--due", help="Due date for --follow-up (YYYY-MM-DD)")
    parser.add_argument("--email", help="Contact email for --add-school")
    parser.add_argument("--state", help="State for --add-school")
    parser.add_argument("--law", default="FOIA", help="Law type for --add-school (TPIA, FOIA, CORA, etc.)")
    parser.add_argument("--notes", help="Notes for --add-school")

    args = parser.parse_args()

    # Ensure DB exists
    init_db()

    if args.check:
        allowed, reason = check_can_send(args.check)
        print(reason)
        sys.exit(0 if allowed else 1)

    elif args.log:
        school_id, action = args.log
        result = log_action(school_id, action,
                          details=args.details,
                          email_subject=args.email_subject,
                          channel=args.channel)
        print(result)

    elif args.follow_up:
        if not args.due:
            print("❌ --due YYYY-MM-DD required with --follow-up")
            sys.exit(1)
        result = schedule_follow_up(args.follow_up, args.due)
        print(result)

    elif args.status:
        print(get_school_status(args.status))

    elif args.pending:
        rows = get_pending_followups()
        if not rows:
            print("✅ No follow-ups due today or earlier.")
        else:
            print(f"\n⏰ {len(rows)} follow-up(s) due:\n")
            for r in rows:
                print(f"  {r['school_name']:35s} | due {r['due_date']} | {r['contact_email'] or 'N/A'}")

    elif args.report:
        print(get_full_report())

    elif args.export_tracker:
        result = export_tracker_md()
        print(result)

    elif args.add_school:
        school_id, school_name = args.add_school
        result = add_school(school_id, school_name,
                          contact_email=args.email,
                          state=args.state,
                          law_type=args.law,
                          notes=args.notes)
        print(result)

if __name__ == "__main__":
    main()
