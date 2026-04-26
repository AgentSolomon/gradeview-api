#!/usr/bin/env python3
"""
Migrate existing tracker.md data into tpia_outreach.db
Run ONCE to populate the database from the current tracker state.

Usage:
    python3 migrate_tracker.py
"""

import os
import sys
import sqlite3
from datetime import datetime

# Import gate module for DB init and helpers
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tpia_gate import init_db, get_db, add_school, log_action, schedule_follow_up

def migrate():
    print("="*60)
    print("TPIA Tracker Migration — tracker.md → tpia_outreach.db")
    print("="*60)

    init_db()

    # ── Schools with received data ─────────────────────────────────────────
    received_schools = [
        ("tamu", "Texas A&M", None, "TX", "TPIA", "Public database — 300,225 rows"),
        ("utaustin", "UT Austin", None, "TX", "TPIA", "FOIA R011712-030226 — 683,150 deduped rows"),
        ("uwmadison", "UW-Madison", None, "WI", "FOIA", "Public database — 6,199 rows"),
        ("uh", "University of Houston", "uh.edu/legal-affairs", "TX", "TPIA", "110,302 rows"),
        ("purdue", "Purdue University", "publicrecords@purdue.edu", "IN", "FOIA", "74,200 rows via Box"),
        ("westga", "University of West Georgia", None, "GA", "FOIA", "63,257 rows"),
        ("shawnee", "Shawnee State University", None, "OH", "FOIA", "45,143 rows"),
        ("nmu", "Northern Michigan University", "foia@nmu.edu", "MI", "FOIA", "12,688 rows"),
        ("neiu", "Northeastern Illinois University", None, "IL", "FOIA", "58,010 rows"),
        ("unlv", "University of Nevada Las Vegas", None, "NV", "FOIA", "33,763 rows — PRR-2026-102"),
        ("valdosta", "Valdosta State University", None, "GA", "FOIA", "Data received"),
        ("niu", "Northern Illinois University", None, "IL", "FOIA", "11062 xlsx received"),
    ]

    # ── Active requests (sent Mar 22, follow-up Apr 5) ─────────────────────
    active_mar22 = [
        ("utdallas", "UT Dallas", "publicinformation@utdallas.edu", "TX", "TPIA"),
        ("utarlington", "UT Arlington", "publicrecords@uta.edu", "TX", "TPIA"),
        ("samhouston", "Sam Houston State University", "openrecords@shsu.edu", "TX", "TPIA"),
        ("txstate", "Texas State University", "TSUSGenCoun@txstate.edu", "TX", "TPIA"),
        ("ohiostate", "Ohio State University", "maher.93@osu.edu", "OH", "FOIA"),
        ("fiu", "Florida International University", "generalc@fiu.edu", "FL", "FOIA"),
        ("uf", "University of Florida", "pr-request@ufl.edu", "FL", "FOIA"),
        ("msu", "Michigan State University", "foia@msu.edu", "MI", "FOIA"),
        ("umichigan", "University of Michigan", "foia-email@umich.edu", "MI", "FOIA"),
        ("umd", "University of Maryland", "PublicInformationAct@umd.edu", "MD", "FOIA"),
        ("ucincinnati", "University of Cincinnati", "gardnesd@ucmail.uc.edu", "OH", "FOIA"),
        ("uky", "University of Kentucky", "ukopenrecords@uky.edu", "KY", "FOIA"),
        ("sunybuffalo", "SUNY Buffalo", "ubfoil@buffalo.edu", "NY", "FOIL"),
        ("rutgers", "Rutgers University", "OPRARU@uec.rutgers.edu", "NJ", "FOIA"),
        ("iowastate", "Iowa State University", "publicrecords@iastate.edu", "IA", "FOIA"),
        ("uiowa", "University of Iowa", "public-records@uiowa.edu", "IA", "FOIA"),
        ("kentstate", "Kent State University", "publicrecords@kent.edu", "OH", "FOIA"),
        ("stonybrook", "Stony Brook University", "RecordsAccessOfficer@stonybrook.edu", "NY", "FOIL"),
        ("ucberkeley", "UC Berkeley", "pra@berkeley.edu", "CA", "CPRA"),
        ("ucla", "UCLA", "records@ucla.edu", "CA", "CPRA"),
        ("ucdavis", "UC Davis", "publicrecords@ucdavis.edu", "CA", "CPRA"),
        ("ucsb", "UC Santa Barbara", "pra@ucsb.edu", "CA", "CPRA"),
        ("tamucommerce", "TAMU-Commerce", "publicinfo@tamuc.edu", "TX", "TPIA"),
        ("tamuk", "TAMU-Kingsville", "publicinfo@tamuk.edu", "TX", "TPIA"),
        ("utep", "UT El Paso", "publicinfo@utep.edu", "TX", "TPIA"),
        ("utpb", "UT Permian Basin", "publicinfo@utpb.edu", "TX", "TPIA"),
        ("uttyler", "UT Tyler", "publicinfo@uttyler.edu", "TX", "TPIA"),
        ("tarleton", "Tarleton State University", "publicinfo@tarleton.edu", "TX", "TPIA"),
        ("uhcl", "University of Houston-Clear Lake", "publicinfo@uhcl.edu", "TX", "TPIA"),
        ("uhd", "University of Houston-Downtown", "publicinfo@uhd.edu", "TX", "TPIA"),
        ("uhv", "University of Houston-Victoria", "publicinfo@uhv.edu", "TX", "TPIA"),
        ("wtamu", "West Texas A&M University", "publicinfo@wtamu.edu", "TX", "TPIA"),
        ("ncat", "NC A&T State University", "public_records@ncat.edu", "NC", "FOIA"),
        ("uni", "University of Northern Iowa", "publicrecords@uni.edu", "IA", "FOIA"),
        ("kysu", "Kentucky State University", "General.Counsel@kysu.edu", "KY", "FOIA"),
        ("fau", "Florida Atlantic University", "publicrecords@fau.edu", "FL", "FOIA"),
        ("fgcu", "Florida Gulf Coast University", "publicrecordsrequest@fgcu.edu", "FL", "FOIA"),
    ]

    # ── Special status schools ─────────────────────────────────────────────
    special_schools = [
        # (id, name, email, state, law, action, date, details)
        ("ucf", "University of Central Florida", "penny.robinson@ucf.edu", "FL", "FOIA",
         "response_received", "2026-03-22", "Redirected to AIP portal: analytics.ucf.edu/data-literacy/help-support/"),
        ("uci", "UC Irvine", "pra@uci.edu", "CA", "CPRA",
         "portal_ready", "2026-03-22", "GovQA R010295-032326 — data ready, SendGrid link in email"),
        ("idahostate", "Idaho State University", None, "ID", "FOIA",
         "portal_submitted", "2026-03-22", "Portal: tigertracks.isu.edu/TDClient/19"),
        ("cuboulder", "CU Boulder", None, "CO", "CORA",
         "response_received", "2026-03-26", "CORA Req No. 205 — extension to April 6"),
        ("umissouri", "University of Missouri (all campuses)", None, "MO", "FOIA",
         "portal_submitted", "2026-03-23", "GovQA R013835-032326"),
        ("uillinois", "University of Illinois (all campuses)", None, "IL", "FOIA",
         "response_received", "2026-03-23", "Clarified non-commercial, all 3 campuses"),
        ("unt", "University of North Texas", None, "TX", "TPIA",
         "initial_request", "2026-02-28", "Alice Hawes, UNT System PIP — follow-up Apr 17"),
        ("texastech", "Texas Tech University", "ttu.edu general counsel", "TX", "TPIA",
         "initial_request", "2026-02-28", None),
    ]

    # ── Denied schools ─────────────────────────────────────────────────────
    denied_schools = [
        ("utennessee", "University of Tennessee", None, "TN", "FOIA",
         "TCA § 10-7-503(a)(2)(A) + § 10-7-504 — reclassified grade data as student directory info"),
    ]

    # ── Bounced schools ────────────────────────────────────────────────────
    bounced_schools = [
        ("latech", "Louisiana Tech University", "publicrecords@latech.edu", "LA", "FOIA",
         "450 4.1.8 Sender address rejected"),
        ("utsa", "UT San Antonio", "openrecords@utsa.edu", "TX", "TPIA", "Address rejected"),
        ("uga", "University of Georgia", "ugaopenrecordsrequest@uga.edu", "GA", "FOIA",
         "Redirected to public fact book"),
        ("tamucc", "TAMU-Corpus Christi", "publicinfo@tamucc.edu", "TX", "TPIA", "Bounced"),
        ("tamusa", "TAMU-San Antonio", "publicinfo@tamusa.edu", "TX", "TPIA", "Bounced"),
        ("tamuct", "TAMU-Central Texas", "publicinfo@tamuct.edu", "TX", "TPIA", "Bounced"),
        ("utrgv", "UT Rio Grande Valley", "publicinfo@utrgv.edu", "TX", "TPIA", "Bounced"),
        ("sfasu", "Stephen F. Austin State University", "publicinfo@sfasu.edu", "TX", "TPIA", "Bounced"),
        ("lamar", "Lamar University", "publicinfo@lamar.edu", "TX", "TPIA", "Bounced"),
        ("mwsu", "Midwestern State University", "publicinfo@mwsu.edu", "TX", "TPIA", "Bounced"),
        ("pvamu", "Prairie View A&M University", "publicinfo@pvamu.edu", "TX", "TPIA", "Bounced"),
        ("tsu", "Texas Southern University", "publicinfo@tsu.edu", "TX", "TPIA", "Bounced"),
        ("angelo", "Angelo State University", "publicinfo@angelo.edu", "TX", "TPIA", "Bounced"),
        ("tamiu", "TAMU International", "publicinfo@tamiu.edu", "TX", "TPIA", "Bounced"),
        ("twu", "Texas Woman's University", "publicinfo@twu.edu", "TX", "TPIA", "Bounced — mail loop"),
    ]

    # ── Other schools from tracker ─────────────────────────────────────────
    other_schools = [
        ("7brew", "7-Brew Franchise Check", None, None, None),
        ("subaton", "Southern University Baton Rouge", "publicrecords@sus.edu", "LA", "FOIA"),
    ]

    added = 0
    logged = 0

    # Process received schools
    print("\n📦 Adding schools with received data...")
    for school_id, name, email, state, law, details in received_schools:
        result = add_school(school_id, name, contact_email=email, state=state, law_type=law)
        print(f"  {result}")
        added += 1
        log_action(school_id, "data_received", details=details, channel="email")
        logged += 1

    # Process active Mar 22 requests
    print("\n📬 Adding active requests (Mar 22 batch)...")
    for school_id, name, email, state, law in active_mar22:
        result = add_school(school_id, name, contact_email=email, state=state, law_type=law)
        print(f"  {result}")
        added += 1
        # Log initial request on Mar 22
        db = get_db()
        db.execute("""
            INSERT INTO outreach (school_id, action, date, channel, details)
            VALUES (?, 'initial_request', '2026-03-22', 'email', NULL)
        """, (school_id,))
        db.commit()
        db.close()
        logged += 1
        # Schedule follow-up for Apr 5
        schedule_follow_up(school_id, "2026-04-05")

    # Add schools with acknowledgments
    ack_schools = {
        "ohiostate": ("response_received", "2026-03-23", "Acknowledged — ref 26-1163"),
        "fiu": ("response_received", "2026-03-23", "Acknowledged — Eli Deville"),
        "umichigan": ("response_received", "2026-03-23", "Replied with full name/address/phone per MI FOIA"),
        "ucincinnati": ("response_received", "2026-03-23", "Acknowledged — Sandy Dellicarpini"),
        "uky": ("response_received", "2026-03-23", "Argued aggregate data not subject to residency exemption"),
        "ucla": ("response_received", "2026-03-23", "Acknowledged — ref 26-6885"),
        "uttyler": ("response_received", "2026-03-26", "Acknowledged — ref R000164-032326"),
    }

    print("\n🔄 Logging acknowledgments...")
    for school_id, (action, date, details) in ack_schools.items():
        db = get_db()
        db.execute("""
            INSERT INTO outreach (school_id, action, date, channel, details)
            VALUES (?, ?, ?, 'email', ?)
        """, (school_id, action, date, details))
        db.commit()
        db.close()
        print(f"  ✅ {school_id}: {action} on {date}")
        logged += 1

    # Process special status schools
    print("\n🔧 Adding special status schools...")
    for school_id, name, email, state, law, action, date, details in special_schools:
        result = add_school(school_id, name, contact_email=email, state=state, law_type=law)
        print(f"  {result}")
        added += 1
        db = get_db()
        db.execute("""
            INSERT INTO outreach (school_id, action, date, channel, details)
            VALUES (?, 'initial_request', '2026-03-22', 'email', NULL)
        """, (school_id,))
        db.execute("""
            INSERT INTO outreach (school_id, action, date, channel, details)
            VALUES (?, ?, ?, 'email', ?)
        """, (school_id, action, date, details))
        db.commit()
        db.close()
        logged += 2

    # Process denied schools
    print("\n❌ Adding denied schools...")
    for school_id, name, email, state, law, details in denied_schools:
        result = add_school(school_id, name, contact_email=email, state=state, law_type=law)
        print(f"  {result}")
        added += 1
        db = get_db()
        db.execute("""
            INSERT INTO outreach (school_id, action, date, channel, details)
            VALUES (?, 'initial_request', '2026-03-22', 'email', NULL)
        """, (school_id,))
        db.execute("""
            INSERT INTO outreach (school_id, action, date, channel, details)
            VALUES (?, 'denied', '2026-03-23', 'email', ?)
        """, (school_id, details))
        db.commit()
        db.close()
        logged += 2

    # Process bounced schools
    print("\n💀 Adding bounced schools...")
    for school_id, name, email, state, law, details in bounced_schools:
        result = add_school(school_id, name, contact_email=email, state=state, law_type=law)
        print(f"  {result}")
        added += 1
        db = get_db()
        db.execute("""
            INSERT INTO outreach (school_id, action, date, channel, details)
            VALUES (?, 'initial_request', '2026-03-22', 'email', NULL)
        """, (school_id,))
        db.execute("""
            INSERT INTO outreach (school_id, action, date, channel, details)
            VALUES (?, 'bounced', '2026-03-22', 'email', ?)
        """, (school_id, details))
        db.commit()
        db.close()
        logged += 2

    # UNT follow-up
    schedule_follow_up("unt", "2026-04-17")

    print(f"\n{'='*60}")
    print(f"Migration complete!")
    print(f"  Schools added: {added}")
    print(f"  Actions logged: {logged}")
    print(f"  Database: {os.path.expanduser('~/.openclaw/workspace/projects/gradeview/scripts/tpia/tpia_outreach.db')}")
    print(f"{'='*60}")
    print(f"\nNext steps:")
    print(f"  1. Verify: python3 tpia_gate.py --report")
    print(f"  2. Test gate: python3 tpia_gate.py --check utaustin  (should BLOCK — data received)")
    print(f"  3. Test gate: python3 tpia_gate.py --check utdallas  (should BLOCK — cooldown)")
    print(f"  4. Export: python3 tpia_gate.py --export-tracker")

if __name__ == "__main__":
    migrate()
