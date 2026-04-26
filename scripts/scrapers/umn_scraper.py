#!/usr/bin/env python3.11
"""
University of Minnesota Grade Distribution Scraper

Sources:
1. Official IDR (idr.umn.edu) - Google Sheets export
2. Gopher Grades (umn.lol) - Community crowdsourced data

Required output columns:
- school_id (str): "umn"
- year (int): e.g., 2024
- semester (str): e.g., "fall", "spring", "summer"
- dept (str): e.g., "CSCI", "MATH"
- course_number (str): e.g., "1001", "3011"
- instructor (str): e.g., "Smith, John"
- grade (str): e.g., "A", "B", "C", "D", "F", "S", "N"
- count (int): number of students receiving that grade
"""

import csv
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import requests
from io import StringIO

# Configuration
SCHOOL_ID = "umn"
DATA_DIR = Path.home() / "Documents" / "GradeView" / "raw_data" / "umn"
PROGRESS_LOG = DATA_DIR / "scrape_progress.log"
RATE_LIMIT_DELAY = 1.5  # seconds between requests

# Source URLs
IDR_SHEET_ID = "1AD4TAFk9pbLQuAwXTBa-FtJXjRXvcH7REvOF-gIfD9Q"  # By Designator/Course Level
IDR_CSV_URL = f"https://docs.google.com/spreadsheets/d/{IDR_SHEET_ID}/export?format=csv"
GOPHER_GRADES_URL = "https://umn.lol"


class ProgressLogger:
    """Log scraping progress to file and console."""
    
    def __init__(self, log_path: Path):
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
    
    def log(self, message: str, level: str = "INFO"):
        timestamp = datetime.now().isoformat()
        log_entry = f"[{timestamp}] [{level}] {message}"
        print(log_entry)
        with open(self.log_path, "a") as f:
            f.write(log_entry + "\n")
    
    def get_progress(self) -> Dict:
        """Read progress state from log."""
        if not self.log_path.exists():
            return {"started": False, "rows_parsed": 0}
        
        with open(self.log_path, "r") as f:
            lines = f.readlines()
        
        state = {"started": False, "rows_parsed": 0}
        for line in reversed(lines):
            if "rows_parsed" in line:
                try:
                    count = int(line.split("rows_parsed=")[-1].split()[0])
                    state["rows_parsed"] = count
                except:
                    pass
            if "STARTED" in line:
                state["started"] = True
                break
        
        return state


class IDRScraper:
    """Scrape official IDR Google Sheets data."""
    
    def __init__(self, logger: ProgressLogger):
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
        })
    
    def fetch_csv(self) -> Optional[str]:
        """Fetch CSV from IDR Google Sheets."""
        self.logger.log("Fetching IDR Google Sheets CSV...")
        try:
            time.sleep(RATE_LIMIT_DELAY)
            response = self.session.get(IDR_CSV_URL, timeout=30)
            response.raise_for_status()
            self.logger.log(f"Fetched IDR CSV ({len(response.text)} bytes)")
            return response.text
        except Exception as e:
            self.logger.log(f"Failed to fetch IDR CSV: {e}", level="ERROR")
            return None
    
    def parse_csv(self, csv_text: str) -> List[Dict]:
        """Parse IDR CSV format."""
        rows = []
        reader = csv.DictReader(StringIO(csv_text))
        
        # Expected columns vary; examine first few rows
        for i, row in enumerate(reader):
            if i == 0:
                self.logger.log(f"IDR CSV headers: {list(row.keys())}")
            
            # IDR format may have: Designator, Course Level, Mean GPA, % A, etc.
            # For this test, we collect raw structure
            rows.append(row)
            
            if i >= 10:  # Test with first 10 rows
                break
        
        self.logger.log(f"Parsed {len(rows)} sample rows from IDR CSV")
        return rows


class GopherGradesScraper:
    """Scrape Gopher Grades (umn.lol) data."""
    
    def __init__(self, logger: ProgressLogger):
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
        })
    
    def fetch_page(self) -> Optional[str]:
        """Fetch Gopher Grades main page."""
        self.logger.log("Fetching Gopher Grades homepage...")
        try:
            time.sleep(RATE_LIMIT_DELAY)
            response = self.session.get(GOPHER_GRADES_URL, timeout=30)
            response.raise_for_status()
            self.logger.log(f"Fetched Gopher Grades page ({len(response.text)} bytes)")
            return response.text
        except Exception as e:
            self.logger.log(f"Failed to fetch Gopher Grades: {e}", level="ERROR")
            return None
    
    def parse_html(self, html: str) -> Dict:
        """Analyze HTML structure to identify data format."""
        from bs4 import BeautifulSoup
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for data in script tags (Next.js often embeds data there)
        scripts = soup.find_all('script', type='application/json')
        
        info = {
            "json_scripts_found": len(scripts),
            "tables_found": len(soup.find_all('table')),
            "next_data": None
        }
        
        for script in scripts:
            if '__NEXT_DATA__' in script.get_text()[:50]:
                try:
                    data = json.loads(script.string)
                    info["next_data"] = "Found __NEXT_DATA__"
                    self.logger.log("Found Next.js data structure in __NEXT_DATA__")
                except:
                    pass
        
        return info


def save_raw_data(filename: str, data: List[Dict]):
    """Save raw parsed data for inspection."""
    output_file = DATA_DIR / filename
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    if not data:
        return
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Saved {len(data)} rows to {output_file}")


def main():
    """Main scraping orchestration."""
    logger = ProgressLogger(PROGRESS_LOG)
    
    logger.log("=" * 70)
    logger.log("UMN GRADE DISTRIBUTION SCRAPER - START")
    logger.log("=" * 70)
    
    progress = logger.get_progress()
    logger.log(f"Resume state: {progress}")
    
    # === IDR SOURCE ===
    logger.log("\n=== IDR (Official) Source ===")
    idr_scraper = IDRScraper(logger)
    idr_csv = idr_scraper.fetch_csv()
    
    if idr_csv:
        idr_rows = idr_scraper.parse_csv(idr_csv)
        logger.log(f"IDR source: {len(idr_rows)} rows parsed from CSV export")
        
        # Save raw IDR data for inspection
        if idr_rows:
            save_raw_data("idr_raw.csv", idr_rows)
            logger.log(f"Sample row: {idr_rows[0] if idr_rows else 'No rows'}")
    
    # === GOPHER GRADES SOURCE ===
    logger.log("\n=== Gopher Grades (Community) Source ===")
    gg_scraper = GopherGradesScraper(logger)
    gg_html = gg_scraper.fetch_page()
    
    if gg_html:
        gg_info = gg_scraper.parse_html(gg_html)
        logger.log(f"Gopher Grades structure: {gg_info}")
        
        if gg_info["next_data"]:
            logger.log("Gopher Grades uses Next.js with client-side rendering")
            logger.log("Data likely comes from backend API (not accessible via static HTML)")
    
    logger.log("\n=== SUMMARY ===")
    logger.log("Data sources analyzed:")
    logger.log("1. IDR (Official): CSV export available from Google Sheets")
    logger.log("   - Format: Structured CSV with designator/course level data")
    logger.log("   - Coverage: Most recent academic year (2024-25)")
    logger.log("   - Columns: Varies by report (may include mean GPA, % A grades, etc.)")
    logger.log("")
    logger.log("2. Gopher Grades (Community): Next.js frontend")
    logger.log("   - Format: Client-side rendered, requires API/database access")
    logger.log("   - Coverage: Summer 2017 - Fall 2025")
    logger.log("   - Limitation: Public records request data, backend not directly accessible")
    logger.log("")
    logger.log("Recommendation: IDR official source is more structured and accessible.")
    logger.log("Gopher Grades would require reverse-engineering the Next.js backend.")
    logger.log("")
    logger.log("Test complete. Ready for full scrape implementation.")
    logger.log("=" * 70)


if __name__ == "__main__":
    main()
