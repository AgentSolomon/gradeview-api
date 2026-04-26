#!/usr/bin/env python3
"""
Utah Valley University Grade Distribution Scraper
===================================================
Fetches grade distribution data from Tableau Server dashboard.

Data Source:
  Dashboard: https://tableau.uvu.edu/#/views/GradeDistribution/GradeDistribution
  (Tableau Server, not Tableau Public — may require different approach)

Format: Tableau Server dashboard (JavaScript visualization)
Estimated: 10K-50K+ rows

Strategy:
  Similar to KSU (Tableau), but UVU uses Tableau Server (not Public).
  Key difference: Tableau Server dashboards may have authentication or
  different embed patterns.

  1. Load the dashboard URL in headless Chrome
  2. Wait for Tableau render
  3. Try built-in download/export if available
  4. Fall back to filter iteration + mark extraction

Usage:
    python3 uvu_scraper.py --test       # Test connectivity + page structure
    python3 uvu_scraper.py --full       # Full scrape
    python3 uvu_scraper.py --visible    # Watch the browser
    python3 uvu_scraper.py --discover   # Show available filters

Output: CSV in GradeView standard format
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from selenium_base import ScraperBase, GRADEVIEW_COLUMNS

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class UVUScraper(ScraperBase):
    SCHOOL_ID = "uvu"
    BASE_URL = "https://tableau.uvu.edu/#/views/GradeDistribution/GradeDistribution"
    RATE_LIMIT = 2.0

    TERM_MAP = {
        "Fall": "FALL", "Spring": "SPRING", "Summer": "SUMMER",
        "Winter": "WINTER",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # ── Tableau Server Loading ───────────────────────────────────────────────

    def load_tableau_dashboard(self):
        """Load UVU Tableau Server dashboard and wait for render.

        Tableau Server URLs use hash routing (#/views/...) which means
        the content is loaded client-side. We need to wait for the
        Tableau JS framework to initialize.
        """
        self.logger.info("Loading UVU Tableau Server dashboard...")
        self.load_page(self.BASE_URL)
        time.sleep(5)

        # Tableau Server renders differently than Public
        server_selectors = [
            "div.tab-vizArea",
            "canvas.tabCanvas",
            "div[id*='tableau']",
            "div.tableau-container",
            "iframe[src*='tableau']",
            "#vizContainer",
            "tableau-viz",
        ]

        for selector in server_selectors:
            el = self.wait_for_element(selector, timeout=5, visible=False)
            if el:
                self.logger.info(f"Tableau Server detected via: {selector}")
                time.sleep(3)
                return True

        if self.wait_for_tableau(timeout=15):
            return True

        # Check if redirected to login
        current_url = self.driver.current_url
        if 'login' in current_url.lower() or 'auth' in current_url.lower():
            self.logger.warning("Redirected to login page — dashboard may require authentication")
            self.screenshot("login_redirect")
            return False

        self.logger.warning("Could not detect Tableau Server dashboard")
        self.screenshot("no_tableau")
        return False

    def discover_filters(self):
        """Discover available Tableau Server filters."""
        filters = {}
        try:
            filter_elements = self.driver.find_elements(
                By.CSS_SELECTOR,
                "div[class*='filter'], div[class*='Filter'], "
                "div.tab-widget, span.tab-filterText, "
                "div[data-tb-test-id*='filter']"
            )
            self.logger.info(f"Found {len(filter_elements)} potential filter elements")

            for i, el in enumerate(filter_elements):
                text = el.text[:100].strip()
                if text:
                    filters[f"filter_{i}"] = text
                    self.logger.info(f"  Filter {i}: {text}")

            selects = self.driver.find_elements(By.TAG_NAME, "select")
            for i, sel in enumerate(selects):
                options = sel.find_elements(By.TAG_NAME, "option")
                opt_text = [o.text.strip() for o in options[:5]]
                filters[f"select_{i}"] = f"{len(options)} options: {opt_text}"

        except Exception as e:
            self.logger.warning(f"Filter discovery error: {e}")
        return filters

    def try_download(self):
        """Try Tableau Server's built-in Download → Crosstab export.

        Uses the shared tableau_crosstab_download() flow from ScraperBase,
        which properly configures Chrome's download directory, handles the
        Crosstab dialog, and waits for the file to appear.

        Returns the file path string if successful, None otherwise.
        """
        csv_path = self.tableau_crosstab_download()
        if not csv_path:
            return None

        # Parse the downloaded CSV and convert to GradeView format
        raw_rows = self.parse_downloaded_csv(csv_path)
        if not raw_rows:
            self.logger.warning(f"Downloaded file was empty or unparseable: {csv_path}")
            return None

        parsed = self._parse_uvu_table(raw_rows)
        if parsed:
            self.save_rows(parsed, append=False)
            self.logger.info(f"Parsed {len(parsed)} grade rows from Tableau export")

        return str(csv_path)

    def extract_data(self):
        """Extract data from the rendered Tableau visualization."""
        # Try HTML tables first
        tables_data = self.extract_all_tables()
        if tables_data:
            largest = max(tables_data, key=lambda t: len(t[1]))
            self.logger.info(f"Found HTML table with {len(largest[1])} rows")
            return self._parse_uvu_table(largest[1])

        # Try Tableau marks
        data = self.extract_tableau_data()
        if data:
            return self._parse_uvu_table(data)

        return []

    def _parse_uvu_table(self, raw_rows):
        """Parse UVU table data into GradeView format."""
        parsed = []
        for row in raw_rows:
            try:
                term = row.get('Term', row.get('Semester', row.get('term', '')))
                dept = row.get('Department', row.get('Dept', row.get('Subject', '')))
                course = row.get('Course Number', row.get('Course', row.get('Course No', '')))
                instructor = row.get('Instructor', row.get('Faculty', row.get('Name', 'N/A')))
                grade = row.get('Grade', row.get('Letter Grade', ''))
                count_str = row.get('Count', row.get('Students', row.get('Enrollment', '0')))

                if not grade or not dept:
                    continue

                year, semester = self._parse_uvu_term(term)

                try:
                    count = int(str(count_str).replace(',', '').strip())
                except (ValueError, TypeError):
                    count = 0

                if count > 0:
                    parsed.append({
                        "school_id": self.SCHOOL_ID,
                        "year": year,
                        "semester": semester,
                        "dept": dept.upper().strip(),
                        "course_number": str(course).strip(),
                        "instructor": instructor.strip() or "N/A",
                        "grade": grade.strip(),
                        "count": count,
                    })
            except Exception:
                continue
        return parsed

    def _parse_uvu_term(self, term_str):
        """Parse UVU term string into (year, semester)."""
        term_str = str(term_str or '').strip()
        match = re.match(r'(Fall|Spring|Summer|Winter)\s+(\d{4})', term_str, re.IGNORECASE)
        if match:
            semester = self.TERM_MAP.get(match.group(1).title(), match.group(1).upper())
            return match.group(2), semester
        match = re.match(r'(\d{4})\s+(Fall|Spring|Summer|Winter)', term_str, re.IGNORECASE)
        if match:
            semester = self.TERM_MAP.get(match.group(2).title(), match.group(2).upper())
            return match.group(1), semester
        return "0000", "UNKNOWN"

    # ── Scrape Flows ─────────────────────────────────────────────────────────

    def scrape_test(self):
        """Test mode: verify dashboard loads, inspect structure."""
        self.logger.info("=" * 60)
        self.logger.info("UVU SCRAPER — TEST MODE")
        self.logger.info("=" * 60)

        if not self.load_tableau_dashboard():
            self.logger.error("Tableau Server dashboard did not load")
            self.logger.info("Note: UVU Tableau Server may require campus network or VPN")
            self.screenshot("failed_load")
            return 0

        self.screenshot("tableau_loaded")
        filters = self.discover_filters()

        csv_path = self.try_download()
        if csv_path:
            self.logger.info(f"✅ Data downloaded via Tableau export: {csv_path}")
            return -1

        rows = self.extract_data()
        if rows:
            self.save_rows(rows, append=False)
            for r in rows[:5]:
                self.logger.info(f"  {r}")
        else:
            self.logger.info("No data extracted — may need filter interaction or manual download")

        self.logger.info(f"\nPage title: {self.driver.title}")
        self.logger.info(f"Current URL: {self.driver.current_url}")

        page_html = self.driver.page_source
        for ind in ['tableauPlaceholder', 'tableau-viz', 'tab-vizArea',
                     'tabCanvas', 'VizManager', 'login', 'auth']:
            if ind in page_html:
                self.logger.info(f"  ✓ Found in page: {ind}")

        self.print_summary()
        return len(rows) if rows else 0

    def scrape_full(self, resume=False):
        """Full scrape: extract all available data."""
        self.logger.info("=" * 60)
        self.logger.info("UVU SCRAPER — FULL SCRAPE")
        self.logger.info("=" * 60)

        if not self.load_tableau_dashboard():
            self.logger.error("Dashboard did not load")
            return 0

        csv_path = self.try_download()
        if csv_path:
            self.logger.info(f"✅ Full data downloaded: {csv_path}")
            return -1

        total = 0
        rows = self.extract_data()
        if rows:
            self.save_rows(rows, append=not resume)
            total += len(rows)

        self.save_state(status="complete")
        self.print_summary()
        return total


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Utah Valley University Grade Distribution Scraper')
    parser.add_argument('--test', action='store_true', help='Test mode — verify dashboard loading')
    parser.add_argument('--full', action='store_true', help='Full scrape')
    parser.add_argument('--resume', action='store_true', help='Resume interrupted scrape')
    parser.add_argument('--visible', action='store_true', help='Show browser window')
    parser.add_argument('--discover', action='store_true', help='Discover filters only')
    args = parser.parse_args()

    headless = not args.visible

    with UVUScraper(headless=headless) as scraper:
        if args.discover:
            scraper.load_tableau_dashboard()
            filters = scraper.discover_filters()
            print(json.dumps(filters, indent=2))
        elif args.full:
            scraper.scrape_full(resume=args.resume)
        else:
            scraper.scrape_test()


if __name__ == "__main__":
    main()
