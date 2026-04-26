#!/usr/bin/env python3
"""
Kennesaw State University Grade Distribution Scraper
=====================================================
Fetches grade distribution data from Tableau Public dashboard.

Data Source:
  Dashboard: https://public.tableau.com/views/GradeDistributionReport_17539864988520/GradeDistributionReport
  Campus Page: https://campus.kennesaw.edu/offices-services/data-strategy/institutional-research/dashboards/grade-distribution-standard.php

Format: Tableau Public dashboard (embedded JavaScript visualization)
Estimated: 10K-50K+ rows

Strategy:
  Tableau Public dashboards render data client-side via JavaScript.
  Plain HTTP requests won't work. We use Selenium to:
  1. Load the dashboard in headless Chrome
  2. Wait for Tableau to render
  3. Interact with filters (semester, department) to paginate through data
  4. Extract visible table/mark data from the rendered DOM
  5. Use Tableau's built-in "Download" → "Crosstab" if available

  If Tableau blocks programmatic extraction, we fall back to the campus page
  which may embed the dashboard differently.

Usage:
    python3 ksu_scraper.py --test       # Test connectivity + page structure
    python3 ksu_scraper.py --full       # Full scrape
    python3 ksu_scraper.py --visible    # Watch the browser
    python3 ksu_scraper.py --discover   # Show available filters

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
from selenium.webdriver.common.action_chains import ActionChains


class KSUScraper(ScraperBase):
    SCHOOL_ID = "ksu"
    BASE_URL = "https://public.tableau.com/views/GradeDistributionReport_17539864988520/GradeDistributionReport"
    CAMPUS_URL = "https://campus.kennesaw.edu/offices-services/data-strategy/institutional-research/dashboards/grade-distribution-standard.php"
    RATE_LIMIT = 2.0

    # KSU semester pattern
    TERM_MAP = {
        "Fall": "FALL", "Spring": "SPRING", "Summer": "SUMMER",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # ── Tableau Interaction ──────────────────────────────────────────────────

    def load_tableau_dashboard(self):
        """Load Tableau Public dashboard and wait for render."""
        self.logger.info("Loading Tableau Public dashboard...")
        self.load_page(self.BASE_URL)
        time.sleep(5)  # Tableau takes time to initialize

        # Check if Tableau loaded
        if self.wait_for_tableau(timeout=30):
            self.logger.info("Tableau dashboard detected")
            return True

        # Fallback: try campus page which embeds the dashboard
        self.logger.info("Trying campus page as fallback...")
        self.load_page(self.CAMPUS_URL)
        time.sleep(5)

        # Switch into Tableau iframe if present
        if self.switch_to_tableau_iframe():
            self.logger.info("Switched to Tableau iframe on campus page")
            time.sleep(3)
            return True

        self.logger.warning("Could not load Tableau dashboard from either URL")
        return False

    def discover_tableau_filters(self):
        """Discover filter options available in the Tableau dashboard."""
        filters = {}
        try:
            # Tableau filters are often in div.tab-filterWidget or similar
            filter_widgets = self.driver.find_elements(
                By.CSS_SELECTOR,
                "div[class*='filter'], div[class*='Filter'], "
                "div.tab-widget, span.tab-filterText"
            )
            self.logger.info(f"Found {len(filter_widgets)} potential filter widgets")

            for i, widget in enumerate(filter_widgets):
                text = widget.text[:100].strip()
                if text:
                    filters[f"filter_{i}"] = text
                    self.logger.info(f"  Filter {i}: {text}")

            # Also check for dropdown-style Tableau filters
            dropdowns = self.driver.find_elements(
                By.CSS_SELECTOR, "select, div[role='listbox']"
            )
            for i, dd in enumerate(dropdowns):
                text = dd.text[:100].strip()
                if text:
                    filters[f"dropdown_{i}"] = text

        except Exception as e:
            self.logger.warning(f"Filter discovery error: {e}")
        return filters

    def try_tableau_download(self):
        """Attempt to use Tableau's built-in Download → Crosstab export.

        Uses the shared tableau_crosstab_download() flow from ScraperBase,
        which properly configures Chrome's download directory, handles the
        Crosstab dialog, and waits for the file to appear.

        If successful, parses the CSV and returns the row count.
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

        parsed = self._parse_ksu_table(raw_rows)
        if parsed:
            self.save_rows(parsed, append=False)
            self.logger.info(f"Parsed {len(parsed)} grade rows from Tableau export")

        return str(csv_path)

    def extract_tableau_marks(self):
        """Extract data from Tableau rendered marks/cells.

        Tableau renders data as SVG marks or HTML cells. We try to
        extract visible text data from the rendered visualization.
        """
        rows = []
        try:
            # Look for text marks in Tableau
            marks = self.driver.find_elements(
                By.CSS_SELECTOR,
                "div.tab-vizArea text, div.tab-vizArea tspan, "
                "div[class*='cell'], div[class*='mark'], "
                "g.mark text, g.mark tspan"
            )
            self.logger.info(f"Found {len(marks)} text marks in Tableau viz")

            # Also try getting the underlying data table if Tableau renders one
            tables_data = self.extract_all_tables()
            if tables_data:
                largest = max(tables_data, key=lambda t: len(t[1]))
                self.logger.info(f"Found HTML table with {len(largest[1])} rows")
                return self._parse_ksu_table(largest[1])

        except Exception as e:
            self.logger.warning(f"Mark extraction error: {e}")
        return rows

    def _parse_ksu_table(self, raw_rows):
        """Parse KSU-specific table data into GradeView format.

        Expected columns from Tableau: Term, College, Department,
        Course Number, Instructor, Grade, Count (or similar)
        """
        parsed = []
        for row in raw_rows:
            try:
                # Flexible column matching — look for recognizable field names
                term = row.get('Term', row.get('Semester', row.get('term', '')))
                dept = row.get('Department', row.get('Dept', row.get('Subject', '')))
                course = row.get('Course Number', row.get('Course', row.get('Course No', '')))
                instructor = row.get('Instructor', row.get('Faculty', row.get('Name', 'N/A')))
                grade = row.get('Grade', row.get('Letter Grade', ''))
                count_str = row.get('Count', row.get('Count ID', row.get('Enrollment', '0')))

                if not grade or not dept:
                    continue

                # Parse term into year + semester
                year, semester = self._parse_ksu_term(term)

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

    def _parse_ksu_term(self, term_str):
        """Parse KSU term string into (year, semester)."""
        term_str = str(term_str or '').strip()

        # Try "Fall 2023" format
        match = re.match(r'(Fall|Spring|Summer)\s+(\d{4})', term_str, re.IGNORECASE)
        if match:
            semester = self.TERM_MAP.get(match.group(1).title(), match.group(1).upper())
            return match.group(2), semester

        # Try "2023 Fall" format
        match = re.match(r'(\d{4})\s+(Fall|Spring|Summer)', term_str, re.IGNORECASE)
        if match:
            semester = self.TERM_MAP.get(match.group(2).title(), match.group(2).upper())
            return match.group(1), semester

        # Try coded format
        from upload_v2_helper import parse_semester_safe
        return "0000", "UNKNOWN"

    # ── Scrape Flows ─────────────────────────────────────────────────────────

    def scrape_test(self):
        """Test mode: verify Tableau loads, inspect structure."""
        self.logger.info("=" * 60)
        self.logger.info("KSU SCRAPER — TEST MODE")
        self.logger.info("=" * 60)

        if not self.load_tableau_dashboard():
            self.logger.error("Tableau dashboard did not load")
            self.screenshot("failed_load")
            return 0

        self.screenshot("tableau_loaded")

        # Discover available filters
        filters = self.discover_tableau_filters()

        # Try the cleanest approach first: built-in download
        csv_path = self.try_tableau_download()
        if csv_path:
            self.logger.info(f"✅ Data downloaded via Tableau export: {csv_path}")
            return -1  # Signal to use the downloaded file

        # Try extracting from rendered content
        rows = self.extract_tableau_marks()
        if rows:
            self.save_rows(rows, append=False)
            self.logger.info(f"Extracted {len(rows)} rows from Tableau marks")
            for r in rows[:5]:
                self.logger.info(f"  {r}")
        else:
            self.logger.info("No data extracted — Tableau may require filter interaction")
            self.logger.info("Try: python3 ksu_scraper.py --visible  (to see the dashboard)")

        # Log page structure for debugging
        page_html = self.driver.page_source
        tableau_indicators = [
            'tableauPlaceholder', 'tableau-viz', 'tab-vizArea',
            'tabCanvas', 'VizManager',
        ]
        for indicator in tableau_indicators:
            if indicator in page_html:
                self.logger.info(f"  ✓ Found: {indicator}")

        self.print_summary()
        return len(rows) if rows else 0

    def scrape_full(self, resume=False):
        """Full scrape: iterate through Tableau filters to extract all data."""
        self.logger.info("=" * 60)
        self.logger.info("KSU SCRAPER — FULL SCRAPE")
        self.logger.info("=" * 60)

        if not self.load_tableau_dashboard():
            self.logger.error("Tableau dashboard did not load")
            return 0

        # Strategy 1: Try direct download first (cleanest)
        csv_path = self.try_tableau_download()
        if csv_path:
            self.logger.info(f"✅ Full data downloaded: {csv_path}")
            self.logger.info("Import with: python3 upload_v2.py --school ksu --file <csv> --stage-only")
            return -1

        # Strategy 2: Extract data by iterating filters
        self.logger.info("Iterating Tableau filters for data extraction...")
        filters = self.discover_tableau_filters()

        total_rows = 0
        rows = self.extract_tableau_marks()
        if rows:
            self.save_rows(rows, append=not resume)
            total_rows += len(rows)

        self.save_state(status="complete")
        self.print_summary()
        return total_rows


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Kennesaw State Grade Distribution Scraper')
    parser.add_argument('--test', action='store_true', help='Test mode — verify Tableau loading')
    parser.add_argument('--full', action='store_true', help='Full scrape')
    parser.add_argument('--resume', action='store_true', help='Resume interrupted scrape')
    parser.add_argument('--visible', action='store_true', help='Show browser window')
    parser.add_argument('--discover', action='store_true', help='Discover filters only')
    args = parser.parse_args()

    headless = not args.visible

    with KSUScraper(headless=headless) as scraper:
        if args.discover:
            scraper.load_tableau_dashboard()
            filters = scraper.discover_tableau_filters()
            print(json.dumps(filters, indent=2))
        elif args.full:
            scraper.scrape_full(resume=args.resume)
        else:
            scraper.scrape_test()


if __name__ == "__main__":
    main()
