#!/usr/bin/env python3
"""
Virginia Tech Grade Distribution Scraper
==========================================
Fetches grade distribution data from University DataCommons (udc.vt.edu).

Data Source: https://udc.vt.edu/irdata/data/courses/grades
Format: SPA with filter dropdowns (Academic Year, Term, Subject) + HTML table
Estimated: 25+ years of data, 250K+ rows

The page uses JavaScript-rendered filters. Selecting a year/term/subject
combination triggers a table update. This scraper automates the full
filter iteration using Selenium.

Output: CSV in GradeView standard format (school_id, year, semester, dept,
        course_number, instructor, grade, count)

Usage:
    # Test mode — load page, extract one filter combo, verify parsing
    python3 vt_scraper.py --test

    # Full scrape — iterate all year/term/subject combinations
    caffeinate -i python3 vt_scraper.py --full

    # Resume interrupted scrape
    caffeinate -i python3 vt_scraper.py --full --resume

    # Non-headless (watch the browser)
    python3 vt_scraper.py --test --visible
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

# Add parent directory for selenium_base import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from selenium_base import ScraperBase, GRADEVIEW_COLUMNS

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class VTScraper(ScraperBase):
    SCHOOL_ID = "vt"
    BASE_URL = "https://udc.vt.edu/irdata/data/courses/grades"
    RATE_LIMIT = 2.0  # VT is an .edu — be extra polite

    # VT grade columns: wide format — percentages × enrollment = counts
    # Table columns (observed): Academic Year, Term, Subject, Course No,
    # Course Title, Instructor, GPA, A, A-, B+, B, B-, C+, C, C-, D+, D, D-,
    # F, Withdraws, Graded Enrollment, CRN, Credits
    GRADE_COLS = {
        7: "A", 8: "A-", 9: "B+", 10: "B", 11: "B-",
        12: "C+", 13: "C", 14: "C-", 15: "D+", 16: "D",
        17: "D-", 18: "F",
    }

    # VT term mapping
    TERM_MAP = {
        "Fall": "FALL",
        "Spring": "SPRING",
        "Winter": "WINTER",
        "Summer I": "SUMMER I",
        "Summer II": "SUMMER II",
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._resume_state = None

    # ── Filter Discovery ─────────────────────────────────────────────────────

    def _get_filter_sections(self):
        """Find the filter control elements on the VT UDC page.
        Returns dict of filter name → container element."""
        filters = {}
        try:
            # UDC uses v-datafilter class for filter sections
            sections = self.driver.find_elements(By.CLASS_NAME, "v-datafilter")
            # Also try generic filter containers
            if not sections:
                sections = self.driver.find_elements(
                    By.CSS_SELECTOR, "div[class*='filter'], div[class*='Filter']"
                )
            for i, section in enumerate(sections):
                text = section.text[:50]
                self.logger.debug(f"Filter section {i}: {text}")
                filters[f"filter_{i}"] = section
        except Exception as e:
            self.logger.warning(f"Error finding filters: {e}")
        return filters

    def discover_filter_options(self):
        """Discover all available filter values (years, terms, subjects).

        VT UDC uses clickable checkboxes/labels inside filter containers.
        Returns dict: {years: [...], terms: [...], subjects: [...]}
        """
        self.logger.info("Discovering filter options...")
        result = {"years": [], "terms": [], "subjects": []}

        try:
            # Wait for filters to render
            time.sleep(2)

            # Try to find select elements first
            selects = self.driver.find_elements(By.TAG_NAME, "select")
            if selects:
                for sel in selects:
                    opts = [(o.get_attribute("value"), o.text.strip())
                            for o in sel.find_elements(By.TAG_NAME, "option")]
                    label = sel.get_attribute("aria-label") or sel.get_attribute("name") or ""
                    self.logger.info(f"Select '{label}': {len(opts)} options")
                    if "year" in label.lower():
                        result["years"] = opts
                    elif "term" in label.lower():
                        result["terms"] = opts
                    elif "subject" in label.lower() or "course" in label.lower():
                        result["subjects"] = opts

            # If no selects, look for checkbox/radio/clickable filters
            if not any(result.values()):
                sections = self.driver.find_elements(By.CLASS_NAME, "v-datafilter")
                for i, section in enumerate(sections):
                    labels = section.find_elements(By.CSS_SELECTOR, "label, span.v-checkbox")
                    options = [(l.text.strip(), l) for l in labels if l.text.strip()]
                    if i == 0:
                        result["years"] = options
                    elif i == 1:
                        result["terms"] = options
                    elif i == 2:
                        result["subjects"] = options

            for key, vals in result.items():
                self.logger.info(f"  {key}: {len(vals)} options")
                if vals and len(vals) <= 10:
                    self.logger.info(f"    Values: {[v[0] if isinstance(v, tuple) else v for v in vals]}")

        except Exception as e:
            self.logger.error(f"Filter discovery failed: {e}")

        return result

    # ── Filter Interaction ───────────────────────────────────────────────────

    def _click_filter_option(self, section_index, option_text):
        """Click a specific filter option by text within a filter section.

        Args:
            section_index: Which filter section (0=year, 1=term, 2=subject)
            option_text: The text of the option to click
        Returns:
            True if clicked successfully
        """
        try:
            sections = self.driver.find_elements(By.CLASS_NAME, "v-datafilter")
            if section_index >= len(sections):
                return False

            section = sections[section_index]
            # Find the option label/checkbox
            labels = section.find_elements(By.CSS_SELECTOR, "label, span, div.v-checkbox")
            for label in labels:
                if label.text.strip() == option_text:
                    self.safe_click(label)
                    self.rate_limit()
                    return True

            self.logger.warning(f"Option '{option_text}' not found in filter section {section_index}")
            return False
        except Exception as e:
            self.logger.warning(f"Click filter failed: {e}")
            return False

    def _select_filter(self, css_selector, value):
        """Select a dropdown value (for <select>-based filters)."""
        return self.set_select_value(css_selector, value)

    def _clear_filters(self):
        """Try to reset all filters to default state."""
        try:
            # Look for "Clear" or "Reset" buttons
            buttons = self.driver.find_elements(By.CSS_SELECTOR, "button, a.reset, a.clear")
            for btn in buttons:
                text = (btn.text or '').lower()
                if text in ('clear', 'reset', 'clear all', 'reset all'):
                    self.safe_click(btn)
                    time.sleep(1)
                    return True
        except Exception:
            pass
        return False

    # ── Table Extraction ─────────────────────────────────────────────────────

    def _wait_for_table_update(self, timeout=15):
        """Wait for the table to update after a filter change."""
        try:
            # Wait for any loading indicator to disappear
            WebDriverWait(self.driver, 3).until(
                EC.invisibility_of_element_located((By.CSS_SELECTOR, ".loading, .spinner"))
            )
        except Exception:
            pass
        # Additional wait for table content
        time.sleep(2)

    def extract_grade_rows(self):
        """Extract grade distribution data from the VT table.

        VT displays data in wide format: one row per course section with
        grade percentages. We convert to standard format: one row per grade.

        Returns list of dicts in GradeView standard format.
        """
        try:
            table = self.driver.find_element(By.TAG_NAME, "table")
            trs = table.find_elements(By.TAG_NAME, "tr")

            if len(trs) <= 1:
                return []

            rows = []
            for tr in trs[1:]:  # Skip header
                cells = tr.find_elements(By.TAG_NAME, "td")
                if not cells or len(cells) < 21:
                    continue

                cell_text = [c.text.strip() for c in cells]

                # Skip placeholder rows
                if "Select a course" in ' '.join(cell_text):
                    continue

                # Parse base fields
                academic_year = cell_text[0]  # e.g., "2023-24"
                term = cell_text[1]           # e.g., "Fall"
                subject = cell_text[2]        # e.g., "CS"
                course_no = cell_text[3]      # e.g., "2114"
                instructor = cell_text[5]     # e.g., "McQuain W.D."

                # Parse year from academic year (take first year)
                year_match = re.match(r'(\d{4})', academic_year)
                year = year_match.group(1) if year_match else academic_year

                # Adjust year for spring/summer (2023-24 Spring = 2024)
                if year_match and '-' in academic_year and term in ('Spring', 'Summer I', 'Summer II'):
                    year = str(int(year) + 1)

                semester = self.TERM_MAP.get(term, term.upper())

                # Get graded enrollment for percentage → count conversion
                try:
                    enrollment = int(cell_text[20]) if cell_text[20].isdigit() else 0
                except (IndexError, ValueError):
                    enrollment = 0

                if enrollment == 0:
                    continue

                # Convert grade percentages to counts
                for col_idx, grade_letter in self.GRADE_COLS.items():
                    try:
                        pct_str = cell_text[col_idx].replace('%', '').strip()
                        pct = float(pct_str) if pct_str else 0
                        count = round((pct / 100) * enrollment)
                        if count > 0:
                            rows.append({
                                "school_id": self.SCHOOL_ID,
                                "year": year,
                                "semester": semester,
                                "dept": subject.upper(),
                                "course_number": course_no,
                                "instructor": instructor,
                                "grade": grade_letter,
                                "count": count,
                            })
                    except (ValueError, IndexError):
                        continue

            return rows

        except Exception as e:
            self.logger.error(f"Table extraction error: {e}")
            return []

    # ── Full Scrape Flow ─────────────────────────────────────────────────────

    def scrape_test(self):
        """Test mode: load page, extract current table, verify parsing."""
        self.logger.info("=" * 60)
        self.logger.info("VT SCRAPER — TEST MODE")
        self.logger.info("=" * 60)

        self.load_page(self.BASE_URL)
        time.sleep(3)

        # Discover filters
        filters = self.discover_filter_options()

        # Take a screenshot for debugging
        self.screenshot("initial_load")

        # Try to extract whatever data is on the page
        rows = self.extract_grade_rows()

        if rows:
            self.save_rows(rows, append=False)
            self.logger.info(f"\nSample rows:")
            for r in rows[:5]:
                self.logger.info(f"  {r}")
        else:
            self.logger.info("No data on initial load (expected — need to select filters)")
            self.logger.info("Attempting to select first available filter combination...")

            # Try clicking the first year + first term
            page_source = self.driver.page_source[:5000]
            self.logger.info(f"Page title: {self.driver.title}")
            self.logger.info(f"Page source preview: {page_source[:500]}...")

        self.print_summary()
        return len(rows)

    def scrape_full(self, resume=False):
        """Full scrape: iterate all filter combinations.

        Strategy:
        1. Load page, discover filter options
        2. For each year × term × subject combination:
           a. Select filters
           b. Wait for table update
           c. Extract and save rows
        3. Save state for resume capability
        """
        self.logger.info("=" * 60)
        self.logger.info("VT SCRAPER — FULL SCRAPE")
        self.logger.info("=" * 60)

        # Load resume state
        skip_to = None
        if resume:
            state = self.load_state()
            if state:
                skip_to = state.get("last_combo")
                self.logger.info(f"Resuming from: {skip_to}")
                self.logger.info(f"Previously scraped: {self._rows_scraped} rows")

        # Fresh output file if not resuming
        if not resume or not self.data_file.exists():
            self.data_file.unlink(missing_ok=True)

        self.load_page(self.BASE_URL)
        time.sleep(3)

        # Discover available filters
        filters = self.discover_filter_options()

        years = filters.get("years", [])
        terms = filters.get("terms", [])
        subjects = filters.get("subjects", [])

        if not years or not terms:
            self.logger.error("Could not discover year/term filters. Aborting.")
            self.screenshot("no_filters")
            return 0

        # If no subjects found, use a single None entry so the loop still runs
        if not subjects:
            subjects = [(None, None)]
            self.logger.info(f"Scrape plan: {len(years)} years × {len(terms)} terms (no subject filter)")
        else:
            self.logger.info(f"Scrape plan: {len(years)} years × {len(terms)} terms × {len(subjects)} subjects")

        total_combos = 0
        found_resume_point = not resume  # If not resuming, start from beginning

        for year_val, year_text in (years if years and isinstance(years[0], tuple) else [(y, y) for y in years]):
            for term_val, term_text in (terms if terms and isinstance(terms[0], tuple) else [(t, t) for t in terms]):
                for subj_val, subj_text in (subjects if subjects and isinstance(subjects[0], tuple) else [(s, s) for s in subjects]):
                    if subj_text:
                        combo_key = f"{year_text}|{term_text}|{subj_text}"
                    else:
                        combo_key = f"{year_text}|{term_text}"

                    if not found_resume_point:
                        if combo_key == skip_to:
                            found_resume_point = True
                        continue

                    if subj_text:
                        self.logger.info(f"\n--- {year_text} / {term_text} / {subj_text} ---")
                    else:
                        self.logger.info(f"\n--- {year_text} / {term_text} ---")

                    # Try to select year + term + subject filters
                    # The exact method depends on whether VT uses <select> or clickable filters
                    # We try both approaches
                    selected = False

                    # Approach 1: <select> dropdowns
                    selects = self.driver.find_elements(By.TAG_NAME, "select")
                    if len(selects) >= 2:
                        self.set_select_value(f"select:nth-of-type(1)", year_val)
                        self.set_select_value(f"select:nth-of-type(2)", term_val)
                        if subj_val and len(selects) >= 3:
                            self.set_select_value(f"select:nth-of-type(3)", subj_val)
                        selected = True
                    else:
                        # Approach 2: Clickable filter checkboxes
                        self._clear_filters()
                        time.sleep(1)
                        yr_ok = self._click_filter_option(0, year_text)
                        tm_ok = self._click_filter_option(1, term_text)
                        selected = yr_ok and tm_ok
                        if selected and subj_val:
                            subj_ok = self._click_filter_option(2, subj_text)
                            if not subj_ok:
                                self.logger.warning(f"Could not select subject filter: {subj_text}")

                    if not selected:
                        self.logger.warning(f"Could not select filters for {combo_key}")
                        continue

                    self._wait_for_table_update()

                    # Extract data
                    rows = self.extract_grade_rows()
                    if rows:
                        self.save_rows(rows)
                        self.logger.info(f"  -> {len(rows)} grade rows")
                    else:
                        self.logger.info(f"  -> No data")

                    total_combos += 1

                    # Save state periodically
                    if total_combos % 10 == 0:
                        self.save_state(last_combo=combo_key)

        self.save_state(last_combo="COMPLETE", status="done")
        self.print_summary()
        return self._rows_scraped


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Virginia Tech Grade Distribution Scraper')
    parser.add_argument('--test', action='store_true', help='Test mode — single page load')
    parser.add_argument('--full', action='store_true', help='Full scrape — all filter combinations')
    parser.add_argument('--resume', action='store_true', help='Resume interrupted full scrape')
    parser.add_argument('--visible', action='store_true', help='Show browser window (non-headless)')
    parser.add_argument('--discover', action='store_true', help='Discover filters only (no scraping)')
    args = parser.parse_args()

    headless = not args.visible

    with VTScraper(headless=headless) as scraper:
        if args.discover:
            scraper.load_page(scraper.BASE_URL)
            time.sleep(3)
            filters = scraper.discover_filter_options()
            print(json.dumps({k: [str(v) for v in vals] for k, vals in filters.items()}, indent=2))
        elif args.full:
            scraper.scrape_full(resume=args.resume)
        else:
            scraper.scrape_test()


if __name__ == "__main__":
    main()
