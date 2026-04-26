#!/usr/bin/env python3
"""
GradeView — Shared Selenium Base for Grade Distribution Scrapers
=================================================================
Common utilities for browser automation across school scrapers.

Provides:
  - Headless Chrome setup with webdriver-manager
  - Rate limiting and retry logic
  - Table extraction (HTML tables and Tableau dashboards)
  - Wait-for-render helpers
  - Resume state management
  - CSV output in GradeView standard format

Usage:
    from selenium_base import ScraperBase

    class MyScraper(ScraperBase):
        SCHOOL_ID = "myschool"
        BASE_URL = "https://example.com/grades"

        def scrape(self):
            self.load_page(self.BASE_URL)
            rows = self.extract_html_table("table.grades")
            self.save_rows(rows)

Dependencies:
    pip3 install selenium webdriver-manager --break-system-packages
"""

import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait, Select
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.service import Service
    from selenium.common.exceptions import (
        TimeoutException, NoSuchElementException,
        StaleElementReferenceException, ElementClickInterceptedException,
    )
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    print("❌ Required packages not installed. Run:")
    print("   pip3 install selenium webdriver-manager --break-system-packages")
    sys.exit(1)


# ── Constants ────────────────────────────────────────────────────────────────

GRADEVIEW_COLUMNS = [
    "school_id", "year", "semester", "dept",
    "course_number", "instructor", "grade", "count",
]

RAW_DATA_BASE = Path.home() / "Documents" / "GradeView" / "raw_data"


# ── Scraper Base Class ───────────────────────────────────────────────────────

class ScraperBase:
    """Base class for all GradeView grade distribution scrapers."""

    SCHOOL_ID = "unknown"
    BASE_URL = ""
    RATE_LIMIT = 1.5        # Seconds between page loads/actions
    PAGE_LOAD_TIMEOUT = 30  # Seconds to wait for page load
    MAX_RETRIES = 3         # Retries on transient failures

    def __init__(self, headless=True, rate_limit=None):
        self.headless = headless
        self.rate_limit_seconds = rate_limit or self.RATE_LIMIT
        self.driver = None
        self._last_request_time = 0
        self._rows_scraped = 0

        # Paths
        self.output_dir = RAW_DATA_BASE / self.SCHOOL_ID
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.data_file = self.output_dir / "grades.csv"
        self.state_file = self.output_dir / ".scraper_state.json"
        self.log_file = self.output_dir / "scrape_progress.log"

        # Download directory for Tableau CSV exports
        self.download_dir = self.output_dir / "_downloads"
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # Logging
        self.logger = logging.getLogger(f"scraper.{self.SCHOOL_ID}")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            fmt = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')
            fh = logging.FileHandler(self.log_file)
            fh.setFormatter(fmt)
            sh = logging.StreamHandler()
            sh.setFormatter(fmt)
            self.logger.addHandler(fh)
            self.logger.addHandler(sh)

    # ── Driver Lifecycle ─────────────────────────────────────────────────────

    def setup_driver(self):
        """Initialize headless Chrome with safe defaults."""
        self.logger.info("Setting up Chrome WebDriver...")
        options = webdriver.ChromeOptions()
        if self.headless:
            options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument(
            '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        # Suppress DevTools listening message
        options.add_experimental_option('excludeSwitches', ['enable-logging'])

        # Configure download directory — required for headless CSV exports
        prefs = {
            "download.default_directory": str(self.download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        options.add_experimental_option("prefs", prefs)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)

        # Headless Chrome also needs CDP command for downloads
        if self.headless:
            self.driver.execute_cdp_cmd("Page.setDownloadBehavior", {
                "behavior": "allow",
                "downloadPath": str(self.download_dir),
            })
        self.driver.set_page_load_timeout(self.PAGE_LOAD_TIMEOUT)
        self.logger.info("WebDriver ready")

    def close_driver(self):
        """Safely close the WebDriver."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None
            self.logger.info("WebDriver closed")

    def __enter__(self):
        self.setup_driver()
        return self

    def __exit__(self, *args):
        self.close_driver()

    # ── Rate Limiting ────────────────────────────────────────────────────────

    def rate_limit(self):
        """Enforce minimum delay between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit_seconds:
            time.sleep(self.rate_limit_seconds - elapsed)
        self._last_request_time = time.time()

    # ── Page Navigation ──────────────────────────────────────────────────────

    def load_page(self, url, wait_for=None, wait_timeout=None):
        """Load a URL with rate limiting and optional wait-for-element.

        Args:
            url: URL to navigate to
            wait_for: CSS selector to wait for after load (optional)
            wait_timeout: Override default timeout (optional)
        """
        timeout = wait_timeout or self.PAGE_LOAD_TIMEOUT
        self.rate_limit()
        self.logger.info(f"Loading: {url}")
        self.driver.get(url)

        if wait_for:
            self.wait_for_element(wait_for, timeout=timeout)

    def wait_for_element(self, css_selector, timeout=None, visible=True):
        """Wait for an element to appear on the page.

        Args:
            css_selector: CSS selector string
            timeout: Seconds to wait (default: PAGE_LOAD_TIMEOUT)
            visible: Wait for visibility (True) or just presence (False)
        Returns:
            The WebElement, or None if timeout
        """
        timeout = timeout or self.PAGE_LOAD_TIMEOUT
        condition = (
            EC.visibility_of_element_located((By.CSS_SELECTOR, css_selector))
            if visible else
            EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
        )
        try:
            element = WebDriverWait(self.driver, timeout).until(condition)
            return element
        except TimeoutException:
            self.logger.warning(f"Timeout waiting for: {css_selector}")
            return None

    def wait_for_text_in_element(self, css_selector, text, timeout=None):
        """Wait until element contains specific text."""
        timeout = timeout or self.PAGE_LOAD_TIMEOUT
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.text_to_be_present_in_element(
                    (By.CSS_SELECTOR, css_selector), text
                )
            )
            return True
        except TimeoutException:
            return False

    def safe_click(self, element, retries=3):
        """Click an element with retry logic for intercepted clicks."""
        for attempt in range(retries):
            try:
                element.click()
                return True
            except ElementClickInterceptedException:
                time.sleep(0.5)
                try:
                    self.driver.execute_script("arguments[0].click();", element)
                    return True
                except Exception:
                    pass
            except StaleElementReferenceException:
                time.sleep(0.5)
        return False

    # ── Select/Dropdown Helpers ──────────────────────────────────────────────

    def get_select_options(self, css_selector):
        """Get all option values from a <select> element.

        Returns:
            List of (value, text) tuples
        """
        try:
            el = self.driver.find_element(By.CSS_SELECTOR, css_selector)
            select = Select(el)
            return [(opt.get_attribute("value"), opt.text.strip())
                    for opt in select.options]
        except Exception as e:
            self.logger.warning(f"Could not read select {css_selector}: {e}")
            return []

    def set_select_value(self, css_selector, value):
        """Set a <select> element to a specific value."""
        try:
            el = self.driver.find_element(By.CSS_SELECTOR, css_selector)
            select = Select(el)
            select.select_by_value(str(value))
            self.rate_limit()
            return True
        except Exception as e:
            self.logger.warning(f"Could not set select {css_selector} to {value}: {e}")
            return False

    # ── Clickable Filter Helpers (for non-select filters) ────────────────────

    def get_filter_options(self, container_css, option_css="label, a, button, span"):
        """Get clickable filter options from a container element.

        Args:
            container_css: CSS selector for the filter container
            option_css: CSS selector for individual options within the container
        Returns:
            List of WebElements
        """
        try:
            container = self.driver.find_element(By.CSS_SELECTOR, container_css)
            return container.find_elements(By.CSS_SELECTOR, option_css)
        except Exception as e:
            self.logger.warning(f"Could not find filter options in {container_css}: {e}")
            return []

    # ── Table Extraction ─────────────────────────────────────────────────────

    def extract_html_table(self, table_css="table", header_row=0):
        """Extract data from an HTML table.

        Args:
            table_css: CSS selector for the table element
            header_row: Which <tr> is the header (0-indexed)
        Returns:
            List of dicts (header → cell value)
        """
        try:
            table = self.driver.find_element(By.CSS_SELECTOR, table_css)
            rows = table.find_elements(By.TAG_NAME, "tr")
            if len(rows) <= header_row + 1:
                return []

            # Extract headers
            header_cells = rows[header_row].find_elements(By.CSS_SELECTOR, "th, td")
            headers = [c.text.strip() for c in header_cells]

            # Extract data rows
            data = []
            for row in rows[header_row + 1:]:
                cells = row.find_elements(By.TAG_NAME, "td")
                if not cells:
                    continue
                values = [c.text.strip() for c in cells]
                if len(values) == len(headers):
                    data.append(dict(zip(headers, values)))
                elif values and any(v for v in values):
                    # Pad or trim to match headers
                    padded = values + [''] * (len(headers) - len(values))
                    data.append(dict(zip(headers, padded[:len(headers)])))

            self.logger.info(f"Extracted {len(data)} rows from {table_css}")
            return data
        except NoSuchElementException:
            self.logger.warning(f"Table not found: {table_css}")
            return []
        except Exception as e:
            self.logger.error(f"Table extraction error: {e}")
            return []

    def extract_all_tables(self):
        """Extract data from all visible tables on the page.

        Returns:
            List of (table_index, [row_dicts]) tuples
        """
        tables = self.driver.find_elements(By.TAG_NAME, "table")
        results = []
        for i, table in enumerate(tables):
            try:
                rows = table.find_elements(By.TAG_NAME, "tr")
                if len(rows) <= 1:
                    continue
                header_cells = rows[0].find_elements(By.CSS_SELECTOR, "th, td")
                headers = [c.text.strip() for c in header_cells]
                data = []
                for row in rows[1:]:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    values = [c.text.strip() for c in cells]
                    if values:
                        padded = values + [''] * (len(headers) - len(values))
                        data.append(dict(zip(headers, padded[:len(headers)])))
                if data:
                    results.append((i, data))
            except Exception:
                continue
        return results

    # ── Tableau-Specific Helpers ─────────────────────────────────────────────

    def wait_for_tableau(self, timeout=30):
        """Wait for a Tableau dashboard to finish rendering.

        Looks for the Tableau viz container and waits for data to load.
        Returns True if Tableau content detected.
        """
        # Tableau Public embeds use these selectors
        selectors = [
            "div.tableauPlaceholder iframe",
            "iframe[src*='tableau']",
            "#tableau_base_widget",
            "div[class*='tableau']",
            "canvas.tabCanvas",
        ]
        for selector in selectors:
            el = self.wait_for_element(selector, timeout=timeout // len(selectors), visible=False)
            if el:
                self.logger.info(f"Tableau detected via: {selector}")
                time.sleep(3)  # Extra wait for Tableau render
                return True
        return False

    def switch_to_tableau_iframe(self):
        """Switch into a Tableau iframe if present.

        Returns True if switched successfully.
        """
        try:
            iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
            for iframe in iframes:
                src = iframe.get_attribute("src") or ""
                if "tableau" in src.lower() or "public.tableau" in src.lower():
                    self.driver.switch_to.frame(iframe)
                    time.sleep(2)
                    self.logger.info("Switched to Tableau iframe")
                    return True
            return False
        except Exception as e:
            self.logger.warning(f"Could not switch to Tableau iframe: {e}")
            return False

    def extract_tableau_data(self):
        """Attempt to extract data from a rendered Tableau dashboard.

        Strategy:
        1. Look for the underlying data table (Tableau "View Data" tables)
        2. Look for tooltip/mark data in the DOM
        3. Look for data in Tableau's internal JS objects

        Returns list of dicts or empty list.
        """
        data = []

        # Strategy 1: Look for rendered HTML tables in Tableau
        tables = self.extract_all_tables()
        if tables:
            # Return the largest table
            largest = max(tables, key=lambda t: len(t[1]))
            return largest[1]

        # Strategy 2: Try Tableau's built-in data export
        # Some Tableau Public dashboards expose a "Download" or "View Data" option
        try:
            # Look for the toolbar download button
            download_btns = self.driver.find_elements(
                By.CSS_SELECTOR,
                "button[data-tb-test-id='download-ToolbarButton'], "
                "div.tab-toolbar button, "
                "span.tabToolbarButton"
            )
            for btn in download_btns:
                if 'download' in (btn.get_attribute('aria-label') or '').lower():
                    self.logger.info("Found Tableau download button")
                    # Don't click automatically — flag for manual handling
                    break
        except Exception:
            pass

        # Strategy 3: Extract from Tableau's internal data via JS
        try:
            js_data = self.driver.execute_script("""
                // Try to access Tableau's internal data model
                var vizzes = document.querySelectorAll('tableau-viz, div.tableauPlaceholder');
                if (vizzes.length > 0) {
                    return 'tableau_viz_found';
                }
                // Try window.tableau
                if (window.tableau && window.tableau.VizManager) {
                    var vizzes = window.tableau.VizManager.getVizs();
                    if (vizzes.length > 0) {
                        return 'viz_manager_found';
                    }
                }
                return null;
            """)
            if js_data:
                self.logger.info(f"Tableau JS probe result: {js_data}")
        except Exception:
            pass

        return data

    # ── Tableau Download Helpers ────────────────────────────────────────────

    def wait_for_download(self, timeout=30, extension=".csv"):
        """Wait for a file to appear in the download directory.

        Watches self.download_dir for a new file with the given extension.
        Ignores partial downloads (.crdownload files).

        Returns:
            Path to the downloaded file, or None on timeout.
        """
        start = time.time()
        # Snapshot existing files before download starts
        existing = set(f.name for f in self.download_dir.iterdir())

        while time.time() - start < timeout:
            time.sleep(1)
            for f in self.download_dir.iterdir():
                if f.name in existing:
                    continue
                if f.suffix == '.crdownload':
                    continue  # still downloading
                if extension and not f.name.endswith(extension):
                    continue
                self.logger.info(f"Download complete: {f.name} ({f.stat().st_size:,} bytes)")
                return f

        self.logger.warning(f"Download timeout ({timeout}s) — no new {extension} file found")
        return None

    def clear_downloads(self):
        """Remove all files from the download directory (prep for clean download)."""
        for f in self.download_dir.iterdir():
            try:
                f.unlink()
            except Exception:
                pass

    def parse_downloaded_csv(self, csv_path):
        """Parse a downloaded CSV file into a list of row dicts.

        The CSV is assumed to have a header row. Returns list of dicts
        with original column names as keys (caller maps to GradeView format).
        """
        rows = []
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(dict(row))
            self.logger.info(f"Parsed {len(rows)} rows from {csv_path.name}")
        except Exception as e:
            self.logger.error(f"CSV parse error ({csv_path}): {e}")
        return rows

    def tableau_crosstab_download(self):
        """Execute the Tableau Download → Crosstab flow.

        This is the standard Tableau export path:
        1. Click the download toolbar button
        2. Click "Crosstab" in the dropdown menu
        3. Handle the optional sheet/tab selector dialog
        4. Click the final download button
        5. Wait for the CSV to appear in download_dir

        Returns:
            Path to the downloaded CSV, or None on failure.
        """
        try:
            # Step 1: Find and click the download button on the toolbar
            download_selectors = [
                "button[data-tb-test-id='download-ToolbarButton']",
                "#download-ToolbarButton",
                "button[aria-label='Download']",
                "button[aria-label='download']",
                "div.tab-toolbar-buttonContainer button",
                "span[class*='download']",
            ]

            download_btn = None
            for selector in download_selectors:
                btns = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for btn in btns:
                    label = (btn.get_attribute('aria-label') or btn.text or '').lower()
                    if 'download' in label or 'export' in label or selector.endswith("'download-ToolbarButton']"):
                        download_btn = btn
                        break
                if download_btn:
                    break

            if not download_btn:
                self.logger.info("No download button found on Tableau toolbar")
                return None

            self.logger.info("Clicking Tableau download button...")
            self.safe_click(download_btn)
            time.sleep(2)

            # Step 2: Click "Crosstab" in the dropdown menu
            crosstab_selectors = [
                "button[data-tb-test-id='DownloadCrosstab-Button']",
                "button[data-tb-test-id='download-crosstab-Button']",
            ]
            crosstab_btn = None
            for selector in crosstab_selectors:
                els = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if els:
                    crosstab_btn = els[0]
                    break

            # Fallback: search all buttons/links for "crosstab" text
            if not crosstab_btn:
                for tag in ("button", "a", "div[role='menuitem']", "span"):
                    els = self.driver.find_elements(By.CSS_SELECTOR, tag)
                    for el in els:
                        if 'crosstab' in (el.text or '').lower():
                            crosstab_btn = el
                            break
                    if crosstab_btn:
                        break

            if not crosstab_btn:
                self.logger.warning("Could not find 'Crosstab' option in download menu")
                # Try pressing Escape to close menu
                from selenium.webdriver.common.keys import Keys
                self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
                return None

            self.logger.info("Clicking 'Crosstab' download option...")
            self.safe_click(crosstab_btn)
            time.sleep(3)

            # Step 3: Handle the sheet/tab selector dialog
            # Tableau often shows a dialog asking which sheet to export.
            # We want to select all sheets or click the download/export button.
            dialog_selectors = [
                "div[class*='tab-dialog']",
                "div[class*='download-dialog']",
                "div[role='dialog']",
            ]
            dialog = None
            for selector in dialog_selectors:
                els = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if els:
                    dialog = els[0]
                    break

            if dialog:
                self.logger.info("Crosstab dialog detected — looking for download button...")
                # Look for the final "Download" or "Export" button inside the dialog
                for tag in ("button", "a"):
                    btns = dialog.find_elements(By.CSS_SELECTOR, tag)
                    for btn in btns:
                        text = (btn.text or '').strip().lower()
                        if text in ('download', 'export', 'ok', 'csv'):
                            self.logger.info(f"Clicking dialog button: '{btn.text}'")
                            self.clear_downloads()  # Clean slate before download
                            self.safe_click(btn)
                            break
                    else:
                        continue
                    break
            else:
                # No dialog — download may have started directly
                self.clear_downloads()
                self.logger.info("No dialog — download may be in progress")

            # Step 4: Wait for the CSV file
            csv_path = self.wait_for_download(timeout=30, extension=".csv")
            if not csv_path:
                # Some Tableau exports produce .xlsx instead
                csv_path = self.wait_for_download(timeout=10, extension=".xlsx")
            return csv_path

        except Exception as e:
            self.logger.warning(f"Tableau crosstab download failed: {e}")
            return None

    # ── CSV Output ───────────────────────────────────────────────────────────

    def save_rows(self, rows, append=True):
        """Save grade rows to CSV in GradeView standard format.

        Args:
            rows: List of dicts with keys matching GRADEVIEW_COLUMNS
            append: Append to existing file (True) or overwrite (False)
        """
        if not rows:
            self.logger.warning("No rows to save")
            return 0

        mode = 'a' if append else 'w'
        file_exists = self.data_file.exists() and append

        with open(self.data_file, mode, newline='') as f:
            writer = csv.DictWriter(f, fieldnames=GRADEVIEW_COLUMNS)
            if not file_exists:
                writer.writeheader()
            for row in rows:
                # Ensure all required fields are present
                clean = {k: row.get(k, '') for k in GRADEVIEW_COLUMNS}
                writer.writerow(clean)

        self._rows_scraped += len(rows)
        self.logger.info(f"Saved {len(rows)} rows → {self.data_file} (total: {self._rows_scraped})")
        return len(rows)

    # ── State Management ─────────────────────────────────────────────────────

    def save_state(self, **kwargs):
        """Save scraper state for resume capability."""
        state = {
            "school_id": self.SCHOOL_ID,
            "rows_scraped": self._rows_scraped,
            "saved_at": datetime.now().isoformat(),
        }
        state.update(kwargs)
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)
        self.logger.info(f"State saved: {self._rows_scraped} rows")

    def load_state(self):
        """Load saved state. Returns dict or None."""
        if self.state_file.exists():
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                self._rows_scraped = state.get('rows_scraped', 0)
                return state
        return None

    def clear_state(self):
        """Clear saved state (fresh start)."""
        if self.state_file.exists():
            self.state_file.unlink()
        self._rows_scraped = 0

    # ── Screenshot (debugging) ───────────────────────────────────────────────

    def screenshot(self, name="debug"):
        """Save a screenshot for debugging."""
        path = self.output_dir / f"screenshot_{name}_{int(time.time())}.png"
        self.driver.save_screenshot(str(path))
        self.logger.info(f"Screenshot saved: {path}")
        return path

    # ── Summary ──────────────────────────────────────────────────────────────

    def print_summary(self):
        """Print scrape summary."""
        print(f"\n{'='*50}")
        print(f"  {self.SCHOOL_ID.upper()} Scraper Summary")
        print(f"{'='*50}")
        print(f"  Rows scraped:  {self._rows_scraped:,}")
        print(f"  Output file:   {self.data_file}")
        print(f"  State file:    {self.state_file}")
        print(f"  Log file:      {self.log_file}")
        if self.data_file.exists():
            size = self.data_file.stat().st_size
            print(f"  File size:     {size:,} bytes")
        print(f"{'='*50}\n")
