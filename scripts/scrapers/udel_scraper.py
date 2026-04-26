#!/usr/bin/env python3.11
"""
University of Delaware Grade Distribution Scraper
Fetches grade distribution data from https://ire.udel.edu/ir/grade-distributions/
Data format: Department-level aggregated grades with percentages
"""

import os
import sys
import json
import csv
import time
import logging
import requests
import pdfplumber
import tempfile
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin
from typing import List, Dict, Optional, Tuple

# Configuration
DATA_DIR = Path.home() / "Documents" / "GradeView" / "raw_data" / "udel"
PROGRESS_LOG = DATA_DIR / "scrape_progress.log"
OUTPUT_CSV = DATA_DIR / "grade_distributions.csv"
CACHE_FILE = DATA_DIR / ".scrape_cache.json"

PORTAL_URL = "https://ire.udel.edu/ir/grade-distributions/"
RATE_LIMIT_DELAY = 1.5  # seconds between requests

# Setup logging
def setup_logging():
    """Configure logging to file and console"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(PROGRESS_LOG),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()


class UDelScraper:
    """Scraper for University of Delaware grade distributions"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.pdf_links = {}
        self.data_rows = []
        self.load_cache()
    
    def load_cache(self):
        """Load cached progress"""
        if CACHE_FILE.exists():
            try:
                with open(CACHE_FILE, 'r') as f:
                    cache = json.load(f)
                    self.pdf_links = cache.get('pdf_links', {})
                    logger.info(f"Loaded cache with {len(self.pdf_links)} PDF links")
            except Exception as e:
                logger.warning(f"Failed to load cache: {e}")
    
    def save_cache(self):
        """Save progress to cache"""
        try:
            CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(CACHE_FILE, 'w') as f:
                json.dump({'pdf_links': self.pdf_links}, f)
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
    
    def fetch_pdf_links(self) -> Dict[str, str]:
        """Fetch all available PDF download links from portal"""
        logger.info(f"Fetching portal: {PORTAL_URL}")
        
        try:
            r = self.session.get(PORTAL_URL, timeout=15)
            r.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch portal: {e}")
            return self.pdf_links  # return cached links if available
        
        # Extract PDF links from select dropdown
        import re
        pattern = r'<option value="([^"]+)">([^<]+)</option>'
        matches = re.findall(pattern, r.text)
        
        pdf_links = {}
        for url, label in matches:
            if url and url.startswith('http'):
                year = label.strip()
                if year and year != '— Year —':
                    pdf_links[year] = url
                    logger.debug(f"Found: {year} -> {url}")
        
        logger.info(f"Found {len(pdf_links)} years of data")
        self.pdf_links = pdf_links
        self.save_cache()
        return pdf_links
    
    def download_pdf(self, year: str, url: str) -> Optional[bytes]:
        """Download a single PDF with retry logic"""
        logger.debug(f"Downloading: {year} from {url}")
        
        for attempt in range(3):
            try:
                time.sleep(RATE_LIMIT_DELAY)
                r = self.session.get(url, timeout=20)
                r.raise_for_status()
                logger.debug(f"Downloaded {year}: {len(r.content)} bytes")
                return r.content
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {year}: {str(e)[:80]}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Failed to download {year} after 3 attempts")
                    return None
        
        return None
    
    def parse_pdf(self, year: str, content: bytes) -> List[Dict]:
        """Parse grade distribution data from PDF"""
        logger.debug(f"Parsing PDF for {year}")
        
        rows = []
        try:
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as f:
                f.write(content)
                temp_path = f.name
            
            with pdfplumber.open(temp_path) as pdf:
                # Extract metadata
                year_match = year  # e.g., "2023"
                semester = "Fall"  # All documents appear to be Fall term
                
                # Process all tables from all pages
                for page_idx, page in enumerate(pdf.pages):
                    tables = page.extract_tables()
                    if not tables:
                        continue
                    
                    table = tables[0]
                    if not table or len(table) < 2:
                        continue
                    
                    # Parse header (row 0)
                    header = table[0]
                    
                    # Process data rows
                    for row in table[1:]:
                        if not row:
                            continue
                        
                        # Determine department name - can be in col 0 or col 1
                        dept_name = None
                        if row[0]:  # College name (first column)
                            dept_name = row[0].strip()
                            # Skip university totals and empty names
                            if dept_name.startswith('University'):
                                continue
                        elif len(row) > 1 and row[1]:  # Department under college (second column)
                            dept_name = row[1].strip()
                        
                        if not dept_name:
                            continue
                        
                        # Extract grade percentages (columns 2-6)
                        # Column indices: 2=A%, 3=B%, 4=C%, 5=D%, 6=F%, 7=Total
                        try:
                            pct_str_a = str(row[2]).strip() if row[2] else '0%'
                            pct_str_b = str(row[3]).strip() if row[3] else '0%'
                            pct_str_c = str(row[4]).strip() if row[4] else '0%'
                            pct_str_d = str(row[5]).strip() if row[5] else '0%'
                            pct_str_f = str(row[6]).strip() if row[6] else '0%'
                            total_str = str(row[7]).strip() if row[7] else '0'
                            
                            pct_a = float(pct_str_a.rstrip('%'))
                            pct_b = float(pct_str_b.rstrip('%'))
                            pct_c = float(pct_str_c.rstrip('%'))
                            pct_d = float(pct_str_d.rstrip('%'))
                            pct_f = float(pct_str_f.rstrip('%'))
                            total_grades = int(total_str.replace(',', ''))
                            
                            # Calculate counts from percentages
                            if total_grades > 0:
                                count_a = round(total_grades * pct_a / 100)
                                count_b = round(total_grades * pct_b / 100)
                                count_c = round(total_grades * pct_c / 100)
                                count_d = round(total_grades * pct_d / 100)
                                count_f = round(total_grades * pct_f / 100)
                                
                                # Create rows for each grade type
                                for grade, count in [
                                    ('A', count_a), ('B', count_b), ('C', count_c),
                                    ('D', count_d), ('F', count_f)
                                ]:
                                    if count > 0:  # Only include non-zero grades
                                        rows.append({
                                            'school_id': 'udel',
                                            'year': year_match,
                                            'semester': semester,
                                            'dept': dept_name,
                                            'course_number': 'DEPT_LEVEL',
                                            'instructor': 'AGGREGATED',
                                            'grade': grade,
                                            'count': count
                                        })
                        except (ValueError, TypeError, IndexError) as e:
                            logger.debug(f"Skipping row {row}: {e}")
                            continue
            
            os.unlink(temp_path)
            logger.info(f"Parsed {year}: {len(rows)} grade records")
            
        except Exception as e:
            logger.error(f"Failed to parse PDF for {year}: {e}")
            import traceback
            traceback.print_exc()
        
        return rows
    
    def save_data(self):
        """Save collected data to CSV"""
        if not self.data_rows:
            logger.warning("No data to save")
            return
        
        try:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            
            with open(OUTPUT_CSV, 'w', newline='') as f:
                fieldnames = ['school_id', 'year', 'semester', 'dept', 'course_number',
                             'instructor', 'grade', 'count']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.data_rows)
            
            logger.info(f"Saved {len(self.data_rows)} rows to {OUTPUT_CSV}")
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
    
    def scrape_sample(self, max_years: int = 1):
        """Scrape sample data to test the scraper"""
        logger.info(f"Starting sample scrape (max {max_years} year(s))")
        
        # Fetch PDF links
        pdf_links = self.fetch_pdf_links()
        if not pdf_links:
            logger.error("No PDF links found")
            return
        
        # Process only first N years for testing
        for i, (year, url) in enumerate(sorted(pdf_links.items(), reverse=True)[:max_years]):
            logger.info(f"Processing {year}...")
            
            content = self.download_pdf(year, url)
            if not content:
                continue
            
            rows = self.parse_pdf(year, content)
            self.data_rows.extend(rows)
            
            logger.info(f"Progress: {len(self.data_rows)} total records")
        
        self.save_data()
        
        # Print summary
        if self.data_rows:
            print(f"\n=== Sample Data Summary ===")
            print(f"Total records: {len(self.data_rows)}")
            print(f"\nSample rows:")
            for row in self.data_rows[:5]:
                print(f"  {row}")
        
        return self.data_rows
    
    def scrape_all(self):
        """Scrape all available data"""
        logger.info("Starting full scrape")
        
        pdf_links = self.fetch_pdf_links()
        if not pdf_links:
            logger.error("No PDF links found")
            return
        
        for year, url in sorted(pdf_links.items(), reverse=True):
            logger.info(f"Processing {year}...")
            
            content = self.download_pdf(year, url)
            if not content:
                continue
            
            rows = self.parse_pdf(year, url)
            self.data_rows.extend(rows)
            
            logger.info(f"Progress: {len(self.data_rows)} total records")
        
        self.save_data()
        logger.info("Scrape complete")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='University of Delaware Grade Distribution Scraper')
    parser.add_argument('--sample', action='store_true', help='Scrape sample data only (1 year)')
    parser.add_argument('--limit', type=int, default=1, help='Limit number of years to scrape (for testing)')
    parser.add_argument('--full', action='store_true', help='Scrape all available data')
    
    args = parser.parse_args()
    
    scraper = UDelScraper()
    
    if args.full:
        scraper.scrape_all()
    else:
        scraper.scrape_sample(max_years=args.limit)


if __name__ == '__main__':
    main()
