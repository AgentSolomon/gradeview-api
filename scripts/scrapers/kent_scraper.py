#!/usr/bin/env python3.11
"""
Kent State Grade Distribution Scraper
Fetches grade distribution data from rpie-apps.kent.edu portal
"""

import requests
from bs4 import BeautifulSoup
import json
import csv
import time
import logging
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Set

# Configuration
BASE_URL = "https://rpie-apps.kent.edu/GradeDistribution/Index.aspx"
DATA_DIR = Path.home() / "Documents" / "GradeView" / "raw_data" / "kent"
CSV_PATH = DATA_DIR / "kent_grades_raw.csv"
PROGRESS_JSON = DATA_DIR / "scrape_progress.json"
LOG_PATH = DATA_DIR / "scrape_progress.log"
RATE_LIMIT_DELAY = 1.5  # seconds between requests

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Session for persistent cookies
session = requests.Session()


def extract_dropdown_values(html: str, dropdown_name: str) -> List[str]:
    """Extract all values from a dropdown/select element"""
    soup = BeautifulSoup(html, 'html.parser')
    select = soup.find('select', {'name': dropdown_name})
    
    if not select:
        logger.warning(f"Could not find dropdown: {dropdown_name}")
        return []
    
    values = []
    for option in select.find_all('option'):
        value = option.get('value', '').strip()
        if value and value != '0':  # Skip "Select" option
            values.append(value)
    
    return values


def extract_hidden_fields(html: str) -> Dict[str, str]:
    """Extract hidden form fields needed for ASP.NET postback"""
    soup = BeautifulSoup(html, 'html.parser')
    hidden_fields = {}
    
    for input_field in soup.find_all('input', {'type': 'hidden'}):
        name = input_field.get('name', '')
        value = input_field.get('value', '')
        if name:
            hidden_fields[name] = value
    
    return hidden_fields


def parse_grade_table(html: str) -> List[Dict]:
    """
    Parse the HTML grade distribution table (gvResult) and return list of grade records.
    
    The table has columns:
    [0] Faculty
    [1] Term
    [2] Campus
    [3] Course College
    [4] Course Department
    [5] Subject Code
    [6] Course Number
    [7] Section ID
    [8] Instructional Method
    [9+] Grade counts (A, B, C, D, F, W, ...)
    """
    soup = BeautifulSoup(html, 'html.parser')
    records = []
    
    # Look for table with id=cphMain_gvResult
    table = soup.find('table', {'id': 'cphMain_gvResult'})
    
    if not table:
        logger.debug("No grade distribution table (gvResult) found in response")
        return records
    
    # Get the header row to find grade column positions
    header_row = table.find('tr')
    if not header_row:
        return records
    
    header_cells = header_row.find_all(['th', 'td'])
    grade_columns = []
    
    # Grades typically start after the first 9 columns
    # Common grade letters: A, B, C, D, F, W, I, etc.
    if len(header_cells) > 9:
        for i, cell in enumerate(header_cells[9:], start=9):
            grade = cell.text.strip()
            if grade and len(grade) <= 2:  # Single or double letter grades
                grade_columns.append((i, grade))
    
    logger.debug(f"Found grade columns: {grade_columns}")
    
    # Get all data rows (skip header)
    rows = table.find_all('tr')[1:]
    
    for row in rows:
        cells = row.find_all('td')
        
        # Skip pagination rows (they'll have minimal cells or be inside tfoot)
        if len(cells) < 9:
            continue
        
        try:
            record = {
                'faculty': cells[0].text.strip(),
                'term': cells[1].text.strip(),
                'campus': cells[2].text.strip(),
                'college': cells[3].text.strip(),
                'department': cells[4].text.strip(),
                'subject_code': cells[5].text.strip(),
                'course_number': cells[6].text.strip(),
                'section_id': cells[7].text.strip(),
                'method': cells[8].text.strip(),
            }
            
            # Extract grade counts
            for col_idx, grade_letter in grade_columns:
                count_text = cells[col_idx].text.strip() if col_idx < len(cells) else '0'
                try:
                    record[f'grade_{grade_letter}'] = int(count_text)
                except ValueError:
                    record[f'grade_{grade_letter}'] = 0
            
            records.append(record)
            
        except (IndexError, AttributeError) as e:
            logger.debug(f"Error parsing row: {e}")
            continue
    
    return records


def load_progress() -> Dict:
    """Load progress tracking from JSON"""
    if PROGRESS_JSON.exists():
        try:
            with open(PROGRESS_JSON, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            logger.warning("Could not load progress file, starting fresh")
            return {'completed': [], 'failed': []}
    
    return {'completed': [], 'failed': []}


def save_progress(progress: Dict):
    """Save progress tracking to JSON"""
    with open(PROGRESS_JSON, 'w') as f:
        json.dump(progress, f, indent=2)


def initialize_csv_header(fieldnames: List[str]):
    """Initialize CSV with header row if it doesn't exist"""
    if not CSV_PATH.exists():
        with open(CSV_PATH, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
        logger.info(f"Created CSV: {CSV_PATH}")


def append_records_to_csv(records: List[Dict], fieldnames: List[str]):
    """Append grade records to CSV"""
    if not records:
        return
    
    with open(CSV_PATH, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, restval='')
        
        for record in records:
            writer.writerow(record)


def fetch_grades(term: str, subject_code: str) -> Tuple[List[Dict], bool]:
    """
    Fetch grade distribution data for a specific term+subject combo
    Returns: (records_list, success_flag)
    """
    try:
        logger.info(f"Fetching data for term={term}, subject_code={subject_code}")
        
        # First, get the page to extract hidden fields
        response = session.get(BASE_URL, timeout=10)
        response.raise_for_status()
        
        hidden_fields = extract_hidden_fields(response.text)
        
        # Prepare form data
        form_data = hidden_fields.copy()
        form_data['ctl00$cphMain$cmbTerm$cmbTerm'] = term
        form_data['ctl00$cphMain$cmbSubjectCode$cmbSubjectCode'] = subject_code
        form_data['ctl00$cphMain$cmbCampus$cmbCampus'] = '0'  # All campuses
        form_data['ctl00$cphMain$cmbCollege$cmbCollege'] = '0'  # All colleges
        form_data['ctl00$cphMain$cmbDepartment$cmbDepartment'] = '0'  # All departments
        
        # Add the search button
        form_data['ctl00$cphMain$btnSearch'] = 'Search'
        
        # POST the form
        time.sleep(RATE_LIMIT_DELAY)  # Rate limiting
        response = session.post(BASE_URL, data=form_data, timeout=10)
        response.raise_for_status()
        
        # Parse the response
        records = parse_grade_table(response.text)
        logger.info(f"Found {len(records)} grade records for term={term}, subject_code={subject_code}")
        
        return records, True
        
    except requests.RequestException as e:
        logger.error(f"Request failed for term={term}, subject_code={subject_code}: {e}")
        return [], False


def get_all_terms() -> List[str]:
    """Fetch all available terms from the dropdown"""
    try:
        response = session.get(BASE_URL, timeout=10)
        response.raise_for_status()
        terms = extract_dropdown_values(response.text, 'ctl00$cphMain$cmbTerm$cmbTerm')
        logger.info(f"Found {len(terms)} available terms")
        return terms
    except Exception as e:
        logger.error(f"Failed to get terms: {e}")
        return []


def get_all_subject_codes() -> List[str]:
    """Fetch all available subject codes from the dropdown"""
    try:
        response = session.get(BASE_URL, timeout=10)
        response.raise_for_status()
        codes = extract_dropdown_values(response.text, 'ctl00$cphMain$cmbSubjectCode$cmbSubjectCode')
        logger.info(f"Found {len(codes)} available subject codes")
        return codes
    except Exception as e:
        logger.error(f"Failed to get subject codes: {e}")
        return []


def scrape(terms: List[str] = None, subject_codes: List[str] = None, test_mode: bool = False):
    """
    Main scraping function
    
    Args:
        terms: List of term codes to scrape. If None, fetch all available.
        subject_codes: List of subject codes to scrape. If None, fetch all available.
        test_mode: If True, only process one term+subject combo
    """
    logger.info("=== Kent State Grade Distribution Scraper Started ===")
    logger.info(f"Test mode: {test_mode}")
    
    # Get terms and subject codes
    if not terms:
        terms = get_all_terms()
    if not subject_codes:
        subject_codes = get_all_subject_codes()
    
    if not terms or not subject_codes:
        logger.error("Could not retrieve terms or subject codes")
        return
    
    # Fetch one sample to determine fieldnames
    logger.info("Fetching sample data to determine column structure...")
    sample_records, _ = fetch_grades(terms[0], subject_codes[0])
    
    if sample_records:
        fieldnames = list(sample_records[0].keys())
    else:
        # Fallback fieldnames if no data
        fieldnames = ['faculty', 'term', 'campus', 'college', 'department', 'subject_code', 
                     'course_number', 'section_id', 'method']
    
    logger.info(f"Determined fieldnames: {fieldnames}")
    
    # Initialize CSV with discovered fieldnames
    initialize_csv_header(fieldnames)
    
    # Load progress
    progress = load_progress()
    completed = set(progress.get('completed', []))
    
    logger.info(f"Will process {len(terms)} terms × {len(subject_codes)} subject codes = {len(terms) * len(subject_codes)} combinations")
    
    total_records = 0
    processed = 0
    
    for term in terms:
        for subject_code in subject_codes:
            combo_key = f"{term}_{subject_code}"
            
            if combo_key in completed:
                logger.debug(f"Skipping already completed: {combo_key}")
                continue
            
            records, success = fetch_grades(term, subject_code)
            
            if success:
                if records:
                    append_records_to_csv(records, fieldnames)
                progress['completed'].append(combo_key)
                total_records += len(records)
                processed += 1
                logger.info(f"Saved {len(records)} records. Progress: {processed}/{len(terms) * len(subject_codes)}")
            else:
                progress['failed'].append(combo_key)
                logger.warning(f"Failed: {combo_key}")
            
            save_progress(progress)
            
            # Test mode: stop after one combo
            if test_mode:
                logger.info("Test mode: completed one combo, stopping")
                break
        
        if test_mode:
            break
    
    logger.info(f"=== Scraping Completed ===")
    logger.info(f"Total records saved: {total_records}")
    logger.info(f"Completed combos: {len(progress['completed'])}")
    logger.info(f"Failed combos: {len(progress['failed'])}")


if __name__ == '__main__':
    import sys
    
    # Test with only term=202580 and subject_code=ACCT
    logger.info("Running in TEST mode with term=202580, subject_code=ACCT")
    scrape(terms=['202580'], subject_codes=['ACCT'], test_mode=True)
