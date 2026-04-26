#!/usr/bin/env python3.11
"""
UTSA Grade Distribution PDF Extractor
Extracts grade data from UTSA PDFs into CSV format
"""

import pdfplumber
import csv
import os
import re
from pathlib import Path
from typing import List, Dict, Tuple

def parse_filename(filename: str) -> Tuple[int, str]:
    """Extract year and semester from filename"""
    # e.g., Grade_Distribution_Fall_2020.pdf
    match = re.search(r'(Fall|Spring|Summer)_(\d{4})', filename)
    if match:
        semester = match.group(1).upper()
        year = int(match.group(2))
        return year, semester
    return None, None

def extract_grades_from_pdf(pdf_path: str) -> List[Dict]:
    """Extract all grade records from a single PDF"""
    records = []
    
    filename = os.path.basename(pdf_path)
    year, semester = parse_filename(filename)
    
    if year is None:
        print(f"Warning: Could not parse year/semester from {filename}")
        return records
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            current_coll = None
            current_dept = None
            current_course = None
            current_section = None
            current_crn = None
            current_title = None
            current_instructor = None
            
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                
                lines = text.split('\n')
                
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Parse COLL line
                    if line.startswith('COLL '):
                        current_coll = line.replace('COLL ', '').strip()
                        continue
                    
                    # Parse DEPT line
                    if line.startswith('DEPT '):
                        current_dept = line.replace('DEPT ', '').strip()
                        continue
                    
                    # Parse SUB/CRS line
                    if line.startswith('SUB/CRS '):
                        current_course = line.replace('SUB/CRS ', '').strip()
                        continue
                    
                    # Parse SEC line
                    if line.startswith('SEC '):
                        current_section = line.replace('SEC ', '').strip()
                        continue
                    
                    # Parse CRN line (has CRN, title, and instructor)
                    if line.startswith('CRN '):
                        # Format: CRN 16592 Foundations Of Accounting Vaello, Linda
                        parts = line.split(maxsplit=2)
                        if len(parts) >= 2:
                            current_crn = parts[1]
                            # Everything after CRN number is title + instructor
                            remainder = line[4+len(current_crn):].strip()
                            
                            # Instructor is "Lastname, Firstname" at end of line.
                            # Find the instructor by locating the last word before a comma
                            # that looks like a proper name (title-cased, no numbers).
                            # Pattern: "Course Title Words Lastname, Firstname MiddleName?"
                            # Strategy: find last comma, then walk back one token = last name.
                            if ',' in remainder:
                                last_comma = remainder.rfind(',')
                                before_comma = remainder[:last_comma].strip()
                                after_comma = remainder[last_comma+1:].strip()
                                # Last word before comma = last name
                                before_words = before_comma.rsplit(None, 1)
                                if len(before_words) == 2 and len(after_comma) < 40:
                                    course_title = before_words[0].strip()
                                    last_name = before_words[1].strip()
                                    first_name = after_comma.strip()
                                    current_title = course_title
                                    current_instructor = f"{last_name}, {first_name}"
                                else:
                                    current_title = remainder
                                    current_instructor = "Unknown"
                            else:
                                current_title = remainder
                                current_instructor = "Unknown"
                        continue
                    
                    # Skip TOTALS lines
                    if 'TOTALS' in line:
                        continue
                    
                    # Parse grade lines: "A 7 12.50%" or "A+ 3 9.09%"
                    # Format: GRADE # OF LTR GRADE %
                    grade_match = re.match(r'^([A-F][+-]?)\s+(\d+)\s+', line)
                    if grade_match and current_course and current_crn:
                        grade = grade_match.group(1)
                        count = int(grade_match.group(2))
                        
                        # Only extract dept code from course (e.g., "ACC" from "ACC 2003")
                        course_parts = current_course.split()
                        dept_code = course_parts[0] if course_parts else current_dept
                        course_number = course_parts[1] if len(course_parts) > 1 else ""
                        
                        record = {
                            'school_id': 'utsa',
                            'year': year,
                            'semester': semester,
                            'dept': dept_code,
                            'course_number': course_number,
                            'instructor': current_instructor or "Unknown",
                            'grade': grade,
                            'count': count
                        }
                        records.append(record)
    
    except Exception as e:
        print(f"Error processing {pdf_path}: {e}")
    
    return records

def main():
    pdf_dir = Path.home() / "Documents" / "GradeView" / "raw_data" / "utsa"
    output_file = pdf_dir / "utsa_grades_extracted.csv"
    
    # Get all PDF files, excluding duplicates
    pdf_files = sorted([f for f in pdf_dir.glob("*.pdf") 
                       if not f.name.endswith("(1).pdf")])
    
    print(f"Found {len(pdf_files)} PDF files")
    
    all_records = []
    failed_files = []
    
    for pdf_file in pdf_files:
        print(f"Processing: {pdf_file.name}...", end=" ", flush=True)
        records = extract_grades_from_pdf(str(pdf_file))
        
        if records:
            all_records.extend(records)
            print(f"✓ ({len(records)} records)")
        else:
            print("⚠ No records extracted")
            failed_files.append(pdf_file.name)
    
    # Write to CSV
    if all_records:
        fieldnames = ['school_id', 'year', 'semester', 'dept', 'course_number', 'instructor', 'grade', 'count']
        
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_records)
        
        print(f"\n✓ Output written to: {output_file}")
        print(f"✓ Total records: {len(all_records)}")
        
        if failed_files:
            print(f"⚠ Failed files: {', '.join(failed_files)}")
        
        return True
    else:
        print("\n✗ No records extracted from any file")
        return False

if __name__ == "__main__":
    main()
