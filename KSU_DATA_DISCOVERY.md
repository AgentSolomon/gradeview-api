# Kennesaw State University - Grade Distribution Data Discovery

## Summary
Successfully located and analyzed the KSU grade distribution data source. The data is hosted on a **Tableau Public dashboard**.

## Data Source Details

| Property | Value |
|----------|-------|
| **Portal URL** | https://campus.kennesaw.edu/offices-services/data-strategy/institutional-research/dashboards/grade-distribution-standard.php |
| **Dashboard Type** | Tableau Public (React SPA) |
| **Dashboard URL** | https://public.tableau.com/views/GradeDistributionReport_17539864988520/GradeDistributionReport |
| **Dashboard ID** | GradeDistributionReport_17539864988520 |
| **Institution** | Kennesaw State University Enterprise Information Management |

## Data Format Found

**Tableau Public Dashboard** - Interactive visualization rendered via React SPA

### Dashboard Features
- Interactive filters for College, Department, Course
- Grade distribution visualization
- Historical data (multiple years/semesters)
- Public access (no authentication required)

## Data Structure

Expected columns:
- **year** - Academic year (e.g., 2023-2024)
- **semester** - Spring, Summer, Fall
- **college** - College/School within KSU
- **department** - Academic department
- **course_number** - Course ID (e.g., CHEM 1011)
- **instructor** - Faculty member name
- **grade** - Grade letter (A, B, C, D, F)
- **count** - Number of students with that grade

## API Endpoints Discovered

| Endpoint | Status | Notes |
|----------|--------|-------|
| Main Dashboard | 200 ✓ | React SPA, requires JS execution |
| VizQL API | Internal | Used by Tableau, not directly accessible |
| CSV Export | 404 | Not available via direct URL |
| REST API | 404 | Tableau Server API not public |

## Data Access Challenge

❌ Data not directly accessible via HTTP  
✓ Requires **browser automation** (Selenium/Puppeteer)  
✓ Must execute JavaScript to load Tableau visualization  
✓ Then extract data from rendered DOM

## Estimated Data Volume

- **Rows**: 10,000 - 50,000+ (estimated)
  - 5-10 academic years
  - 3-4 semesters per year
  - Hundreds of courses
  - Multiple instructors per course
  - 5+ grade levels per combination

## Comparison with ANEX.us (Texas A&M)

| Aspect | ANEX | KSU |
|--------|------|-----|
| Platform | Custom web app | Tableau Public |
| Data Source | Direct table | Tableau viz |
| API Available | Yes | VizQL (complex) |
| Auth Required | No | No |
| Accessibility | High | Requires JS execution |

## Scraper Status

✅ **Scraper Created**: `ksu_scraper.py`  
✅ **Connectivity Verified**: Can reach portal and discover Tableau  
✅ **Framework in Place**: Resume-capable, rate-limited, logging  
⏳ **Data Extraction**: Pending Selenium/Playwright integration  

### Test Results
```
2026-04-01 14:56:10 - Successfully verified connectivity
2026-04-01 14:56:10 - Located Tableau iframe
2026-04-01 14:56:10 - Dashboard accessible (200 OK)
2026-04-01 14:56:10 - Identified dynamic data loading
```

## Implementation Path

1. ✅ Discover data source (DONE)
2. ✅ Create scraper framework (DONE)
3. ⏳ Integrate Selenium for JS execution
4. ⏳ Wait for Tableau data to render
5. ⏳ Parse table/JSON from DOM
6. ⏳ Handle pagination and filters
7. ⏳ Full scrape with resume capability

## Files

- `ksu_scraper.py` - Main scraper script (ready for Selenium integration)
- `scrape_progress.log` - Execution log
- `KSU_DATA_DISCOVERY.md` - This file

## Technical Notes

- Dashboard uses AWS WAF protection
- React SPA with dynamic content loading
- Tableau VizQL API handles data transport
- No public API documented by KSU
- Rate limiting: 1.5s between requests recommended
