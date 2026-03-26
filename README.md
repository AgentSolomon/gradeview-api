# GradeView

A multi-university grade distribution iOS app. Think anex.us but national, polished, and on the App Store.

## Vision
Students search by university → course or professor → see grade distribution charts (A/B/C/D/F/W) broken down by semester and instructor.

## Status
- [x] Domain: gradeview.app ✅
- [x] Apple Developer Account ✅
- [ ] Data sourcing
- [ ] App design
- [ ] Build
- [ ] App Store submission

## Target Schools (Phase 1)
### Texas
- Texas A&M (data proven accessible via anex.us)
- UT Austin
- UT Dallas (data proven accessible via utdgrades.com)
- Texas Tech
- University of Houston

### SEC
- Alabama, Auburn, LSU, Ole Miss, Mississippi State
- Georgia, Tennessee, Missouri, Arkansas
- Florida (strong Sunshine Law)
- Texas A&M (covered above)
- Oklahoma, Texas (new additions)

## Competition
- anex.us — TAMU only, web only
- utdgrades.com — UTD only, web only
- PlanetTerp — UMaryland only, web only
- Rate My Professors — reviews only, no grade data
- **Gap: no multi-university, mobile-first grade distribution app exists**

## Data Strategy
- Public universities subject to state open records laws
- Data = aggregate grade counts per course/section/professor/semester (not individual student records, so FERPA-safe)
- Texas schools: Texas Public Information Act
- Other states: equivalent open records laws
- Some schools post data proactively via IR office portals

## App Structure
1. Home — school picker + search bar
2. Results — bar/pie chart, filter by semester/professor
3. Professor Profile — all courses, grade trends over time
4. Comparison view — two professors side by side

## Tech Stack (planned)
- SwiftUI (native iOS)
- Xcode (on Mac mini)
- Backend TBD (to serve grade data via API)

## App Store Requirements Checklist
- [ ] Apple Developer Account ($99/yr, Individual)
- [ ] Privacy policy URL
- [ ] Real data (no placeholder content at submission)
- [ ] Native app feel (not a web wrapper)
- [ ] Clean UI meeting Apple HIG guidelines
- [ ] App Store screenshots + description

## Domain
- gradeview.app via Cloudflare Registrar
