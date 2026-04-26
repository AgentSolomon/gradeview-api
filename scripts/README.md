# GradeView Upload Scripts

## Standard Upload (upload_v2.py)

```bash
cd ~/.openclaw/workspace/projects/gradeview/scripts

# AUTO-MAP (recommended for new schools — inspects headers, picks format)
python3 upload_v2.py --school gvsu --file ~/Downloads/gvsu.xlsx --auto-map --stage-only

# CSV upload with default mapping
python3 upload_v2.py --school ferrisstate --file ~/Downloads/ferris_grades.csv

# XLSX upload
python3 upload_v2.py --school cincinnati --file ~/Downloads/cincinnati.xlsx

# Multiple CSV files (glob)
python3 upload_v2.py --school utaustin --file "~/Downloads/UTAUSTIN/*.csv" --map utaustin

# Specific column mapping
python3 upload_v2.py --school tamu --file ~/Downloads/tamu.csv --map tamu
```

## Auto-Mapper (auto_mapper.py)

Inspects file headers and generates a column mapping automatically. Handles both standard format (one row per grade) and wide format (grade columns like A, B+, etc.).

```bash
# Inspect a file and see the proposed mapping
python3 auto_mapper.py --file ~/Downloads/newschool.xlsx --school newschool

# Just show headers and sample data (no mapping attempt)
python3 auto_mapper.py --file ~/Downloads/newschool.xlsx --inspect-only

# Output JSON for programmatic use
python3 auto_mapper.py --file ~/Downloads/newschool.xlsx --json
```

The auto-mapper detects: standard vs wide format, compound section columns (e.g., "AAA 200 01" split into dept + course), year embedded in semester strings (e.g., "Spring / Summer 2021"), and grade columns by name.

When used via `upload_v2.py --auto-map`, it also handles section-level deduplication (aggregating counts across sections of the same course).

## Column Mappings

| Mapping | Use for |
|---------|---------|
| `--auto-map` | NEW DEFAULT for unknown schools — auto-detects everything |
| `xlsx_generic` | Fallback — tries common column name variants |
| `utaustin` | UT Austin CSVs (Semester, Course Prefix, etc.) |
| `tamu` | TAMU format |
| `unlv` | UNLV wide format (A_GRADE, B_GRADE columns) |
| `neiu` | NEIU wide format (A/B/C/D/F columns per row, reads "Data" sheet) |

To add a new mapping, edit the `MAPPINGS` dict in `upload_v2.py`.

## Flags

| Flag | When to use |
|------|-------------|
| *(none)* | First time adding a school — deletes existing rows and re-uploads |
| `--append` | New semester update — adds rows without deleting existing data |
| `--resume` | Upload was interrupted — picks up where it left off |

## How It Works

1. Reads CSV/XLSX into memory
2. Builds a local SQLite staging DB (fast, no API calls)
3. Deletes existing rows for that school from Turso
4. Pushes staging DB → Turso via CLI (bulk, fast)
5. Verifies row count matches source

## After Every Upload

- ✅ Verify count printed at end
- ✅ Update `MEMORY.md` data table with new row count
- ✅ Update iCloud Excel tracker: `~/Library/Mobile Documents/com~apple~CloudDocs/OpenClaw/GradeView/GradeView_TPIA_Tracker.xlsx`

## School IDs

| School | ID |
|--------|----|
| Texas A&M | tamu |
| UT Austin | utaustin |
| U of Houston | uh |
| Purdue | purdue |
| U of West Georgia | westga |
| Shawnee State | shawnee |
| Northern Michigan | nmu |
| UW-Madison | uwmadison |
| Ferris State | ferrisstate |
| (new schools) | lowercase, no spaces, no dots |
