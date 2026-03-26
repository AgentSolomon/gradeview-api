# UT Austin Load Results

## Summary
- Files processed: 16
- Rows inserted: 484804
- Rows skipped (unmapped grades): 100244

## Verification
- `SELECT COUNT(*) FROM grades WHERE school_id='utaustin'`: **907804**
- `SELECT COUNT(*) FROM grades` (entire DB): **1517830**
