#!/usr/bin/env python3
"""
██████████████████████████████████████████████████████
██  DEPRECATED — DO NOT USE                         ██
██  Use upload_v2.py instead.                       ██
██  This script has been replaced for safety.       ██
██  See RULES.md Rule #5.                           ██
██████████████████████████████████████████████████████

Reason: HTTP batch uploads are unreliable for large datasets.
        Token was hardcoded. No transaction safety.
        Replaced by upload_v2.py with parameterized queries,
        progress checkpointing, and integrity verification.

Old script preserved at: upload_school_DEPRECATED.py
"""
import sys
print("❌ This script is DEPRECATED. Use upload_v2.py instead.")
print("   See RULES.md Rule #5.")
print("   Example: caffeinate -i python3 upload_v2.py --school <id> --file <path>")
sys.exit(1)
