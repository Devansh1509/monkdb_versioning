# MonkDB Versioning & Rollback Demo (fixed)

## Contents
- data_v1.csv      (100 rows)
- data_v2.csv      (100 rows)
- insert_versions.py
- rollback_to_v1.py
- requirements.txt

## Requirements
- Python 3.8+
- MonkDB running and reachable at host/port configured in scripts (default localhost:4200)

## Quick usage (Linux / WSL)
1. Create venv and install:
   ```
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
2. Run insertion (creates repo, inserts v1, snapshots, inserts v2):
   ```
   python3 insert_versions.py
   ```
3. Inspect the table in MonkDB (optional).
4. Run rollback to revert to v1:
   ```
   python3 rollback_to_v1.py
   ```

## Notes
- Snapshot repository location is set to: /tmp/monk_snapshots
- If you are on Windows, change REPO_LOCATION in insert_versions.py to a Windows path (e.g., C:\\monk_snapshots) and ensure MonkDB has access.
- Scripts intentionally avoid unsupported SQL syntax; repository creation tolerates existing repo by catching errors.