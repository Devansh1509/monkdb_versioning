# MonkDB Data Versioning + Rollback Test

This project demonstrates how to:
1. Insert CSV data as version 1 (v1)
2. Snapshot the data
3. Append a new CSV as version 2 (v2)
4. Roll back to v1 using MonkDB snapshot + atomic swap

## 🧩 Steps to Run

```bash
# Step 1: Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Step 2: Install dependencies
pip install -r requirements.txt

# Step 3: Generate test CSV files
python generate_csvs.py

# Step 4: Run the versioning + rollback test
python main.py
```
