import os
import pandas as pd
from monkdb import client
from monkdb.client.exceptions import MonkProgrammingError

DB_HOST = "localhost"
DB_PORT = 4200
DB_USER = "devansh"
DB_PASSWORD = "devansh"
DB_SCHEMA = "public"
TABLE_NAME = "customer_data"
REPO_NAME = "repo"
REPO_LOCATION = "/tmp/monk_snapshots"
SNAPSHOT_NAME = "snap_v1"

def safe_create_repository(cursor):
    # MonkDB may error if repository exists; attempt create and ignore "already exists" errors.
    try:
        cursor.execute(f"CREATE REPOSITORY {REPO_NAME} TYPE fs WITH (location = '{REPO_LOCATION}')")
    except MonkProgrammingError as e:
        # If the repo already exists, a specific error will be raised; just print and continue.
        print(f"ℹ️ Repository creation warning (may already exist): {e}")

print("🚀 Connecting to MonkDB...")
connection = client.connect(
    f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
    username=DB_USER
)
cursor = connection.cursor()
print("✅ Connected to MonkDB successfully.\n")

print("🗂️ Ensuring snapshot repository exists (or create)...")
safe_create_repository(cursor)
print(f"✅ Repository '{REPO_NAME}' ensured at {REPO_LOCATION}\n")

# Create table
cursor.execute(f"DROP TABLE IF EXISTS {DB_SCHEMA}.{TABLE_NAME}")
cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{TABLE_NAME} (
    id INT PRIMARY KEY,
    name TEXT,
    city TEXT,
    country TEXT,
    purchase_amount FLOAT,
    loyalty_score FLOAT,
    version INT
)
""")
connection.commit()
print("✅ Table created successfully.\n")

# Insert V1
print("📥 Loading and inserting Version 1 data (data_v1.csv)...")
df_v1 = pd.read_csv("data_v1.csv")
for _, row in df_v1.iterrows():
    cursor.execute(f"""INSERT INTO {DB_SCHEMA}.{TABLE_NAME}
        (id, name, city, country, purchase_amount, loyalty_score, version)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [int(row.id), row.name, row.city, row.country, float(row.purchase_amount), float(row.loyalty_score), 1])
connection.commit()
cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{TABLE_NAME} WHERE version=1")
count_v1 = cursor.fetchone()[0]
print(f"✅ Inserted {count_v1} records for version 1.\n")

# Create snapshot
snapshot_full = f"{REPO_NAME}.{SNAPSHOT_NAME}"
print(f"📸 Creating snapshot: {snapshot_full} ...")
cursor.execute(f"""CREATE SNAPSHOT {snapshot_full} TABLE {DB_SCHEMA}.{TABLE_NAME} WITH (wait_for_completion = true)""")
connection.commit()
print("✅ Snapshot created successfully.\n")

# Insert V2
print("📥 Loading and inserting Version 2 data (data_v2.csv)...")
df_v2 = pd.read_csv("data_v2.csv")
for _, row in df_v2.iterrows():
    cursor.execute(f"""INSERT INTO {DB_SCHEMA}.{TABLE_NAME}
        (id, name, city, country, purchase_amount, loyalty_score, version)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [int(row.id), row.name, row.city, row.country, float(row.purchase_amount), float(row.loyalty_score), 2])
connection.commit()
cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{TABLE_NAME}")
total = cursor.fetchone()[0]
print(f"✅ Inserted v2 data. Total rows now: {total}\n")

print("🎯 insert_versions.py completed. Inspect the table in MonkDB before running rollback_to_v1.py.")