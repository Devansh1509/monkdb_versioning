import os
import pandas as pd
from monkdb import client

DB_HOST = "localhost"
DB_PORT = 4200
DB_USER = "devansh"
DB_PASSWORD = "devansh"
DB_SCHEMA = "public"
TABLE_NAME = "customer_data"
REPO_LOCATION = "/tmp/monk_snapshots/"
SNAPSHOT_REPO = "repo"
SNAPSHOT_NAME = "snap_v1"

print("🚀 Connecting to MonkDB...")
connection = client.connect(
    f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
    username=DB_USER
)
cursor = connection.cursor()
print("✅ Connected to MonkDB successfully.\n")

print(f"🗂️ Ensuring snapshot repository '{SNAPSHOT_REPO}' exists...")
cursor.execute(f"""
CREATE REPOSITORY IF NOT EXISTS {SNAPSHOT_REPO}
TYPE fs
WITH (location = '{REPO_LOCATION}');
""")
connection.commit()

# ================================
# CREATE TABLE
# ================================
print("🧱 Creating table...")
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

# ================================
# INSERT VERSION 1 DATA
# ================================
print("📥 Loading Version 1 data...")
df_v1 = pd.read_csv("data_v1.csv")
for _, row in df_v1.iterrows():
    cursor.execute(f"""
        INSERT INTO {DB_SCHEMA}.{TABLE_NAME}
        (id, name, city, country, purchase_amount, loyalty_score, version)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [int(row.id), row.name, row.city, row.country, row.purchase_amount, row.loyalty_score, 1])
connection.commit()

cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{TABLE_NAME} WHERE version=1")
v1_count = cursor.fetchone()[0]
print(f"✅ Inserted {v1_count} records for version 1.\n")

# ================================
# CREATE SNAPSHOT
# ================================
snapshot_name = f"{SNAPSHOT_REPO}.{SNAPSHOT_NAME}"
print(f"📸 Creating snapshot: {snapshot_name} ...")
cursor.execute(f"""
CREATE SNAPSHOT {snapshot_name}
TABLE {DB_SCHEMA}.{TABLE_NAME}
WITH (wait_for_completion = true)
""")
connection.commit()
print("✅ Snapshot created successfully.\n")

# ================================
# INSERT VERSION 2 DATA
# ================================
print("📥 Loading Version 2 data...")
df_v2 = pd.read_csv("data_v2.csv")
for _, row in df_v2.iterrows():
    cursor.execute(f"""
        INSERT INTO {DB_SCHEMA}.{TABLE_NAME}
        (id, name, city, country, purchase_amount, loyalty_score, version)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [int(row.id), row.name, row.city, row.country, row.purchase_amount, row.loyalty_score, 2])
connection.commit()

cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{TABLE_NAME}")
total_count = cursor.fetchone()[0]
print(f"✅ Version 2 inserted successfully. Total records: {total_count}\n")

print("🎯 Step 1 Completed — Snapshot created and both versions inserted.")
print("👉 You can now check MonkDB for versioned data before running rollback.")
