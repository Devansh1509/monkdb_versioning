import os
import pandas as pd
from monkdb import client

DB_HOST = "localhost"
DB_PORT = 4200
DB_USER = "devansh"
DB_PASSWORD = "devansh"
DB_SCHEMA = "public"
TABLE_NAME = "customer_data"

print("🚀 Connecting to MonkDB...")
connection = client.connect(
    f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
    username=DB_USER
)
cursor = connection.cursor()
print("✅ Connected to MonkDB successfully.\n")

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

print("📥 Loading and inserting Version 1 data...")
df_v1 = pd.read_csv("data_v1.csv")
for _, row in df_v1.iterrows():
    cursor.execute(f"""
        INSERT INTO {DB_SCHEMA}.{TABLE_NAME}
        (id, name, city, country, purchase_amount, loyalty_score, version)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [int(row.id), row.name, row.city, row.country, row.purchase_amount, row.loyalty_score, 1])
connection.commit()

cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{TABLE_NAME} WHERE version=1")
count_v1 = cursor.fetchone()[0]
print(f"✅ Inserted {count_v1} records for version 1.\n")

snapshot_name = "repo.snap_v1"
print(f"📸 Creating snapshot: {snapshot_name} ...")
cursor.execute(f"""
CREATE SNAPSHOT {snapshot_name}
TABLE {DB_SCHEMA}.{TABLE_NAME}
WITH (wait_for_completion = true)
""")
print("✅ Snapshot created successfully.\n")

print("📥 Loading and inserting Version 2 data...")
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
print(f"✅ Version 2 data inserted. Total records now: {total_count}\n")

cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{TABLE_NAME} WHERE version=1")
v1_count = cursor.fetchone()[0]
cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{TABLE_NAME} WHERE version=2")
v2_count = cursor.fetchone()[0]
print(f"📊 Record Count Before Rollback → v1: {v1_count}, v2: {v2_count}\n")

print("⏪ Rolling back to Version 1...")
cursor.execute(f"""
RESTORE SNAPSHOT {snapshot_name}
TABLE {DB_SCHEMA}.{TABLE_NAME} AS {DB_SCHEMA}.{TABLE_NAME}_restored
WITH (wait_for_completion = true)
""")
connection.commit()
print("✅ Restored snapshot as separate table.\n")

print("🔄 Swapping active table with restored v1 table...")
cursor.execute(f"""
ALTER CLUSTER SWAP TABLE {DB_SCHEMA}.{TABLE_NAME} TO {DB_SCHEMA}.{TABLE_NAME}_restored
""")
connection.commit()
print("✅ Rollback completed successfully!\n")

cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{TABLE_NAME}")
final_count = cursor.fetchone()[0]
print(f"📊 Record Count After Rollback → {final_count} rows (should equal v1 count = {v1_count})\n")

print("🎯 MonkDB Versioning + Rollback Demo Completed Successfully ✅")
