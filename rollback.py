from monkdb import client

DB_HOST = "localhost"
DB_PORT = 4200
DB_USER = "devansh"
DB_PASSWORD = "devansh"
DB_SCHEMA = "public"
TABLE_NAME = "customer_data"
SNAPSHOT_REPO = "repo"
SNAPSHOT_NAME = "snap_v1"

print("🚀 Connecting to MonkDB for rollback...")
connection = client.connect(
    f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
    username=DB_USER
)
cursor = connection.cursor()
print("✅ Connected successfully.\n")

snapshot_name = f"{SNAPSHOT_REPO}.{SNAPSHOT_NAME}"

# ================================
# RESTORE SNAPSHOT INTO TEMP TABLE
# ================================
print(f"⏪ Restoring snapshot '{snapshot_name}'...")
cursor.execute(f"""
RESTORE SNAPSHOT {snapshot_name}
TABLE {DB_SCHEMA}.{TABLE_NAME} AS {DB_SCHEMA}.{TABLE_NAME}_restored
WITH (wait_for_completion = true)
""")
connection.commit()
print("✅ Snapshot restored into temporary table.\n")

# ================================
# SWAP TABLES
# ================================
print("🔄 Swapping current and restored table...")
cursor.execute(f"""
ALTER CLUSTER SWAP TABLE {DB_SCHEMA}.{TABLE_NAME} TO {DB_SCHEMA}.{TABLE_NAME}_restored
""")
connection.commit()
print("✅ Rollback to Version 1 completed.\n")

# ================================
# VALIDATE FINAL STATE
# ================================
cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{TABLE_NAME}")
final_count = cursor.fetchone()[0]
print(f"📊 Record count after rollback: {final_count} (should match v1 count)\n")

print("🎯 MonkDB Rollback Validation Completed Successfully ✅")
