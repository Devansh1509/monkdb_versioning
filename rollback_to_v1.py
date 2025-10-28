from monkdb import client
from monkdb.client.exceptions import MonkProgrammingError

DB_HOST = "localhost"
DB_PORT = 4200
DB_USER = "devansh"
DB_PASSWORD = "devansh"
DB_SCHEMA = "public"
TABLE_NAME = "customer_data"
REPO_NAME = "repo"
SNAPSHOT_NAME = "snap_v1"

print("🚀 Connecting to MonkDB for rollback...")
connection = client.connect(
    f"http://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}",
    username=DB_USER
)
cursor = connection.cursor()
print("✅ Connected successfully.\n")

snapshot_full = f"{REPO_NAME}.{SNAPSHOT_NAME}"
# Restore snapshot to temporary table
print(f"⏪ Restoring snapshot {snapshot_full} into temporary table {DB_SCHEMA}.{TABLE_NAME}_restored ...")
try:
    cursor.execute(f"""RESTORE SNAPSHOT {snapshot_full} TABLE {DB_SCHEMA}.{TABLE_NAME} AS {DB_SCHEMA}.{TABLE_NAME}_restored WITH (wait_for_completion = true)""")
    connection.commit()
except MonkProgrammingError as e:
    print(f"❌ Error restoring snapshot: {e}")
    raise

print("✅ Snapshot restored to temporary table.\n")
# Swap tables
print("🔄 Swapping active table with restored table...")
cursor.execute(f"""ALTER CLUSTER SWAP TABLE {DB_SCHEMA}.{TABLE_NAME} TO {DB_SCHEMA}.{TABLE_NAME}_restored""")
connection.commit()
print("✅ Swap complete. Rollback applied.\n")

cursor.execute(f"SELECT COUNT(*) FROM {DB_SCHEMA}.{TABLE_NAME}")
final_count = cursor.fetchone()[0]
print(f"📊 Final row count after rollback: {final_count}")