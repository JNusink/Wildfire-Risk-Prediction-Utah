import duckdb
from datetime import datetime

print("=== INSPECTING eco_pyric.duckdb ===")
print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

con = duckdb.connect('eco_pyric.duckdb')

# List all tables
tables = con.execute("SHOW TABLES").fetchdf()
print("\nTables in database:")
print(tables)

# For each table: show row count and first 5 rows
for table_name in tables['name']:
    try:
        # Row count
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"\nTable '{table_name}': {count:,} rows")

        # First 5 rows (sample)
        sample = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
        print(f"First 5 rows of '{table_name}':")
        print(sample.to_string(index=False))
    except Exception as e:
        print(f"  Could not access '{table_name}': {e}")

con.close()
print("\nInspection complete.")