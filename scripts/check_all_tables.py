import duckdb

print("=== CHECKING ALL TABLES IN YOUR DATABASE ===")

con = duckdb.connect('eco_pyric.duckdb')

# List all tables
tables = con.execute("SHOW TABLES").fetchdf()
print("\nTables in database:")
print(tables)

# For each table, show row count and first 5 rows
for table_name in tables['name']:
    try:
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"\nTable '{table_name}': {count:,} rows")
        
        # Show first 5 rows
        sample = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
        print(f"First 5 rows:")
        print(sample)
    except Exception as e:
        print(f"  Could not access '{table_name}': {e}")

con.close()
print("\nCheck complete.")