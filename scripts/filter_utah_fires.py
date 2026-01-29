import duckdb

print("=== FILTERING TO UTAH-ONLY FIRES ===")

con = duckdb.connect('eco_pyric.duckdb')

# Check if dust table exists
tables = con.execute("SHOW TABLES").fetchdf()['name'].tolist()

source_table = 'fire_events_with_dust' if 'fire_events_with_dust' in tables else 'fire_events'
print(f"Using source table: {source_table}")

con.execute(f"""
CREATE OR REPLACE TABLE fire_events_utah AS
SELECT *
FROM {source_table}
WHERE latitude BETWEEN 37 AND 42
  AND longitude BETWEEN -114 AND -109
""")

utah_count = con.execute("SELECT COUNT(*) FROM fire_events_utah").fetchone()[0]
print(f"Utah-only table created with {utah_count:,} fires")

# Utah yearly summary
print("\nUtah fires by year:")
print(con.execute("""
SELECT strftime('%Y', strptime(acq_date, '%Y-%m-%d')) AS year, COUNT(*) AS fire_count
FROM fire_events_utah
GROUP BY year
ORDER BY year
""").fetchdf())

con.close()
print("Done.")