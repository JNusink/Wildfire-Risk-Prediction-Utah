import duckdb
import os
from datetime import datetime

print("=== STEP 1: FRESH INGESTION WITH DUCKDB ===")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

db_file = "eco_pyric.duckdb"
target_files = [
    'data/firms/fire_archive_SV-C2_708466.csv',
    'data/firms/fire_nrt_SV-C2_708466.csv'
]

MIN_LAT, MAX_LAT = 31.0, 49.0
MIN_LON, MAX_LON = -125.0, -102.0
CONFIDENCE_EXCLUDE = 'l'

print(f"Database: {db_file}")
print(f"Files: {len(target_files)}")
print(f"Bounding box: Lat {MIN_LAT}–{MAX_LAT}, Lon {MIN_LON}–{MAX_LON}")
print(f"Excluding confidence = '{CONFIDENCE_EXCLUDE}'")
print("=" * 50 + "\n")

con = duckdb.connect(db_file)

# Drop any old table (clean start)
con.execute("DROP TABLE IF EXISTS fire_events")

# Create table
con.execute('''
CREATE TABLE fire_events (
    source_file VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    acq_date VARCHAR,
    brightness DOUBLE,
    frp DOUBLE,
    confidence VARCHAR
)
''')

total_rows = 0

for csv_file in target_files:
    if not os.path.exists(csv_file):
        print(f"[SKIP] File not found: {csv_file}")
        continue
    
    file_name = os.path.basename(csv_file)
    print(f"Processing {file_name}...")

    con.execute(f"""
    INSERT INTO fire_events
    SELECT
        '{file_name}' AS source_file,
        latitude,
        longitude,
        acq_date,
        brightness,
        frp,
        confidence
    FROM read_csv_auto('{csv_file}')
    WHERE latitude BETWEEN {MIN_LAT} AND {MAX_LAT}
      AND longitude BETWEEN {MIN_LON} AND {MAX_LON}
      AND confidence != '{CONFIDENCE_EXCLUDE}'
    """)

    rows_added = con.execute(f"SELECT COUNT(*) FROM fire_events WHERE source_file = '{file_name}'").fetchone()[0]
    total_rows += rows_added
    print(f"  Added {rows_added:,} rows")

print("\n" + "="*50)
print("INGESTION COMPLETE")
print(f"Total fires stored: {total_rows:,}")
print("Breakdown by source:")
print(con.execute("SELECT source_file, COUNT(*) FROM fire_events GROUP BY source_file").fetchdf())
print("Date range:")
print(con.execute("SELECT MIN(acq_date), MAX(acq_date) FROM fire_events").fetchdf())
print("="*50)

con.close()
print("Done. Database ready.")