import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

print("=== CREATING UTAH GRID + BINARY IGNITION LABELS ===")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

con = duckdb.connect('eco_pyric.duckdb')

# Load Utah fire events
df_fires = con.execute("""
SELECT latitude, longitude, acq_date
FROM fire_events_utah
""").fetchdf()

print(f"Loaded {len(df_fires):,} Utah fire events")

# Step 1: Define Utah grid (0.1 degree resolution)
lat_min, lat_max = 37.0, 42.0
lon_min, lon_max = -114.0, -109.0
lat_step, lon_step = 0.1, 0.1

lats = np.round(np.arange(lat_min, lat_max + lat_step, lat_step), 1)
lons = np.round(np.arange(lon_min, lon_max + lon_step, lon_step), 1)

grid = [(lat, lon) for lat in lats for lon in lons]
print(f"Created {len(grid):,} grid cells")

# Step 2: Create date range
min_date = pd.to_datetime(df_fires['acq_date']).min().date()
max_date = pd.to_datetime(df_fires['acq_date']).max().date()
date_range = pd.date_range(min_date, max_date).date

print(f"Date range: {min_date} to {max_date} ({len(date_range)} days)")

# Step 3: Generate grid-date combinations (no ignition column yet)
df_grid_dates = pd.DataFrame(
    [(lat, lon, date) for lat, lon in grid for date in date_range],
    columns=['grid_lat', 'grid_lon', 'date']
)
print(f"Total grid-date combinations: {len(df_grid_dates):,}")

# Step 4: Label ignition (1 if any fire in cell on that day)
df_fires['acq_date'] = pd.to_datetime(df_fires['acq_date']).dt.date

# Round fire locations to grid resolution (use round to nearest 0.1)
df_fires['grid_lat'] = np.round(df_fires['latitude'] / lat_step) * lat_step
df_fires['grid_lon'] = np.round(df_fires['longitude'] / lon_step) * lon_step

# Group fires by grid + date
df_fires_grouped = (
    df_fires
    .groupby(['grid_lat', 'grid_lon', 'acq_date'])
    .size()
    .reset_index(name='fire_count')
)
df_fires_grouped['ignition'] = 1

print("Labeling ignition days...")

# Merge to set ignition = 1 where fires occurred
df_grid_dates = df_grid_dates.merge(
    df_fires_grouped[['grid_lat', 'grid_lon', 'acq_date', 'ignition']],
    left_on=['grid_lat', 'grid_lon', 'date'],
    right_on=['grid_lat', 'grid_lon', 'acq_date'],
    how='left'
)

# Fill missing ignition values with 0 and ensure int type
df_grid_dates['ignition'] = df_grid_dates['ignition'].fillna(0).astype(int)

# Clean up extra column from merge
df_grid_dates = df_grid_dates.drop(columns=['acq_date'], errors='ignore')

print(f"Grid-date dataset created: {len(df_grid_dates):,} rows")
print("Ignition class balance:")
print(df_grid_dates['ignition'].value_counts(normalize=True))

# Save to DuckDB
con.register('grid_labels', df_grid_dates)
con.execute("CREATE OR REPLACE TABLE utah_grid_ignition_labels AS SELECT * FROM grid_labels")

con.close()
print("Saved Utah grid + binary ignition labels to table 'utah_grid_ignition_labels'")
print("Done — ready for training a daily risk classifier!")