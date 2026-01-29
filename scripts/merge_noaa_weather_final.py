import duckdb
import pandas as pd
import numpy as np
import os
from datetime import datetime
from tqdm import tqdm
from scipy.spatial.distance import cdist

print("=== FINAL NOAA WEATHER MERGE – UTAH-FOCUSED ===")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ================= CONFIGURATION =================
DB_FILE = 'eco_pyric.duckdb'
WEATHER_DIR = 'data/weather_noaa/'  # Your weather folder
OUTPUT_TABLE = 'fire_events_with_noaa_weather'

# ================= CONNECT TO DUCKDB =================
con = duckdb.connect(DB_FILE)
print("Connected to DuckDB.")

# ================= LOAD & CLEAN NOAA WEATHER =================
print("Loading NOAA weather files...")
weather_dfs = []

for filename in os.listdir(WEATHER_DIR):
    if filename.startswith('weather') or filename.endswith('.csv'):
        filepath = os.path.join(WEATHER_DIR, filename)
        try:
            df = pd.read_csv(filepath, parse_dates=['DATE'])
            print(f"  Loaded {filename} ({len(df):,} rows)")
            weather_dfs.append(df)
        except Exception as e:
            print(f"  Error loading {filename}: {e}")

if not weather_dfs:
    print("No files loaded — check 'data/weather_noaa/' folder.")
    exit(1)

df_weather = pd.concat(weather_dfs, ignore_index=True)
print(f"Total raw weather records: {len(df_weather):,}")

# Deduplicate: keep row with most non-null values per STATION + DATE
df_weather['DATE'] = pd.to_datetime(df_weather['DATE']).dt.date
df_weather['non_null_count'] = df_weather.notna().sum(axis=1)

df_weather_clean = df_weather.loc[df_weather.groupby(['STATION', 'DATE'])['non_null_count'].idxmax()]
df_weather_clean = df_weather_clean.drop(columns=['non_null_count'])

print(f"After deduplication: {len(df_weather_clean):,} unique station-days")

# Keep useful columns
keep_cols = ['STATION', 'NAME', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'DATE', 'TAVG', 'TMAX', 'TMIN', 'AWND', 'PRCP', 'SNOW', 'SNWD', 'WSF2', 'WSF5']
df_weather_clean = df_weather_clean[[col for col in keep_cols if col in df_weather_clean.columns]]

# ================= LOAD UTAH FIRE DATA (FAST & FOCUSED) =================
print("Loading Utah fire data...")
df_fires = con.execute("""
SELECT latitude, longitude, acq_date, dust_exposure
FROM fire_events_utah
""").fetchdf()

print(f"Loaded {len(df_fires):,} Utah fire events")

# ================= NEAREST STATION FOR EACH FIRE =================
print("Mapping nearest stations...")
fire_coords = df_fires[['latitude', 'longitude']].values
station_coords = df_weather_clean[['LATITUDE', 'LONGITUDE']].drop_duplicates().values

distances = cdist(fire_coords, station_coords, metric='euclidean')
nearest_idx = distances.argmin(axis=1)

df_fires['nearest_station'] = df_weather_clean.iloc[nearest_idx]['STATION'].values

# ================= MERGE WEATHER =================
df_fires['acq_date'] = pd.to_datetime(df_fires['acq_date']).dt.date
df_weather_clean['DATE'] = pd.to_datetime(df_weather_clean['DATE']).dt.date

df_merged = df_fires.merge(
    df_weather_clean,
    left_on=['nearest_station', 'acq_date'],
    right_on=['STATION', 'DATE'],
    how='left'
)

print("\nSample merged rows (first 10):")
print(df_merged[['acq_date', 'latitude', 'longitude', 'dust_exposure', 'TAVG', 'TMAX', 'TMIN', 'AWND', 'PRCP', 'SNOW']].head(10))

# Save merged table
con.register('merged_fires', df_merged)
con.execute(f"CREATE OR REPLACE TABLE {OUTPUT_TABLE} AS SELECT * FROM merged_fires")

print(f"\nSaved merged data to table '{OUTPUT_TABLE}'")
print("Done.")