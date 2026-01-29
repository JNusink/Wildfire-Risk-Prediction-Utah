import duckdb
import requests
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import json
import os
import time

CACHE_FILE = "weather_cache_nws.json"

print("=== ADDING LIVE WEATHER (NWS API + Cache) ===")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load or create cache
cache = {}
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, 'r') as f:
        cache = json.load(f)
print(f"Cache entries: {len(cache)}")

con = duckdb.connect('eco_pyric.duckdb')

# Load a small test batch (increase LIMIT or remove for full run)
df_fires = con.execute("""
SELECT latitude, longitude, acq_date, dust_exposure
FROM fire_events_with_dust
LIMIT 100
""").fetchdf()

print(f"Fetching weather for {len(df_fires):,} fire events")

weather_data = []
success = 0
fail = 0
cache_hits = 0

headers = {
    'User-Agent': '(WildfireRiskCapstone, jared@example.com)',  # Required - change email
    'Accept': 'application/geo+json'
}

for idx, row in tqdm(df_fires.iterrows(), total=len(df_fires), desc="Fetching weather"):
    key = f"{row['latitude']:.4f}_{row['longitude']:.4f}_{row['acq_date']}"
    
    if key in cache:
        weather_data.append({**cache[key], 'index': idx})
        cache_hits += 1
        success += 1
        continue
    
    try:
        lat = row['latitude']
        lon = row['longitude']

        # Step 1: Get point info
        url = f"https://api.weather.gov/points/{lat},{lon}"
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        point_data = r.json()

        # Step 2: Get observation station
        station_url = point_data['properties']['observationStations']
        r_station = requests.get(station_url, headers=headers, timeout=15)
        r_station.raise_for_status()
        stations = r_station.json()

        if 'observationStations' in stations and stations['observationStations']:
            station_id = stations['observationStations'][0]
            obs_url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
            r_obs = requests.get(obs_url, headers=headers, timeout=15)
            r_obs.raise_for_status()
            obs = r_obs.json()['properties']

            entry = {
                'tavg_c': obs.get('temperature', {}).get('value'),
                'rh_pct': obs.get('relativeHumidity', {}).get('value'),
                'wspd_kmh': obs.get('windSpeed', {}).get('value'),
                'prcp_mm': obs.get('precipitationLast3Hours', {}).get('value') or obs.get('precipitationLastHour', {}).get('value')
            }
            weather_data.append({**entry, 'index': idx})
            cache[key] = entry
            success += 1
        else:
            fail += 1
    except Exception as e:
        fail += 1

# Save updated cache
with open(CACHE_FILE, 'w') as f:
    json.dump(cache, f)

print(f"\nWeather fetch: {success} succeeded ({cache_hits} from cache), {fail} failed/skipped")

if weather_data:
    weather_df = pd.DataFrame(weather_data).set_index('index')
    df_merged = df_fires.join(weather_df)

    print("\nSample rows with NWS weather + dust (first 10):")
    print(df_merged[['acq_date', 'latitude', 'longitude', 'tavg_c', 'rh_pct', 'wspd_kmh', 'prcp_mm', 'dust_exposure']].head(10))

    con.register('merged_fires', df_merged)
    con.execute("CREATE OR REPLACE TABLE fire_events_with_weather_dust AS SELECT * FROM merged_fires")
    print("Saved to table 'fire_events_with_weather_dust'")
else:
    print("No weather data fetched — check network or NWS coverage.")

con.close()
print("Done.")