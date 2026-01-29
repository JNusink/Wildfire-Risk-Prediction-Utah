import pandas as pd
import os
from datetime import datetime
from meteostat import stations, daily  # FIXED: lowercase 'stations' and 'daily'
from tqdm import tqdm
import numpy as np  # For np.arange

# ================= CONFIGURATION =================
MIN_LAT, MAX_LAT = 31, 49
MIN_LON, MAX_LON = -125, -102

LAT_STEP = 2.0
LON_STEP = 2.0

START_DATE = datetime(2020, 1, 1)
END_DATE = datetime(2024, 12, 31)

OUTPUT_DIR = "weather_grid_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ================= MAIN SCRIPT =================
print(f"--- STARTING WESTERN US WEATHER GRID HARVEST ---")
print(f"Scanning from {MIN_LAT}N to {MAX_LAT}N and {MIN_LON}W to {MAX_LON}W...")

downloaded_stations = set()
manifest_data = []

# Progress bar setup
lat_points = np.arange(MIN_LAT, MAX_LAT + LAT_STEP, LAT_STEP)
lon_points = np.arange(MIN_LON, MAX_LON + LON_STEP, LON_STEP)
total_points = len(lat_points) * len(lon_points)

with tqdm(total=total_points, desc="Grid scan") as pbar:
    current_lat = MIN_LAT
    while current_lat <= MAX_LAT:
        current_lon = MIN_LON
        while current_lon <= MAX_LON:
            
            try:
                # Create Stations instance
                nearby = stations()
                # Find stations near this grid point
                nearby = nearby.nearby(current_lat, current_lon)
                # Filter for stations with daily data in 2023 (to ensure active)
                nearby = nearby.inventory('daily', (datetime(2023, 1, 1)))
                
                # Fetch the closest one
                station = nearby.fetch(1)
                
                if not station.empty:
                    station_id = station.index[0]
                    station_name = station.iloc[0]['name']
                    
                    if station_id not in downloaded_stations:
                        
                        print(f"   Grid ({current_lat}, {current_lon}) -> Found: {station_name} ({station_id})")
                        
                        # Download data
                        data = daily(station_id, START_DATE, END_DATE)
                        df = data.fetch()
                        
                        if not df.empty:
                            clean_df = df[['tavg', 'tmax', 'tmin', 'wspd', 'prcp']]
                            
                            filename = f"{station_id}.csv"
                            filepath = os.path.join(OUTPUT_DIR, filename)
                            clean_df.to_csv(filepath)
                            
                            manifest_data.append({
                                'station_id': station_id,
                                'name': station_name,
                                'latitude': station.iloc[0]['latitude'],
                                'longitude': station.iloc[0]['longitude'],
                                'filename': filename
                            })
                            
                            downloaded_stations.add(station_id)
                    
            except Exception as e:
                print(f"Error at {current_lat}, {current_lon}: {e}")
            
            current_lon += LON_STEP
            pbar.update(1)
        current_lat += LAT_STEP

# Save manifest
manifest_df = pd.DataFrame(manifest_data)
manifest_df.to_csv("weather_stations_manifest.csv", index=False)

print("\n" + "="*30)
print(f"HARVEST COMPLETE.")
print(f"Total Stations Downloaded: {len(downloaded_stations)}")
print(f"Data saved in folder: {OUTPUT_DIR}/")
print(f"Index file created: weather_stations_manifest.csv")
print("="*30)