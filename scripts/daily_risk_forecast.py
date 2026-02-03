
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import duckdb
import folium
from folium.plugins import HeatMap

print("=== DAILY UTAH WILDFIRE RISK FORECAST V2 ===")
print(f"Date: {datetime.now().strftime('%Y-%m-%d')}")

con = duckdb.connect('eco_pyric.duckdb')

# Load full Utah grid + proximity
df_grid = con.execute("""
SELECT grid_lat, grid_lon, dist_to_road_km
FROM utah_grid_ignition_labels_proximity
""").fetchdf()

print(f"Loaded {len(df_grid):,} grid cells")

# Precompute dust exposure per grid cell (higher near lake center)
lake_lat, lake_lon = 41.0, -112.5
df_grid['dist_to_lake_km'] = np.sqrt((df_grid['grid_lat'] - lake_lat)**2 + (df_grid['grid_lon'] + lake_lon)**2) * 111
df_grid['dust_exposure'] = 1 / (df_grid['dist_to_lake_km'] + 1)
df_grid['dust_exposure'] = df_grid['dust_exposure'].clip(upper=1.0)

# Get today's weather forecast (demo uses one point - expand later)
url = "https://api.open-meteo.com/v1/forecast?latitude=40.5&longitude=-111.9&daily=temperature_2m_mean,relative_humidity_2m_mean,wind_speed_10m_max,precipitation_sum&timezone=auto"
r = requests.get(url)
data = r.json()['daily']

tavg = data['temperature_2m_mean'][0]
rh = data['relative_humidity_2m_mean'][0]
wspd = data['wind_speed_10m_max'][0]
prcp = data['precipitation_sum'][0]

print(f"Today's forecast: Tavg {tavg}°C, RH {rh}%, Wind {wspd} km/h, Precip {prcp} mm")

# Month for seasonal factor
df_grid['month'] = datetime.now().month

# Dryness proxies
df_grid['vpd_proxy'] = 0.6108 * np.exp(17.27 * tavg / (tavg + 237.3)) * (1 - rh / 100)
df_grid['vpd_proxy'] = df_grid['vpd_proxy'].clip(lower=0)

df_grid['dryness_proxy'] = (tavg - (tavg - 10)) / 10
df_grid['low_precip_dryness'] = np.where(prcp < 1, 1.0, 0.5)

# Risk score
df_grid['dryness'] = df_grid['vpd_proxy'] / df_grid['vpd_proxy'].max()
df_grid['precip_factor'] = np.maximum(0, 1 - prcp / 5)
df_grid['wind_factor'] = np.minimum(wspd / 20, 1.0)
df_grid['dust_factor'] = df_grid['dust_exposure'] * 2.0
df_grid['human_factor'] = np.maximum(0, 1 - df_grid['dist_to_road_km'] / 10)

df_grid['risk_score'] = (
    df_grid['dryness'] + df_grid['precip_factor'] + df_grid['wind_factor'] +
    df_grid['dust_factor'] + df_grid['human_factor'] + df_grid['low_precip_dryness']
) / 6

print("\nTop 10 highest risk grid cells today:")
print(df_grid.sort_values('risk_score', ascending=False)[['grid_lat', 'grid_lon', 'risk_score', 'dust_exposure', 'dist_to_road_km']].head(10))

# Create interactive map
m = folium.Map(location=[39.5, -111.5], zoom_start=7, tiles='CartoDB positron')

# Heatmap for risk score
heat_data = [[row['grid_lat'], row['grid_lon'], row['risk_score']] for _, row in df_grid.iterrows()]
HeatMap(heat_data, radius=12, max_zoom=13, gradient={0.2: 'blue', 0.5: 'yellow', 0.8: 'orange', 1.0: 'red'}).add_to(m)

# Clickable markers for high-risk cells
high_risk = df_grid[df_grid['risk_score'] > 0.5]
for _, row in high_risk.iterrows():
    folium.CircleMarker(
        location=[row['grid_lat'], row['grid_lon']],
        radius=5,
        color='red',
        fill=True,
        fill_color='red',
        fill_opacity=0.7,
        popup=f"Risk: {row['risk_score']:.3f}<br>Dust: {row['dust_exposure']:.3f}<br>Dist to road: {row['dist_to_road_km']:.1f} km"
    ).add_to(m)

m.save('plots/utah_daily_risk_map_v2.html')
print("Daily risk map V2 saved: plots/utah_daily_risk_map_v2.html — open in browser!")