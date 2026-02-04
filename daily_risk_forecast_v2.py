# scripts/daily_risk_forecast_v2.py
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import duckdb
import folium
from folium.plugins import HeatMap
from math import radians, sin, cos, sqrt, atan2
import os

print("=== DAILY UTAH WILDFIRE RISK FORECAST V2 ===")
print(f"Date: {datetime.now().strftime('%Y-%m-%d')}")

con = duckdb.connect('eco_pyric.duckdb')

# Load full Utah grid + proximity
df_grid = con.execute("""
SELECT grid_lat, grid_lon, dist_to_road_km
FROM utah_grid_ignition_labels_proximity
""").fetchdf()

print(f"Loaded {len(df_grid):,} grid cells")

# Restrict to Utah bounding box
UT_LAT_MIN, UT_LAT_MAX = 37.0, 42.0
UT_LON_MIN, UT_LON_MAX = -114.0, -109.0

df_grid = df_grid[
    (df_grid['grid_lat'] >= UT_LAT_MIN) &
    (df_grid['grid_lat'] <= UT_LAT_MAX) &
    (df_grid['grid_lon'] >= UT_LON_MIN) &
    (df_grid['grid_lon'] <= UT_LON_MAX)
].copy()

print(f"Grid cells after Utah clip: {len(df_grid):,}")

# Precompute dust exposure per grid cell (higher near lake center)
lake_lat, lake_lon = 41.0, -112.5
df_grid['dist_to_lake_km'] = np.sqrt(
    (df_grid['grid_lat'] - lake_lat) ** 2 +
    (df_grid['grid_lon'] - lake_lon) ** 2
) * 111
df_grid['dust_exposure'] = 1 / (df_grid['dist_to_lake_km'] + 1)
df_grid['dust_exposure'] = df_grid['dust_exposure'].clip(upper=1.0)

# NEW: Major Utah cities for human proximity [web:7][web:10]
cities = [
    ('Salt Lake City', 40.76, -111.89),
    ('West Valley City', 40.69, -112.00),
    ('Provo', 40.23, -111.66),
    ('West Jordan', 40.61, -111.94),
    ('Orem', 40.30, -111.70),
    ('Sandy', 40.59, -111.88),
    ('St. George', 37.10, -113.58),
    ('Ogden', 41.22, -111.97),
    ('Layton', 41.06, -111.97),
    ('Lehi', 40.39, -111.85),
    ('Logan', 41.74, -111.83),
    ('South Jordan', 40.56, -111.93)
]

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth km
    dlat, dlon = radians(lat2-lat1), radians(lon2-lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

# NEW: Nearest city distance per grid cell
df_grid['dist_to_city_km'] = df_grid.apply(
    lambda row: min(haversine(row['grid_lat'], row['grid_lon'], city_lat, city_lon) 
                    for _, city_lat, city_lon in cities), axis=1
)

# Get today's weather forecast
url = (
    "https://api.open-meteo.com/v1/forecast?"
    "latitude=40.5&longitude=-111.9&"
    "daily=temperature_2m_mean,relative_humidity_2m_mean,"
    "wind_speed_10m_max,precipitation_sum&timezone=auto"
)
r = requests.get(url)
data = r.json()['daily']

tavg = data['temperature_2m_mean'][0]
rh = data['relative_humidity_2m_mean'][0]
wspd = data['wind_speed_10m_max'][0]
prcp = data['precipitation_sum'][0]

print(
    f"Today's forecast (Saratoga Springs area): "
    f"Tavg {tavg}°C, RH {rh}%, Wind {wspd} km/h, Precip {prcp} mm"
)

# Month for seasonal factor
df_grid['month'] = datetime.now().month

# Dryness proxies
df_grid['vpd_proxy'] = (
    0.6108 * np.exp(17.27 * tavg / (tavg + 237.3)) * (1 - rh / 100)
)
df_grid['vpd_proxy'] = df_grid['vpd_proxy'].clip(lower=0)

df_grid['dryness_proxy'] = (tavg - (tavg - 10)) / 10
df_grid['low_precip_dryness'] = np.where(prcp < 1, 1.0, 0.5)

# Risk score components (UPDATED human_factor)
df_grid['dryness'] = df_grid['vpd_proxy'] / df_grid['vpd_proxy'].max()
df_grid['precip_factor'] = np.maximum(0, 1 - prcp / 5)
df_grid['wind_factor'] = np.minimum(wspd / 20, 1.0)
df_grid['dust_factor'] = df_grid['dust_exposure'] * 2.0
df_grid['human_factor'] = np.maximum(0, 1 - (df_grid['dist_to_road_km'] + df_grid['dist_to_city_km']) / 20)

df_grid['risk_score'] = (
    df_grid['dryness'] +
    df_grid['precip_factor'] +
    df_grid['wind_factor'] +
    df_grid['dust_factor'] +
    df_grid['human_factor'] +
    df_grid['low_precip_dryness']
) / 6

print("\nTop 10 highest risk grid cells today:")
print(
    df_grid.sort_values('risk_score', ascending=False)[
        ['grid_lat', 'grid_lon', 'risk_score',
         'dust_exposure', 'dist_to_road_km', 'dist_to_city_km']
    ].head(10)
)

# Clean NaNs before mapping
df_grid = df_grid.dropna(subset=['grid_lat', 'grid_lon', 'risk_score'])
print(f"\nTotal grid cells (after dropna): {len(df_grid):,}")

# High-risk subset for prediction markers
high_risk = df_grid[df_grid['risk_score'] > 0.5]
print(f"High-risk cells (>0.5): {len(high_risk):,}")

# Try to load current active fires from InciWeb API
current_fires = []
try:
    # InciWeb API for active fires in Utah
    inciweb_url = "https://inciweb.wildfire.gov/api/v1/fires"
    r = requests.get(inciweb_url, timeout=10)
    
    if r.status_code == 200:
        fires_data = r.json().get('fires', [])
        today = datetime.now().date()
        week_ago = today - timedelta(days=7)
        
        for fire in fires_data:
            # Filter for Utah fires that are currently active
            if (fire.get('state') == 'UT' and 
                fire.get('fireStatus') in ['Active', 'Contained', 'Controlled'] and
                fire.get('latitude') and fire.get('longitude')):
                
                fire_date = datetime.strptime(fire.get('startedOnDate', ''), '%Y-%m-%d').date()
                if week_ago <= fire_date <= today:
                    current_fires.append({
                        'lat': float(fire['latitude']),
                        'lon': float(fire['longitude']),
                        'name': fire.get('fireName', 'Unknown'),
                        'acres': fire.get('acresBurned', 0),
                        'status': fire.get('fireStatus', 'Active')
                    })
        print(f"Found {len(current_fires)} current Utah fires")
    else:
        print("InciWeb API unavailable, skipping current fires")
        
except Exception as e:
    print(f"Error fetching current fires: {e}")
    current_fires = []

# Create interactive map, centered on Utah
m = folium.Map(
    location=[39.0, -111.5],
    zoom_start=6,
    tiles='CartoDB positron'
)

# Force the map container to have height
m.get_root().html.add_child(folium.Element("""
<style>
  #map { position: relative; width: 100%; height: 600px; }
</style>
"""))

# Prediction Risk Heatmap (blue-green gradient)
heat_data = df_grid[['grid_lat', 'grid_lon', 'risk_score']].values.tolist()
print("Sample heat_data rows:", heat_data[:5])

if len(heat_data) > 0:
    HeatMap(
        heat_data,
        radius=8,
        blur=15,
        gradient={0.2: 'blue', 0.4: 'green', 0.6: 'yellow', 0.8: 'orange', 1.0: 'red'},
        min_opacity=0.3,
        max_zoom=13
    ).add_to(m)
else:
    print("WARNING: No heatmap data; skipping heatmap layer.")

# Add high-risk prediction markers (orange circles) - UPDATED POPUP
MAX_PREDICTION_MARKERS = 500
if len(high_risk) > MAX_PREDICTION_MARKERS:
    high_risk_sampled = high_risk.sample(n=MAX_PREDICTION_MARKERS, random_state=42)
    print(f"Downsampled prediction markers to {MAX_PREDICTION_MARKERS}")
else:
    high_risk_sampled = high_risk

for _, row in high_risk_sampled.iterrows():
    folium.CircleMarker(
        location=[row['grid_lat'], row['grid_lon']],
        radius=4,
        color='orange',
        weight=2,
        fill=True,
        fillColor='orange',
        fillOpacity=0.6,
        popup=(
            f"🔮 Predicted Risk: {row['risk_score']:.3f}<br>"
            f"💨 Dust: {row['dust_exposure']:.3f}<br>"
            f"🛣️ Dist to road: {row['dist_to_road_km']:.1f} km<br>"
            f"🏙️ Dist to city: {row['dist_to_city_km']:.1f} km"
        )
    ).add_to(m)

# Add current fire markers (red icons)
for fire in current_fires:
    folium.Marker(
        location=[fire['lat'], fire['lon']],
        popup=(
            f"🔥 CURRENT FIRE: {fire['name']}<br>"
            f"📏 Acres: {fire['acres']:,}<br>"
            f"📊 Status: {fire['status']}<br>"
            f"📍 Lat/Lon: {fire['lat']:.4f}, {fire['lon']:.4f}"
        ),
        icon=folium.Icon(color='red', icon='fire', prefix='fa')
    ).add_to(m)

# Updated Legend
legend_html = '''
<div style="position: fixed; bottom: 50px; left: 50px; width: 200px; height: 200px;
            border:2px solid grey; z-index:9999; font-size:12px;
            background-color:white; padding: 10px; border-radius: 8px;">
    <b>🔥 Utah Fire Risk Map</b><br><br>
    <i style="background:linear-gradient(90deg,blue 0%,green 25%,yellow 50%,orange 75%,red 100%); 
               width:120px;height:15px;display:block;margin:2px 0;"></i>
    Predicted Risk (Heatmap)<br>
    <i style="background:orange; width:15px;height:15px;float:left;margin:2px 5px;"></i>
    High Risk Zones (>0.5)<br>
    <i style="color:red; font-size:18px;">🔥</i> 
    Current Active Fires
</div>
'''
m.get_root().html.add_child(folium.Element(legend_html))

# Save interactive map
os.makedirs("plots", exist_ok=True)
html_path = os.path.abspath("plots/utah_daily_risk_map_v2.html")
m.save(html_path)
print(f"Interactive map saved: {html_path}")
print(f"Map includes: {len(heat_data):,} prediction cells, {len(high_risk_sampled):,} risk markers, {len(current_fires):,} current fires")
