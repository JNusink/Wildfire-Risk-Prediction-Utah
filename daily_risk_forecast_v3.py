# scripts/daily_risk_forecast_v3.py — ML-based version
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import duckdb
import folium
from folium.plugins import HeatMap
from joblib import load
import os

print("=== DAILY UTAH WILDFIRE RISK FORECAST V3 (ML CLASSIFIER) ===")
print(f"Date: {datetime.now().strftime('%Y-%m-%d')}")

# Load the trained classifier model
try:
    model = load('risk_classifier_model.joblib')
    print("Loaded trained classifier model")
except FileNotFoundError:
    print("ERROR: Model file 'risk_classifier_model.joblib' not found.")
    print("Please run the training script first and make sure it saves the model.")
    exit(1)

con = duckdb.connect('eco_pyric.duckdb')

# Load full Utah grid + proximity
df_grid = con.execute("""
SELECT grid_lat, grid_lon, dist_to_road_km
FROM utah_grid_ignition_labels_proximity
""").fetchdf()

print(f"Loaded {len(df_grid):,} grid cells")

# Precompute dust exposure per grid cell
lake_lat, lake_lon = 41.0, -112.5
df_grid['dist_to_lake_km'] = np.sqrt(
    (df_grid['grid_lat'] - lake_lat)**2 + 
    (df_grid['grid_lon'] + lake_lon)**2
) * 111
df_grid['dust_exposure'] = 1 / (df_grid['dist_to_lake_km'] + 1)
df_grid['dust_exposure'] = df_grid['dust_exposure'].clip(upper=1.0)

# Get today's weather forecast (demo point - expand to per-cell later)
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

# Features must match exactly what was used in training
features = [
    'dist_to_road_km',
    'month',
    'vpd_proxy',
    'dryness_proxy',
    'low_precip_dryness',
    'grid_lat',
    'grid_lon'
]

# Predict probability using the trained classifier
print("Predicting ignition probabilities with trained model...")
df_grid['predicted_prob'] = model.predict_proba(df_grid[features])[:, 1]

print("\nTop 10 highest ML-predicted risk grid cells today:")
print(df_grid.sort_values('predicted_prob', ascending=False)[
    ['grid_lat', 'grid_lon', 'predicted_prob', 'dust_exposure', 'dist_to_road_km']
].head(10))

# Clean NaNs before mapping
df_grid = df_grid.dropna(subset=['grid_lat', 'grid_lon', 'predicted_prob'])
print(f"\nTotal grid cells (after dropna): {len(df_grid):,}")

# High-risk subset for markers
high_risk = df_grid[df_grid['predicted_prob'] > 0.5]
print(f"High-risk cells (>0.5): {len(high_risk):,}")

# Create interactive map
m = folium.Map(
    location=[39.5, -111.5],
    zoom_start=7,
    tiles='CartoDB positron'
)

# Heatmap for ML-predicted probability
heat_data = [[row['grid_lat'], row['grid_lon'], row['predicted_prob']] for _, row in df_grid.iterrows()]
HeatMap(
    heat_data,
    radius=8,
    blur=15,
    gradient={0.2: 'blue', 0.5: 'yellow', 0.8: 'orange', 1.0: 'red'},
    min_opacity=0.3,
    max_zoom=13
).add_to(m)

# Add legend
legend_html = '''
<div style="position: fixed; bottom: 50px; left: 50px; width: 180px; height: 140px; 
            border:2px solid grey; z-index:9999; font-size:14px; 
            background-color:white; padding: 10px;">
&nbsp; Predicted Ignition Probability<br>
&nbsp; <i style="background:red; width:20px;height:20px;float:left;"></i> High >0.5<br>
&nbsp; <i style="background:orange; width:20px;height:20px;float:left;"></i> Medium 0.3-0.5<br>
&nbsp; <i style="background:yellow; width:20px;height:20px;float:left;"></i> Low 0.1-0.3<br>
&nbsp; <i style="background:blue; width:20px;height:20px;float:left;"></i> Very Low <0.1
</div>
'''
m.get_root().html.add_child(folium.Element(legend_html))

# Save map
os.makedirs("plots", exist_ok=True)
m.save('plots/utah_daily_risk_map_ml_v2.html')
print("ML-based risk map saved: plots/utah_daily_risk_map_ml_v2.html — open in browser!")
print("Done.")