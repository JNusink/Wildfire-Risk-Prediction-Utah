# dashboard.py
import streamlit as st
import pandas as pd
import numpy as np
import folium
from folium.plugins import HeatMap
from streamlit_folium import st_folium
from datetime import datetime
import duckdb
from joblib import load

st.set_page_config(page_title="Utah Wildfire Risk Dashboard", layout="wide")

st.title("Utah Daily Wildfire Ignition Risk Dashboard")
st.markdown("Predicts probability of new fire ignition per grid cell using trained XGBoost classifier")

# Show current time
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Sidebar controls
st.sidebar.header("Settings")
selected_date = st.sidebar.date_input("Forecast date", datetime.now())
zoom_level = st.sidebar.slider("Map zoom level", 6, 10, 7)

# Region selection (dropdown instead of slider for clearer control)
region = st.sidebar.selectbox(
    "Select Utah Region to View",
    [
        "Full State (slowest – up to 13.5M cells)",
        "Great Salt Lake & Northern Utah (lat 40–42)",
        "Wasatch Front & Central Utah (lat 39–41)",
        "Southern Utah (lat 37–39)",
        "Custom (use sliders below)"
    ]
)

# Optional custom lat/lon range if "Custom" is selected
custom_lat_min = 37.0
custom_lat_max = 42.0
custom_lon_min = -114.0
custom_lon_max = -109.0

if region == "Custom (use sliders below)":
    st.sidebar.subheader("Custom Region")
    custom_lat_min = st.sidebar.slider("Min Latitude", 37.0, 42.0, 37.0, 0.1)
    custom_lat_max = st.sidebar.slider("Max Latitude", 37.0, 42.0, 42.0, 0.1)
    custom_lon_min = st.sidebar.slider("Min Longitude", -114.0, -109.0, -114.0, 0.1)
    custom_lon_max = st.sidebar.slider("Max Longitude", -114.0, -109.0, -109.0, 0.1)

# Map region to lat/lon bounds
if region == "Full State (slowest – up to 13.5M cells)":
    lat_min, lat_max = 37.0, 42.0
    lon_min, lon_max = -114.0, -109.0
elif region == "Great Salt Lake & Northern Utah (lat 40–42)":
    lat_min, lat_max = 40.0, 42.0
    lon_min, lon_max = -114.0, -109.0
elif region == "Wasatch Front & Central Utah (lat 39–41)":
    lat_min, lat_max = 39.0, 41.0
    lon_min, lon_max = -114.0, -109.0
elif region == "Southern Utah (lat 37–39)":
    lat_min, lat_max = 37.0, 39.0
    lon_min, lon_max = -114.0, -109.0
else:  # Custom
    lat_min, lat_max = custom_lat_min, custom_lat_max
    lon_min, lon_max = custom_lon_min, custom_lon_max

# Demo weather (replace with real API later)
tavg = 5.4
rh = 48
wspd = 16.6
prcp = 0.0

st.sidebar.write("Today's forecast (demo point - Saratoga Springs area)")
st.sidebar.write(f"Tavg: {tavg}°C")
st.sidebar.write(f"RH: {rh}%")
st.sidebar.write(f"Wind: {wspd} km/h")
st.sidebar.write(f"Precip: {prcp} mm")

# Load grid data for the selected region
con = duckdb.connect('eco_pyric.duckdb')
query = f"""
SELECT grid_lat, grid_lon, dist_to_road_km
FROM utah_grid_ignition_labels_proximity
WHERE grid_lat BETWEEN {lat_min} AND {lat_max}
  AND grid_lon BETWEEN {lon_min} AND {lon_max}
"""
df_grid = con.execute(query).fetchdf()

st.sidebar.write(f"Loaded {len(df_grid):,} grid cells for selected region")

# Compute features
df_grid['month'] = selected_date.month
df_grid['vpd_proxy'] = 0.6108 * np.exp(17.27 * tavg / (tavg + 237.3)) * (1 - rh / 100)
df_grid['vpd_proxy'] = df_grid['vpd_proxy'].clip(lower=0)
df_grid['dryness_proxy'] = (tavg - (tavg - 10)) / 10
df_grid['low_precip_dryness'] = 1.0 if prcp < 1 else 0.5

# Load real model predictions if available
try:
    model = load('risk_classifier_model.joblib')
    features = ['dist_to_road_km', 'month', 'vpd_proxy', 'dryness_proxy', 'low_precip_dryness', 'grid_lat', 'grid_lon']
    df_grid['predicted_prob'] = model.predict_proba(df_grid[features])[:, 1]
    st.sidebar.success("Using real ML classifier predictions")
except FileNotFoundError:
    df_grid['predicted_prob'] = np.random.uniform(0.05, 0.35, len(df_grid))
    st.sidebar.warning("Model file not found — using random demo values")

# Show top risk cells
st.subheader("Top 10 highest risk grid cells in selected region")
top_risk = df_grid.sort_values('predicted_prob', ascending=False).head(10)
st.dataframe(
    top_risk[['grid_lat', 'grid_lon', 'predicted_prob', 'dist_to_road_km']],
    use_container_width=True
)

# Interactive map
st.subheader("Predicted Risk Map")
m = folium.Map(location=[(lat_min + lat_max)/2, (lon_min + lon_max)/2], zoom_start=zoom_level, tiles='CartoDB positron')

heat_data = [[row['grid_lat'], row['grid_lon'], row['predicted_prob']] for _, row in df_grid.iterrows()]
HeatMap(heat_data, radius=8, blur=15, gradient={0.2: 'blue', 0.5: 'yellow', 0.8: 'orange', 1.0: 'red'}).add_to(m)

st_folium(m, width=1200, height=600)

# SHAP plot
st.subheader("Feature Importance (SHAP)")
st.image("plots/shap_summary_classifier_full.png", use_container_width=True)

st.markdown("---")
st.caption("Note: Weather is currently fixed to demo point. Full per-cell weather and real-time updates coming soon.")