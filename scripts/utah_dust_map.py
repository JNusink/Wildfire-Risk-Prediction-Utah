import duckdb
import folium
from folium.plugins import HeatMap
import pandas as pd
import os

print("=== UTAH FIRE MAP WITH DUST EXPOSURE (INTERACTIVE HTML) ===")

# Connect to DuckDB
con = duckdb.connect('eco_pyric.duckdb')

# Load Utah fires with dust (full or subset for testing)
df = con.execute("""
SELECT latitude, longitude, acq_date, dust_exposure
FROM fire_events_utah_with_dust
-- LIMIT 10000  -- Uncomment for testing on subset
""").fetchdf()

print(f"Loaded {len(df):,} Utah fire events")

# Create base map centered on Great Salt Lake / Utah
m = folium.Map(location=[39.5, -111.5], zoom_start=7, tiles='CartoDB positron')

# Add heatmap layer for dust exposure (intensity = dust_exposure)
heat_data = [[row['latitude'], row['longitude'], row['dust_exposure']] for _, row in df.iterrows()]
HeatMap(heat_data, radius=15, max_zoom=13, gradient={0.2: 'blue', 0.5: 'yellow', 0.8: 'orange', 1.0: 'red'}).add_to(m)

# Add clickable markers for fires
for idx, row in df.iterrows():
    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=3,
        color='red',
        fill=True,
        fill_color='red',
        fill_opacity=0.6,
        popup=f"Date: {row['acq_date']}<br>Dust Exposure: {row['dust_exposure']:.4f}"
    ).add_to(m)

# Save as interactive HTML
output_path = 'plots/utah_fire_dust_map.html'
m.save(output_path)
print(f"Interactive map saved: {output_path}")
print("Open it in a browser to zoom, pan, and click fire points!")