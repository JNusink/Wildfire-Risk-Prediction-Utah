import duckdb
import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist  # FIXED: Import cdist

print("=== ADDING PROXIMITY TO PEOPLE (ROADS) TO UTAH GRID ===")

con = duckdb.connect('eco_pyric.duckdb')

# Load grid labels (from your previous script)
df_grid = con.execute("""
SELECT grid_lat, grid_lon, date, ignition
FROM utah_grid_ignition_labels
""").fetchdf()

print(f"Loaded {len(df_grid):,} grid-date rows")

# Example major roads in Utah (lat/lon points along highways — add more for accuracy)
# You can expand this with real road coordinates from OpenStreetMap or manual lookup
major_roads = [
    (40.76, -111.89),  # I-15 near SLC
    (40.75, -111.90),  # Another point on I-15
    (41.0, -112.0),    # Near lake
    (40.3, -111.7),    # Provo area
    (38.0, -112.0),    # Southern Utah
    # Add 10–20 more points along I-15, I-80, I-70, US-89, etc.
]

road_coords = np.array(major_roads)

# Calculate distance to nearest road for each grid cell
print("Calculating distance to nearest road...")
grid_coords = df_grid[['grid_lat', 'grid_lon']].values
distances = cdist(grid_coords, road_coords, metric='euclidean') * 111  # approx km conversion
df_grid['dist_to_road_km'] = distances.min(axis=1)

print("Added 'dist_to_road_km' feature (lower = closer to roads = higher human ignition risk)")
print(df_grid[['grid_lat', 'grid_lon', 'date', 'dist_to_road_km', 'ignition']].head(10))

# Save back with new feature
con.register('grid_with_proximity', df_grid)
con.execute("CREATE OR REPLACE TABLE utah_grid_ignition_labels_proximity AS SELECT * FROM grid_with_proximity")

con.close()
print("Saved with proximity feature to 'utah_grid_ignition_labels_proximity'")
print("Done.")