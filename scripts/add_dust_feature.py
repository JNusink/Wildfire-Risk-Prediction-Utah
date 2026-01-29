import duckdb
from shapely.geometry import Point
import geopandas as gpd
import pandas as pd

print("=== ADDING GREAT SALT LAKE DUST EXPOSURE ===")

con = duckdb.connect('eco_pyric.duckdb')

# Get all fire points (or subset for speed)
df = con.execute("""
SELECT latitude, longitude, acq_date, brightness, frp, confidence, source_file
FROM fire_events
""").fetchdf()

print(f"Loaded {len(df):,} fire records")

# Convert to GeoDataFrame
gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df.longitude, df.latitude),
    crs="EPSG:4326"
)

# Lake center (approximate centroid)
lake_center = Point(-112.5, 41.0)

# Distance in km (rough spherical conversion: 1 degree ≈ 111 km)
gdf['distance_to_lake_km'] = gdf.geometry.distance(lake_center) * 111

# Dust exposure score: inverse distance, higher = more exposure
gdf['dust_exposure'] = 1 / (gdf['distance_to_lake_km'] + 1)  # +1 avoids division by zero
gdf['dust_exposure'] = gdf['dust_exposure'].clip(upper=1.0)  # Cap at 1.0

# Quick look
print("\nSample dust exposure (first 10 rows):")
print(gdf[['latitude', 'longitude', 'distance_to_lake_km', 'dust_exposure']].head(10))

# Optional: Save back to DuckDB for later use
con.register('gdf_with_dust', gdf.drop(columns='geometry'))  # Drop geometry column for DB
con.execute("""
CREATE OR REPLACE TABLE fire_events_with_dust AS
SELECT * FROM gdf_with_dust
""")

print("\nDust exposure added to table 'fire_events_with_dust'")
print("Max dust exposure:", gdf['dust_exposure'].max())
print("Mean dust exposure:", gdf['dust_exposure'].mean())

con.close()
print("Done.")