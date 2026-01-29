import duckdb

print("=== ADDING DUST EXPOSURE TO UTAH FIRES ===")

con = duckdb.connect('eco_pyric.duckdb')

# Check if dust table exists
tables = con.execute("SHOW TABLES").fetchdf()['name'].tolist()

if 'fire_events_with_dust' in tables:
    print("Dust table exists — filtering to Utah...")
    con.execute("""
    CREATE OR REPLACE TABLE fire_events_utah_with_dust AS
    SELECT *
    FROM fire_events_with_dust
    WHERE latitude BETWEEN 37 AND 42
      AND longitude BETWEEN -114 AND -109
    """)
else:
    print("Dust table missing — adding dust to Utah fires...")
    df = con.execute("""
    SELECT latitude, longitude, acq_date, brightness, frp, confidence, source_file
    FROM fire_events_utah
    """).fetchdf()

    from shapely.geometry import Point
    import geopandas as gpd

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )

    lake_center = Point(-112.5, 41.0)
    gdf['distance_to_lake_km'] = gdf.geometry.distance(lake_center) * 111
    gdf['dust_exposure'] = 1 / (gdf['distance_to_lake_km'] + 1)
    gdf['dust_exposure'] = gdf['dust_exposure'].clip(upper=1.0)

    con.register('gdf_utah_dust', gdf.drop(columns='geometry'))
    con.execute("CREATE OR REPLACE TABLE fire_events_utah_with_dust AS SELECT * FROM gdf_utah_dust")

utah_count = con.execute("SELECT COUNT(*) FROM fire_events_utah_with_dust").fetchone()[0]
print(f"Utah table with dust created: {utah_count:,} rows")

con.close()
print("Done.")