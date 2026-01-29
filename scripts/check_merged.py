import duckdb

con = duckdb.connect('eco_pyric.duckdb')
print("Tables in DB:")
print(con.execute("SHOW TABLES").fetchdf())

try:
    print("\nSample from merged table (first 10 rows):")
    print(con.execute("""
    SELECT acq_date, latitude, longitude, dust_exposure, TAVG, AWND, PRCP, SNOW
    FROM fire_events_with_noaa_weather_clean
    LIMIT 10
    """).fetchdf())
except:
    print("Merged table not found yet.")
con.close()