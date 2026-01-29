import duckdb

con = duckdb.connect('eco_pyric.duckdb')
print("Tables in DB:")
print(con.execute("SHOW TABLES").fetchdf())

try:
    print("\nUtah merged table row count:")
    print(con.execute("SELECT COUNT(*) FROM fire_events_utah_with_weather").fetchone()[0])

    print("\nNon-NaN weather counts:")
    print(con.execute("""
    SELECT 
        COUNT(*) AS total,
        COUNT(TAVG) AS non_nan_tavg,
        COUNT(AWND) AS non_nan_awnd,
        COUNT(PRCP) AS non_nan_prcp,
        COUNT(SNOW) AS non_nan_snow
    FROM fire_events_utah_with_weather
    """).fetchdf())

    print("\nSample 10 rows with weather:")
    print(con.execute("""
    SELECT acq_date, latitude, longitude, dust_exposure, TAVG, AWND, PRCP, SNOW
    FROM fire_events_utah_with_weather
    LIMIT 10
    """).fetchdf())
except:
    print("Table not found yet.")
con.close()