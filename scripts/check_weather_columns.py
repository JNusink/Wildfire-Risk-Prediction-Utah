import duckdb

con = duckdb.connect('eco_pyric.duckdb')
print("Columns in merged Utah table:")
columns = con.execute("DESCRIBE fire_events_utah_with_weather").fetchdf()
print(columns)

print("\nNon-NaN counts for all weather columns:")
print(con.execute("""
SELECT 
    COUNT(*) AS total,
    COUNT(TAVG) AS tavg,
    COUNT(TMAX) AS tmax,
    COUNT(TMIN) AS tmin,
    COUNT(AWND) AS awnd,
    COUNT(WSF2) AS wsf2,
    COUNT(WSF5) AS wsf5,
    COUNT(PRCP) AS prcp,
    COUNT(SNOW) AS snow,
    COUNT(SNWD) AS snwd
FROM fire_events_utah_with_weather
""").fetchdf())

con.close()