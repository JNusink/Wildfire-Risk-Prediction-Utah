from meteostat import stations, daily
from datetime import datetime

print("Testing Meteostat import...")

# Create an instance of the Stations class
s = stations()  # <-- This is correct

# Find stations near Salt Lake City
s = s.nearby(40.76, -111.89)  # SLC area

# Filter for stations with daily data in 2023
s = s.inventory('daily', (datetime(2023, 1, 1)))

# Fetch the closest one
station = s.fetch(1)

print("Closest station(s):")
print(station)

# Test fetching data for one station (example date range)
if not station.empty:
    station_id = station.index[0]
    data = daily(station_id, datetime(2025, 1, 1), datetime(2025, 1, 10))
    df = data.fetch()
    print("\nSample data from station:")
    print(df.head())
else:
    print("No stations found near this location.")