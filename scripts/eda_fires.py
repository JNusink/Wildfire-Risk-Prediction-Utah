# eda_fires.py
# EDA for cleaned Western US wildfire data (DuckDB)

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

# Config
DB_FILE = "eco_pyric.duckdb"
PLOTS_DIR = "plots"
os.makedirs(PLOTS_DIR, exist_ok=True)

print("=== WILDFIRE EDA – DUCKDB ===")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Database: {DB_FILE}")
print("=" * 50 + "\n")

con = duckdb.connect(DB_FILE)

# Total count check
total = con.execute("SELECT COUNT(*) FROM fire_events").fetchone()[0]
print(f"Total fires in DB: {total:,}")

# Helper to save plots
def save_plot(fig, name):
    path = os.path.join(PLOTS_DIR, name)
    fig.savefig(path, dpi=150, bbox_inches='tight')
    print(f"Saved: {path}")
    plt.close(fig)

# 1. Yearly Trend
yearly = con.execute("""
SELECT 
    strftime('%Y', strptime(acq_date, '%Y-%m-%d')) AS year,
    COUNT(*) AS fire_count
FROM fire_events
GROUP BY year
ORDER BY year
""").fetchdf()

fig1 = plt.figure(figsize=(12, 6))
sns.barplot(x='year', y='fire_count', data=yearly, color='orange')
plt.title('Annual Wildfire Detections – Western US (2012–2026)')
plt.xlabel('Year')
plt.ylabel('Number of Detections')
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
save_plot(fig1, 'yearly_trend.png')
print("\nYearly summary:\n", yearly)

# 2. Monthly Pattern
monthly = con.execute("""
SELECT 
    strftime('%m', strptime(acq_date, '%Y-%m-%d')) AS month,
    COUNT(*) AS fire_count
FROM fire_events
GROUP BY month
ORDER BY month
""").fetchdf()

fig2 = plt.figure(figsize=(10, 5))
sns.barplot(x='month', y='fire_count', data=monthly, color='darkred')
plt.title('Monthly Wildfire Pattern (All Years)')
plt.xlabel('Month')
plt.ylabel('Total Detections')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
save_plot(fig2, 'monthly_pattern.png')
print("\nMonthly summary:\n", monthly)

# 3. Utah Area Yearly
utah_yearly = con.execute("""
SELECT 
    strftime('%Y', strptime(acq_date, '%Y-%m-%d')) AS year,
    COUNT(*) AS fire_count
FROM fire_events
WHERE latitude BETWEEN 37 AND 42
  AND longitude BETWEEN -114 AND -109
GROUP BY year
ORDER BY year
""").fetchdf()

fig3 = plt.figure(figsize=(12, 6))
sns.barplot(x='year', y='fire_count', data=utah_yearly, color='forestgreen')
plt.title('Annual Wildfire Detections – Utah Area (2012–2026)')
plt.xlabel('Year')
plt.ylabel('Number of Detections')
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
save_plot(fig3, 'utah_yearly.png')
print("\nUtah yearly summary:\n", utah_yearly)

# 4. Recent Daily (last 12 months approx)
recent = con.execute("""
SELECT 
    acq_date,
    COUNT(*) AS daily_count
FROM fire_events
WHERE strptime(acq_date, '%Y-%m-%d') >= date '2025-01-01'
GROUP BY acq_date
ORDER BY acq_date
""").fetchdf()

fig4 = plt.figure(figsize=(14, 6))
plt.plot(pd.to_datetime(recent['acq_date']), recent['daily_count'], color='darkorange', linewidth=1.5)
plt.title('Daily Wildfire Detections – Recent Period')
plt.xlabel('Date')
plt.ylabel('Daily Count')
plt.grid(True, linestyle='--', alpha=0.7)
plt.tight_layout()
save_plot(fig4, 'recent_daily.png')

print("\n" + "="*50)
print("EDA COMPLETE – Plots saved in 'plots/' folder")
print("Files created:")
print(" - yearly_trend.png")
print(" - monthly_pattern.png")
print(" - utah_yearly.png")
print(" - recent_daily.png")
print("="*50)

con.close()
print("Done.")