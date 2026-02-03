# Wildfire Risk Prediction – Utah Focus

Capstone project predicting wildfire ignition risk in Utah using NASA FIRMS data, with a novel focus on **Great Salt Lake dust exposure** and NOAA weather integration.

## Project Overview
This project analyzes historical wildfire data (2012–2026) to:
- Identify patterns in fire intensity (brightness as proxy)
- Incorporate **Utah-specific dust risk** from the shrinking Great Salt Lake
- Merge real NOAA weather observations (precipitation, snow, wind, temperature)
- Build a hybrid ML + physics model for better interpretability
- Generate daily risk forecasts and visualizations

## Key Features & Novelty
- **Great Salt Lake dust exposure score** — inverse distance to lake center (higher = higher drying/ignition risk)
- **Proximity to roads** — proxy for human-caused ignition (major factor in Utah)
- **Dryness proxies** — VPD approximation, diurnal temperature range, low-precip boost
- Weather merge from NOAA GHCN-Daily stations
- XGBoost classifier on grid-based daily data
- Interactive daily risk forecast map
- SHAP interpretability (shows feature importance & interactions)

## Folders & Files
- `scripts/` — all Python code
  - `1_ingest_fires_duckdb.py` — initial data ingestion
  - `add_dust_feature.py` — dust exposure calculation
  - `filter_utah_fires.py` — Utah subset
  - `merge_noaa_weather_cleaned.py` — weather merge
  - `daily_risk_forecast_v2.py` — daily risk prediction & map
  - `train_daily_risk_classifier.py` — model training
  - `utah_dust_map.py` — dust exposure map
  - `check_*` scripts — database inspection
- `plots/` — generated visuals
  - EDA graphs (yearly/monthly trends)
  - SHAP summary & interaction plots
  - Static risk map screenshots (interactive HTML maps kept local)
- `data/` — raw FIRMS & NOAA files (not in repo — too large)
- `eco_pyric.duckdb` — DuckDB database (not in repo)

## How to Run (Locally)
1. Clone the repo
2. Install dependencies: