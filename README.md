# Wildfire Risk Prediction – Utah Focus

Capstone project predicting wildfire ignition risk in Utah using NASA FIRMS data, with novel Great Salt Lake dust exposure feature and NOAA weather merge.

## Progress
- Ingested 3M+ Western US fires into DuckDB.
- Filtered to Utah subset (~77k rows).
- Added dust exposure score (inverse distance to lake).
- Merged NOAA daily weather (precip, snow, etc.) from nearest stations.
- Trained XGBoost baseline + hybrid physics adjustment (FWI proxy).
- Generated SHAP plots for interpretability.

## Folders
- `scripts/` — Python code (ingestion, dust, weather merge, model).
- `plots/` — EDA and SHAP graphs.
- `data/` — Raw FIRMS and NOAA files (not pushed).

Next: Refine physics boost, add more weather variables, dashboard.