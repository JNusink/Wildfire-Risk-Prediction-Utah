# Wildfire Risk Prediction – Utah Focus

**Capstone Project – Midterm Progress**  
**Jared Nusink**  
**February 2026**

This project predicts **wildfire ignition risk** in Utah using NASA FIRMS data, with a novel focus on **Great Salt Lake dust exposure** as a fuel-drying and ignition factor, combined with proximity to roads (human-caused risk) and weather/dryness proxies.

## Problem & Research Question

**Question**:  
How can we improve wildfire ignition risk prediction in Utah by incorporating local environmental factors — particularly dust deposition from the shrinking Great Salt Lake — that most national models overlook?

**Why Utah?**  
- Local relevance to the Wasatch Front and Saratoga Springs  
- Unique dust risk from exposed playa as the lake recedes  
- High proportion of human-caused ignitions near roads, cities, and recreation areas  
- Seasonal dryness and drought patterns amplify fire probability

## Novelty & Real-world Impact

- Explicit modeling of **Great Salt Lake dust exposure** (inverse distance score) — likely the first in a predictive fire model  
- Integration of **human proximity** (distance to roads) as a proxy for ignition likelihood  
- Hybrid approach: ML (XGBoost classifier) + simple physics-inspired dryness adjustment  
- Interpretable results via SHAP (explains why certain conditions lead to higher risk)  
- Practical value: better localized risk awareness for Wasatch Front communities and recreation areas

## Dataset & Processing

- **Source**: NASA FIRMS VIIRS active fire detections (2012–2026)  
- **Size**: ~3 million high-confidence records → filtered to **77,566 Utah fires**  
- **Key fields**: latitude, longitude, acq_date, brightness (intensity proxy), frp, confidence  
- **Cleaning & filtering**:  
  - Removed low-confidence detections  
  - Clipped to Utah bounding box (lat 37–42, lon -114 to -109)  
- **Feature engineering**:  
  - Dust exposure score (inverse distance to lake center)  
  - Road proximity (distance to major highways)  
  - Dryness proxies (VPD approximation, diurnal temperature range, low-precip boost)  
  - Month, latitude, longitude  
- **Data structure**: ~13.57 million grid-date rows (0.1° cells × daily history) with binary ignition label (0/1)  
- Large files (DuckDB database, raw data) kept local — not in repo

## Modeling

- **Task**: Binary classification — predict whether a fire will occur in a 0.1° grid cell on a given day  
- **Model**: XGBoost Classifier with imbalance handling (`scale_pos_weight`)  
- **Training data**: Full ~13.57 million grid-date rows (trained on 10M+ subset for midterm)  
- **Performance** (10M-row subset):  
  - Accuracy: 90.80%  
  - **AUC-ROC: 0.9671** (very strong for rare-event prediction)  
  - Recall for fire days (class 1): 0.91 (catches 91% of actual fires)  
- **Interpretability**: SHAP values show feature importance and interactions

## Daily Risk Forecast Map

Interactive map showing predicted ignition probability (0–1) across Utah grid cells for the current day:
- Uses real weather forecast (Open-Meteo)
- Incorporates dust exposure, road proximity, dryness proxies  
- **Interactive version** (large HTML file) kept local due to size  
- **Static screenshots** are in the `plots/` folder

## Repo Structure

- `scripts/` — all Python code  
  - data ingestion, dust calculation, weather merge  
  - daily risk forecast & ML classifier training  
  - map generation  
- `plots/` — SHAP summaries, static map screenshots  
- `README.md` — this file  
- `.gitignore` — excludes large files (DuckDB, raw data, interactive HTML maps)

## How to Run (Locally)

```bash
# Install dependencies
pip install -r requirements.txt

# Generate daily risk forecast map
python scripts/daily_risk_forecast_v2.py

# Train classifier (takes time on large grid)
python scripts/train_daily_risk_classifier_full.py