# Wildfire Risk Prediction – Utah Focus

**Capstone Project – Midterm Progress**  
**Jared Nusink**  
**February 2026**

This project predicts **wildfire ignition risk** in Utah using NASA FIRMS data, with a novel focus on **Great Salt Lake dust exposure** as a fuel-drying and ignition factor, combined with proximity to roads (human-caused risk) and weather/dryness proxies.

## Problem & Research Question

**Question**  
How can we improve wildfire ignition risk prediction in Utah by incorporating Great Salt Lake dust exposure and human proximity factors that most national models overlook?

**Why Utah?**  
- Unique dust risk from the shrinking Great Salt Lake (exposed playa dries vegetation and increases flammability)  
- High rate of human-caused ignitions near roads, cities, and recreation areas  
- Seasonal dryness and drought amplify risk  
- Local relevance to Wasatch Front communities (Salt Lake City, Provo, Ogden, Saratoga Springs)

## Novelty & Real-world Impact

- **Novel feature**: Great Salt Lake dust exposure score (inverse distance to lake center) — likely the first in a predictive fire model  
- **Human ignition proxy**: Distance to major roads and cities  
- **Hybrid approach**: XGBoost classifier + physics-inspired dryness adjustment  
- **Interpretability**: SHAP values explain predictions and feature interactions  
- **Daily forecast map**: Shows predicted ignition probability across Utah grid cells  
- **Impact**: Improved localized risk awareness and prevention for Utah communities

## Dataset & Processing

- **Source**: NASA FIRMS VIIRS active fire detections (2012–2026)  
- **Size**: ~3 million high-confidence records → filtered to **77,566 Utah fires**  
- **Key fields**: latitude, longitude, acq_date, brightness (intensity proxy), frp, confidence  
- **Cleaning**: Removed low-confidence detections, clipped to Utah bounding box  
- **Grid construction**: 0.1° resolution (~2,652 cells) × daily history = **13,570,284 grid-date rows**  
- **Labeling**: Binary ignition (1 if any fire detected in cell that day, 0 otherwise)  
- **Feature engineering**:  
  - Dust exposure (inverse distance to lake)  
  - Road proximity (distance to major highways)  
  - Dryness proxies (VPD approximation, diurnal temp range, low-precip boost)  
  - Month, latitude, longitude  
- **Storage**: DuckDB database (`eco_pyric.duckdb`) — large file kept local

## Modeling

- **Task**: Binary classification — predict ignition (0/1) per grid cell on any day  
- **Model**: XGBoost Classifier with imbalance handling (`scale_pos_weight = 3301`)  
- **Training**: Full 13,570,284 rows (10M+ subset used for experiments)  
- **Features**: dust_exposure, dist_to_road_km, dryness proxies, month, lat/lon  
- **Performance** (10M-row subset):  
  - Accuracy: 90.80%  
  - **AUC-ROC: 0.9671** (excellent for rare-event prediction)  
  - Recall for fire days: 0.91 (catches 91% of actual fires)  
- **Interpretability**: SHAP values show feature importance and interactions

## Daily Risk Forecast Map

Interactive map predicting ignition probability (0–1) across Utah grid cells for the current day:  
- Uses real weather forecast (Open-Meteo)  
- Incorporates dust exposure, road proximity, dryness proxies  
- **ML-powered** (V3 uses trained classifier probabilities)  
- **Interactive HTML** kept local (large file size)  
- **Static screenshots** available in `plots/`

## Interactive Dashboard (Bonus)

A simple Streamlit dashboard visualizes:  
- Daily risk map  
- Top high-risk grid cells  
- SHAP feature importance  

Run locally:  
```bash
pip install streamlit streamlit-folium
streamlit run dashboard.py