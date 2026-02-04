# Wildfire Risk Prediction – Utah Focus

**Midterm Capstone Progress – Jared Nusink**

Predicting wildfire ignition risk in Utah using NASA FIRMS data, with a novel focus on **Great Salt Lake dust exposure** as a drying/ignition factor.

## Problem Definition / Research Question
How can we better predict wildfire ignition risk in Utah by incorporating local environmental factors (dust from the shrinking Great Salt Lake) that traditional models ignore?

**Need**: Utah has unique risks from dust deposition on fuels (especially in northern areas), human activity near roads/cities, and seasonal dryness. Most broad US models miss this regional signal.

## Novelty / Real-world Impact
- First attempt (to my knowledge) to explicitly include Great Salt Lake dust exposure in a fire risk model.
- Focus on Utah / Wasatch Front → direct relevance to population centers and recreation areas.
- Hybrid ML + physics approach + SHAP interpretability → helps explain why certain conditions lead to higher risk.

## Dataset Description + Metadata
- **Source**: NASA FIRMS (VIIRS) active fire detections (2012–2026)
- **Size**: ~3 million high-confidence records (filtered), Utah subset ~77,566 fires
- **Key fields**: latitude, longitude, acq_date, brightness (intensity proxy), frp, confidence
- **Cleaning**: Removed low-confidence detections, clipped to Utah bounding box (lat 37–42, lon -114 to -109)
- **Augmentation**: Added dust exposure score (inverse distance to lake center)

## Data Cleaning & Processing
- DuckDB database (`eco_pyric.duckdb`) for efficient querying
- Scripts: ingestion, filtering, dust calculation, NOAA weather merge
- Feature engineering: dust_exposure, month, year, lat/lon, dryness proxies (VPD approx, temp range)

## Data Analysis & Visualization
- Yearly/monthly fire trends in Utah
- Dust exposure distribution (higher near lake)
- Interactive daily risk forecast map (heatmap of predicted ignition probability)
- SHAP plots (feature importance & interactions)

## Modeling
- **Model**: XGBoost Classifier (binary ignition prediction on grid cells)
- **Features**: dust_exposure, dist_to_road_km (human risk), dryness proxies, month, lat/lon
- **Training data**: ~13.5M grid-date rows (0/1 ignition label), trained on 10M subset
- **Performance** (10M-row subset): AUC-ROC = **0.9607** (strong for rare-event prediction)
- **Hybrid adjustment**: Physics-based boost (FWI proxy using dust + weather)
- **Interpretability**: SHAP shows dust and road proximity are important drivers

## Results & Insights
- Dust exposure amplifies risk in dry, low-precip conditions (SHAP interaction plots)
- Human proximity (roads) is a dominant factor in populated areas
- Daily risk map shows hotspots near lake and urban corridors

## Repo Structure
- `scripts/` — all code (ingestion, dust, weather merge, forecast, classifier)
- `plots/` — SHAP plots, static risk map screenshots
- Large files (DuckDB, interactive HTML maps, raw data) kept local

## How to Run
```bash
# Install dependencies
pip install -r requirements.txt

# Run daily risk forecast
python scripts/daily_risk_forecast_v2.py

# Train classifier
python scripts/train_daily_risk_classifier.py