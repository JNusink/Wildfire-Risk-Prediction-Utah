# scripts/train_daily_risk_classifier_full.py
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report, confusion_matrix
import shap
import matplotlib.pyplot as plt
import time
import psutil
from joblib import dump

print("=== TRAINING DAILY IGNITION CLASSIFIER – FULL GRID ===")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

start_time = time.time()
start_mem = psutil.Process().memory_info().rss / 1024**2  # MB

con = duckdb.connect('eco_pyric.duckdb')

print("Loading FULL grid data... (13.5M rows — may take several minutes)")
df = con.execute("""
SELECT grid_lat, grid_lon, date, ignition, dist_to_road_km
FROM utah_grid_ignition_labels_proximity
""").fetchdf()

load_time = time.time() - start_time
print(f"Loaded {len(df):,} rows in {load_time:.1f} seconds")
print(f"Memory usage after loading: {psutil.Process().memory_info().rss / 1024**2:.1f} MB")

# Add dryness proxies
df['month'] = pd.to_datetime(df['date']).dt.month
df['tavg'] = 10   # Placeholder — replace with real forecast avg if available
df['prcp'] = 0    # Placeholder
df['vpd_proxy'] = 0.6108 * np.exp(17.27 * df['tavg'] / (df['tavg'] + 237.3)) * (1 - 50 / 100)
df['vpd_proxy'] = df['vpd_proxy'].clip(lower=0)
df['dryness_proxy'] = (df['tavg'] - (df['tavg'] - 10)) / 10
df['low_precip_dryness'] = np.where(df['prcp'] < 1, 1.0, 0.5)

features = [
    'dist_to_road_km',
    'month',
    'vpd_proxy',
    'dryness_proxy',
    'low_precip_dryness',
    'grid_lat',
    'grid_lon'
]

X = df[features]
y = df['ignition']

print("Splitting data...")
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

scale_pos_weight = (y == 0).sum() / (y == 1).sum()
print(f"Scale pos weight: {scale_pos_weight:.2f}")

model = XGBClassifier(
    n_estimators=200,
    learning_rate=0.05,
    max_depth=7,
    scale_pos_weight=scale_pos_weight,
    random_state=42,
    tree_method='hist',         # Much faster on large data
    enable_categorical=False
)

print("Training model... (this may take 20–90 minutes on 13.5M rows)")
model.fit(X_train, y_train)

print("Evaluating...")
y_pred = model.predict(X_test)
y_pred_proba = model.predict_proba(X_test)[:, 1]

acc = accuracy_score(y_test, y_pred)
auc = roc_auc_score(y_test, y_pred_proba)

print(f"Accuracy: {acc:.4f}")
print(f"AUC-ROC: {auc:.4f}")

print("\nClassification Report:")
print(classification_report(y_test, y_pred))

print("\nConfusion Matrix:")
print(confusion_matrix(y_test, y_pred))

# SHAP on a small subset to save time/memory
print("Generating SHAP summary (on 10k test samples)...")
explainer = shap.Explainer(model)
shap_values = explainer(X_test.sample(10000, random_state=42))

shap.summary_plot(shap_values, X_test.sample(10000, random_state=42), show=False)
plt.savefig("plots/shap_summary_classifier_full.png", dpi=150, bbox_inches='tight')
print("Saved SHAP summary: plots/shap_summary_classifier_full.png")

# Save the trained model for use in forecast script
dump(model, 'risk_classifier_model.joblib')
print("Trained model saved as 'risk_classifier_model.joblib'")

con.close()

end_time = time.time()
end_mem = psutil.Process().memory_info().rss / 1024**2
print(f"Training finished in {end_time - start_time:.1f} seconds")
print(f"Peak memory usage: {end_mem - start_mem:.1f} MB")
print("Done!")