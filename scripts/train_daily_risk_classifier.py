import duckdb
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report, confusion_matrix
import shap
import matplotlib.pyplot as plt

print("=== TRAINING DAILY IGNITION CLASSIFIER (UTAH GRID) ===")

con = duckdb.connect('eco_pyric.duckdb')

df = con.execute("""
SELECT grid_lat, grid_lon, date, ignition, dist_to_road_km
FROM utah_grid_ignition_labels_proximity
LIMIT 10000000  -- 10M rows - your latest run
""").fetchdf()

print(f"Loaded {len(df):,} grid-date rows for training")

# Add dryness proxies
df['month'] = pd.to_datetime(df['date']).dt.month
df['tavg'] = 10  # Placeholder - replace with real forecast avg
df['prcp'] = 0   # Placeholder
df['vpd_proxy'] = 0.6108 * np.exp(17.27 * df['tavg'] / (df['tavg'] + 237.3)) * (1 - 50 / 100)
df['vpd_proxy'] = df['vpd_proxy'].clip(lower=0)
df['dryness_proxy'] = (df['tavg'] - (df['tavg'] - 10)) / 10
df['low_precip_dryness'] = np.where(df['prcp'] < 1, 1.0, 0.5)

features = ['dist_to_road_km', 'month', 'vpd_proxy', 'dryness_proxy', 'low_precip_dryness', 'grid_lat', 'grid_lon']
X = df[features]
y = df['ignition']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

scale_pos_weight = (y == 0).sum() / (y == 1).sum()
model = XGBClassifier(n_estimators=200, learning_rate=0.05, max_depth=7, scale_pos_weight=scale_pos_weight, random_state=42)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
y_pred_proba = model.predict_proba(X_test)[:, 1]

acc = accuracy_score(y_test, y_pred)
auc = roc_auc_score(y_test, y_pred_proba)
print(f"Accuracy: {acc:.4f}")
print(f"AUC-ROC: {auc:.4f}")

# === NEW: Classification Report & Confusion Matrix ===
print("\nClassification Report:")
print(classification_report(y_test, y_pred))

print("\nConfusion Matrix:")
print(confusion_matrix(y_test, y_pred))

# SHAP
explainer = shap.Explainer(model)
shap_values = explainer(X_test)

shap.summary_plot(shap_values, X_test, show=False)
plt.savefig("plots/shap_summary_classifier.png", dpi=150, bbox_inches='tight')
print("Saved SHAP summary: plots/shap_summary_classifier.png")

con.close()
print("Classifier trained!")