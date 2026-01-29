import duckdb
import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error
import shap
import matplotlib.pyplot as plt
from datetime import datetime
from math import sqrt

print("=== HYBRID MODEL V3: TUNED PHYSICS ADJUSTMENT (UTAH) ===")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

con = duckdb.connect('eco_pyric.duckdb')

df = con.execute("""
SELECT latitude, longitude, acq_date, brightness, dust_exposure, TAVG, TMAX, TMIN, AWND, PRCP, SNOW, SNWD
FROM fire_events_utah_with_weather
""").fetchdf()

print(f"Loaded {len(df):,} Utah rows with weather")

# Fill NaN
df = df.fillna({'TAVG': 10, 'TMAX': 15, 'TMIN': 5, 'AWND': 5, 'PRCP': 0, 'SNOW': 0, 'SNWD': 0})

df['month'] = pd.to_datetime(df['acq_date']).dt.month
df['year'] = pd.to_datetime(df['acq_date']).dt.year

features = ['dust_exposure', 'TAVG', 'TMAX', 'TMIN', 'AWND', 'PRCP', 'SNOW', 'SNWD', 'month', 'year', 'latitude', 'longitude']
X = df[features]
y = df['brightness']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = XGBRegressor(n_estimators=200, learning_rate=0.05, max_depth=7, random_state=42)  # Tuned params
model.fit(X_train, y_train)

y_pred_ml = model.predict(X_test)
rmse_ml = sqrt(mean_squared_error(y_test, y_pred_ml))
print(f"ML RMSE: {rmse_ml:.2f}")

# Tuned physics adjustment (conditional boost)
def fwi_proxy(row):
    dryness = max(0, (row['TMAX'] - row['TMIN']) / 20)  # Diurnal range
    precip_factor = max(0, 1 - row['PRCP'] / 5)  # Low precip boosts
    wind_factor = min(row['AWND'] / 15, 1.5)  # Cap wind boost
    dust_factor = row['dust_exposure'] * 2.0
    fwi = dryness + precip_factor + wind_factor + dust_factor
    
    # Only boost if FWI > threshold (dangerous conditions)
    if fwi > 5:
        boost = 1.0 + min(fwi * 0.1, 0.3)  # Cap boost at +30%
    else:
        boost = 1.0  # No boost for low-risk conditions
    return boost

df_test = X_test.copy()
df_test['ml_pred'] = y_pred_ml
df_test['physics_boost'] = df_test.apply(fwi_proxy, axis=1)
df_test['hybrid_pred'] = df_test['ml_pred'] * df_test['physics_boost']

rmse_hybrid = sqrt(mean_squared_error(y_test, df_test['hybrid_pred']))
print(f"Hybrid RMSE: {rmse_hybrid:.2f}")
print(f"Improvement: {rmse_ml - rmse_hybrid:.2f} lower error")

# SHAP
explainer = shap.Explainer(model)
shap_values = explainer(X_test)

shap.summary_plot(shap_values, X_test, show=False)
plt.savefig("plots/shap_summary_utah_v3.png", dpi=150, bbox_inches='tight')
print("Saved SHAP summary V3: plots/shap_summary_utah_v3.png")

shap.dependence_plot("dust_exposure", shap_values.values, X_test, interaction_index="PRCP", show=False)
plt.savefig("plots/shap_interaction_dust_prcp_v3.png", dpi=150, bbox_inches='tight')
print("Saved interaction plot (dust vs precip) V3: plots/shap_interaction_dust_prcp_v3.png")

con.close()
print("Modeling V3 complete!")