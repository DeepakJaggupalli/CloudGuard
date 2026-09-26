import pandas as pd
import joblib

from pathlib import Path
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
)

DATA_PATH = Path("data/Data set/vm_monitoring_10000.csv")
MODEL_PATH = Path("ml/models/health_model.joblib")

# --------------------------------------------------
# 1. Load model
# --------------------------------------------------

saved = joblib.load(MODEL_PATH)

model = saved["model"]
FEATURES = saved["features"]

print("=" * 60)
print("CLOUDGUARD AI - EXTERNAL MODEL EVALUATION")
print("=" * 60)

# --------------------------------------------------
# 2. Load separate dataset
# --------------------------------------------------

df = pd.read_csv(DATA_PATH)

print("\nDataset shape:", df.shape)

print("\nActual health distribution:")
print(df["health_status"].value_counts())

# --------------------------------------------------
# 3. Prepare data
# --------------------------------------------------

X = df[FEATURES]
y = df["health_status"]

# --------------------------------------------------
# 4. Predict
# --------------------------------------------------

predictions = model.predict(X)

# --------------------------------------------------
# 5. Evaluate
# --------------------------------------------------

accuracy = accuracy_score(y, predictions)

print("\n" + "=" * 60)
print("EXTERNAL DATASET RESULTS")
print("=" * 60)

print(f"\nAccuracy: {accuracy:.4f}")

print("\nClassification Report:")
print(
    classification_report(
        y,
        predictions,
        digits=4
    )
)

print("\nConfusion Matrix:")
print(confusion_matrix(y, predictions))
