import pandas as pd
import joblib

from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    accuracy_score,
)

# --------------------------------------------------
# 1. Load dataset
# --------------------------------------------------

DATA_PATH = Path("data/Data set/vm_monitoring_train_8000.csv")
MODEL_PATH = Path("ml/models/health_model.joblib")

df = pd.read_csv(DATA_PATH)

print("=" * 60)
print("CLOUDGUARD AI - VM HEALTH MODEL")
print("=" * 60)

print(f"Dataset shape: {df.shape}")


# --------------------------------------------------
# 2. Select features
# --------------------------------------------------

FEATURES = [
    "cpu_utilization_pct",
    "memory_utilization_pct",
    "disk_utilization_pct",
    "network_in_mbps",
    "network_out_mbps",
    "disk_iops",
    "latency_ms",
    "packet_loss_pct",
    "temperature_c",
    "uptime_minutes",
]

TARGET = "health_status"

X = df[FEATURES]
y = df[TARGET]


# --------------------------------------------------
# 3. Split data
# --------------------------------------------------

X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.20,
    random_state=42,
    stratify=y,
)

print("\nTraining records:", len(X_train))
print("Testing records :", len(X_test))


# --------------------------------------------------
# 4. Train model
# --------------------------------------------------

model = RandomForestClassifier(
    n_estimators=300,
    random_state=42,
    class_weight="balanced",
    n_jobs=-1,
)

print("\nTraining Random Forest...")

model.fit(X_train, y_train)


# --------------------------------------------------
# 5. Evaluate
# --------------------------------------------------

predictions = model.predict(X_test)

accuracy = accuracy_score(y_test, predictions)

print("\n" + "=" * 60)
print("MODEL RESULTS")
print("=" * 60)

print(f"\nAccuracy: {accuracy:.4f}")

print("\nClassification Report:")
print(classification_report(y_test, predictions))

print("\nConfusion Matrix:")
print(confusion_matrix(y_test, predictions))


# --------------------------------------------------
# 6. Feature importance
# --------------------------------------------------

importance = pd.DataFrame({
    "feature": FEATURES,
    "importance": model.feature_importances_,
}).sort_values(
    "importance",
    ascending=False,
)

print("\nFeature Importance:")
print(importance.to_string(index=False))


# --------------------------------------------------
# 7. Save model
# --------------------------------------------------

MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)

joblib.dump(
    {
        "model": model,
        "features": FEATURES,
    },
    MODEL_PATH,
)

print("\nModel saved to:")
print(MODEL_PATH)
