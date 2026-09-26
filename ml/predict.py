import sys
import pandas as pd
import joblib

MODEL_PATH = "ml/models/health_model.joblib"

saved = joblib.load(MODEL_PATH)

model = saved["model"]
features = saved["features"]


def predict_health(data):
    df = pd.DataFrame([data])

    prediction = model.predict(df[features])[0]

    probabilities = model.predict_proba(df[features])[0]

    confidence = max(probabilities)

    return prediction, confidence


if __name__ == "__main__":

    sample = {
        "cpu_utilization_pct": 42.49,
        "memory_utilization_pct": 94.32,
        "disk_utilization_pct": 60.89,
        "network_in_mbps": 74.53,
        "network_out_mbps": 167.70,
        "disk_iops": 1285.12,
        "latency_ms": 52.44,
        "packet_loss_pct": 0.07,
        "temperature_c": 60.32,
        "uptime_minutes": 92330,
    }

    prediction, confidence = predict_health(sample)

    print("Prediction :", prediction)
    print("Confidence:", f"{confidence:.2%}")
