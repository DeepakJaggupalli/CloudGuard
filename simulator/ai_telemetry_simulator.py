import pandas as pd
import time
import joblib

from pathlib import Path


# --------------------------------------------------
# Paths
# --------------------------------------------------

DATA_PATH = Path(
    "data/Data set/vm_monitoring_10000.csv"
)

MODEL_PATH = Path(
    "ml/models/health_model.joblib"
)


# --------------------------------------------------
# Load dataset
# --------------------------------------------------

df = pd.read_csv(DATA_PATH)


# --------------------------------------------------
# Load ML model
# --------------------------------------------------

saved = joblib.load(MODEL_PATH)

model = saved["model"]
features = saved["features"]


print("=" * 60)
print("CLOUDGUARD AI REAL-TIME TELEMETRY SIMULATOR")
print("=" * 60)

print(f"Loaded telemetry records : {len(df)}")
print("ML model loaded          : YES")

print("\nStarting simulation...\n")


# --------------------------------------------------
# Replay telemetry
# --------------------------------------------------

for _, row in df.iterrows():

    telemetry = row.to_dict()

    # Create dataframe for ML prediction
    input_data = pd.DataFrame(
        [{
            feature: telemetry[feature]
            for feature in features
        }]
    )

    # Predict
    prediction = model.predict(input_data)[0]

    probabilities = model.predict_proba(input_data)[0]

    confidence = max(probabilities)


    # --------------------------------------------------
    # Display result
    # --------------------------------------------------

    print("=" * 60)

    print("VM HEALTH PREDICTION")

    print("=" * 60)

    print(f"VM ID       : {telemetry['vm_id']}")
    print(f"Application : {telemetry['application']}")

    print()

    print(
        f"CPU         : "
        f"{telemetry['cpu_utilization_pct']:.2f}%"
    )

    print(
        f"Memory      : "
        f"{telemetry['memory_utilization_pct']:.2f}%"
    )

    print(
        f"Disk        : "
        f"{telemetry['disk_utilization_pct']:.2f}%"
    )

    print(
        f"Latency     : "
        f"{telemetry['latency_ms']:.2f} ms"
    )

    print(
        f"Packet Loss : "
        f"{telemetry['packet_loss_pct']:.2f}%"
    )

    print(
        f"Temperature : "
        f"{telemetry['temperature_c']:.2f} C"
    )

    print()

    print(f"Actual Status : {telemetry['health_status'].upper()}")
    print(f"AI Prediction : {prediction.upper()}")
    print(f"Confidence    : {confidence:.2%}")

    print("=" * 60)

    time.sleep(1)
