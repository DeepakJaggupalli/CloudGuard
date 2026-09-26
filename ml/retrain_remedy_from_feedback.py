from pathlib import Path
import sys

import joblib
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import RandomForestClassifier

ROOT = Path(__file__).resolve().parents[1]

DATASET = (
    ROOT
    / "data"
    / "Data set"
    / "vm_events_remedies_train_8000.csv"
)

MODEL_PATH = (
    ROOT
    / "ml"
    / "models"
    / "remedy_random_forest.joblib"
)

sys.path.insert(0, str(ROOT))

from remediation.feedback import get_training_feedback


FEATURES = [
    "event_type",
    "event_category",
    "severity",
]

TARGET = "target_label"


def load_original_training_data():
    df = pd.read_csv(DATASET)

    df = df[FEATURES + [TARGET]].copy()

    df = df.fillna("unknown")

    for column in FEATURES + [TARGET]:
        df[column] = df[column].astype(str)

    return df


def build_feedback_dataframe():
    feedback = get_training_feedback()

    if not feedback:
        return pd.DataFrame(
            columns=FEATURES + [TARGET]
        )

    rows = []

    for item in feedback:

        anomaly_type = str(
            item.get("anomaly_type", "Performance Anomaly")
        )

        priority = str(
            item.get("priority", "P3")
        )

        remedy = str(
            item.get("recommended_remedy", "")
        )

        if not remedy:
            continue

        if "Network" in anomaly_type or "Packet" in anomaly_type:
            category = "network"

        elif "Disk" in anomaly_type:
            category = "storage"

        else:
            category = "performance"

        if priority == "P1":
            severity = "critical"
        elif priority == "P2":
            severity = "high"
        else:
            severity = "medium"

        rows.append({
            "event_type": anomaly_type,
            "event_category": category,
            "severity": severity,
            "target_label": remedy,
        })

    return pd.DataFrame(rows)


def train():

    print("=" * 64)
    print("CLOUDGUARD - REMEDY MODEL RETRAINING")
    print("=" * 64)

    base_df = load_original_training_data()

    feedback_df = build_feedback_dataframe()

    print(f"Original training rows : {len(base_df)}")
    print(f"Approved feedback rows : {len(feedback_df)}")

    if len(feedback_df) > 0:
        combined = pd.concat(
            [base_df, feedback_df],
            ignore_index=True
        )
    else:
        combined = base_df

    X = combined[FEATURES]
    y = combined[TARGET]

    prep = ColumnTransformer([
        (
            "cat",
            OneHotEncoder(handle_unknown="ignore"),
            FEATURES,
        )
    ])

    model = RandomForestClassifier(
        n_estimators=300,
        random_state=42,
        class_weight="balanced",
    )

    pipeline = Pipeline([
        ("prep", prep),
        ("model", model),
    ])

    pipeline.fit(X, y)

    MODEL_PATH.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    joblib.dump(
        pipeline,
        MODEL_PATH
    )

    print()
    print("Retraining complete.")
    print(f"Total training rows : {len(combined)}")
    print(f"Classes             : {len(y.unique())}")
    print(f"Model saved         : {MODEL_PATH}")
    print("=" * 64)


if __name__ == "__main__":
    train()
