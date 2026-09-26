import sys
import json
import os
import uuid
from datetime import datetime
from pathlib import Path

# ============================================================
# PROJECT PATH SETUP
# ============================================================

ROOT = Path(__file__).resolve().parents[1]
ML_DIR = ROOT / "ml"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if str(ML_DIR) not in sys.path:
    sys.path.insert(0, str(ML_DIR))


# ============================================================
# IMPORTS
# ============================================================

import joblib
import pandas as pd

from kafka import KafkaConsumer, KafkaProducer

from features import FEATURES


# ============================================================
# MODEL PATHS
# ============================================================

ANOMALY_MODEL = (
    ROOT
    / "ml"
    / "models"
    / "isolation_forest.joblib"
)

REMEDY_MODEL = (
    ROOT
    / "ml"
    / "models"
    / "remedy_random_forest.joblib"
)


# ============================================================
# KAFKA CONFIGURATION
# ============================================================

KAFKA_SERVER = os.getenv(
    "KAFKA_SERVER",
    "localhost:9092"
)

INPUT_TOPIC = "cloudguard-telemetry"

OUTPUT_TOPIC = "cloudguard-predictions"

REMEDIATION_TOPIC = "cloudguard-remediation"


# ============================================================
# PRIORITY ENGINE
# ============================================================

def priority_for(row, anomaly_score):

    cpu = float(
        row.get("cpu_utilization_pct", 0) or 0
    )

    memory = float(
        row.get("memory_utilization_pct", 0) or 0
    )

    latency = float(
        row.get("latency_ms", 0) or 0
    )

    packet_loss = float(
        row.get("packet_loss_pct", 0) or 0
    )

    # --------------------------------------------------------
    # P1 - CRITICAL
    # --------------------------------------------------------

    if (
        cpu >= 90
        or memory >= 95
        or packet_loss >= 10
        or latency >= 250
    ):
        return "P1"

    # --------------------------------------------------------
    # P2 - HIGH
    # --------------------------------------------------------

    if (
        cpu >= 75
        or memory >= 85
        or packet_loss >= 3
        or latency >= 150
        or anomaly_score < -0.12
    ):
        return "P2"

    # --------------------------------------------------------
    # P3 - MEDIUM
    # --------------------------------------------------------

    return "P3"


# ============================================================
# EVENT TYPE DETECTION
# ============================================================

def event_type_from(row):

    cpu = float(
        row.get("cpu_utilization_pct", 0) or 0
    )

    memory = float(
        row.get("memory_utilization_pct", 0) or 0
    )

    disk = float(
        row.get("disk_utilization_pct", 0) or 0
    )

    latency = float(
        row.get("latency_ms", 0) or 0
    )

    packet_loss = float(
        row.get("packet_loss_pct", 0) or 0
    )

    # Network problems first

    if packet_loss >= 3:
        return "Packet Loss Detected"

    if latency >= 150:
        return "High Network Latency"

    # Compute problems

    if cpu >= 80:
        return "High CPU Utilization"

    if memory >= 90:
        return "High Memory Utilization"

    # Storage problem

    if disk >= 90:
        return "Disk Space Critical"

    return "Performance Anomaly"


# ============================================================
# EVENT CATEGORY
# ============================================================

def category_for(event_type):

    if (
        "Network" in event_type
        or "Packet" in event_type
    ):
        return "network"

    if "Disk" in event_type:
        return "storage"

    return "performance"


# ============================================================
# LOAD AI MODELS
# ============================================================

def load_models():

    print()
    print("Loading AI models...")

    if not ANOMALY_MODEL.exists():

        raise FileNotFoundError(
            f"Anomaly model not found:\n{ANOMALY_MODEL}"
        )

    if not REMEDY_MODEL.exists():

        raise FileNotFoundError(
            f"Remedy model not found:\n{REMEDY_MODEL}"
        )

    anomaly_model = joblib.load(
        ANOMALY_MODEL
    )

    remedy_model = joblib.load(
        REMEDY_MODEL
    )

    print("Model 1 : Isolation Forest")
    print("Model 2 : Random Forest")
    print("AI models loaded successfully.")

    return anomaly_model, remedy_model


# ============================================================
# KAFKA CONNECTION
# ============================================================

def create_kafka():

    print()
    print("Connecting to Kafka...")

    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="latest",
        value_deserializer=lambda data: json.loads(
            data.decode("utf-8")
        ),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print(f"Kafka server : {KAFKA_SERVER}")
    print(f"Input topic  : {INPUT_TOPIC}")
    print(f"Output topic : {OUTPUT_TOPIC}")
    print(f"Remedy topic : {REMEDIATION_TOPIC}")
    print("Kafka connected successfully.")

    return consumer, producer

# ============================================================
# PROCESS TELEMETRY
# ============================================================

def process_telemetry(
    row,
    anomaly_model,
    remedy_model
):

    # ========================================================
    # FEATURE PREPARATION
    # ========================================================

    feature_values = {}

    for feature in FEATURES:

        value = row.get(
            feature,
            0
        )

        try:

            feature_values[feature] = float(
                value or 0
            )

        except (
            TypeError,
            ValueError
        ):

            feature_values[feature] = 0.0

    X = pd.DataFrame(
        [feature_values],
        columns=FEATURES
    )

    X = X.apply(
        pd.to_numeric,
        errors="coerce"
    ).fillna(0)

    # ========================================================
    # MODEL 1
    # ISOLATION FOREST
    # ========================================================

    model1_flag = int(
        anomaly_model.predict(X)[0]
    )

    anomaly_score = float(
        anomaly_model.decision_function(X)[0]
    )

    is_anomaly = (
        model1_flag == -1
    )

    # ========================================================
    # INCIDENT INFORMATION
    # ========================================================

    event_type = event_type_from(row)

    event_category = category_for(
        event_type
    )

    priority = priority_for(
        row,
        anomaly_score
    )

    # ========================================================
    # DEFAULT NORMAL VALUES
    # ========================================================

    remedy = "none"

    confidence = 0.0

    decision = "no_anomaly"

    remediation_action = "monitor_only"

    reason = (
        "Model 1 classified the telemetry as normal."
    )

    # ========================================================
    # MODEL 2
    # RANDOM FOREST REMEDY PREDICTION
    #
    # IMPORTANT:
    # Model 2 ONLY RECOMMENDS a remedy.
    #
    # It DOES NOT execute the remedy.
    # ========================================================

    if is_anomaly:

        severity = (
            "critical"
            if priority == "P1"
            else (
                "high"
                if priority == "P2"
                else "medium"
            )
        )

        remedy_features = pd.DataFrame(
            [{
                "event_type": event_type,
                "event_category": event_category,
                "severity": severity,
            }]
        )

        # ----------------------------------------------------
        # Predict recommended remedy
        # ----------------------------------------------------

        remedy = str(
            remedy_model.predict(
                remedy_features
            )[0]
        )

        # ----------------------------------------------------
        # Calculate confidence
        # ----------------------------------------------------

        probabilities = (
            remedy_model.predict_proba(
                remedy_features
            )[0]
        )

        confidence = float(
            max(probabilities)
        )

        # ====================================================
        # IS IT A KNOWN ANOMALY?
        # ====================================================
        # Model 2 predicts the remedy and calculates confidence score.
        # Known Anomaly -> Model 2 Random Forest predicts remedy -> User Approval
        # Unknown Anomaly -> Human Intervention directly.
        # ====================================================

        is_known_anomaly = (confidence >= 0.50) and (remedy not in ["none", "unknown", ""])

        if is_known_anomaly:
            decision = "pending_approval"
            remediation_action = "awaiting_user_approval"
            reason = (
                "Known anomaly detected. Model 2 recommended a remediation. "
                "Awaiting operator approval."
            )
        else:
            decision = "human_intervention"
            remediation_action = "human_operator_required"
            reason = (
                "Unknown anomaly pattern detected by Isolation Forest. "
                "Human operator intervention required to resolve."
            )
    else:
        is_known_anomaly = False

    # ========================================================
    # CREATE UNIQUE EVENT ID
    # ========================================================

    event_id = str(
        uuid.uuid4()
    )

    # ========================================================
    # BUILD OUTPUT MESSAGE
    # ========================================================

    output = {

        # ----------------------------------------------------
        # UNIQUE EVENT IDENTIFIER
        # ----------------------------------------------------

        "event_id":
            event_id,

        # ----------------------------------------------------
        # BASIC INFORMATION
        # ----------------------------------------------------

        "timestamp":
            row.get(
                "timestamp"
            )
            or datetime.utcnow().isoformat(),

        "vm_id":
            row.get(
                "vm_id",
                "unknown"
            ),

        "application":
            row.get(
                "application",
                "unknown"
            ),

        "region":
            row.get(
                "region",
                "unknown"
            ),

        "environment":
            row.get(
                "environment",
                "unknown"
            ),

        # ----------------------------------------------------
        # TELEMETRY
        # ----------------------------------------------------

        "cpu":
            feature_values[
                "cpu_utilization_pct"
            ],

        "memory":
            feature_values[
                "memory_utilization_pct"
            ],

        "latency":
            feature_values[
                "latency_ms"
            ],

        # ----------------------------------------------------
        # MODEL 1
        # ----------------------------------------------------

        "model1":
            "anomaly"
            if is_anomaly
            else "normal",

        "anomaly_score":
            round(
                anomaly_score,
                5
            ),

        # ----------------------------------------------------
        # INCIDENT PRIORITY
        # ----------------------------------------------------

        "priority":
            priority
            if is_anomaly
            else "P4",

        "event_type":
            event_type
            if is_anomaly
            else "Normal Telemetry",

        "event_category":
            event_category
            if is_anomaly
            else "normal",

        # ----------------------------------------------------
        # MODEL 2
        # ----------------------------------------------------

        "model2_remedy":
            remedy,

        "is_known_anomaly":
            is_known_anomaly,

        "confidence":
            round(
                confidence * 100,
                2
            ),

        # ----------------------------------------------------
        # REMEDIATION DECISION
        # ----------------------------------------------------

        "decision":
            decision,

        "remediation_action":
            remediation_action,

        "reason":
            reason,

        # ----------------------------------------------------
        # USER APPROVAL STATE
        # ----------------------------------------------------

        "approval_status":
            "pending"
            if is_anomaly
            else "not_required",

        # ----------------------------------------------------
        # GROUND TRUTH FROM DATASET
        # ----------------------------------------------------

        "actual_status":
            str(
                row.get(
                    "health_status",
                    "unknown"
                )
            ).lower(),
    }

    return output


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():

    print()
    print("=" * 64)

    print(
        "CLOUDGUARD AI - "
        "ANOMALY + REMEDIATION PIPELINE"
    )

    print("=" * 64)

    # ========================================================
    # LOAD MODELS
    # ========================================================

    anomaly_model, remedy_model = (
        load_models()
    )

    # ========================================================
    # CONNECT KAFKA
    # ========================================================

    consumer, producer = (
        create_kafka()
    )

    print()
    print("=" * 64)

    print(
        "Pipeline is READY."
    )

    print(
        "Waiting for telemetry..."
    )

    print("=" * 64)
    print()

    # ========================================================
    # TELEMETRY LOOP
    # ========================================================

    try:

        for message in consumer:

            row = message.value

            try:

                output = process_telemetry(
                    row,
                    anomaly_model,
                    remedy_model
                )

                # ------------------------------------------------
                # Publish prediction
                # ------------------------------------------------

                producer.send(
                    OUTPUT_TOPIC,
                    value=output
                )

                # ------------------------------------------------
                # Publish remediation event
                #
                # This contains the recommendation.
                # It does NOT execute anything.
                # ------------------------------------------------

                if output["model1"] == "anomaly":

                    producer.send(
                        REMEDIATION_TOPIC,
                        value=output
                    )

                producer.flush()

                # =================================================
                # CONSOLE OUTPUT
                # =================================================

                print("-" * 64)

                print(
                    f"Event ID    : "
                    f"{output['event_id']}"
                )

                print(
                    f"VM          : "
                    f"{output['vm_id']}"
                )

                print(
                    f"Application : "
                    f"{output['application']}"
                )

                print(
                    f"CPU         : "
                    f"{output['cpu']:.2f}%"
                )

                print(
                    f"Memory      : "
                    f"{output['memory']:.2f}%"
                )

                print(
                    f"Latency     : "
                    f"{output['latency']:.2f} ms"
                )

                print(
                    f"Model 1     : "
                    f"{output['model1']}"
                )

                print(
                    f"Priority    : "
                    f"{output['priority']}"
                )

                print(
                    f"Model 2     : "
                    f"{output['model2_remedy']}"
                )

                print(
                    f"Confidence  : "
                    f"{output['confidence']:.2f}%"
                )

                print(
                    f"Decision    : "
                    f"{output['decision']}"
                )

                print(
                    f"Action      : "
                    f"{output['remediation_action']}"
                )

                print(
                    f"Approval    : "
                    f"{output['approval_status']}"
                )

                print(
                    f"Published   : "
                    f"{OUTPUT_TOPIC}"
                )

                if output["model1"] == "anomaly":

                    print(
                        f"Remediation : "
                        f"{REMEDIATION_TOPIC}"
                    )

                print("-" * 64)

            except Exception as error:

                print()
                print(
                    "ERROR processing telemetry:"
                )

                print(error)
                print()

    except KeyboardInterrupt:

        print()
        print(
            "Stopping CloudGuard AI pipeline..."
        )

    finally:

        try:
            consumer.close()
        except Exception:
            pass

        try:
            producer.close()
        except Exception:
            pass

        print(
            "Kafka connections closed."
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
