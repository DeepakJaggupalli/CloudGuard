import sys
import json
import os
import uuid
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ML_DIR = ROOT / "ml"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if str(ML_DIR) not in sys.path:
    sys.path.insert(0, str(ML_DIR))

import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from features import FEATURES

ANOMALY_MODEL = ROOT / "ml" / "models" / "isolation_forest.joblib"
REMEDY_MODEL = ROOT / "ml" / "models" / "remedy_random_forest.joblib"

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
INPUT_TOPIC = "cloudguard-telemetry"
OUTPUT_TOPIC = "cloudguard-predictions-v2"
REMEDIATION_TOPIC = "cloudguard-remediation"


def priority_for(row, is_anomaly):
    cpu = float(row.get("cpu_utilization_pct", 0) or 0)
    memory = float(row.get("memory_utilization_pct", 0) or 0)
    latency = float(row.get("latency_ms", 0) or 0)
    packet_loss = float(row.get("packet_loss_pct", 0) or 0)

    if cpu >= 85 or memory >= 90 or packet_loss >= 8 or latency >= 200:
        return "P1"
    if cpu >= 70 or memory >= 80 or packet_loss >= 3 or latency >= 120:
        return "P2"
    if is_anomaly:
        return "P3"
    return "P4"


def event_type_from(row):
    cpu = float(row.get("cpu_utilization_pct", 0) or 0)
    memory = float(row.get("memory_utilization_pct", 0) or 0)
    disk = float(row.get("disk_utilization_pct", 0) or 0)
    latency = float(row.get("latency_ms", 0) or 0)
    packet_loss = float(row.get("packet_loss_pct", 0) or 0)

    if packet_loss >= 3:
        return "Packet Loss Detected"
    if latency >= 120:
        return "High Network Latency"
    if cpu >= 70:
        return "High CPU Utilization"
    if memory >= 80:
        return "High Memory Utilization"
    if disk >= 85:
        return "Disk Space Critical"
    return "Performance Anomaly"


def category_for(event_type):
    if "Network" in event_type or "Packet" in event_type:
        return "network"
    if "Disk" in event_type:
        return "storage"
    return "performance"


def load_models():
    print("Loading AI models...")
    anomaly_model = joblib.load(ANOMALY_MODEL)
    remedy_model = joblib.load(REMEDY_MODEL)
    print("AI models loaded successfully.")
    return anomaly_model, remedy_model


def create_kafka():
    print("Connecting to Kafka at:", KAFKA_SERVER)
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="latest",
        value_deserializer=lambda data: json.loads(data.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    return consumer, producer


def process_telemetry(row, anomaly_model, remedy_model):
    feature_values = {}
    for feature in FEATURES:
        value = row.get(feature, 0)
        try:
            feature_values[feature] = float(value or 0)
        except (TypeError, ValueError):
            feature_values[feature] = 0.0

    X = pd.DataFrame([feature_values], columns=FEATURES).apply(pd.to_numeric, errors="coerce").fillna(0)

    cpu = feature_values["cpu_utilization_pct"]
    mem = feature_values["memory_utilization_pct"]
    lat = feature_values["latency_ms"]
    pkt = feature_values["packet_loss_pct"]

    # Explicit baseline check vs ML anomaly detection
    model1_flag = int(anomaly_model.predict(X)[0])
    anomaly_score = float(anomaly_model.decision_function(X)[0])

    # Rules + ML score for robust classification
    if cpu >= 70.0 or mem >= 80.0 or lat >= 120.0 or pkt >= 3.0:
        is_anomaly = True
    elif cpu <= 50.0 and mem <= 60.0 and lat <= 80.0 and pkt == 0.0:
        is_anomaly = False
    else:
        is_anomaly = (model1_flag == -1)

    priority = priority_for(row, is_anomaly)
    event_type = event_type_from(row) if is_anomaly else "Normal Telemetry"
    event_category = category_for(event_type) if is_anomaly else "normal"

    if is_anomaly:
        severity = "critical" if priority == "P1" else ("high" if priority == "P2" else "medium")
        remedy_features = pd.DataFrame([{
            "event_type": event_type,
            "event_category": event_category,
            "severity": severity,
        }])
        
        try:
            remedy = str(remedy_model.predict(remedy_features)[0])
            probs = remedy_model.predict_proba(remedy_features)[0]
            confidence = float(max(probs))
        except Exception:
            remedy = "scale_compute"
            confidence = 0.85

        is_known_anomaly = (remedy not in ["none", "unknown", ""])
        if is_known_anomaly:
            decision = "pending_approval"
            remediation_action = f"recommend_{remedy}"
            reason = f"Anomalous metric detected ({event_type}). Recommended remedy: {remedy}. Awaiting operator approval."
        else:
            decision = "human_intervention"
            remediation_action = "human_operator_required"
            reason = "Unknown anomaly pattern detected. Human intervention required."
    else:
        remedy = "none"
        confidence = 0.0
        is_known_anomaly = False
        decision = "no_anomaly"
        remediation_action = "monitor_only"
        reason = "Telemetry metrics operating within normal baseline boundaries."

    output = {
        "event_id": str(uuid.uuid4()),
        "timestamp": row.get("timestamp", datetime.utcnow().isoformat() + "Z"),
        "vm_id": row.get("vm_id", "i-0123456789abcdef0"),
        "application": row.get("application", "CloudGuard Demo Workload"),
        "region": row.get("region", "us-east-1"),
        "environment": row.get("environment", "live-production"),
        "cpu": round(cpu, 2),
        "memory": round(mem, 2),
        "latency": round(lat, 2),
        "model1": "anomaly" if is_anomaly else "normal",
        "anomaly_score": round(anomaly_score, 5),
        "priority": priority if is_anomaly else "P4",
        "event_type": event_type,
        "event_category": event_category,
        "model2_remedy": remedy,
        "is_known_anomaly": is_known_anomaly,
        "confidence": round(confidence * 100, 2),
        "decision": decision,
        "remediation_action": remediation_action,
        "reason": reason,
        "approval_status": "pending" if decision == "pending_approval" else ("human" if decision == "human_intervention" else "not_required"),
        "actual_status": str(row.get("health_status", "healthy")).lower(),
    }
    return output


def main():
    print("CLOUDGUARD AI PIPELINE STARTING...")
    anomaly_model, remedy_model = load_models()
    consumer, producer = create_kafka()
    print("Pipeline READY. Listening for telemetry...")

    try:
        for message in consumer:
            row = message.value
            output = process_telemetry(row, anomaly_model, remedy_model)
            producer.send(OUTPUT_TOPIC, value=output)
            if output["model1"] == "anomaly":
                producer.send(REMEDIATION_TOPIC, value=output)
            producer.flush()
            print(f"[{output['priority']}] {output['vm_id']} | CPU={output['cpu']}% | Model1={output['model1']} | Decision={output['decision']}")
    except KeyboardInterrupt:
        print("Pipeline stopped.")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
