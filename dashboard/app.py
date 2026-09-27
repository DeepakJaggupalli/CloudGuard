import json
import threading
import requests
import os
import sys
import time
from pathlib import Path
from collections import deque, Counter
import queue
from flask import Flask, jsonify, render_template, request, Response

subscribers = []

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kafka import KafkaConsumer
from remediation.engine import apply_remediation
from remediation.feedback import init_db, store_feedback, get_feedback
from remediation.aws_scaling import load_aws_config, save_aws_config

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
TOPIC = "cloudguard-predictions"
MAX_RECORDS = 300

app = Flask(__name__, template_folder=".")
records = deque(maxlen=MAX_RECORDS)
lock = threading.Lock()


# --------------------------------------------------
# Kafka listener
# --------------------------------------------------

def listen():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                auto_offset_reset="earliest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
            print("Dashboard listening on Kafka topic:", TOPIC)
            for msg in consumer:
                data = msg.value
                if "event_id" not in data:
                    data["event_id"] = f"{data.get('vm_id', 'unknown')}-{data.get('timestamp', 'unknown')}"
                with lock:
                    records.append(data)
                    for q in list(subscribers):
                        try:
                            q.put_nowait(data)
                        except Exception:
                            pass
        except Exception as e:
            print(f"Kafka consumer error in dashboard: {e}. Retrying in 3s...")
            time.sleep(3)


# --------------------------------------------------
# Dashboard Home
# --------------------------------------------------


# --------------------------------------------------
# Live Kafka Relay Stream (SSE)
# --------------------------------------------------

@app.get("/api/stream")
def stream():
    def event_stream():
        q = queue.Queue()
        with lock:
            subscribers.append(q)
        try:
            yield "data: {\"status\": \"connected\"}\n\n"
            while True:
                data = q.get()
                yield f"data: {json.dumps(data)}\n\n"
        except GeneratorExit:
            with lock:
                if q in subscribers:
                    subscribers.remove(q)

    return Response(event_stream(), content_type="text/event-stream")


@app.get("/")
def index():
    return render_template("index.html")


# --------------------------------------------------
# Predictions API
# --------------------------------------------------

@app.get("/api/predictions")
def predictions():
    with lock:
        data = list(records)
    return jsonify(data[::-1])


# --------------------------------------------------
# Statistics
# --------------------------------------------------

@app.get("/api/stats")
def stats():
    with lock:
        data = list(records)

    anomalies = [x for x in data if x.get("model1") == "anomaly"]
    known_anomalies = [x for x in data if x.get("is_known_anomaly") is True]
    unknown_anomalies = [x for x in data if x.get("model1") == "anomaly" and x.get("is_known_anomaly") is False]
    pending = [x for x in data if x.get("decision") == "pending_approval"]
    human = [x for x in data if x.get("decision") == "human_intervention"]
    approved = [x for x in data if x.get("approval") == "approved"]
    rejected = [x for x in data if x.get("approval") == "rejected"]

    priorities = Counter(x.get("priority", "P4") for x in anomalies)

    return jsonify({
        "total": len(data),
        "anomalies": len(anomalies),
        "normal": len(data) - len(anomalies),
        "known_anomalies": len(known_anomalies),
        "unknown_anomalies": len(unknown_anomalies),
        "pending_approval": len(pending),
        "human_intervention": len(human),
        "approved": len(approved),
        "rejected": len(rejected),
        "p1": priorities.get("P1", 0),
        "p2": priorities.get("P2", 0),
        "p3": priorities.get("P3", 0),
        "p4": priorities.get("P4", 0)
    })


# --------------------------------------------------
# USER APPROVAL (AI EXECUTES AWS/CONTAINER REMEDIATION)
# --------------------------------------------------

@app.post("/api/remediation/approve")
def approve_remediation():
    payload = request.get_json(silent=True) or {}
    event_id = payload.get("event_id")
    operator_note = payload.get("operator_note", "")

    if not event_id:
        return jsonify({"success": False, "error": "event_id is required"}), 400

    with lock:
        event = next((x for x in records if x.get("event_id") == event_id), None)

    if event is None:
        return jsonify({"success": False, "error": "Event not found"}), 404

    remedy = event.get("model2_remedy", "none")
    priority = event.get("priority", "P4")
    confidence = float(event.get("confidence", 0)) / 100.0
    vm_id = event.get("vm_id", "unknown")

    # --------------------------------------------------
    # AI Executes AWS EC2 / ASG / Container Scaling
    # --------------------------------------------------
    result = apply_remediation(remedy, instance_id=vm_id)
    execution_status = result.get("status", "executed")

    event["approval"] = "approved"
    event["decision"] = "approved_and_executed"
    event["remediation_action"] = result.get("action", remedy)
    event["execution_status"] = execution_status
    event["aws_mode"] = result.get("mode", "simulated")
    event["aws_details"] = result.get("aws_details", "")

    store_feedback(
        event_id=event_id,
        vm_id=vm_id,
        anomaly_type=event.get("event_type", "unknown"),
        recommended_remedy=remedy,
        confidence=confidence,
        priority=priority,
        approval="approved",
        execution_status=execution_status,
        operator_note=operator_note,
    )

    return jsonify({
        "success": True,
        "event_id": event_id,
        "approval": "approved",
        "execution": result,
    })


# --------------------------------------------------
# USER REJECTION (HUMAN INTERVENTION)
# --------------------------------------------------

@app.post("/api/remediation/reject")
def reject_remediation():
    payload = request.get_json(silent=True) or {}
    event_id = payload.get("event_id")
    operator_note = payload.get("operator_note", "")

    if not event_id:
        return jsonify({"success": False, "error": "event_id is required"}), 400

    with lock:
        event = next((x for x in records if x.get("event_id") == event_id), None)

    if event is None:
        return jsonify({"success": False, "error": "Event not found"}), 404

    event["approval"] = "rejected"
    event["decision"] = "human_intervention"
    event["remediation_action"] = "manual_resolution"

    store_feedback(
        event_id=event_id,
        vm_id=event.get("vm_id", "unknown"),
        anomaly_type=event.get("event_type", "unknown"),
        recommended_remedy=event.get("model2_remedy", "none"),
        confidence=float(event.get("confidence", 0)) / 100.0,
        priority=event.get("priority", "P4"),
        approval="rejected",
        execution_status="human_intervention",
        operator_note=operator_note,
    )

    return jsonify({
        "success": True,
        "event_id": event_id,
        "approval": "rejected",
        "decision": "human_intervention",
    })


# --------------------------------------------------
# ONLINE MODEL RETRAINING API
# --------------------------------------------------

@app.post("/api/retrain")
def retrain_model():
    """Trigger model retraining using feedback stored from human operator decisions."""
    try:
        from ml.retrain_remedy_from_feedback import train
        train()
        return jsonify({
            "success": True,
            "message": "Model 2 (Random Forest Remedy Model) successfully retrained from stored feedback!"
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# --------------------------------------------------
# AWS CONFIGURATION API
# --------------------------------------------------

@app.get("/api/aws/config")
def get_aws_config_route():
    cfg = load_aws_config()
    # Mask secret key for UI display
    if cfg.get("aws_secret_access_key"):
        cfg["aws_secret_access_key_masked"] = "****************" + cfg["aws_secret_access_key"][-4:]
    return jsonify(cfg)


@app.post("/api/aws/config")
def set_aws_config_route():
    payload = request.get_json(silent=True) or {}
    updated = save_aws_config(payload)
    return jsonify({"success": True, "config": updated})


# --------------------------------------------------
# DEMO APPS & HIGH CPU TRIGGER
# --------------------------------------------------

@app.post("/api/demo/trigger-load")
def trigger_demo_load():
    """Trigger CPU load on live demo app (Local Docker, AWS VM, or Local Workload)."""
    custom_url = request.args.get("url")
    urls_to_try = [
        custom_url,
        "http://localhost:8080/load",
        "http://127.0.0.1:8080/load",
        "http://demo-app:8080/load",
        "http://cloudguard-demo-app:8080/load",
        "http://host.docker.internal:8080/load"
    ]
    urls_to_try = [u for u in urls_to_try if u]

    for url in urls_to_try:
        try:
            res = requests.get(url, timeout=4)
            if res.status_code == 200:
                return jsonify({
                    "success": True,
                    "message": f"CPU load triggered successfully on {url}",
                    "response": res.json()
                })
        except Exception:
            pass

    # Self-contained CPU load burn so trigger always succeeds for demo
    def burn_cpu():
        import time
        start = time.time()
        while time.time() - start < 4.0:
            pass

    threading.Thread(target=burn_cpu, daemon=True).start()

    return jsonify({
        "success": True,
        "message": "High CPU load spike generated successfully on workload",
        "response": {"duration_seconds": 4, "mode": "workload_cpu_burn"}
    })


# --------------------------------------------------
# FEEDBACK API
# --------------------------------------------------

@app.get("/api/feedback")
def feedback():
    return jsonify(get_feedback())


# --------------------------------------------------
# START SERVER
# --------------------------------------------------

if __name__ == "__main__":
    init_db()

    threading.Thread(target=listen, daemon=True).start()

    app.run(host="0.0.0.0", port=5000, debug=False)
