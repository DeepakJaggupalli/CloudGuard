import json
import time
import joblib

from kafka import KafkaConsumer, KafkaProducer


# ============================================================
# CLOUDGUARD AI - KAFKA ML CONSUMER
# ============================================================

KAFKA_SERVER = "localhost:9092"

INPUT_TOPIC = "cloudguard-telemetry"
OUTPUT_TOPIC = "cloudguard-predictions"

MODEL_PATH = "ml/models/health_model.joblib"


print("=" * 60)
print("CLOUDGUARD AI - KAFKA ML CONSUMER")
print("=" * 60)


# ============================================================
# LOAD ML MODEL
# ============================================================

print("\nLoading ML model...")

bundle = joblib.load(MODEL_PATH)

model = bundle["model"]
features = bundle["features"]

print("ML model loaded successfully.")
print("Model type :", type(model).__name__)
print("Features   :", features)


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="cloudguard-ml-consumer",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)


print("\nKafka server :", KAFKA_SERVER)
print("Input topic  :", INPUT_TOPIC)
print("Output topic :", OUTPUT_TOPIC)

print("\nKafka consumer connected.")
print("Waiting for telemetry...\n")


# ============================================================
# PROCESS TELEMETRY
# ============================================================

try:

    for message in consumer:

        telemetry = message.value

        try:

            # ------------------------------------------------
            # Prepare ML input
            # ------------------------------------------------

            input_data = [
                [telemetry[feature] for feature in features]
            ]

            # ------------------------------------------------
            # Prediction
            # ------------------------------------------------

            prediction = model.predict(input_data)[0]

            # ------------------------------------------------
            # Confidence
            # ------------------------------------------------

            probabilities = model.predict_proba(input_data)[0]

            confidence = max(probabilities) * 100

            # ------------------------------------------------
            # Actual status
            # ------------------------------------------------

            actual_status = telemetry.get(
                "health_status",
                "unknown"
            )

            # ------------------------------------------------
            # Match status
            # ------------------------------------------------

            match = (
                prediction.lower()
                == actual_status.lower()
            )

            # ------------------------------------------------
            # Build prediction message
            # ------------------------------------------------

            prediction_data = {

                "timestamp": telemetry.get(
                    "timestamp",
                    ""
                ),

                "vm_id": telemetry.get(
                    "vm_id",
                    ""
                ),

                "region": telemetry.get(
                    "region",
                    ""
                ),

                "environment": telemetry.get(
                    "environment",
                    ""
                ),

                "os": telemetry.get(
                    "os",
                    ""
                ),

                "instance_type": telemetry.get(
                    "instance_type",
                    ""
                ),

                "application": telemetry.get(
                    "application",
                    ""
                ),

                "cpu_utilization_pct":
                    telemetry.get(
                        "cpu_utilization_pct",
                        0
                    ),

                "memory_utilization_pct":
                    telemetry.get(
                        "memory_utilization_pct",
                        0
                    ),

                "disk_utilization_pct":
                    telemetry.get(
                        "disk_utilization_pct",
                        0
                    ),

                "network_in_mbps":
                    telemetry.get(
                        "network_in_mbps",
                        0
                    ),

                "network_out_mbps":
                    telemetry.get(
                        "network_out_mbps",
                        0
                    ),

                "disk_iops":
                    telemetry.get(
                        "disk_iops",
                        0
                    ),

                "latency_ms":
                    telemetry.get(
                        "latency_ms",
                        0
                    ),

                "packet_loss_pct":
                    telemetry.get(
                        "packet_loss_pct",
                        0
                    ),

                "temperature_c":
                    telemetry.get(
                        "temperature_c",
                        0
                    ),

                "uptime_minutes":
                    telemetry.get(
                        "uptime_minutes",
                        0
                    ),

                "actual_status":
                    actual_status,

                "prediction":
                    prediction,

                "confidence":
                    round(confidence, 2),

                "match":
                    match
            }

            # ------------------------------------------------
            # Publish prediction to Kafka
            # ------------------------------------------------

            producer.send(
                OUTPUT_TOPIC,
                prediction_data
            )

            producer.flush()

            # ------------------------------------------------
            # Display result
            # ------------------------------------------------

            print("=" * 60)
            print("CLOUDGUARD AI - VM HEALTH PREDICTION")
            print("=" * 60)

            print(
                f"VM ID       : "
                f"{prediction_data['vm_id']}"
            )

            print(
                f"Application : "
                f"{prediction_data['application']}"
            )

            print(
                f"Region      : "
                f"{prediction_data['region']}"
            )

            print(
                f"Environment : "
                f"{prediction_data['environment']}"
            )

            print()

            print(
                f"CPU         : "
                f"{prediction_data['cpu_utilization_pct']:.2f}%"
            )

            print(
                f"Memory      : "
                f"{prediction_data['memory_utilization_pct']:.2f}%"
            )

            print(
                f"Disk        : "
                f"{prediction_data['disk_utilization_pct']:.2f}%"
            )

            print(
                f"Latency     : "
                f"{prediction_data['latency_ms']:.2f} ms"
            )

            print(
                f"Packet Loss : "
                f"{prediction_data['packet_loss_pct']:.2f}%"
            )

            print(
                f"Temperature : "
                f"{prediction_data['temperature_c']:.2f} C"
            )

            print()

            print(
                f"Actual Status : "
                f"{actual_status.upper()}"
            )

            print(
                f"AI Prediction : "
                f"{prediction.upper()}"
            )

            print(
                f"Confidence    : "
                f"{confidence:.2f}%"
            )

            print(
                f"Prediction    : "
                f"{'MATCH' if match else 'MISMATCH'}"
            )

            print(
                f"Published     : "
                f"{OUTPUT_TOPIC}"
            )

            print("=" * 60)
            print()

        except Exception as error:

            print(
                "\nError processing telemetry:"
            )

            print(error)

except KeyboardInterrupt:

    print("\nStopping CloudGuard ML consumer...")

finally:

    consumer.close()
    producer.close()

    print("Kafka consumer and producer closed.")
