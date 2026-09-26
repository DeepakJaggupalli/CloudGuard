import json
import time

import pandas as pd
from kafka import KafkaProducer


# --------------------------------------------------
# Configuration
# --------------------------------------------------

KAFKA_SERVER = "localhost:9092"
TOPIC = "cloudguard-telemetry"

DATA_PATH = "data/Data set/vm_monitoring_10000.csv"


# --------------------------------------------------
# Load telemetry dataset
# --------------------------------------------------

df = pd.read_csv(DATA_PATH)

print("=" * 60)
print("CLOUDGUARD KAFKA TELEMETRY PRODUCER")
print("=" * 60)

print(f"Loaded records : {len(df)}")
print(f"Kafka server   : {KAFKA_SERVER}")
print(f"Kafka topic    : {TOPIC}")


# --------------------------------------------------
# Create Kafka producer
# --------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("\nKafka producer connected successfully.")
print("Starting telemetry stream...\n")


# --------------------------------------------------
# Send telemetry
# --------------------------------------------------

try:

    for _, row in df.iterrows():

        telemetry = row.to_dict()

        # Convert pandas/numpy values to normal Python values
        telemetry = {
            key: value.item() if hasattr(value, "item") else value
            for key, value in telemetry.items()
        }

        # Send message to Kafka
        producer.send(
            TOPIC,
            value=telemetry
        )

        producer.flush()

        print(
            f"Sent -> "
            f"{telemetry['vm_id']} | "
            f"CPU={telemetry['cpu_utilization_pct']:.2f}% | "
            f"Memory={telemetry['memory_utilization_pct']:.2f}% | "
            f"Status={telemetry['health_status']}"
        )

        time.sleep(1)


except KeyboardInterrupt:

    print("\nStopping telemetry producer...")


finally:

    producer.close()

    print("Kafka producer closed.")
