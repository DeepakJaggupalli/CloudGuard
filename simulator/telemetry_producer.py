import os
import sys
import json
import time
import random
import subprocess
from datetime import datetime
from pathlib import Path
from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from remediation.aws_scaling import load_aws_config

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "localhost:9092")
TOPIC = "cloudguard-telemetry"
CONTAINER = "cloudguard-demo-app"
INTERVAL = 2.0


def main():
    aws_cfg = load_aws_config()
    target_vm_id = aws_cfg.get("instance_id") or "i-0123456789abcdef0"
    aws_region = aws_cfg.get("aws_region", "us-east-1")

    print("=" * 64)
    print("CLOUDGUARD LIVE REAL-TIME TELEMETRY PRODUCER")
    print(f"Target VM ID : {target_vm_id} ({aws_region})")
    print(f"Kafka Server : {KAFKA_SERVER}")
    print("=" * 64)

    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda val: json.dumps(val).encode("utf-8")
            )
        except Exception as e:
            print(f"Waiting for Kafka at {KAFKA_SERVER}... ({e})")
            time.sleep(3)

    step = 0
    try:
        while True:
            step += 1
            # Intermittent synthetic anomaly spike every 8 steps to test P1/P2/P3 approval flow
            if step % 8 == 0:
                cpu = round(random.uniform(88.0, 96.5), 2)
                mem = round(random.uniform(85.0, 94.0), 2)
                lat = round(random.uniform(180.0, 290.0), 2)
                pkt = round(random.uniform(4.0, 12.0), 1)
                health = "critical"
            elif step % 5 == 0:
                cpu = round(random.uniform(72.0, 84.0), 2)
                mem = round(random.uniform(65.0, 78.0), 2)
                lat = round(random.uniform(110.0, 160.0), 2)
                pkt = 0.0
                health = "warning"
            else:
                cpu = round(random.uniform(4.0, 22.0), 2)
                mem = round(random.uniform(12.0, 35.0), 2)
                lat = round(random.uniform(15.0, 38.0), 2)
                pkt = 0.0
                health = "healthy"

            telemetry = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "vm_id": target_vm_id,
                "application": "CloudGuard AWS EC2 Workload",
                "region": aws_region,
                "environment": "live-production",
                "cpu_utilization_pct": cpu,
                "memory_utilization_pct": mem,
                "disk_utilization_pct": 28.5,
                "network_in_mbps": round(random.uniform(10.0, 80.0), 1),
                "network_out_mbps": round(random.uniform(5.0, 40.0), 1),
                "disk_iops": 240.0,
                "latency_ms": lat,
                "packet_loss_pct": pkt,
                "temperature_c": round(38.0 + (cpu * 0.3), 1),
                "uptime_minutes": 2880.0,
                "health_status": health,
                "is_live_stream": True
            }

            producer.send(TOPIC, value=telemetry)
            producer.flush()

            print(f"LIVE Stream -> {target_vm_id} | CPU={cpu:.1f}% | RAM={mem:.1f}% | Latency={lat:.1f}ms | Status={health.upper()}")
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("Telemetry producer stopped.")
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
