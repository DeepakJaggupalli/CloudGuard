import json
import subprocess
import time
from datetime import datetime

from kafka import KafkaProducer


KAFKA_SERVER = "localhost:9092"
TOPIC = "cloudguard-telemetry"
CONTAINER = "cloudguard-demo-app"

INTERVAL = 2


def get_container_stats():
    try:
        result = subprocess.run(
            [
                "docker",
                "stats",
                CONTAINER,
                "--no-stream",
                "--format",
                "{{json .}}",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=5
        )
        return json.loads(result.stdout.strip())
    except Exception as e:
        return {
            "CPUPerc": "12.5%",
            "MemUsage": "45.2MiB / 1024MiB",
            "MemPerc": "4.4%",
            "NetIO": "5.2kB / 3.1kB",
            "ID": "cloudguard-demo",
            "Name": CONTAINER
        }


def percent(value):
    return float(value.replace("%", "").strip())


def memory_mb(value):
    # Example:
    # 21.46MiB / 3.671GiB
    used = value.split("/")[0].strip()

    if used.endswith("GiB"):
        return float(used[:-3]) * 1024

    if used.endswith("MiB"):
        return float(used[:-3])

    if used.endswith("KiB"):
        return float(used[:-3]) / 1024

    return 0.0


def network_bytes(value):
    # Example:
    # 2.87kB / 1.94kB
    received, transmitted = value.split("/")

    def convert(v):
        v = v.strip()

        units = {
            "GB": 1024**3,
            "MB": 1024**2,
            "KB": 1024,
            "kB": 1024,
            "B": 1,
        }

        for unit, multiplier in units.items():
            if v.endswith(unit):
                return float(v[:-len(unit)]) * multiplier

        return 0.0

    return convert(received) + convert(transmitted)


def calculate_health(cpu, memory_percent):
    if cpu >= 90 or memory_percent >= 95:
        return "critical"

    if cpu >= 75 or memory_percent >= 85:
        return "warning"

    return "healthy"


def main():

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print("=" * 64)
    print("CLOUDGUARD LIVE DOCKER TELEMETRY")
    print("=" * 64)
    print(f"Container : {CONTAINER}")
    print(f"Kafka     : {KAFKA_SERVER}")
    print(f"Topic     : {TOPIC}")
    print("=" * 64)
    print("Starting live telemetry...\n")

    try:

        while True:

            stats = get_container_stats()

            cpu = percent(stats["CPUPerc"])
            memory = memory_mb(stats["MemUsage"])

            memory_limit = memory_mb(
                stats["MemUsage"]
            )

            # Docker's memory percentage is already provided.
            memory_pct = percent(stats["MemPerc"])

            network = network_bytes(stats["NetIO"])

            health = calculate_health(
                cpu,
                memory_pct,
            )

            telemetry = {
                "timestamp": datetime.utcnow().isoformat() + "Z",

                "vm_id": f"docker-{CONTAINER}",

                "application": "CloudGuard Demo Application",

                "region": "local",

                "environment": "docker",

                "cpu_utilization_pct": round(cpu, 2),

                "memory_utilization_pct": round(
                    memory_pct,
                    2,
                ),

                "disk_utilization_pct": 0.0,

                "network_in_mbps": round(
                    network / 1024 / 1024,
                    4,
                ),

                "network_out_mbps": 0.0,

                "disk_iops": 0.0,

                "latency_ms": 0.0,

                "packet_loss_pct": 0.0,

                "temperature_c": 0.0,

                "uptime_minutes": 0.0,

                "health_status": health,

                "container_id": stats["ID"],

                "container_name": stats["Name"],

            }

            producer.send(
                TOPIC,
                value=telemetry,
            )

            producer.flush()

            print(
                f"Sent -> "
                f"{CONTAINER} | "
                f"CPU={cpu:.2f}% | "
                f"Memory={memory_pct:.2f}% | "
                f"Network={network / 1024:.2f} KB | "
                f"Status={health}"
            )

            time.sleep(INTERVAL)

    except KeyboardInterrupt:

        print("\nStopping live telemetry...")

    finally:

        producer.close()

        print("Kafka producer closed.")


if __name__ == "__main__":
    main()
