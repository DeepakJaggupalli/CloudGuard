import pandas as pd
import time
import json
from pathlib import Path

DATA_PATH = Path(
    "data/Data set/vm_monitoring_10000.csv"
)

df = pd.read_csv(DATA_PATH)

print("=" * 60)
print("CLOUDGUARD TELEMETRY SIMULATOR")
print("=" * 60)

print(f"Loaded {len(df)} telemetry records")

print("\nStarting real-time replay...\n")

for _, row in df.iterrows():

    telemetry = row.to_dict()

    print(json.dumps(telemetry, indent=2))

    print("-" * 60)

    time.sleep(1)
