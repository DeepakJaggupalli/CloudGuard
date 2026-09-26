FEATURES = [
    'cpu_utilization_pct',
    'memory_utilization_pct',
    'disk_utilization_pct',
    'network_in_mbps',
    'network_out_mbps',
    'disk_iops',
    'latency_ms',
    'packet_loss_pct',
    'temperature_c',
    'uptime_minutes',
]


def row_to_vector(row):
    return [float(row.get(f, 0) or 0) for f in FEATURES]
