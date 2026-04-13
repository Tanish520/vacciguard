#!/usr/bin/env python3
"""
Generate realistic NDJSON test workloads for smoke tests.

Usage:
    python3 generate-test-workload.py --events 6000 --eps 100 --output workload-100eps.ndjson
    python3 generate-test-workload.py --events 60000 --eps 1000 --output workload-1000eps.ndjson
"""

import argparse
import json
import random
import string
from datetime import datetime, timedelta


def random_hex(length):
    return "".join(random.choices("0123456789abcdef", k=length))


def random_device_id():
    return f"dev-{random_hex(8)}"


def random_event_id():
    return f"evt-{random_hex(12)}"


# Known device IDs that match the device-facility lookup table
KNOWN_DEVICES = ["FR-0101", "FR-0102", "FR-0103"]


def generate_event(base_time, sequence):
    device_id = random.choice(KNOWN_DEVICES)
    event_time = (base_time + timedelta(seconds=sequence * 0.01)).isoformat() + "Z"

    return {
        "event_id": random_event_id(),
        "device_id": device_id,
        "event_time": event_time,
        "temperature_c": round(random.uniform(2.0, 8.0), 2),
        "door_open": random.choice([True, False]),
        "battery_pct": random.randint(15, 100),
        "location_lat": round(random.uniform(24.0, 25.0), 6),
        "location_lon": round(random.uniform(73.0, 74.0), 6),
    }


def generate_workload(num_events, output_path):
    base_time = datetime(2026, 4, 13, 0, 0, 0)
    print(f"Generating {num_events} events -> {output_path}")

    with open(output_path, "w") as f:
        for i in range(num_events):
            event = generate_event(base_time, i)
            f.write(json.dumps(event, separators=(",", ":")) + "\n")

    print(f"Done. File size: {len(open(output_path, 'rb').read()) / 1024:.1f} KB")


def main():
    parser = argparse.ArgumentParser(description="Generate NDJSON test workloads")
    parser.add_argument("--events", type=int, required=True, help="Number of events to generate")
    parser.add_argument("--eps", type=int, required=True, help="Target EPS (used for filename if output not specified)")
    parser.add_argument("--output", type=str, help="Output file path")
    args = parser.parse_args()

    output = args.output or f"workload-{args.eps}eps.ndjson"
    generate_workload(args.events, output)


if __name__ == "__main__":
    main()
