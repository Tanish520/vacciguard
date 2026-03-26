"""
simulator.py
Generates mock vaccine-fridge telemetry and publishes it to the
VacciGuard Kinesis stream in small batches.

Usage:
    python simulator/simulator.py

Prerequisites:
    - AWS credentials configured (env vars, ~/.aws/credentials, or IAM role)
    - Kinesis stream already created (vacciguard-stream)
"""

import json
import random
import time
from datetime import datetime, timezone

import boto3

# Import shared constants — never hardcode resource names here
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import KINESIS_STREAM_NAME, REGION

# ---------------------------------------------------------------------------
# Static reference data
# ---------------------------------------------------------------------------

LOCATIONS = [
    ("Rohini-PHC",          "North-West-Delhi", "Delhi"),
    ("Sadar-Bazar-CHC",     "Central-Delhi",    "Delhi"),
    ("Lajpat-Nagar-PHC",    "South-Delhi",      "Delhi"),
    ("Connaught-Place-CHC", "New-Delhi",        "Delhi"),
    ("Dwarka-PHC",          "South-West-Delhi", "Delhi"),
    ("Anand-Vihar-CHC",     "East-Delhi",       "Delhi"),
    ("Pitampura-PHC",       "North-Delhi",      "Delhi"),
    ("Mehrauli-CHC",        "South-Delhi",      "Delhi"),
]

# Total number of records to push in one simulator run
TOTAL_RECORDS  = 500
# Kinesis PutRecords accepts up to 500 records per call; we use 25 for safety
BATCH_SIZE     = 25


def generate_record(fridge_id: str) -> dict:
    """Return one mock telemetry record matching the VacciGuard schema."""
    loc, district, state = random.choice(LOCATIONS)
    return {
        "fridge_id":     fridge_id,
        "temperature":   round(random.uniform(2.0, 10.0), 1),   # °C  (safe: 2–8)
        "door_open":     random.random() < 0.05,                 # 5 % chance open
        "battery_level": random.randint(60, 100),
        "location":      loc,
        "district":      district,
        "state":         state,
        "timestamp":     datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def build_kinesis_entry(record: dict) -> dict:
    """Wrap a record dict into the shape Kinesis PutRecords expects."""
    return {
        "Data":         json.dumps(record).encode("utf-8"),
        "PartitionKey": record["fridge_id"],   # partition by fridge so shard
                                               # affinity is stable per device
    }


def main():
    kinesis = boto3.client("kinesis", region_name=REGION)

    print(f"[simulator] Sending {TOTAL_RECORDS} records to '{KINESIS_STREAM_NAME}' ...")

    sent = 0
    for batch_start in range(0, TOTAL_RECORDS, BATCH_SIZE):
        batch_records = []
        for i in range(batch_start, min(batch_start + BATCH_SIZE, TOTAL_RECORDS)):
            # Pad fridge_id to 4 digits: VCF-0001 … VCF-1000
            fridge_id = f"VCF-{(i % 1000) + 1:04d}"
            record    = generate_record(fridge_id)
            batch_records.append(build_kinesis_entry(record))

        response = kinesis.put_records(
            StreamName=KINESIS_STREAM_NAME,
            Records=batch_records,
        )

        failed = response.get("FailedRecordCount", 0)
        sent  += len(batch_records) - failed

        if failed:
            print(f"[simulator] WARNING: {failed} records failed in this batch.")

        # Brief pause between batches to avoid throttling on a single-shard stream
        time.sleep(0.1)

    print(f"[simulator] Done. {sent}/{TOTAL_RECORDS} records successfully sent.")


if __name__ == "__main__":
    main()
