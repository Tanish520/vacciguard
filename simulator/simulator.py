"""
simulator.py
Generates mock vaccine-fridge telemetry and publishes it to the
VacciGuard Kinesis stream in controlled batches.

Usage:
    python simulator/simulator.py

Prerequisites:
    - AWS credentials configured (env vars, ~/.aws/credentials, or IAM role)
    - Kinesis stream already created (vacciguard-stream)
"""

import json
import os
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

def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


# Rate-controlled experiment parameters
ACTIVE_FRIDGES = _env_int("VACCIGUARD_SIMULATOR_ACTIVE_FRIDGES", 500)
RECORDS_PER_SECOND = _env_int("VACCIGUARD_SIMULATOR_RECORDS_PER_SECOND", ACTIVE_FRIDGES)
DURATION_SECONDS = _env_int("VACCIGUARD_SIMULATOR_DURATION_SECONDS", 300)
RUN_LABEL = os.getenv("VACCIGUARD_SIMULATOR_RUN_LABEL", "baseline")

# Kinesis PutRecords accepts up to 500 records per call; we use 25 for smoother pacing
BATCH_SIZE = _env_int("VACCIGUARD_SIMULATOR_BATCH_SIZE", 25)


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


def chunked(items, chunk_size: int):
    for start in range(0, len(items), chunk_size):
        yield items[start:start + chunk_size]


def main():
    kinesis = boto3.client("kinesis", region_name=REGION)
    total_target_records = RECORDS_PER_SECOND * DURATION_SECONDS

    print(
        f"[simulator] Run '{RUN_LABEL}' starting: "
        f"{ACTIVE_FRIDGES} active fridges, "
        f"{RECORDS_PER_SECOND} records/sec, "
        f"{DURATION_SECONDS} seconds, "
        f"target {total_target_records} records."
    )
    print(f"[simulator] Sending data to '{KINESIS_STREAM_NAME}' ...")

    sent = 0
    failed_total = 0
    sequence_number = 0

    for second_index in range(DURATION_SECONDS):
        second_start = time.monotonic()
        second_records = []

        for _ in range(RECORDS_PER_SECOND):
            fridge_id = f"VCF-{(sequence_number % ACTIVE_FRIDGES) + 1:04d}"
            record = generate_record(fridge_id)
            second_records.append(build_kinesis_entry(record))
            sequence_number += 1

        for batch_records in chunked(second_records, BATCH_SIZE):
            response = kinesis.put_records(
                StreamName=KINESIS_STREAM_NAME,
                Records=batch_records,
            )

            failed = response.get("FailedRecordCount", 0)
            failed_total += failed
            sent += len(batch_records) - failed

            if failed:
                print(f"[simulator] WARNING: {failed} records failed in this batch.")

        elapsed = time.monotonic() - second_start
        if second_index == 0 or (second_index + 1) % 30 == 0 or second_index == DURATION_SECONDS - 1:
            print(
                f"[simulator] Progress: {second_index + 1}/{DURATION_SECONDS} sec, "
                f"sent={sent}, failed={failed_total}"
            )

        remaining = 1.0 - elapsed
        if remaining > 0:
            time.sleep(remaining)

    print(
        f"[simulator] Done. {sent}/{total_target_records} records successfully sent "
        f"(failed={failed_total})."
    )


if __name__ == "__main__":
    main()
