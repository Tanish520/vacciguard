#!/usr/bin/env python3
"""
VacciGuard Replay Producer

Reads a precomputed NDJSON workload file and publishes events to a Kafka topic
at a configurable rate. Rate control uses a next-send-time approach so the
actual throughput stays accurate even at high rates.

Environment variables (all required unless noted):
  KAFKA_BOOTSTRAP_SERVERS   e.g. kafka:9092
  WORKLOAD_FILE             path to the NDJSON file inside the container
  KAFKA_TOPIC               (optional, default: vacciguard-telemetry)
  EVENTS_PER_SECOND         (optional, default: 5.0)
  LOOP                      (optional, default: false) replay file repeatedly
"""

import logging
import os
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
WORKLOAD_FILE           = os.environ["WORKLOAD_FILE"]
KAFKA_TOPIC             = os.environ.get("KAFKA_TOPIC", "vacciguard-telemetry")
EVENTS_PER_SECOND       = float(os.environ.get("EVENTS_PER_SECOND", "5.0"))
LOOP                    = os.environ.get("LOOP", "false").lower() == "true"

# ── Kafka connection ──────────────────────────────────────────────────────────

def connect(retries=15, delay=5):
    """Retry connecting to Kafka until the broker is ready."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                acks="all",          # wait for all in-sync replicas
                retries=3,
                linger_ms=5,         # small batching for efficiency at high rates
            )
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            log.warning(
                "Kafka not ready (attempt %d/%d) — retrying in %ds",
                attempt, retries, delay,
            )
            time.sleep(delay)
    raise RuntimeError(
        f"Could not connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS} "
        f"after {retries} attempts"
    )

# ── Workload loading ──────────────────────────────────────────────────────────

def load_events(path):
    """Load all non-empty lines from the NDJSON file as pre-encoded bytes."""
    events = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(line.encode("utf-8"))
    if not events:
        raise ValueError(f"Workload file is empty: {path}")
    return events

# ── Replay ────────────────────────────────────────────────────────────────────

def replay(producer, events, eps):
    """
    Publish all events at the target rate using next-send-time scheduling.

    next-send-time keeps the actual rate accurate at high eps values (e.g. 500)
    where time.sleep(1/eps) alone would drift due to Python overhead.
    """
    interval  = 1.0 / eps
    total     = len(events)
    sent      = 0
    start     = time.monotonic()
    next_send = start

    for payload in events:
        # wait until the scheduled send time
        now = time.monotonic()
        gap = next_send - now
        if gap > 0:
            time.sleep(gap)

        producer.send(KAFKA_TOPIC, value=payload)
        sent     += 1
        next_send = start + sent * interval

        if sent % 50 == 0 or sent == total:
            elapsed    = time.monotonic() - start
            actual_eps = sent / elapsed if elapsed > 0 else 0
            log.info("Sent %d/%d  actual %.1f eps", sent, total, actual_eps)

    producer.flush()
    elapsed = time.monotonic() - start
    log.info(
        "Replay complete: %d events in %.1fs  avg %.1f eps",
        sent, elapsed, sent / elapsed,
    )

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    log.info(
        "Starting  WORKLOAD=%s  EPS=%.1f  TOPIC=%s  LOOP=%s",
        WORKLOAD_FILE, EVENTS_PER_SECOND, KAFKA_TOPIC, LOOP,
    )

    events   = load_events(WORKLOAD_FILE)
    log.info("Loaded %d events", len(events))

    producer = connect()

    run = 0
    while True:
        run += 1
        log.info("--- Run %d ---", run)
        replay(producer, events, EVENTS_PER_SECOND)
        if not LOOP:
            break

    producer.close()
    log.info("Producer finished")


if __name__ == "__main__":
    main()
