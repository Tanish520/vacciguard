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
import json
import tempfile
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse

import boto3
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
REPLAY_METRICS_PORT     = int(os.environ.get("REPLAY_METRICS_PORT", "9109"))
REPLAY_METRICS_DRAIN_SECONDS = float(os.environ.get("REPLAY_METRICS_DRAIN_SECONDS", "15"))


class ReplayMetricsRegistry:
    def __init__(self):
        self._lock = threading.Lock()
        self._metrics = {
            "vacciguard_replay_loaded_events": 0,
            "vacciguard_replay_sent_events_total": 0,
            "vacciguard_replay_configured_rate_events_per_second": 0.0,
            "vacciguard_replay_duration_seconds": 0.0,
            "vacciguard_replay_completion_status": 0,
            "vacciguard_replay_run_started_timestamp_seconds": 0.0,
            "vacciguard_replay_completion_timestamp_seconds": 0.0,
        }

    def record_loaded_events(self, event_count):
        with self._lock:
            self._metrics["vacciguard_replay_loaded_events"] = event_count

    def begin_run(self, configured_events_per_second, run_started_timestamp_seconds=None):
        with self._lock:
            self._metrics["vacciguard_replay_configured_rate_events_per_second"] = (
                configured_events_per_second
            )
            self._metrics["vacciguard_replay_duration_seconds"] = 0.0
            self._metrics["vacciguard_replay_completion_status"] = 0
            self._metrics["vacciguard_replay_run_started_timestamp_seconds"] = (
                time.time()
                if run_started_timestamp_seconds is None
                else run_started_timestamp_seconds
            )
            self._metrics["vacciguard_replay_completion_timestamp_seconds"] = 0.0

    def record_sent_event(self, duration_seconds=None):
        with self._lock:
            self._metrics["vacciguard_replay_sent_events_total"] += 1
            if duration_seconds is not None:
                self._metrics["vacciguard_replay_duration_seconds"] = duration_seconds

    def record_completion(
        self,
        *,
        duration_seconds,
        configured_events_per_second,
        completion_status=1,
        completion_timestamp_seconds=None,
    ):
        with self._lock:
            self._metrics["vacciguard_replay_duration_seconds"] = duration_seconds
            self._metrics["vacciguard_replay_configured_rate_events_per_second"] = (
                configured_events_per_second
            )
            self._metrics["vacciguard_replay_completion_status"] = completion_status
            self._metrics["vacciguard_replay_completion_timestamp_seconds"] = (
                time.time()
                if completion_timestamp_seconds is None
                else completion_timestamp_seconds
            )

    def render_prometheus(self):
        with self._lock:
            snapshot = tuple(self._metrics.items())

        return "\n".join(f"{name} {value}" for name, value in snapshot) + "\n"


REPLAY_METRICS_REGISTRY = ReplayMetricsRegistry()


def metrics_http_payload(registry):
    return registry.render_prometheus()


def start_metrics_server(port, registry):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_response(404)
                self.end_headers()
                return

            payload = metrics_http_payload(registry).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format, *args):
            return

    server = HTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def stop_metrics_server(server):
    if server is None:
        return
    server.shutdown()
    server.server_close()


def drain_metrics_server(scrape_window_seconds):
    if scrape_window_seconds <= 0:
        return
    time.sleep(scrape_window_seconds)

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
    """Load all non-empty lines from the NDJSON file as JSON payloads."""
    events = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    if not events:
        raise ValueError(f"Workload file is empty: {path}")
    return events


def resolve_workload_file(path_or_uri, temp_dir=None):
    if not path_or_uri.startswith("s3://"):
        return path_or_uri

    parsed = urlparse(path_or_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    local_dir = temp_dir or tempfile.mkdtemp(prefix="vacciguard-workload-")
    local_path = Path(local_dir) / Path(key).name
    local_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("Downloading workload from s3://%s/%s", bucket, key)
    with local_path.open("wb") as handle:
        boto3.client("s3").download_fileobj(bucket, key, handle)
    return str(local_path)


def encode_event_payload(event, sent_at=None):
    stamped_event = dict(event)
    replay_sent_at = sent_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stamped_event["replay_sent_at"] = replay_sent_at
    return json.dumps(stamped_event, separators=(",", ":")).encode("utf-8")

# ── Replay ────────────────────────────────────────────────────────────────────

def replay(producer, events, eps, metrics_registry=None):
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

    if metrics_registry is not None:
        metrics_registry.begin_run(configured_events_per_second=eps)

    completion_status = 1
    try:
        for event in events:
            # wait until the scheduled send time
            now = time.monotonic()
            gap = next_send - now
            if gap > 0:
                time.sleep(gap)

            payload = encode_event_payload(event)
            send_future = producer.send(KAFKA_TOPIC, value=payload)
            send_future.get()
            sent += 1
            if metrics_registry is not None:
                metrics_registry.record_sent_event(
                    duration_seconds=time.monotonic() - start,
                )
            next_send = start + sent * interval

            if sent % 50 == 0 or sent == total:
                elapsed    = time.monotonic() - start
                actual_eps = sent / elapsed if elapsed > 0 else 0
                log.info("Sent %d/%d  actual %.1f eps", sent, total, actual_eps)

        producer.flush()
    except Exception:
        completion_status = 2
        raise
    finally:
        elapsed = time.monotonic() - start
        if metrics_registry is not None:
            metrics_registry.record_completion(
                duration_seconds=elapsed,
                configured_events_per_second=eps,
                completion_status=completion_status,
            )

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

    metrics_server = start_metrics_server(REPLAY_METRICS_PORT, REPLAY_METRICS_REGISTRY)
    producer = None
    REPLAY_METRICS_REGISTRY.begin_run(configured_events_per_second=EVENTS_PER_SECOND)
    startup_started_at = time.monotonic()
    replay_started = False

    try:
        resolved_workload_file = resolve_workload_file(WORKLOAD_FILE)
        events = load_events(resolved_workload_file)
        REPLAY_METRICS_REGISTRY.record_loaded_events(len(events))
        log.info("Loaded %d events", len(events))

        producer = connect()

        run = 0
        while True:
            run += 1
            log.info("--- Run %d ---", run)
            replay_started = True
            replay(
                producer,
                events,
                EVENTS_PER_SECOND,
                metrics_registry=REPLAY_METRICS_REGISTRY,
            )
            if not LOOP:
                break
    except Exception:
        if not replay_started:
            REPLAY_METRICS_REGISTRY.record_completion(
                duration_seconds=time.monotonic() - startup_started_at,
                configured_events_per_second=EVENTS_PER_SECOND,
                completion_status=2,
            )
        raise
    finally:
        if producer is not None:
            producer.close()
            log.info("Producer finished")
        drain_metrics_server(REPLAY_METRICS_DRAIN_SECONDS)
        stop_metrics_server(metrics_server)


if __name__ == "__main__":
    main()
