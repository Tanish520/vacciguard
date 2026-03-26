"""
pipeline.py
PyFlink job: Kinesis -> DynamoDB (+ Phase 5 breach detection and SNS alerts).

The pipeline stays measurable as a research baseline while adding the features
needed for Phase 5:
- input validation
- breach detection
- idempotent DynamoDB writes
- optional SNS FIFO alert publishing

Run inside Docker:
    docker compose up flink-pipeline
"""

import hashlib
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kinesis import FlinkKinesisConsumer
from pyflink.datastream.functions import MapFunction

# Import shared constants - never hardcode resource names here.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import (
    ALERT_MESSAGE_GROUP_ID,
    BREACH_TEMP_CELSIUS,
    CHECKPOINT_INTERVAL_MS,
    DYNAMO_TABLE_NAME,
    ENABLE_ALERTS,
    FLINK_PIPELINE_PARALLELISM,
    KINESIS_CONNECTOR_JAR,
    KINESIS_INITIAL_POSITION,
    KINESIS_STREAM_NAME,
    METRICS_NAMESPACE,
    METRICS_SUMMARY_INTERVAL,
    REGION,
    SLA_THRESHOLD_MS,
    SNS_ALERT_TOPIC_ARN,
    WARNING_TEMP_CELSIUS,
)

REQUIRED_FIELDS = {
    "fridge_id",
    "temperature",
    "door_open",
    "battery_level",
    "location",
    "district",
    "state",
    "timestamp",
}
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class RecordValidationError(ValueError):
    """Raised when an input event does not match the expected schema."""


def parse_timestamp(raw_timestamp: str) -> datetime:
    return datetime.strptime(raw_timestamp, TIMESTAMP_FORMAT)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def latency_ms_from_timestamp(raw_timestamp: str) -> int:
    event_time = parse_timestamp(raw_timestamp).replace(tzinfo=timezone.utc)
    return max(0, int((now_utc() - event_time).total_seconds() * 1000))


def percentile(values: List[int], q: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(q * len(ordered)) - 1))
    return ordered[index]


def parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
    raise RecordValidationError(f"door_open must be boolean-like, got {value!r}")


def validate_record(record: Dict) -> Dict:
    missing = sorted(REQUIRED_FIELDS - record.keys())
    if missing:
        raise RecordValidationError(f"missing required field(s): {', '.join(missing)}")

    parse_timestamp(record["timestamp"])

    try:
        temperature = float(record["temperature"])
        battery_level = int(record["battery_level"])
    except (TypeError, ValueError) as exc:
        raise RecordValidationError(f"invalid numeric value: {exc}") from exc

    return {
        "fridge_id": str(record["fridge_id"]),
        "temperature": round(temperature, 1),
        "door_open": parse_bool(record["door_open"]),
        "battery_level": battery_level,
        "location": str(record["location"]),
        "district": str(record["district"]),
        "state": str(record["state"]),
        "timestamp": str(record["timestamp"]),
    }


def classify_record(record: Dict) -> Dict:
    breach_reasons: List[str] = []
    if record["temperature"] > BREACH_TEMP_CELSIUS:
        breach_reasons.append("temperature_breach")
    if record["door_open"]:
        breach_reasons.append("door_open")

    if breach_reasons:
        severity = "critical"
    elif record["temperature"] >= WARNING_TEMP_CELSIUS:
        severity = "warning"
    else:
        severity = "normal"

    enriched = dict(record)
    enriched["breach_detected"] = bool(breach_reasons)
    enriched["breach_reasons"] = breach_reasons
    enriched["alert_severity"] = severity
    enriched["doses_at_risk"] = 0
    return enriched


class Phase5Sink(MapFunction):
    """Validate each event, persist it to DynamoDB, and publish optional alerts."""

    def open(self, runtime_context):
        session = boto3.Session(region_name=REGION)
        self._dynamodb = session.resource("dynamodb")
        self._table = self._dynamodb.Table(DYNAMO_TABLE_NAME)
        self._sns = None
        self._cloudwatch = session.client("cloudwatch")
        self._alerts_configured = False
        self._warned_about_standard_topic = False
        self._alert_dedup_cache = set()
        self._started_at_monotonic = time.monotonic()

        self._metrics = {
            "records_processed_total": 0,
            "records_failed_total": 0,
            "invalid_records_total": 0,
            "breach_events_total": 0,
            "alerts_published_total": 0,
            "duplicate_alerts_total": 0,
            "dynamodb_write_failures_total": 0,
            "sns_publish_failures_total": 0,
            "sla_violations_total": 0,
        }
        self._latencies_dynamodb_ms: List[int] = []
        self._latencies_alert_ms: List[int] = []

        if ENABLE_ALERTS and SNS_ALERT_TOPIC_ARN:
            self._sns = session.client("sns")
            self._alerts_configured = True

    def map(self, value: str):
        try:
            raw_record = json.loads(value)
            record = classify_record(validate_record(raw_record))
        except (json.JSONDecodeError, RecordValidationError) as exc:
            self._metrics["records_failed_total"] += 1
            self._metrics["invalid_records_total"] += 1
            print(f"[pipeline] Skipping invalid record: {exc}")
            return value

        dynamodb_latency_ms = latency_ms_from_timestamp(record["timestamp"])
        try:
            self._table.put_item(Item=self._build_dynamo_item(record))
        except (BotoCoreError, ClientError) as exc:
            self._metrics["records_failed_total"] += 1
            self._metrics["dynamodb_write_failures_total"] += 1
            print(f"[pipeline] DynamoDB write failed for {record['fridge_id']}: {exc}")
            return value

        self._metrics["records_processed_total"] += 1
        self._latencies_dynamodb_ms.append(dynamodb_latency_ms)

        if record["breach_detected"]:
            self._metrics["breach_events_total"] += 1
            if dynamodb_latency_ms > SLA_THRESHOLD_MS:
                self._metrics["sla_violations_total"] += 1
            self._publish_alert(record)

        if self._metrics["records_processed_total"] % METRICS_SUMMARY_INTERVAL == 0:
            self._emit_metrics_summary()

        return json.dumps(record, sort_keys=True)

    def _build_dynamo_item(self, record: Dict) -> Dict:
        return {
            "fridge_id": record["fridge_id"],
            "timestamp": record["timestamp"],
            "temperature": str(record["temperature"]),
            "door_open": record["door_open"],
            "battery_level": record["battery_level"],
            "location": record["location"],
            "district": record["district"],
            "state": record["state"],
            "breach_detected": record["breach_detected"],
            "breach_reasons": record["breach_reasons"],
            "alert_severity": record["alert_severity"],
            "doses_at_risk": record["doses_at_risk"],
        }

    def _publish_alert(self, record: Dict) -> None:
        if not self._alerts_configured:
            return

        if not SNS_ALERT_TOPIC_ARN.endswith(".fifo"):
            if not self._warned_about_standard_topic:
                print(
                    "[pipeline] SNS alerts are enabled, but the configured topic is not FIFO. "
                    "Skipping publish because Phase 5 requires MessageDeduplicationId."
                )
                self._warned_about_standard_topic = True
            return

        dedup_source = ":".join(
            [record["fridge_id"], record["timestamp"], "|".join(record["breach_reasons"])]
        )
        deduplication_id = hashlib.md5(dedup_source.encode("utf-8")).hexdigest()
        alert_latency_ms = latency_ms_from_timestamp(record["timestamp"])
        if deduplication_id in self._alert_dedup_cache:
            self._metrics["duplicate_alerts_total"] += 1
        else:
            self._alert_dedup_cache.add(deduplication_id)

        message = {
            "fridge_id": record["fridge_id"],
            "timestamp": record["timestamp"],
            "temperature": record["temperature"],
            "door_open": record["door_open"],
            "battery_level": record["battery_level"],
            "location": record["location"],
            "district": record["district"],
            "state": record["state"],
            "breach_reasons": record["breach_reasons"],
            "severity": record["alert_severity"],
        }

        try:
            self._sns.publish(
                TopicArn=SNS_ALERT_TOPIC_ARN,
                Subject="VacciGuard breach detected",
                Message=json.dumps(message),
                MessageGroupId=ALERT_MESSAGE_GROUP_ID,
                MessageDeduplicationId=deduplication_id,
            )
        except (BotoCoreError, ClientError) as exc:
            self._metrics["records_failed_total"] += 1
            self._metrics["sns_publish_failures_total"] += 1
            print(f"[pipeline] SNS publish failed for {record['fridge_id']}: {exc}")
            return

        self._metrics["alerts_published_total"] += 1
        self._latencies_alert_ms.append(alert_latency_ms)
        if alert_latency_ms > SLA_THRESHOLD_MS:
            self._metrics["sla_violations_total"] += 1
        print(
            f"[pipeline] Alert published for {record['fridge_id']} "
            f"({', '.join(record['breach_reasons'])})"
        )

    def _emit_metrics_summary(self) -> None:
        elapsed_seconds = max(1.0, time.monotonic() - self._started_at_monotonic)
        throughput = round(self._metrics["records_processed_total"] / elapsed_seconds, 2)
        dynamodb_p50 = percentile(self._latencies_dynamodb_ms, 0.50)
        dynamodb_p90 = percentile(self._latencies_dynamodb_ms, 0.90)
        dynamodb_p99 = percentile(self._latencies_dynamodb_ms, 0.99)
        alert_p50 = percentile(self._latencies_alert_ms, 0.50)
        alert_p90 = percentile(self._latencies_alert_ms, 0.90)
        alert_p99 = percentile(self._latencies_alert_ms, 0.99)

        summary = {
            "records_processed_total": self._metrics["records_processed_total"],
            "records_failed_total": self._metrics["records_failed_total"],
            "invalid_records_total": self._metrics["invalid_records_total"],
            "breach_events_total": self._metrics["breach_events_total"],
            "alerts_published_total": self._metrics["alerts_published_total"],
            "duplicate_alerts_total": self._metrics["duplicate_alerts_total"],
            "dynamodb_write_failures_total": self._metrics["dynamodb_write_failures_total"],
            "sns_publish_failures_total": self._metrics["sns_publish_failures_total"],
            "sla_violations_total": self._metrics["sla_violations_total"],
            "throughput_records_per_second": throughput,
            "dynamodb_latency_ms_p50": dynamodb_p50,
            "dynamodb_latency_ms_p90": dynamodb_p90,
            "dynamodb_latency_ms_p99": dynamodb_p99,
            "alert_latency_ms_p50": alert_p50,
            "alert_latency_ms_p90": alert_p90,
            "alert_latency_ms_p99": alert_p99,
        }

        print(f"[pipeline][metrics] {json.dumps(summary, sort_keys=True)}")

        metric_data = [
            {"MetricName": "RecordsProcessedTotal", "Value": self._metrics["records_processed_total"], "Unit": "Count"},
            {"MetricName": "RecordsFailedTotal", "Value": self._metrics["records_failed_total"], "Unit": "Count"},
            {"MetricName": "BreachEventsTotal", "Value": self._metrics["breach_events_total"], "Unit": "Count"},
            {"MetricName": "AlertsPublishedTotal", "Value": self._metrics["alerts_published_total"], "Unit": "Count"},
            {"MetricName": "DuplicateAlertsTotal", "Value": self._metrics["duplicate_alerts_total"], "Unit": "Count"},
            {"MetricName": "SlaViolationsTotal", "Value": self._metrics["sla_violations_total"], "Unit": "Count"},
            {"MetricName": "ThroughputRecordsPerSecond", "Value": throughput, "Unit": "Count/Second"},
            {"MetricName": "DynamoDbLatencyP50Ms", "Value": dynamodb_p50, "Unit": "Milliseconds"},
            {"MetricName": "DynamoDbLatencyP90Ms", "Value": dynamodb_p90, "Unit": "Milliseconds"},
            {"MetricName": "DynamoDbLatencyP99Ms", "Value": dynamodb_p99, "Unit": "Milliseconds"},
            {"MetricName": "AlertLatencyP50Ms", "Value": alert_p50, "Unit": "Milliseconds"},
            {"MetricName": "AlertLatencyP90Ms", "Value": alert_p90, "Unit": "Milliseconds"},
            {"MetricName": "AlertLatencyP99Ms", "Value": alert_p99, "Unit": "Milliseconds"},
        ]
        try:
            self._cloudwatch.put_metric_data(Namespace=METRICS_NAMESPACE, MetricData=metric_data)
        except (BotoCoreError, ClientError) as exc:
            print(f"[pipeline] CloudWatch metric publish failed: {exc}")


def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(FLINK_PIPELINE_PARALLELISM)
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    env.add_jars(KINESIS_CONNECTOR_JAR)

    consumer_config = {
        "aws.region": REGION,
        "flink.stream.initpos": KINESIS_INITIAL_POSITION,
    }

    kinesis_source = FlinkKinesisConsumer(
        KINESIS_STREAM_NAME,
        SimpleStringSchema(),
        consumer_config,
    )

    stream = env.add_source(kinesis_source, source_name="Kinesis-vacciguard-stream")

    stream.map(Phase5Sink(), output_type=Types.STRING())
    return env


def main():
    print("[pipeline] Starting VacciGuard Flink job ...")
    print(f"[pipeline]   Source      : Kinesis stream '{KINESIS_STREAM_NAME}' ({REGION})")
    print(f"[pipeline]   Hot sink    : DynamoDB table '{DYNAMO_TABLE_NAME}'")
    print(f"[pipeline]   Alerts      : {'enabled' if ENABLE_ALERTS and SNS_ALERT_TOPIC_ARN else 'disabled'}")
    if SNS_ALERT_TOPIC_ARN:
        print(f"[pipeline]   SNS topic    : {SNS_ALERT_TOPIC_ARN}")
    print(f"[pipeline]   Thresholds  : breach>{BREACH_TEMP_CELSIUS}C, warning>={WARNING_TEMP_CELSIUS}C")
    print(f"[pipeline]   SLA         : {SLA_THRESHOLD_MS} ms")
    print(f"[pipeline]   Metrics ns  : {METRICS_NAMESPACE}")
    print(f"[pipeline]   Metrics log : every {METRICS_SUMMARY_INTERVAL} records")
    print(f"[pipeline]   Checkpoints : every {CHECKPOINT_INTERVAL_MS} ms")

    env = build_pipeline()
    env.execute("VacciGuard-Phase5-Pipeline")


if __name__ == "__main__":
    main()
