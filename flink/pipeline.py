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
import os
import sys
from datetime import datetime
from typing import Dict, List

import boto3
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
    REGION,
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
        self._alerts_configured = False
        self._warned_about_standard_topic = False

        if ENABLE_ALERTS and SNS_ALERT_TOPIC_ARN:
            self._sns = session.client("sns")
            self._alerts_configured = True

    def map(self, value: str):
        try:
            raw_record = json.loads(value)
            record = classify_record(validate_record(raw_record))
        except (json.JSONDecodeError, RecordValidationError) as exc:
            print(f"[pipeline] Skipping invalid record: {exc}")
            return value

        self._table.put_item(Item=self._build_dynamo_item(record))

        if record["breach_detected"]:
            self._publish_alert(record)

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

        self._sns.publish(
            TopicArn=SNS_ALERT_TOPIC_ARN,
            Subject="VacciGuard breach detected",
            Message=json.dumps(message),
            MessageGroupId=ALERT_MESSAGE_GROUP_ID,
            MessageDeduplicationId=deduplication_id,
        )

        print(
            f"[pipeline] Alert published for {record['fridge_id']} "
            f"({', '.join(record['breach_reasons'])})"
        )


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
    print(f"[pipeline]   Checkpoints : every {CHECKPOINT_INTERVAL_MS} ms")

    env = build_pipeline()
    env.execute("VacciGuard-Phase5-Pipeline")


if __name__ == "__main__":
    main()
