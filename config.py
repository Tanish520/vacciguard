# config.py
# Shared runtime configuration for the VacciGuard pipeline.
# All scripts must import from here and avoid hardcoding resource names.

import os


def _env_flag(name: str, default: str) -> bool:
    value = os.getenv(name, default).strip().lower()
    return value in {"1", "true", "yes", "on"}


REGION = os.getenv("VACCIGUARD_REGION", "ap-south-1")
KINESIS_STREAM_NAME = os.getenv("VACCIGUARD_KINESIS_STREAM_NAME", "vacciguard-stream")
DYNAMO_TABLE_NAME = os.getenv("VACCIGUARD_DYNAMO_TABLE_NAME", "VacciguardFridgeState")
SNS_ALERT_TOPIC_ARN = os.getenv("VACCIGUARD_SNS_ALERT_TOPIC_ARN", "")

BREACH_TEMP_CELSIUS = float(os.getenv("VACCIGUARD_BREACH_TEMP_CELSIUS", "8.0"))
WARNING_TEMP_CELSIUS = float(os.getenv("VACCIGUARD_WARNING_TEMP_CELSIUS", "7.5"))
CHECKPOINT_INTERVAL_MS = int(os.getenv("VACCIGUARD_CHECKPOINT_INTERVAL_MS", "30000"))
FLINK_PIPELINE_PARALLELISM = int(os.getenv("VACCIGUARD_FLINK_PARALLELISM", "1"))
KINESIS_INITIAL_POSITION = os.getenv("VACCIGUARD_KINESIS_INITIAL_POSITION", "LATEST")
KINESIS_CONNECTOR_JAR = os.getenv(
    "VACCIGUARD_KINESIS_CONNECTOR_JAR",
    "file:///app/lib/flink-sql-connector-kinesis-4.3.0-1.18.jar",
)
ENABLE_ALERTS = _env_flag("VACCIGUARD_ENABLE_ALERTS", "true")
ALERT_MESSAGE_GROUP_ID = os.getenv(
    "VACCIGUARD_ALERT_MESSAGE_GROUP_ID",
    "vacciguard-breaches",
)
