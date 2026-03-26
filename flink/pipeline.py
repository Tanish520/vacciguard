"""
pipeline.py
PyFlink job: Kinesis → DynamoDB (Phase 4 minimal pipeline).

Reads JSON telemetry records from the VacciGuard Kinesis stream and writes
each record to DynamoDB using the composite primary key (fridge_id, timestamp).

Run locally (after `pip install apache-flink boto3`):
    python flink/pipeline.py

Notes:
  - The Kinesis consumer uses LATEST as the starting position so it only
    processes records produced after the job starts.  Change to TRIM_HORIZON
    to reprocess all existing records in the stream.
  - DynamoDB writes are performed inside a custom sink (MapFunction + side
    effect) because PyFlink's built-in connectors do not yet ship a
    first-party DynamoDB sink.  For production, replace with the official
    Flink DynamoDB Sink (Flink ≥ 1.17 Java connector via fat JAR).
"""

import json
import sys
import os

import boto3
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kinesis import FlinkKinesisConsumer
from pyflink.datastream.functions import MapFunction

# Import shared constants — never hardcode resource names here
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import KINESIS_STREAM_NAME, DYNAMO_TABLE_NAME, REGION


# ---------------------------------------------------------------------------
# DynamoDB sink (custom MapFunction with side-effects)
# ---------------------------------------------------------------------------

class DynamoDBSink(MapFunction):
    """
    Receives a JSON string, parses it, and upserts the record into DynamoDB.

    Primary key schema (must match the table definition):
        Partition key : fridge_id  (String)
        Sort key      : timestamp  (String)

    All other fields are stored as additional attributes.
    """

    def open(self, runtime_context):
        # Create the boto3 resource once per task manager, not per record
        self._dynamodb = boto3.resource("dynamodb", region_name=REGION)
        self._table    = self._dynamodb.Table(DYNAMO_TABLE_NAME)

    def map(self, value: str):
        record = json.loads(value)

        # Build the DynamoDB item — every field in the schema is stored as-is
        item = {
            "fridge_id":     record["fridge_id"],      # partition key
            "timestamp":     record["timestamp"],       # sort key
            "temperature":   str(record["temperature"]),# DynamoDB Number via Decimal
            "door_open":     record["door_open"],
            "battery_level": record["battery_level"],
            "location":      record["location"],
            "district":      record["district"],
            "state":         record["state"],
        }

        self._table.put_item(Item=item)

        # Return the value unchanged so the DAG can be extended later
        return value


# ---------------------------------------------------------------------------
# Pipeline definition
# ---------------------------------------------------------------------------

def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Single parallelism keeps things simple for Phase 4
    env.set_parallelism(1)
    # Register the Kinesis connector JAR downloaded during Docker build
    env.add_jars("file:///app/lib/flink-sql-connector-kinesis-4.3.0-1.18.jar")

    # -- Kinesis source configuration ---------------------------------------
    consumer_config = {
        "aws.region": REGION,
        # Start from the latest records; use TRIM_HORIZON to replay the stream
        "flink.stream.initpos": "LATEST",
    }

    kinesis_source = FlinkKinesisConsumer(
        KINESIS_STREAM_NAME,
        SimpleStringSchema(),   # raw JSON string per record
        consumer_config,
    )

    # -- Stream topology ----------------------------------------------------
    stream = env.add_source(kinesis_source, source_name="Kinesis-vacciguard-stream")

    # Parse, validate, and write to DynamoDB
    (
        stream
        .map(DynamoDBSink(), output_type=Types.STRING())
        # Additional operators (alerts, S3 sink, etc.) will be chained here
        # in later phases.
    )

    return env


def main():
    print(f"[pipeline] Starting VacciGuard Flink job ...")
    print(f"[pipeline]   Source  : Kinesis stream '{KINESIS_STREAM_NAME}' ({REGION})")
    print(f"[pipeline]   Sink    : DynamoDB table '{DYNAMO_TABLE_NAME}'")

    env = build_pipeline()
    env.execute("VacciGuard-Phase4-Pipeline")


if __name__ == "__main__":
    main()
