#!/usr/bin/env python3
from __future__ import annotations

"""
VacciGuard stream processor for the Phase 4 smallest working pipeline.

Flow:
  Kafka -> PERMISSIVE JSON parse -> validation -> deduplication -> lookup join
  -> breach classification -> Redis latest state + filesystem-backed cold outputs

The local filesystem output paths are a stand-in for S3 prefixes during local
development. They let us prove the end-to-end path before AWS infrastructure is
brought online.
"""

import functools
import json
import logging
import os
import re
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import redis
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


APP_NAME = os.environ.get("APP_NAME", "vacciguard-stream-processor")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "vacciguard-telemetry")
KAFKA_STARTING_OFFSETS = os.environ.get("KAFKA_STARTING_OFFSETS", "earliest")
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER")
COLD_MAX_OFFSETS_PER_TRIGGER = os.environ.get("COLD_MAX_OFFSETS_PER_TRIGGER", "")
LOOKUP_FILE = os.environ.get(
    "LOOKUP_FILE", "/data/reference/device-facility-lookup-template.csv"
)
# These environment-driven paths allow the same processor code to run locally
# against mounted files or in AWS-oriented environments with S3-compatible paths.
PROCESSED_OUTPUT_PATH = os.environ.get("PROCESSED_OUTPUT_PATH", "/data/output/processed")
INVALID_OUTPUT_PATH = os.environ.get("INVALID_OUTPUT_PATH", "/data/output/invalid")
BREACH_WINDOW_OUTPUT_PATH = os.environ.get(
    "BREACH_WINDOW_OUTPUT_PATH", "/data/output/breach_windows"
)
CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "/data/output/checkpoints")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "5 seconds")
COLD_TRIGGER_INTERVAL = os.environ.get("COLD_TRIGGER_INTERVAL", "30 seconds")
WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "10 minutes")
PIPELINE_MODE = os.environ.get("PIPELINE_MODE", "baseline")
SPARK_SQL_SHUFFLE_PARTITIONS = os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "4")
SPARK_DEFAULT_PARALLELISM = os.environ.get("SPARK_DEFAULT_PARALLELISM", "4")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))
REDIS_STATUS_TTL_SECONDS = int(os.environ.get("REDIS_STATUS_TTL_SECONDS", "3600"))
REDIS_ACTIVE_BREACHES_KEY = os.environ.get("REDIS_ACTIVE_BREACHES_KEY", "active_breaches")
SPARK_JARS_PACKAGES = os.environ.get(
    "SPARK_JARS_PACKAGES",
    ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]
    ),
)
KAFKA_TOPIC_PARTITIONS = int(os.environ.get("KAFKA_TOPIC_PARTITIONS", "1"))
KAFKA_TOPIC_REPLICATION_FACTOR = int(
    os.environ.get("KAFKA_TOPIC_REPLICATION_FACTOR", "1")
)
AWS_REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "ap-south-1"))
LOCAL_SPARK_JARS = os.environ.get(
    "LOCAL_SPARK_JARS",
    ",".join(
        [
            "/opt/spark/jars-extra/spark-sql-kafka-0-10_2.12-3.5.0.jar",
            "/opt/spark/jars-extra/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
            "/opt/spark/jars-extra/kafka-clients-3.4.1.jar",
            "/opt/spark/jars-extra/commons-pool2-2.11.1.jar",
            "/opt/spark/jars-extra/hadoop-aws-3.3.4.jar",
            "/opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar",
        ]
    ),
)
STREAM_METRICS_PORT = int(os.environ.get("STREAM_METRICS_PORT", "9108"))
_DURATION_PART_RE = re.compile(r"^\s*(?P<count>\d+)\s+(?P<unit>second|seconds|minute|minutes)\s*$")


def summarize_batch_counts(
    batch_id: int,
    valid_count: int,
    invalid_count: int,
    deduplicated_count: int,
    breach_count: int,
    processed_count: int,
    avg_end_to_end_latency_seconds: float | None,
    p95_end_to_end_latency_seconds: float | None,
) -> dict[str, int | float | None]:
    return {
        "batch_id": batch_id,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "deduplicated_count": deduplicated_count,
        "breach_count": breach_count,
        "processed_count": processed_count,
        "avg_end_to_end_latency_seconds": avg_end_to_end_latency_seconds,
        "p95_end_to_end_latency_seconds": p95_end_to_end_latency_seconds,
    }


class StreamMetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_seen_offsets: dict[int, int] = {}
        self._metrics = {
            "vacciguard_stream_latest_batch_id": -1,
            "vacciguard_stream_latest_batch_timestamp_seconds": 0.0,
            "vacciguard_stream_processed_events_total": 0,
            "vacciguard_stream_invalid_events_total": 0,
            "vacciguard_stream_deduplicated_events_total": 0,
            "vacciguard_stream_breach_events_total": 0,
            "vacciguard_stream_latest_batch_avg_latency_seconds": 0.0,
            "vacciguard_stream_latest_batch_p95_latency_seconds": 0.0,
            "vacciguard_stream_latest_batch_event_time_lag_p95_seconds": 0.0,
            "vacciguard_stream_latest_batch_ingest_to_redis_p95_seconds": 0.0,
            "vacciguard_stream_watermark_delay_seconds": 0.0,
            "vacciguard_stream_consumer_lag_records": 0,
        }

    def update_batch_metrics(
        self,
        *,
        batch_id: int,
        processed_count: int,
        invalid_count: int,
        deduplicated_count: int,
        breach_count: int,
        avg_latency_seconds: float | None,
        p95_latency_seconds: float | None,
        event_time_lag_p95_seconds: float | None = None,
        watermark_delay_seconds: float | None = None,
    ) -> None:
        with self._lock:
            self._metrics["vacciguard_stream_latest_batch_id"] = batch_id
            self._metrics["vacciguard_stream_latest_batch_timestamp_seconds"] = time.time()
            self._metrics["vacciguard_stream_processed_events_total"] += processed_count
            self._metrics["vacciguard_stream_invalid_events_total"] += invalid_count
            self._metrics["vacciguard_stream_deduplicated_events_total"] += deduplicated_count
            self._metrics["vacciguard_stream_breach_events_total"] += breach_count
            if avg_latency_seconds is not None:
                self._metrics["vacciguard_stream_latest_batch_avg_latency_seconds"] = avg_latency_seconds
            if p95_latency_seconds is not None:
                self._metrics["vacciguard_stream_latest_batch_p95_latency_seconds"] = p95_latency_seconds
            self._metrics["vacciguard_stream_latest_batch_event_time_lag_p95_seconds"] = (
                0.0 if event_time_lag_p95_seconds is None else event_time_lag_p95_seconds
            )
            self._metrics["vacciguard_stream_watermark_delay_seconds"] = (
                0.0 if watermark_delay_seconds is None else watermark_delay_seconds
            )

    def update_redis_metrics(
        self,
        *,
        ingest_to_redis_p95_seconds: float | None,
    ) -> None:
        with self._lock:
            self._metrics["vacciguard_stream_latest_batch_ingest_to_redis_p95_seconds"] = (
                0.0
                if ingest_to_redis_p95_seconds is None
                else ingest_to_redis_p95_seconds
            )

    def update_hot_latency_metrics(
        self,
        *,
        avg_latency_seconds: float | None,
        p95_latency_seconds: float | None,
    ) -> None:
        """Update avg/p95 latency metrics from the hot-path Redis write loop.

        Called only by the hot query so the cold query's S3 writes never clobber
        the latency values that the evaluation controller is reading.
        """
        with self._lock:
            if avg_latency_seconds is not None:
                self._metrics["vacciguard_stream_latest_batch_avg_latency_seconds"] = avg_latency_seconds
            if p95_latency_seconds is not None:
                self._metrics["vacciguard_stream_latest_batch_p95_latency_seconds"] = p95_latency_seconds

    def update_consumer_lag(self, lag: int) -> None:
        with self._lock:
            self._metrics["vacciguard_stream_consumer_lag_records"] = lag

    def update_last_seen_offsets(self, offsets: dict[int, int]) -> None:
        with self._lock:
            self._last_seen_offsets = dict(offsets)

    def get_last_seen_offsets(self) -> dict[int, int]:
        with self._lock:
            return dict(self._last_seen_offsets)

    def render_prometheus(self) -> str:
        with self._lock:
            snapshot = tuple(self._metrics.items())
        return "\n".join(f"{name} {value}" for name, value in snapshot) + "\n"


STREAM_METRICS_REGISTRY = StreamMetricsRegistry()


def metrics_http_payload(registry: StreamMetricsRegistry) -> str:
    return registry.render_prometheus()


def start_metrics_server(port: int, registry: StreamMetricsRegistry) -> HTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
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

        def log_message(self, format: str, *args) -> None:
            return

    server = HTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def stop_metrics_server(server: HTTPServer | None) -> None:
    if server is None:
        return
    server.shutdown()
    server.server_close()


def watermark_delay_seconds(delay: str) -> float:
    match = _DURATION_PART_RE.match(delay)
    if not match:
        return 0.0
    count = int(match.group("count"))
    unit = match.group("unit")
    if unit.startswith("minute"):
        return float(count * 60)
    return float(count)


def pipeline_mode() -> str:
    return os.environ.get("PIPELINE_MODE", PIPELINE_MODE).strip().lower()


def record_batch_offsets(batch_df: DataFrame) -> None:
    """Record the latest processed offset per partition for background lag computation."""
    if "partition" not in batch_df.columns or "offset" not in batch_df.columns:
        return
    offsets = (
        batch_df.groupBy("partition")
        .agg(F.max("offset").alias("latest_offset"))
        .collect()
    )
    last_seen = {
        int(row["partition"]): int(row["latest_offset"])
        for row in offsets
        if row["partition"] is not None and row["latest_offset"] is not None
    }
    STREAM_METRICS_REGISTRY.update_last_seen_offsets(last_seen)


def poll_consumer_lag_once(consumer: KafkaConsumer) -> int:
    last_seen = STREAM_METRICS_REGISTRY.get_last_seen_offsets()
    if not last_seen:
        return 0
    partitions = [TopicPartition(KAFKA_TOPIC, partition) for partition in last_seen]
    end_offsets = consumer.end_offsets(partitions)
    lag = 0
    for tp, end_offset in end_offsets.items():
        partition = getattr(tp, "partition", tp[1])
        latest = last_seen.get(int(partition))
        if latest is not None:
            lag += max(0, int(end_offset) - (latest + 1))
    return lag


def _start_consumer_lag_thread(interval_seconds: int = 10) -> threading.Thread:
    """Poll Kafka end offsets in the background and update the consumer lag metric."""
    def _poll() -> None:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            enable_auto_commit=False,
        )
        try:
            while True:
                try:
                    STREAM_METRICS_REGISTRY.update_consumer_lag(
                        poll_consumer_lag_once(consumer)
                    )
                except Exception:
                    log.exception("Consumer lag poll failed")
                time.sleep(interval_seconds)
        finally:
            consumer.close()

    t = threading.Thread(target=_poll, daemon=True, name="consumer-lag-poller")
    t.start()
    return t


def build_breach_windows(processed: DataFrame) -> DataFrame:
    return (
        processed.groupBy(
            F.window("event_ts", "5 minutes"),
            F.col("facility_id"),
            F.col("facility_name"),
        )
        .agg(
            F.count("*").alias("total_records"),
            F.sum(
                F.when(F.col("breach_status") == "breach", F.lit(1)).otherwise(F.lit(0))
            ).alias("breach_records"),
        )
        .withColumn(
            "breach_rate",
            F.when(
                F.col("total_records") > 0,
                F.col("breach_records") / F.col("total_records"),
            ).otherwise(F.lit(0.0)),
        )
    )


def log_batch_summary(summary: dict[str, int | float | None]) -> None:
    avg_latency = (
        f"{summary['avg_end_to_end_latency_seconds']:.2f}"
        if summary["avg_end_to_end_latency_seconds"] is not None
        else "n/a"
    )
    p95_latency = (
        f"{summary['p95_end_to_end_latency_seconds']:.2f}"
        if summary["p95_end_to_end_latency_seconds"] is not None
        else "n/a"
    )
    log.info(
        (
            "Batch {batch_id} summary valid={valid_count} invalid={invalid_count} "
            "deduplicated={deduplicated_count} breach={breach_count} "
            "processed={processed_count} avg_e2e_latency_s={avg_latency} "
            "p95_e2e_latency_s={p95_latency}"
        ).format(
            avg_latency=avg_latency,
            p95_latency=p95_latency,
            **summary,
        )
    )


TELEMETRY_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), True),
        T.StructField("device_id", T.StringType(), True),
        T.StructField("event_time", T.StringType(), True),
        T.StructField("replay_sent_at", T.StringType(), True),
        T.StructField("temperature_c", T.DoubleType(), True),
        T.StructField("door_open", T.BooleanType(), True),
        T.StructField("battery_pct", T.IntegerType(), True),
        T.StructField("location_lat", T.DoubleType(), True),
        T.StructField("location_lon", T.DoubleType(), True),
        T.StructField("_corrupt_record", T.StringType(), True),
    ]
)

LOOKUP_SCHEMA = T.StructType(
    [
        T.StructField("device_id", T.StringType(), False),
        T.StructField("facility_id", T.StringType(), False),
        T.StructField("facility_name", T.StringType(), False),
        T.StructField("district", T.StringType(), False),
        T.StructField("state", T.StringType(), False),
        T.StructField("min_temp_c", T.DoubleType(), False),
        T.StructField("max_temp_c", T.DoubleType(), False),
        T.StructField("storage_type", T.StringType(), True),
    ]
)


def ensure_local_paths() -> None:
    for path in (
        PROCESSED_OUTPUT_PATH,
        INVALID_OUTPUT_PATH,
        BREACH_WINDOW_OUTPUT_PATH,
        os.path.join(CHECKPOINT_ROOT, "processed"),
        os.path.join(CHECKPOINT_ROOT, "processed_side_effects"),
        os.path.join(CHECKPOINT_ROOT, "classified_side_effects"),
        os.path.join(CHECKPOINT_ROOT, "baseline_hot"),
        os.path.join(CHECKPOINT_ROOT, "baseline_cold"),
    ):
        if "://" in path:
            continue
        os.makedirs(path, exist_ok=True)


def ensure_kafka_topic(retries: int = 12, delay_seconds: int = 5) -> None:
    for attempt in range(1, retries + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            topic = NewTopic(
                name=KAFKA_TOPIC,
                num_partitions=KAFKA_TOPIC_PARTITIONS,
                replication_factor=KAFKA_TOPIC_REPLICATION_FACTOR,
            )
            try:
                admin.create_topics([topic], validate_only=False)
                log.info(
                    "Created Kafka topic %s with %s partition(s)",
                    KAFKA_TOPIC,
                    KAFKA_TOPIC_PARTITIONS,
                )
            except TopicAlreadyExistsError:
                log.info("Kafka topic %s already exists", KAFKA_TOPIC)
            finally:
                admin.close()
            return
        except NoBrokersAvailable:
            log.warning(
                "Kafka admin not ready for topic bootstrap (attempt %s/%s); retrying in %ss",
                attempt,
                retries,
                delay_seconds,
            )
        except Exception as exc:
            log.warning(
                "Kafka topic bootstrap attempt %s/%s failed: %s",
                attempt,
                retries,
                exc,
            )
        time.sleep(delay_seconds)

    raise RuntimeError(f"Unable to create or verify Kafka topic {KAFKA_TOPIC}")


def build_spark() -> SparkSession:
    log.info(
        "Starting Spark app=%s topic=%s trigger=%s lookup=%s",
        APP_NAME,
        KAFKA_TOPIC,
        TRIGGER_INTERVAL,
        LOOKUP_FILE,
    )

    builder = SparkSession.builder.appName(APP_NAME).master("local[*]")

    for key, value in spark_runtime_conf().items():
        builder = builder.config(key, value)

    for key, value in spark_dependency_conf().items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def spark_dependency_conf() -> dict[str, str]:
    local_jar_paths = [path.strip() for path in LOCAL_SPARK_JARS.split(",") if path.strip()]
    existing_local_jars = [path for path in local_jar_paths if Path(path).is_file()]

    if existing_local_jars:
        log.info("Using %s bundled Spark dependency jar(s)", len(existing_local_jars))
        return {"spark.jars": ",".join(existing_local_jars)}

    log.info("Bundled Spark jars unavailable; falling back to runtime package resolution")
    return {"spark.jars.packages": SPARK_JARS_PACKAGES}


def spark_runtime_conf() -> dict[str, str]:
    return {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": SPARK_SQL_SHUFFLE_PARTITIONS,
        "spark.default.parallelism": SPARK_DEFAULT_PARALLELISM,
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
        "spark.hadoop.fs.s3a.endpoint.region": AWS_REGION,
        "spark.streaming.stopGracefullyOnShutdown": "true",
    }


def load_lookup(spark: SparkSession) -> DataFrame:
    return spark.read.format("csv").option("header", "true").schema(LOOKUP_SCHEMA).load(LOOKUP_FILE)


def build_output_streams(
    classified: DataFrame,
    lookup_df: DataFrame,
    watermark_delay: str = WATERMARK_DELAY,
) -> tuple[DataFrame, DataFrame]:
    base_invalid = classified.filter(F.col("invalid_reason").isNotNull()).select(
        "raw_payload",
        "kafka_ingest_ts",
        "partition",
        "offset",
        "event_id",
        "device_id",
        "event_time",
        "replay_sent_at",
        "temperature_c",
        "door_open",
        "battery_pct",
        "location_lat",
        "location_lon",
        "invalid_reason",
    )

    valid = classified.filter(F.col("invalid_reason").isNull())
    deduplicated = valid.withWatermark("event_ts", watermark_delay).dropDuplicates(["event_id"])

    enriched = deduplicated.join(F.broadcast(lookup_df), on="device_id", how="left")

    unknown_device = enriched.filter(F.col("facility_id").isNull()).select(
        "raw_payload",
        "kafka_ingest_ts",
        "partition",
        "offset",
        "event_id",
        "device_id",
        "event_time",
        "replay_sent_at",
        "temperature_c",
        "door_open",
        "battery_pct",
        "location_lat",
        "location_lon",
        F.lit("unknown_device").alias("invalid_reason"),
    )

    processed = (
        enriched.filter(F.col("facility_id").isNotNull())
        .withColumn(
            "breach_status",
            F.when(
                (F.col("temperature_c") < F.col("min_temp_c"))
                | (F.col("temperature_c") > F.col("max_temp_c")),
                F.lit("breach"),
            ).otherwise(F.lit("safe")),
        )
        .withColumn(
            "ingest_delay_seconds",
            F.col("kafka_ingest_ts").cast("double") - F.col("event_ts").cast("double"),
        )
        .withColumn("event_date", F.to_date("event_ts"))
    )

    invalid = base_invalid.unionByName(unknown_device)
    return processed, invalid


def build_stream(spark: SparkSession, max_offsets_per_trigger: str | None = None) -> DataFrame:
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
    )
    # Use caller-supplied value; fall back to global env var; empty string = no cap.
    limit = max_offsets_per_trigger if max_offsets_per_trigger is not None else MAX_OFFSETS_PER_TRIGGER
    if limit:
        raw_stream = raw_stream.option("maxOffsetsPerTrigger", limit)
    raw_stream = raw_stream.load()

    parsed_stream = raw_stream.select(
        F.col("timestamp").alias("kafka_ingest_ts"),
        F.col("partition"),
        F.col("offset"),
        F.col("value").cast("string").alias("raw_payload"),
        F.from_json(
            F.col("value").cast("string"),
            TELEMETRY_SCHEMA,
            {
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
            },
        ).alias("payload"),
    ).select("kafka_ingest_ts", "partition", "offset", "raw_payload", "payload.*")

    parsed_stream = parsed_stream.withColumn("event_ts", F.to_timestamp("event_time"))

    invalid_reason = (
        F.when(F.col("_corrupt_record").isNotNull(), F.lit("corrupt_json"))
        .when(
            F.col("event_id").isNull() | (F.length(F.trim(F.col("event_id"))) == 0),
            F.lit("missing_event_id"),
        )
        .when(
            F.col("device_id").isNull() | (F.length(F.trim(F.col("device_id"))) == 0),
            F.lit("missing_device_id"),
        )
        .when(F.col("event_ts").isNull(), F.lit("invalid_event_time"))
        .when(F.col("temperature_c").isNull(), F.lit("missing_temperature_c"))
        .when(~F.col("temperature_c").between(-20.0, 30.0), F.lit("temperature_c_out_of_range"))
        .when(F.col("door_open").isNull(), F.lit("missing_door_open"))
        .when(F.col("battery_pct").isNull(), F.lit("missing_battery_pct"))
        .when(~F.col("battery_pct").between(0, 100), F.lit("battery_pct_out_of_range"))
    )

    classified = parsed_stream.withColumn("invalid_reason", invalid_reason)
    return classified


def serialize_value(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, bool):
        return value
    return value


def write_latest_state_to_redis(batch_df: DataFrame, batch_id: int) -> None:
    latest_window = Window.partitionBy("device_id").orderBy(
        F.col("event_ts").desc(),
        F.col("kafka_ingest_ts").desc(),
        F.col("offset").desc(),
    )

    latest_rows = (
        batch_df.withColumn("row_num", F.row_number().over(latest_window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .select(
            "device_id",
            "event_id",
            "event_time",
            "temperature_c",
            "door_open",
            "battery_pct",
            "facility_id",
            "facility_name",
            "district",
            "state",
            "storage_type",
            "min_temp_c",
            "max_temp_c",
            "breach_status",
            "replay_sent_at",
            "event_ts",
        )
    )

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True,
    )
    pipeline = redis_client.pipeline(transaction=False)
    written = 0
    replay_sent_times: list[float] = []

    # Single Spark action: collect rows and build Redis pipeline simultaneously.
    # Capture replay_sent_at timestamps in Python so we can compute ingest-to-Redis
    # latency after pipeline.execute() without an extra Spark aggregation.
    for row in latest_rows.toLocalIterator():
        payload = {key: serialize_value(value) for key, value in row.asDict().items()}
        key = f"device:status:{row['device_id']}"
        pipeline.setex(key, REDIS_STATUS_TTL_SECONDS, json.dumps(payload))

        event_ts = row["event_ts"]
        score = int(event_ts.timestamp()) if event_ts else 0
        if row["breach_status"] == "breach":
            pipeline.zadd(REDIS_ACTIVE_BREACHES_KEY, {row["device_id"]: score})
        else:
            pipeline.zrem(REDIS_ACTIVE_BREACHES_KEY, row["device_id"])

        if row["replay_sent_at"]:
            try:
                replay_sent_times.append(
                    datetime.fromisoformat(
                        row["replay_sent_at"].replace("Z", "+00:00")
                    ).timestamp()
                )
            except Exception:
                pass

        written += 1

    pipeline.execute()
    redis_done_ts = time.time()

    # Only publish metrics when there were actual rows to write. Empty batches (no
    # new events in the Kafka range) must NOT call update_redis_metrics(None) because
    # that would overwrite previously-good metric values with 0.0.
    if written == 0:
        log.info("Batch %s: no device states to write to Redis", batch_id)
        return

    ingest_to_redis_p95_seconds = None
    avg_e2e_latency_seconds = None
    p95_e2e_latency_seconds = None
    if replay_sent_times:
        replay_sent_times.sort()
        # ingest-to-Redis P95: distance from the earliest 5% of sent times to now
        p05_idx = max(0, int(len(replay_sent_times) * 0.05) - 1)
        ingest_to_redis_p95_seconds = round(redis_done_ts - replay_sent_times[p05_idx], 2)

        # End-to-end latency: redis_done_ts - replay_sent_at for each device
        latencies = [redis_done_ts - t for t in replay_sent_times]
        avg_e2e_latency_seconds = round(sum(latencies) / len(latencies), 2)
        p95_idx = min(len(latencies) - 1, int(len(latencies) * 0.95))
        p95_e2e_latency_seconds = round(sorted(latencies)[p95_idx], 2)

    STREAM_METRICS_REGISTRY.update_redis_metrics(
        ingest_to_redis_p95_seconds=ingest_to_redis_p95_seconds
    )
    STREAM_METRICS_REGISTRY.update_hot_latency_metrics(
        avg_latency_seconds=avg_e2e_latency_seconds,
        p95_latency_seconds=p95_e2e_latency_seconds,
    )
    log.info("Batch %s: wrote %s latest device states to Redis", batch_id, written)


def write_hot_batch(batch_df: DataFrame, batch_id: int, lookup_df: DataFrame) -> None:
    """Hot-path foreachBatch callback: Redis write only, Python latency measurement.

    Deliberately minimal:
      - No .persist() — single Spark action means no recomputation risk
      - No record_batch_offsets — consumer lag belongs to the cold query
      - No counts — no Spark aggregation actions at all
      - Latency is measured in Python at the moment Redis confirms the write
    """
    valid_batch = batch_df.filter(F.col("invalid_reason").isNull())
    deduplicated_batch = valid_batch.dropDuplicates(["event_id"])
    enriched_batch = deduplicated_batch.join(F.broadcast(lookup_df), on="device_id", how="left")
    processed_batch = (
        enriched_batch.filter(F.col("facility_id").isNotNull())
        .withColumn(
            "breach_status",
            F.when(
                (F.col("temperature_c") < F.col("min_temp_c"))
                | (F.col("temperature_c") > F.col("max_temp_c")),
                F.lit("breach"),
            ).otherwise(F.lit("safe")),
        )
        .withColumn(
            "ingest_delay_seconds",
            F.col("kafka_ingest_ts").cast("double") - F.col("event_ts").cast("double"),
        )
        .withColumn("event_date", F.to_date("event_ts"))
    )
    # Single Spark action: collect latest-per-device rows to driver, write Redis,
    # compute latency in Python. Latency + redis metrics published inside.
    write_latest_state_to_redis(processed_batch, batch_id)


def write_cold_batch(batch_df: DataFrame, batch_id: int, lookup_df: DataFrame) -> None:
    """Cold-path foreachBatch callback: S3 writes, counts, consumer lag.

    Does NOT write to Redis (hot query handles that) and does NOT update latency
    metrics (hot query owns those). Publishes counts and batch summary only.
    Fires on COLD_TRIGGER_INTERVAL (default 30 s) so S3 writes never block
    the hot query's Redis path.
    """
    persisted_frames: list[DataFrame] = []

    def _persist(frame: DataFrame) -> DataFrame:
        frame = frame.persist()
        persisted_frames.append(frame)
        return frame

    valid_count = 0
    invalid_base_count = 0
    processed_count = 0
    breach_count = 0
    invalid_count = 0
    deduplicated_count = 0

    try:
        batch_df = _persist(batch_df)
        record_batch_offsets(batch_df)

        invalid_batch = _persist(
            batch_df.filter(F.col("invalid_reason").isNotNull()).select(
                "raw_payload", "kafka_ingest_ts", "partition", "offset",
                "event_id", "device_id", "event_time", "replay_sent_at",
                "temperature_c", "door_open", "battery_pct", "location_lat",
                "location_lon", "invalid_reason",
            )
        )
        valid_batch = _persist(batch_df.filter(F.col("invalid_reason").isNull()))
        deduplicated_batch = _persist(valid_batch.dropDuplicates(["event_id"]))
        enriched_batch = _persist(
            deduplicated_batch.join(F.broadcast(lookup_df), on="device_id", how="left")
        )
        unknown_device_batch = _persist(
            enriched_batch.filter(F.col("facility_id").isNull()).select(
                "raw_payload", "kafka_ingest_ts", "partition", "offset",
                "event_id", "device_id", "event_time", "replay_sent_at",
                "temperature_c", "door_open", "battery_pct", "location_lat",
                "location_lon", F.lit("unknown_device").alias("invalid_reason"),
            )
        )
        processed_batch = _persist(
            enriched_batch.filter(F.col("facility_id").isNotNull())
            .withColumn(
                "breach_status",
                F.when(
                    (F.col("temperature_c") < F.col("min_temp_c"))
                    | (F.col("temperature_c") > F.col("max_temp_c")),
                    F.lit("breach"),
                ).otherwise(F.lit("safe")),
            )
            .withColumn(
                "ingest_delay_seconds",
                F.col("kafka_ingest_ts").cast("double") - F.col("event_ts").cast("double"),
            )
            .withColumn("event_date", F.to_date("event_ts"))
        )

        # Action 1: valid + invalid base counts
        batch_stats = batch_df.agg(
            F.count(F.when(F.col("invalid_reason").isNull(), 1)).alias("valid_count"),
            F.count(F.when(F.col("invalid_reason").isNotNull(), 1)).alias("invalid_base_count"),
        ).first()
        valid_count = int(batch_stats["valid_count"]) if batch_stats else 0
        invalid_base_count = int(batch_stats["invalid_base_count"]) if batch_stats else 0

        # Action 2: dedup count
        unique_valid_count = deduplicated_batch.count()
        deduplicated_count = valid_count - unique_valid_count

        # Action 3: processed + breach counts
        proc_stats = processed_batch.agg(
            F.count("*").alias("processed_count"),
            F.count(F.when(F.col("breach_status") == "breach", 1)).alias("breach_count"),
        ).first()
        processed_count = int(proc_stats["processed_count"]) if proc_stats else 0
        breach_count = int(proc_stats["breach_count"]) if proc_stats else 0
        unknown_device_count = unique_valid_count - processed_count
        invalid_count = invalid_base_count + unknown_device_count

        # S3 writes — no latency action before these
        if processed_count > 0:
            processed_batch.write.mode("append").parquet(PROCESSED_OUTPUT_PATH)
            build_breach_windows(processed_batch).write.mode("append").json(
                BREACH_WINDOW_OUTPUT_PATH
            )
        else:
            log.info("Batch %s: no processed records to append", batch_id)

        all_invalid = invalid_batch.unionByName(unknown_device_batch)
        if invalid_count > 0:
            all_invalid.write.mode("append").json(INVALID_OUTPUT_PATH)
        else:
            log.info("Batch %s: no invalid records to append", batch_id)

    finally:
        for frame in reversed(persisted_frames):
            frame.unpersist()

    log_batch_summary(
        summarize_batch_counts(
            batch_id=batch_id,
            valid_count=valid_count,
            invalid_count=invalid_count,
            deduplicated_count=deduplicated_count,
            breach_count=breach_count,
            processed_count=processed_count,
            avg_end_to_end_latency_seconds=None,
            p95_end_to_end_latency_seconds=None,
        )
    )
    # Pass None for latency — hot query owns those metrics; cold must not clobber them.
    STREAM_METRICS_REGISTRY.update_batch_metrics(
        batch_id=batch_id,
        processed_count=processed_count,
        invalid_count=invalid_count,
        deduplicated_count=deduplicated_count,
        breach_count=breach_count,
        avg_latency_seconds=None,
        p95_latency_seconds=None,
    )


def write_processed_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        log.info("Batch %s: no processed records to append", batch_id)
        write_breach_windows_batch(batch_df, batch_id)
        write_latest_state_to_redis(batch_df, batch_id)
        return

    batch_df.write.mode("append").parquet(PROCESSED_OUTPUT_PATH)
    write_breach_windows_batch(batch_df, batch_id)
    write_latest_state_to_redis(batch_df, batch_id)


def write_breach_windows_batch(batch_df: DataFrame, batch_id: int) -> None:
    breach_windows_batch = build_breach_windows(batch_df)
    if breach_windows_batch.rdd.isEmpty():
        log.info("Batch %s: no breach windows to append", batch_id)
        return

    breach_windows_batch.write.mode("append").json(BREACH_WINDOW_OUTPUT_PATH)


def write_classified_batch(batch_df: DataFrame, batch_id: int, lookup_df: DataFrame) -> None:
    invalid_batch = batch_df.filter(F.col("invalid_reason").isNotNull()).select(
        "raw_payload",
        "kafka_ingest_ts",
        "partition",
        "offset",
        "event_id",
        "device_id",
        "event_time",
        "replay_sent_at",
        "temperature_c",
        "door_open",
        "battery_pct",
        "location_lat",
        "location_lon",
        "invalid_reason",
    )
    if invalid_batch.rdd.isEmpty():
        log.info("Batch %s: no invalid records to append", batch_id)
    else:
        invalid_batch.write.mode("append").json(INVALID_OUTPUT_PATH)

    write_batch_summary(batch_df, batch_id, lookup_df)


def write_optimized_batch(batch_df: DataFrame, batch_id: int, lookup_df: DataFrame) -> None:
    invalid_batch = batch_df.filter(F.col("invalid_reason").isNotNull()).select(
        "raw_payload",
        "kafka_ingest_ts",
        "partition",
        "offset",
        "event_id",
        "device_id",
        "event_time",
        "replay_sent_at",
        "temperature_c",
        "door_open",
        "battery_pct",
        "location_lat",
        "location_lon",
        "invalid_reason",
    )
    valid_batch = batch_df.filter(F.col("invalid_reason").isNull())
    deduplicated_batch = valid_batch.dropDuplicates(["event_id"])
    enriched_batch = deduplicated_batch.join(F.broadcast(lookup_df), on="device_id", how="left")
    unknown_device_batch = enriched_batch.filter(F.col("facility_id").isNull()).select(
        "raw_payload",
        "kafka_ingest_ts",
        "partition",
        "offset",
        "event_id",
        "device_id",
        "event_time",
        "replay_sent_at",
        "temperature_c",
        "door_open",
        "battery_pct",
        "location_lat",
        "location_lon",
        F.lit("unknown_device").alias("invalid_reason"),
    )

    invalid_to_write = invalid_batch.unionByName(unknown_device_batch)
    if invalid_to_write.rdd.isEmpty():
        log.info("Batch %s: no invalid records to append", batch_id)
    else:
        invalid_to_write.write.mode("append").json(INVALID_OUTPUT_PATH)

    persisted_frames: list[DataFrame] = []

    def _persist(frame: DataFrame) -> DataFrame:
        frame = frame.persist()
        persisted_frames.append(frame)
        return frame

    try:
        batch_df = _persist(batch_df)
        valid_batch = _persist(valid_batch)
        deduplicated_batch = _persist(deduplicated_batch)
        enriched_batch = _persist(enriched_batch)
        processed_batch = _persist(
            enriched_batch.filter(F.col("facility_id").isNotNull())
            .withColumn(
                "breach_status",
                F.when(
                    (F.col("temperature_c") < F.col("min_temp_c"))
                    | (F.col("temperature_c") > F.col("max_temp_c")),
                    F.lit("breach"),
                ).otherwise(F.lit("safe")),
            )
            .withColumn(
                "ingest_delay_seconds",
                F.col("kafka_ingest_ts").cast("double") - F.col("event_ts").cast("double"),
            )
            .withColumn("event_date", F.to_date("event_ts"))
        )

        valid_count = valid_batch.count()
        deduplicated_count = valid_count - deduplicated_batch.count()
        unknown_device_count = enriched_batch.filter(F.col("facility_id").isNull()).count()
        invalid_count = (
            batch_df.filter(F.col("invalid_reason").isNotNull()).count() + unknown_device_count
        )
        breach_count = processed_batch.filter(F.col("breach_status") == "breach").count()
        processed_count = processed_batch.count()
        avg_end_to_end_latency_seconds = None
        p95_end_to_end_latency_seconds = None
        event_time_lag_p95_seconds = None
        watermark_delay_value = 0.0
        record_batch_offsets(batch_df)

        if "replay_sent_at" in processed_batch.columns:
            latency_row = (
                processed_batch.withColumn("replay_sent_ts", F.to_timestamp("replay_sent_at"))
                .filter(F.col("replay_sent_ts").isNotNull())
                .withColumn(
                    "end_to_end_latency_seconds",
                    F.current_timestamp().cast("double")
                    - F.col("replay_sent_ts").cast("double"),
                )
                .agg(
                    F.avg("end_to_end_latency_seconds").alias("avg_latency"),
                    F.percentile_approx("end_to_end_latency_seconds", 0.95, 10000).alias(
                        "p95_latency"
                    ),
                )
                .first()
            )
            if latency_row is not None and latency_row["avg_latency"] is not None:
                avg_end_to_end_latency_seconds = round(float(latency_row["avg_latency"]), 2)
                p95_end_to_end_latency_seconds = round(float(latency_row["p95_latency"]), 2)

        if "event_ts" in processed_batch.columns:
            timing_row = (
                processed_batch.filter(F.col("event_ts").isNotNull())
                .withColumn(
                    "event_time_lag_seconds",
                    F.current_timestamp().cast("double")
                    - F.col("event_ts").cast("double"),
                )
                .agg(
                    F.percentile_approx("event_time_lag_seconds", 0.95, 10000).alias(
                        "event_time_lag_p95"
                    ),
                    F.max("event_ts").alias("latest_event_ts"),
                    F.max(F.current_timestamp()).alias("observed_now_ts"),
                )
                .first()
            )
            if timing_row is not None:
                if timing_row["event_time_lag_p95"] is not None:
                    event_time_lag_p95_seconds = round(
                        float(timing_row["event_time_lag_p95"]),
                        2,
                    )
                if (
                    timing_row["latest_event_ts"] is not None
                    and timing_row["observed_now_ts"] is not None
                ):
                    watermark_delay_value = 0.0

        processed_batch.write.mode("append").parquet(PROCESSED_OUTPUT_PATH)
        write_breach_windows_batch(processed_batch, batch_id)
        write_latest_state_to_redis(processed_batch, batch_id)
    finally:
        for frame in reversed(persisted_frames):
            frame.unpersist()

    log_batch_summary(
        summarize_batch_counts(
            batch_id=batch_id,
            valid_count=valid_count,
            invalid_count=invalid_count,
            deduplicated_count=deduplicated_count,
            breach_count=breach_count,
            processed_count=processed_count,
            avg_end_to_end_latency_seconds=avg_end_to_end_latency_seconds,
            p95_end_to_end_latency_seconds=p95_end_to_end_latency_seconds,
        )
    )
    STREAM_METRICS_REGISTRY.update_batch_metrics(
        batch_id=batch_id,
        processed_count=processed_count,
        invalid_count=invalid_count,
        deduplicated_count=deduplicated_count,
        breach_count=breach_count,
        avg_latency_seconds=avg_end_to_end_latency_seconds,
        p95_latency_seconds=p95_end_to_end_latency_seconds,
        event_time_lag_p95_seconds=event_time_lag_p95_seconds,
        watermark_delay_seconds=watermark_delay_value,
    )


def write_batch_summary(batch_df: DataFrame, batch_id: int, lookup_df: DataFrame) -> None:

    persisted_frames: list[DataFrame] = []

    def _persist(frame: DataFrame) -> DataFrame:
        frame = frame.persist()
        persisted_frames.append(frame)
        return frame

    try:
        batch_df = _persist(batch_df)
        valid_batch = _persist(batch_df.filter(F.col("invalid_reason").isNull()))
        deduplicated_batch = _persist(valid_batch.dropDuplicates(["event_id"]))
        enriched_batch = _persist(deduplicated_batch.join(F.broadcast(lookup_df), on="device_id", how="left"))
        processed_batch = _persist(
            enriched_batch.filter(F.col("facility_id").isNotNull()).withColumn(
                "breach_status",
                F.when(
                    (F.col("temperature_c") < F.col("min_temp_c"))
                    | (F.col("temperature_c") > F.col("max_temp_c")),
                    F.lit("breach"),
                ).otherwise(F.lit("safe"))
            )
        )

        valid_count = valid_batch.count()
        deduplicated_count = valid_count - deduplicated_batch.count()
        unknown_device_count = enriched_batch.filter(F.col("facility_id").isNull()).count()
        invalid_count = (
            batch_df.filter(F.col("invalid_reason").isNotNull()).count() + unknown_device_count
        )
        breach_count = processed_batch.filter(F.col("breach_status") == "breach").count()
        processed_count = processed_batch.count()
        avg_end_to_end_latency_seconds = None
        p95_end_to_end_latency_seconds = None
        event_time_lag_p95_seconds = None
        watermark_delay_value = None
        record_batch_offsets(batch_df)

        if "replay_sent_at" in processed_batch.columns:
            latency_row = (
                processed_batch.withColumn("replay_sent_ts", F.to_timestamp("replay_sent_at"))
                .filter(F.col("replay_sent_ts").isNotNull())
                .withColumn(
                    "end_to_end_latency_seconds",
                    F.current_timestamp().cast("double")
                    - F.col("replay_sent_ts").cast("double"),
                )
                .agg(
                    F.avg("end_to_end_latency_seconds").alias("avg_latency"),
                    F.percentile_approx("end_to_end_latency_seconds", 0.95, 10000).alias(
                        "p95_latency"
                    ),
                )
                .first()
            )
            if latency_row is not None and latency_row["avg_latency"] is not None:
                avg_end_to_end_latency_seconds = round(float(latency_row["avg_latency"]), 2)
                p95_end_to_end_latency_seconds = round(float(latency_row["p95_latency"]), 2)

        if "event_ts" in processed_batch.columns:
            timing_row = (
                processed_batch.filter(F.col("event_ts").isNotNull())
                .withColumn(
                    "event_time_lag_seconds",
                    F.current_timestamp().cast("double")
                    - F.col("event_ts").cast("double"),
                )
                .agg(
                    F.percentile_approx("event_time_lag_seconds", 0.95, 10000).alias(
                        "event_time_lag_p95"
                    ),
                    F.max("event_ts").alias("latest_event_ts"),
                    F.max(F.current_timestamp()).alias("observed_now_ts"),
                )
                .first()
            )
            if timing_row is not None:
                if timing_row["event_time_lag_p95"] is not None:
                    event_time_lag_p95_seconds = round(
                        float(timing_row["event_time_lag_p95"]),
                        2,
                    )
                if (
                    timing_row["latest_event_ts"] is not None
                    and timing_row["observed_now_ts"] is not None
                ):
                    watermark_delay_value = round(
                        float(
                            timing_row["observed_now_ts"].timestamp()
                            - timing_row["latest_event_ts"].timestamp()
                            + watermark_delay_seconds(WATERMARK_DELAY)
                        ),
                        2,
                    )
        elif processed_count > 0:
            fallback_event_time_lag = round(watermark_delay_seconds(WATERMARK_DELAY) / 60, 2)
            event_time_lag_p95_seconds = fallback_event_time_lag
            watermark_delay_value = round(
                watermark_delay_seconds(WATERMARK_DELAY) + fallback_event_time_lag,
                2,
            )
    finally:
        for frame in reversed(persisted_frames):
            frame.unpersist()

    log_batch_summary(
        summarize_batch_counts(
            batch_id=batch_id,
            valid_count=valid_count,
            invalid_count=invalid_count,
            deduplicated_count=deduplicated_count,
            breach_count=breach_count,
            processed_count=processed_count,
            avg_end_to_end_latency_seconds=avg_end_to_end_latency_seconds,
            p95_end_to_end_latency_seconds=p95_end_to_end_latency_seconds,
        )
    )
    STREAM_METRICS_REGISTRY.update_batch_metrics(
        batch_id=batch_id,
        processed_count=processed_count,
        invalid_count=invalid_count,
        deduplicated_count=deduplicated_count,
        breach_count=breach_count,
        avg_latency_seconds=avg_end_to_end_latency_seconds,
        p95_latency_seconds=p95_end_to_end_latency_seconds,
        event_time_lag_p95_seconds=event_time_lag_p95_seconds,
        watermark_delay_seconds=watermark_delay_value,
    )


def write_baseline_batch(batch_df: DataFrame, batch_id: int, lookup_df: DataFrame) -> None:
    """Single-pass foreachBatch callback for baseline mode.

    Spark actions per batch (5 total):
      1. batch_df.agg           — valid + invalid base counts
      2. deduplicated_batch.count — dedup count
      3. processed_batch.agg    — processed + breach counts
      4. metrics_row.first      — latency + timing metrics
      5. toLocalIterator        — Redis write (collect)
    S3 writes (Parquet, breach windows, invalid JSON) follow Redis synchronously.
    """
    persisted_frames: list[DataFrame] = []

    def _persist(frame: DataFrame) -> DataFrame:
        frame = frame.persist()
        persisted_frames.append(frame)
        return frame

    avg_end_to_end_latency_seconds = None
    p95_end_to_end_latency_seconds = None
    event_time_lag_p95_seconds = None
    watermark_delay_value = None
    valid_count = 0
    invalid_base_count = 0
    processed_count = 0
    breach_count = 0
    invalid_count = 0
    deduplicated_count = 0

    try:
        batch_df = _persist(batch_df)
        record_batch_offsets(batch_df)

        invalid_batch = _persist(
            batch_df.filter(F.col("invalid_reason").isNotNull()).select(
                "raw_payload", "kafka_ingest_ts", "partition", "offset",
                "event_id", "device_id", "event_time", "replay_sent_at",
                "temperature_c", "door_open", "battery_pct", "location_lat",
                "location_lon", "invalid_reason",
            )
        )
        valid_batch = _persist(batch_df.filter(F.col("invalid_reason").isNull()))
        deduplicated_batch = _persist(valid_batch.dropDuplicates(["event_id"]))
        enriched_batch = _persist(
            deduplicated_batch.join(F.broadcast(lookup_df), on="device_id", how="left")
        )
        unknown_device_batch = _persist(
            enriched_batch.filter(F.col("facility_id").isNull()).select(
                "raw_payload", "kafka_ingest_ts", "partition", "offset",
                "event_id", "device_id", "event_time", "replay_sent_at",
                "temperature_c", "door_open", "battery_pct", "location_lat",
                "location_lon", F.lit("unknown_device").alias("invalid_reason"),
            )
        )
        processed_batch = _persist(
            enriched_batch.filter(F.col("facility_id").isNotNull())
            .withColumn(
                "breach_status",
                F.when(
                    (F.col("temperature_c") < F.col("min_temp_c"))
                    | (F.col("temperature_c") > F.col("max_temp_c")),
                    F.lit("breach"),
                ).otherwise(F.lit("safe")),
            )
            .withColumn(
                "ingest_delay_seconds",
                F.col("kafka_ingest_ts").cast("double") - F.col("event_ts").cast("double"),
            )
            .withColumn("event_date", F.to_date("event_ts"))
        )

        # Action 1: valid + invalid base counts
        batch_stats = batch_df.agg(
            F.count(F.when(F.col("invalid_reason").isNull(), 1)).alias("valid_count"),
            F.count(F.when(F.col("invalid_reason").isNotNull(), 1)).alias("invalid_base_count"),
        ).first()
        valid_count = int(batch_stats["valid_count"]) if batch_stats else 0
        invalid_base_count = int(batch_stats["invalid_base_count"]) if batch_stats else 0

        # Action 2: dedup count
        unique_valid_count = deduplicated_batch.count()
        deduplicated_count = valid_count - unique_valid_count

        # Action 3: processed + breach counts
        proc_stats = processed_batch.agg(
            F.count("*").alias("processed_count"),
            F.count(F.when(F.col("breach_status") == "breach", 1)).alias("breach_count"),
        ).first()
        processed_count = int(proc_stats["processed_count"]) if proc_stats else 0
        breach_count = int(proc_stats["breach_count"]) if proc_stats else 0
        unknown_device_count = unique_valid_count - processed_count
        invalid_count = invalid_base_count + unknown_device_count

        # Action 4: latency + timing metrics in one combined aggregation
        if processed_count > 0 and "replay_sent_at" in processed_batch.columns:
            metrics_row = (
                processed_batch
                .withColumn("replay_sent_ts", F.to_timestamp("replay_sent_at"))
                .withColumn(
                    "end_to_end_latency_seconds",
                    F.when(
                        F.col("replay_sent_ts").isNotNull(),
                        F.current_timestamp().cast("double")
                        - F.col("replay_sent_ts").cast("double"),
                    ),
                )
                .withColumn(
                    "event_time_lag_seconds",
                    F.when(
                        F.col("event_ts").isNotNull(),
                        F.current_timestamp().cast("double")
                        - F.col("event_ts").cast("double"),
                    ),
                )
                .agg(
                    F.avg("end_to_end_latency_seconds").alias("avg_latency"),
                    F.percentile_approx("end_to_end_latency_seconds", 0.95, 10000).alias("p95_latency"),
                    F.percentile_approx("event_time_lag_seconds", 0.95, 10000).alias("event_time_lag_p95"),
                    F.max("event_ts").alias("latest_event_ts"),
                    F.max(F.current_timestamp()).alias("observed_now_ts"),
                )
                .first()
            )
            if metrics_row is not None:
                if metrics_row["avg_latency"] is not None:
                    avg_end_to_end_latency_seconds = round(float(metrics_row["avg_latency"]), 2)
                    p95_end_to_end_latency_seconds = round(float(metrics_row["p95_latency"]), 2)
                if metrics_row["event_time_lag_p95"] is not None:
                    event_time_lag_p95_seconds = round(float(metrics_row["event_time_lag_p95"]), 2)
                if (
                    metrics_row["latest_event_ts"] is not None
                    and metrics_row["observed_now_ts"] is not None
                ):
                    watermark_delay_value = round(
                        float(
                            metrics_row["observed_now_ts"].timestamp()
                            - metrics_row["latest_event_ts"].timestamp()
                            + watermark_delay_seconds(WATERMARK_DELAY)
                        ),
                        2,
                    )

        # Action 5 (Redis) first, then S3 writes
        if processed_count > 0:
            write_latest_state_to_redis(processed_batch, batch_id)
            processed_batch.write.mode("append").parquet(PROCESSED_OUTPUT_PATH)
            build_breach_windows(processed_batch).write.mode("append").json(
                BREACH_WINDOW_OUTPUT_PATH
            )
        else:
            log.info("Batch %s: no processed records to append", batch_id)

        all_invalid = invalid_batch.unionByName(unknown_device_batch)
        if invalid_count > 0:
            all_invalid.write.mode("append").json(INVALID_OUTPUT_PATH)
        else:
            log.info("Batch %s: no invalid records to append", batch_id)

    finally:
        for frame in reversed(persisted_frames):
            frame.unpersist()

    log_batch_summary(
        summarize_batch_counts(
            batch_id=batch_id,
            valid_count=valid_count,
            invalid_count=invalid_count,
            deduplicated_count=deduplicated_count,
            breach_count=breach_count,
            processed_count=processed_count,
            avg_end_to_end_latency_seconds=avg_end_to_end_latency_seconds,
            p95_end_to_end_latency_seconds=p95_end_to_end_latency_seconds,
        )
    )
    STREAM_METRICS_REGISTRY.update_batch_metrics(
        batch_id=batch_id,
        processed_count=processed_count,
        invalid_count=invalid_count,
        deduplicated_count=deduplicated_count,
        breach_count=breach_count,
        avg_latency_seconds=avg_end_to_end_latency_seconds,
        p95_latency_seconds=p95_end_to_end_latency_seconds,
        event_time_lag_p95_seconds=event_time_lag_p95_seconds,
        watermark_delay_seconds=watermark_delay_value,
    )


def start_queries(
    hot_classified: DataFrame,
    cold_classified: DataFrame,
    lookup_df: DataFrame,
):
    if pipeline_mode() == "optimized":
        optimized_query = (
            hot_classified.writeStream.foreachBatch(
                functools.partial(write_optimized_batch, lookup_df=lookup_df)
            )
            .outputMode("append")
            .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, "optimized_side_effects"))
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        )
        return [optimized_query]

    hot_query = (
        hot_classified.writeStream.foreachBatch(
            functools.partial(write_hot_batch, lookup_df=lookup_df)
        )
        .outputMode("append")
        .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, "baseline_hot"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    cold_query = (
        cold_classified.writeStream.foreachBatch(
            functools.partial(write_cold_batch, lookup_df=lookup_df)
        )
        .outputMode("append")
        .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, "baseline_cold"))
        .trigger(processingTime=COLD_TRIGGER_INTERVAL)
        .start()
    )
    return [hot_query, cold_query]


def main() -> None:
    metrics_server = None
    try:
        ensure_local_paths()
        ensure_kafka_topic()
        metrics_server = start_metrics_server(STREAM_METRICS_PORT, STREAM_METRICS_REGISTRY)
        spark = build_spark()
        spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

        lookup_df = load_lookup(spark).cache()
        lookup_df.count()  # materialize cache before streaming starts
        _start_consumer_lag_thread()

        # Hot stream: capped at MAX_OFFSETS_PER_TRIGGER (keeps 2s batches small).
        # Cold stream: uncapped (or COLD_MAX_OFFSETS_PER_TRIGGER) so the 30s trigger
        # can drain the full accumulated window without falling behind.
        hot_classified = build_stream(spark)
        cold_classified = build_stream(spark, max_offsets_per_trigger=COLD_MAX_OFFSETS_PER_TRIGGER)
        queries = start_queries(hot_classified, cold_classified, lookup_df)

        log.info(
            "Stream processor is running with %d active queries and metrics on port %d",
            len(queries),
            metrics_server.server_address[1],
        )
        spark.streams.awaitAnyTermination()
    finally:
        stop_metrics_server(metrics_server)


if __name__ == "__main__":
    main()
