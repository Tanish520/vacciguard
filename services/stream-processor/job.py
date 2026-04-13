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

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import redis
from kafka import KafkaAdminClient
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
WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "10 minutes")
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


def summarize_batch_counts(
    batch_id: int,
    valid_count: int,
    invalid_count: int,
    deduplicated_count: int,
    breach_count: int,
    processed_count: int,
    avg_end_to_end_latency_seconds: float | None,
    p95_end_to_end_latency_seconds: float | None,
    batch_processing_duration_seconds: float | None = None,
    ingest_delay_avg_seconds: float | None = None,
    ingest_delay_p95_seconds: float | None = None,
    alert_latency_avg_seconds: float | None = None,
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
        "batch_processing_duration_seconds": (
            f"{batch_processing_duration_seconds:.2f}"
            if batch_processing_duration_seconds is not None
            else "n/a"
        ),
        "ingest_delay_avg_seconds": (
            f"{ingest_delay_avg_seconds:.2f}"
            if ingest_delay_avg_seconds is not None
            else "n/a"
        ),
        "ingest_delay_p95_seconds": (
            f"{ingest_delay_p95_seconds:.2f}"
            if ingest_delay_p95_seconds is not None
            else "n/a"
        ),
        "alert_latency_avg_seconds": (
            f"{alert_latency_avg_seconds:.2f}"
            if alert_latency_avg_seconds is not None
            else "n/a"
        ),
    }


class StreamMetricsRegistry:
    """Prometheus-compatible metrics for the stream processor.

    Supports both gauge-style metrics (single value) and histogram-style
    metrics (sum + count for computing averages and percentiles).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # Gauge-style metrics (overwritten each batch)
        self._metrics = {
            "vacciguard_stream_latest_batch_id": -1,
            "vacciguard_stream_latest_batch_timestamp_seconds": 0.0,
            "vacciguard_stream_processed_events_total": 0,
            "vacciguard_stream_invalid_events_total": 0,
            "vacciguard_stream_deduplicated_events_total": 0,
            "vacciguard_stream_breach_events_total": 0,
            # Per-batch processing duration
            "vacciguard_stream_batch_processing_duration_seconds": 0.0,
            # Redis metrics
            "vacciguard_stream_redis_write_duration_seconds": 0.0,
            "vacciguard_stream_redis_latest_keys_written": 0,
            # S3 write metrics
            "vacciguard_stream_s3_processed_write_duration_seconds": 0.0,
            "vacciguard_stream_s3_breach_write_duration_seconds": 0.0,
            # Ingest delay metrics
            "vacciguard_stream_ingest_delay_avg_seconds": 0.0,
            "vacciguard_stream_ingest_delay_p95_seconds": 0.0,
            # Alert-path latency (to Redis write) - histogram (stored as instance attrs)
            # Legacy latency gauges (kept for backward compatibility)
            "vacciguard_stream_latest_batch_avg_latency_seconds": 0.0,
            "vacciguard_stream_latest_batch_p95_latency_seconds": 0.0,
            # Query progress metrics
            "vacciguard_stream_input_rows_per_second": 0.0,
            "vacciguard_stream_processed_rows_per_second": 0.0,
            "vacciguard_stream_batch_duration_ms": 0.0,
            "vacciguard_stream_state_total_rows": 0,
        }
        # Histogram bucket configuration
        self._histogram_buckets = [0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0]
        # Per-bucket counters for alert latency histogram
        self._alert_latency_buckets = {b: 0 for b in self._histogram_buckets}
        self._alert_latency_buckets[float("inf")] = 0
        # Histogram sum and count (separate from _metrics to avoid naming conflicts)
        self._alert_latency_sum_seconds = 0.0
        self._alert_latency_count = 0

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
        batch_processing_duration_seconds: float | None = None,
        redis_write_duration_seconds: float | None = None,
        redis_keys_written: int = 0,
        s3_processed_write_duration_seconds: float | None = None,
        s3_breach_write_duration_seconds: float | None = None,
        ingest_delay_avg_seconds: float | None = None,
        ingest_delay_p95_seconds: float | None = None,
        alert_latency_sum_seconds: float | None = None,
        alert_latency_count: int = 0,
        alert_latency_buckets: dict[float, int] | None = None,
    ) -> None:
        with self._lock:
            self._metrics["vacciguard_stream_latest_batch_id"] = batch_id
            self._metrics["vacciguard_stream_latest_batch_timestamp_seconds"] = time.time()
            self._metrics["vacciguard_stream_processed_events_total"] += processed_count
            self._metrics["vacciguard_stream_invalid_events_total"] += invalid_count
            self._metrics["vacciguard_stream_deduplicated_events_total"] += deduplicated_count
            self._metrics["vacciguard_stream_breach_events_total"] += breach_count
            self._metrics["vacciguard_stream_latest_batch_avg_latency_seconds"] = (
                0.0 if avg_latency_seconds is None else avg_latency_seconds
            )
            self._metrics["vacciguard_stream_latest_batch_p95_latency_seconds"] = (
                0.0 if p95_latency_seconds is None else p95_latency_seconds
            )
            if batch_processing_duration_seconds is not None:
                self._metrics["vacciguard_stream_batch_processing_duration_seconds"] = (
                    batch_processing_duration_seconds
                )
            if redis_write_duration_seconds is not None:
                self._metrics["vacciguard_stream_redis_write_duration_seconds"] = (
                    redis_write_duration_seconds
                )
            self._metrics["vacciguard_stream_redis_latest_keys_written"] = redis_keys_written
            if s3_processed_write_duration_seconds is not None:
                self._metrics["vacciguard_stream_s3_processed_write_duration_seconds"] = (
                    s3_processed_write_duration_seconds
                )
            if s3_breach_write_duration_seconds is not None:
                self._metrics["vacciguard_stream_s3_breach_write_duration_seconds"] = (
                    s3_breach_write_duration_seconds
                )
            if ingest_delay_avg_seconds is not None:
                self._metrics["vacciguard_stream_ingest_delay_avg_seconds"] = (
                    ingest_delay_avg_seconds
                )
            if ingest_delay_p95_seconds is not None:
                self._metrics["vacciguard_stream_ingest_delay_p95_seconds"] = (
                    ingest_delay_p95_seconds
                )
            if alert_latency_sum_seconds is not None:
                self._alert_latency_sum_seconds = alert_latency_sum_seconds
            self._alert_latency_count = alert_latency_count
            if alert_latency_buckets is not None:
                self._alert_latency_buckets.update(alert_latency_buckets)

    def update_query_progress(
        self,
        *,
        input_rows_per_second: float = 0.0,
        processed_rows_per_second: float = 0.0,
        batch_duration_ms: float = 0.0,
        state_total_rows: int = 0,
    ) -> None:
        with self._lock:
            self._metrics["vacciguard_stream_input_rows_per_second"] = input_rows_per_second
            self._metrics["vacciguard_stream_processed_rows_per_second"] = processed_rows_per_second
            self._metrics["vacciguard_stream_batch_duration_ms"] = batch_duration_ms
            self._metrics["vacciguard_stream_state_total_rows"] = state_total_rows

    def update_write_timing(
        self,
        *,
        redis_write_duration_seconds: float = 0.0,
        redis_keys_written: int = 0,
        s3_processed_write_duration_seconds: float = 0.0,
    ) -> None:
        with self._lock:
            self._metrics["vacciguard_stream_redis_write_duration_seconds"] = (
                redis_write_duration_seconds
            )
            self._metrics["vacciguard_stream_redis_latest_keys_written"] = redis_keys_written
            self._metrics["vacciguard_stream_s3_processed_write_duration_seconds"] = (
                s3_processed_write_duration_seconds
            )

    def render_prometheus(self) -> str:
        with self._lock:
            snapshot = tuple(self._metrics.items())
            bucket_snapshot = dict(self._alert_latency_buckets)

        lines = []
        for name, value in snapshot:
            lines.append(f"{name} {value}")

        # Render histogram-style metrics for alert latency
        cumulative = 0
        for bound in sorted(b for b in bucket_snapshot if b != float("inf")):
            cumulative += bucket_snapshot[bound]
            lines.append(
                f"vacciguard_stream_alert_latency_seconds_bucket{{le=\"{bound}\"}} {cumulative}"
            )
        cumulative += bucket_snapshot.get(float("inf"), 0)
        lines.append(
            f"vacciguard_stream_alert_latency_seconds_bucket{{le=\"+Inf\"}} {cumulative}"
        )
        lines.append(
            f"vacciguard_stream_alert_latency_seconds_sum "
            f"{self._alert_latency_sum_seconds}"
        )
        lines.append(
            f"vacciguard_stream_alert_latency_seconds_count "
            f"{self._alert_latency_count}"
        )

        return "\n".join(lines) + "\n"


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
    batch_proc = summary.get("batch_processing_duration_seconds", "n/a")
    ingest_avg = summary.get("ingest_delay_avg_seconds", "n/a")
    ingest_p95 = summary.get("ingest_delay_p95_seconds", "n/a")
    alert_avg = summary.get("alert_latency_avg_seconds", "n/a")

    log.info(
        (
            "Batch {batch_id} summary valid={valid_count} invalid={invalid_count} "
            "deduplicated={deduplicated_count} breach={breach_count} "
            "processed={processed_count} avg_e2e_latency_s={avg_latency} "
            "p95_e2e_latency_s={p95_latency} batch_proc_s={batch_proc} "
            "ingest_delay_avg_s={ingest_avg} ingest_delay_p95_s={ingest_p95} "
            "alert_latency_avg_s={alert_avg}"
        ).format(
            avg_latency=avg_latency,
            p95_latency=p95_latency,
            batch_proc=batch_proc,
            ingest_avg=ingest_avg,
            ingest_p95=ingest_p95,
            alert_avg=alert_avg,
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
            F.col("kafka_ingest_ts").cast("long") - F.col("event_ts").cast("long"),
        )
        .withColumn("event_date", F.to_date("event_ts"))
    )

    invalid = base_invalid.unionByName(unknown_device)
    return processed, invalid


def build_stream(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

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
    processed, invalid = build_output_streams(
        classified,
        load_lookup(spark),
        watermark_delay=WATERMARK_DELAY,
    )
    return processed, invalid, classified


def serialize_value(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, bool):
        return value
    return value


def write_latest_state_to_redis(batch_df: DataFrame, batch_id: int) -> tuple[float, int]:
    """Write latest device state to Redis and return (duration_seconds, keys_written)."""
    if batch_df.rdd.isEmpty():
        log.info("Batch %s: no valid records to write to Redis", batch_id)
        return 0.0, 0

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
            "event_ts",
            "replay_sent_at",  # needed for alert-path latency
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

    # Collect alert latencies: time from replay_sent_at to Redis write
    alert_latencies = []
    redis_write_start_ts = time.time()

    for row in latest_rows.toLocalIterator():
        payload = {key: serialize_value(value) for key, value in row.asDict().items()}
        key = f"device:status:{row['device_id']}"
        pipeline.setex(key, REDIS_STATUS_TTL_SECONDS, json.dumps(payload))

        # Record alert-path latency if replay_sent_at is available
        replay_sent_at_str = row["replay_sent_at"]
        if replay_sent_at_str:
            try:
                replay_sent_at_dt = datetime.strptime(
                    replay_sent_at_str, "%Y-%m-%dT%H:%M:%S.%fZ"
                ).replace(tzinfo=timezone.utc)
                alert_latency = time.time() - replay_sent_at_dt.timestamp()
                alert_latencies.append(max(0.0, alert_latency))
            except ValueError:
                # Fallback for old-format timestamps
                try:
                    replay_sent_at_dt = datetime.strptime(
                        replay_sent_at_str, "%Y-%m-%dT%H:%M:%SZ"
                    ).replace(tzinfo=timezone.utc)
                    alert_latency = time.time() - replay_sent_at_dt.timestamp()
                    alert_latencies.append(max(0.0, alert_latency))
                except ValueError:
                    pass

        event_ts = row["event_ts"]
        score = int(event_ts.timestamp()) if event_ts else 0
        if row["breach_status"] == "breach":
            pipeline.zadd(REDIS_ACTIVE_BREACHES_KEY, {row["device_id"]: score})
        else:
            pipeline.zrem(REDIS_ACTIVE_BREACHES_KEY, row["device_id"])
        written += 1

    pipeline.execute()
    redis_duration = time.time() - redis_write_start_ts
    log.info(
        "Batch %s: wrote %s latest device states to Redis in %.3fs",
        batch_id, written, redis_duration,
    )
    return redis_duration, written


def write_processed_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Write processed batch to S3, breach windows, and Redis.

    Also records Redis and S3 write timing metrics to the shared registry
    (only the write-duration gauges, not the counts — counts are handled
    by write_batch_summary to avoid double-counting).
    """
    if batch_df.rdd.isEmpty():
        log.info("Batch %s: no processed records to append", batch_id)
        write_breach_windows_batch(batch_df, batch_id)
        redis_dur, keys_written = write_latest_state_to_redis(batch_df, batch_id)
        STREAM_METRICS_REGISTRY.update_write_timing(
            redis_write_duration_seconds=redis_dur,
            redis_keys_written=keys_written,
            s3_processed_write_duration_seconds=0.0,
        )
        return

    s3_start = time.monotonic()
    batch_df.write.mode("append").parquet(PROCESSED_OUTPUT_PATH)
    s3_processed_dur = time.monotonic() - s3_start

    write_breach_windows_batch(batch_df, batch_id)

    redis_dur, keys_written = write_latest_state_to_redis(batch_df, batch_id)

    STREAM_METRICS_REGISTRY.update_write_timing(
        redis_write_duration_seconds=redis_dur,
        redis_keys_written=keys_written,
        s3_processed_write_duration_seconds=s3_processed_dur,
    )


def write_breach_windows_batch(batch_df: DataFrame, batch_id: int) -> float:
    """Write breach windows to S3. Returns write duration in seconds."""
    breach_windows_batch = build_breach_windows(batch_df)
    if breach_windows_batch.rdd.isEmpty():
        log.info("Batch %s: no breach windows to append", batch_id)
        return 0.0

    s3_start = time.monotonic()
    breach_windows_batch.write.mode("append").json(BREACH_WINDOW_OUTPUT_PATH)
    return time.monotonic() - s3_start


def write_classified_batch(batch_df: DataFrame, batch_id: int) -> None:
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

    write_batch_summary(batch_df, batch_id)


def write_batch_summary(batch_df: DataFrame, batch_id: int) -> None:
    """Compute and log per-batch metrics from the classified DataFrame.

    This function ONLY computes metrics -- it does NOT write outputs.
    Actual writes (S3, breach windows, Redis) happen in write_processed_batch.
    """
    batch_start = time.monotonic()
    spark = batch_df.sparkSession
    lookup_df = F.broadcast(load_lookup(spark))

    persisted_frames: list[DataFrame] = []

    def _persist(frame: DataFrame) -> DataFrame:
        frame = frame.persist()
        persisted_frames.append(frame)
        return frame

    try:
        batch_df = _persist(batch_df)
        valid_batch = _persist(batch_df.filter(F.col("invalid_reason").isNull()))
        deduplicated_batch = _persist(valid_batch.dropDuplicates(["event_id"]))
        enriched_batch = _persist(deduplicated_batch.join(lookup_df, on="device_id", how="left"))
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

        # Also compute ingest_delay_seconds for this batch so we can expose it
        if "kafka_ingest_ts" in processed_batch.columns and "event_ts" in processed_batch.columns:
            processed_batch = _persist(
                processed_batch.withColumn(
                    "ingest_delay_seconds",
                    F.col("kafka_ingest_ts").cast("long") - F.col("event_ts").cast("long"),
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

        # Measure batch processing duration (Spark computation, not trigger wait)
        batch_processing_duration = time.monotonic() - batch_start

        avg_end_to_end_latency_seconds = None
        p95_end_to_end_latency_seconds = None
        ingest_delay_avg_seconds = None
        ingest_delay_p95_seconds = None

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

        # Expose ingest_delay_seconds (was computed but never exposed)
        if "ingest_delay_seconds" in processed_batch.columns:
            ingest_row = (
                processed_batch.filter(F.col("ingest_delay_seconds").isNotNull())
                .agg(
                    F.avg("ingest_delay_seconds").alias("avg_ingest_delay"),
                    F.percentile_approx("ingest_delay_seconds", 0.95, 10000).alias(
                        "p95_ingest_delay"
                    ),
                )
                .first()
            )
            if ingest_row is not None and ingest_row["avg_ingest_delay"] is not None:
                ingest_delay_avg_seconds = round(float(ingest_row["avg_ingest_delay"]), 2)
                ingest_delay_p95_seconds = round(float(ingest_row["p95_ingest_delay"]), 2)

    finally:
        for frame in reversed(persisted_frames):
            frame.unpersist()

    # Compute alert-path latency approximation
    # True per-event measurement happens inside write_latest_state_to_redis
    # Here we approximate from batch timing
    alert_latency_avg = None
    if processed_count > 0:
        # Approximate alert latency = batch processing time
        # (actual Redis write happens in write_processed_batch, not here)
        alert_latency_avg = batch_processing_duration

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
            batch_processing_duration_seconds=batch_processing_duration,
            ingest_delay_avg_seconds=ingest_delay_avg_seconds,
            ingest_delay_p95_seconds=ingest_delay_p95_seconds,
            alert_latency_avg_seconds=alert_latency_avg,
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
        batch_processing_duration_seconds=batch_processing_duration,
        ingest_delay_avg_seconds=ingest_delay_avg_seconds,
        ingest_delay_p95_seconds=ingest_delay_p95_seconds,
        alert_latency_sum_seconds=(
            alert_latency_avg * processed_count if alert_latency_avg else 0.0
        ),
        alert_latency_count=processed_count,
    )


def start_queries(processed: DataFrame, invalid: DataFrame, classified: DataFrame):
    processed_side_effects_query = (
        processed.writeStream.foreachBatch(write_processed_batch)
        .outputMode("append")
        .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, "processed_side_effects"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    classified_side_effects_query = (
        classified.writeStream.foreachBatch(write_classified_batch)
        .outputMode("append")
        .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, "classified_side_effects"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    return [
        processed_side_effects_query,
        classified_side_effects_query,
    ]


def main() -> None:
    metrics_server = None
    try:
        ensure_local_paths()
        ensure_kafka_topic()
        metrics_server = start_metrics_server(STREAM_METRICS_PORT, STREAM_METRICS_REGISTRY)
        spark = build_spark()
        spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

        # Register streaming query progress listener
        _register_query_progress_listener(spark)

        processed, invalid, classified = build_stream(spark)
        queries = start_queries(processed, invalid, classified)

        log.info(
            "Stream processor is running with %d active queries and metrics on port %d",
            len(queries),
            metrics_server.server_address[1],
        )
        spark.streams.awaitAnyTermination()
    finally:
        stop_metrics_server(metrics_server)


def _register_query_progress_listener(spark: SparkSession) -> None:
    """Register a Spark StreamingQueryListener that forwards query progress to Prometheus.

    This exposes inputRowsPerSecond, processedRowsPerSecond, batchDuration, and
    state operator metrics -- critical for understanding if the stream is keeping
    up or falling behind at high event rates.
    """
    try:
        from pyspark.sql.streaming import StreamingQueryListener
    except ImportError:
        log.warning("StreamingQueryListener not available; query progress metrics disabled")
        return

    class VacciGuardQueryListener(StreamingQueryListener):
        def onQueryStarted(self, event):
            try:
                log.info("Query started: %s (id=%s)", getattr(event, "name", "unknown"),
                         getattr(event, "id", "unknown"))
            except Exception:
                pass

        def onQueryProgress(self, event):
            try:
                progress = getattr(event, "progress", None)
                if progress is None:
                    return

                # PySpark may return progress as a dict or a StreamingQueryProgress object
                if isinstance(progress, dict):
                    input_rate = progress.get("inputRowsPerSecond", 0.0) or 0.0
                    processed_rate = progress.get("processedRowsPerSecond", 0.0) or 0.0
                    batch_duration = progress.get("batchDuration", 0.0) or 0.0
                    operators = progress.get("stateOperators", [])
                else:
                    input_rate = getattr(progress, "inputRowsPerSecond", 0.0) or 0.0
                    processed_rate = getattr(progress, "processedRowsPerSecond", 0.0) or 0.0
                    batch_duration = getattr(progress, "batchDuration", 0.0) or 0.0
                    operators = getattr(progress, "stateOperators", []) or []

                state_total_rows = 0
                for op in operators:
                    if isinstance(op, dict):
                        state_total_rows += op.get("numRowsTotal", 0) or 0
                        state_total_rows += op.get("numRowsUpdated", 0) or 0
                    else:
                        state_total_rows += getattr(op, "numRowsTotal", 0) or 0
                        state_total_rows += getattr(op, "numRowsUpdated", 0) or 0

                STREAM_METRICS_REGISTRY.update_query_progress(
                    input_rows_per_second=float(input_rate),
                    processed_rows_per_second=float(processed_rate),
                    batch_duration_ms=float(batch_duration),
                    state_total_rows=state_total_rows,
                )
            except Exception as e:
                log.debug("Query progress update failed: %s", e)

        def onQueryTerminated(self, event):
            try:
                log.info("Query terminated: %s (id=%s)", getattr(event, "name", "unknown"),
                         getattr(event, "id", "unknown"))
            except Exception:
                pass

        def onQueryIdle(self, event):
            # Required by PySpark 3.5+ abstract method
            pass

    listener = VacciGuardQueryListener()
    spark.streams.addListener(listener)


if __name__ == "__main__":
    main()
