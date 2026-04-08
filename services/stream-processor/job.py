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
import time
from datetime import datetime
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
        self._metrics = {
            "vacciguard_stream_latest_batch_id": 0,
            "vacciguard_stream_latest_batch_timestamp_seconds": 0.0,
            "vacciguard_stream_processed_events_total": 0,
            "vacciguard_stream_invalid_events_total": 0,
            "vacciguard_stream_deduplicated_events_total": 0,
            "vacciguard_stream_breach_events_total": 0,
            "vacciguard_stream_latest_batch_avg_latency_seconds": 0.0,
            "vacciguard_stream_latest_batch_p95_latency_seconds": 0.0,
        }

    def update_batch_metrics(
        self,
        batch_id: int,
        processed_events: int,
        invalid_events: int,
        deduplicated_events: int,
        breach_events: int,
        avg_latency_seconds: float | None,
        p95_latency_seconds: float | None,
    ) -> None:
        self._metrics["vacciguard_stream_latest_batch_id"] = batch_id
        self._metrics["vacciguard_stream_latest_batch_timestamp_seconds"] = time.time()
        self._metrics["vacciguard_stream_processed_events_total"] += processed_events
        self._metrics["vacciguard_stream_invalid_events_total"] += invalid_events
        self._metrics["vacciguard_stream_deduplicated_events_total"] += deduplicated_events
        self._metrics["vacciguard_stream_breach_events_total"] += breach_events
        self._metrics["vacciguard_stream_latest_batch_avg_latency_seconds"] = (
            0.0 if avg_latency_seconds is None else avg_latency_seconds
        )
        self._metrics["vacciguard_stream_latest_batch_p95_latency_seconds"] = (
            0.0 if p95_latency_seconds is None else p95_latency_seconds
        )

    def render_prometheus(self) -> str:
        return "\n".join(f"{name} {value}" for name, value in self._metrics.items())


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


def write_latest_state_to_redis(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        log.info("Batch %s: no valid records to write to Redis", batch_id)
        return

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
        written += 1

    pipeline.execute()
    log.info("Batch %s: wrote %s latest device states to Redis", batch_id, written)


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
    ensure_local_paths()
    ensure_kafka_topic()
    spark = build_spark()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))

    processed, invalid, classified = build_stream(spark)
    queries = start_queries(processed, invalid, classified)

    log.info("Stream processor is running with %d active queries", len(queries))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
