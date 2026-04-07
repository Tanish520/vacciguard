#!/usr/bin/env python3
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
PROCESSED_OUTPUT_PATH = os.environ.get("PROCESSED_OUTPUT_PATH", "/data/output/processed")
INVALID_OUTPUT_PATH = os.environ.get("INVALID_OUTPUT_PATH", "/data/output/invalid")
CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "/data/output/checkpoints")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "5 seconds")
WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "10 minutes")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))
REDIS_STATUS_TTL_SECONDS = int(os.environ.get("REDIS_STATUS_TTL_SECONDS", "3600"))
REDIS_ACTIVE_BREACHES_KEY = os.environ.get("REDIS_ACTIVE_BREACHES_KEY", "active_breaches")
SPARK_JARS_PACKAGES = os.environ.get(
    "SPARK_JARS_PACKAGES",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
)
KAFKA_TOPIC_PARTITIONS = int(os.environ.get("KAFKA_TOPIC_PARTITIONS", "1"))
KAFKA_TOPIC_REPLICATION_FACTOR = int(
    os.environ.get("KAFKA_TOPIC_REPLICATION_FACTOR", "1")
)


def summarize_batch_counts(
    batch_id: int,
    valid_count: int,
    invalid_count: int,
    deduplicated_count: int,
    breach_count: int,
) -> dict[str, int]:
    return {
        "batch_id": batch_id,
        "valid_count": valid_count,
        "invalid_count": invalid_count,
        "deduplicated_count": deduplicated_count,
        "breach_count": breach_count,
    }


def log_batch_summary(summary: dict[str, int]) -> None:
    log.info(
        "Batch %(batch_id)s summary valid=%(valid_count)s invalid=%(invalid_count)s "
        "deduplicated=%(deduplicated_count)s breach=%(breach_count)s",
        summary,
    )


TELEMETRY_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), True),
        T.StructField("device_id", T.StringType(), True),
        T.StructField("event_time", T.StringType(), True),
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
        os.path.join(CHECKPOINT_ROOT, "processed"),
        os.path.join(CHECKPOINT_ROOT, "invalid"),
        os.path.join(CHECKPOINT_ROOT, "redis"),
        os.path.join(CHECKPOINT_ROOT, "batch_summary"),
    ):
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
    return (
        SparkSession.builder.appName(APP_NAME)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", SPARK_JARS_PACKAGES)
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


def load_lookup(spark: SparkSession) -> DataFrame:
    return spark.read.format("csv").option("header", "true").schema(LOOKUP_SCHEMA).load(LOOKUP_FILE)


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

    base_invalid = classified.filter(F.col("invalid_reason").isNotNull()).select(
        "raw_payload",
        "kafka_ingest_ts",
        "partition",
        "offset",
        "event_id",
        "device_id",
        "event_time",
        "temperature_c",
        "door_open",
        "battery_pct",
        "location_lat",
        "location_lon",
        "invalid_reason",
    )

    valid = classified.filter(F.col("invalid_reason").isNull())
    deduplicated = valid.withWatermark("event_ts", WATERMARK_DELAY).dropDuplicates(["event_id"])

    lookup_df = F.broadcast(load_lookup(spark))
    enriched = deduplicated.join(lookup_df, on="device_id", how="left")

    unknown_device = enriched.filter(F.col("facility_id").isNull()).select(
        "raw_payload",
        "kafka_ingest_ts",
        "partition",
        "offset",
        "event_id",
        "device_id",
        "event_time",
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


def write_batch_summary(batch_df: DataFrame, batch_id: int) -> None:
    spark = batch_df.sparkSession
    lookup_df = F.broadcast(load_lookup(spark))

    valid_batch = batch_df.filter(F.col("invalid_reason").isNull())
    deduplicated_batch = valid_batch.dropDuplicates(["event_id"])
    enriched_batch = deduplicated_batch.join(lookup_df, on="device_id", how="left")
    processed_batch = enriched_batch.filter(F.col("facility_id").isNotNull()).withColumn(
        "breach_status",
        F.when(
            (F.col("temperature_c") < F.col("min_temp_c"))
            | (F.col("temperature_c") > F.col("max_temp_c")),
            F.lit("breach"),
        ).otherwise(F.lit("safe"))
    )

    valid_count = valid_batch.count()
    deduplicated_count = max(valid_count - deduplicated_batch.count(), 0)
    unknown_device_count = enriched_batch.filter(F.col("facility_id").isNull()).count()
    invalid_count = batch_df.filter(F.col("invalid_reason").isNotNull()).count() + unknown_device_count
    breach_count = processed_batch.filter(F.col("breach_status") == "breach").count()

    log_batch_summary(
        summarize_batch_counts(
            batch_id=batch_id,
            valid_count=valid_count,
            invalid_count=invalid_count,
            deduplicated_count=deduplicated_count,
            breach_count=breach_count,
        )
    )


def start_queries(processed: DataFrame, invalid: DataFrame, classified: DataFrame):
    processed_query = (
        processed.writeStream.format("parquet")
        .outputMode("append")
        .option("path", PROCESSED_OUTPUT_PATH)
        .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, "processed"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    invalid_query = (
        invalid.writeStream.format("json")
        .outputMode("append")
        .option("path", INVALID_OUTPUT_PATH)
        .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, "invalid"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    redis_query = (
        processed.writeStream.foreachBatch(write_latest_state_to_redis)
        .outputMode("append")
        .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, "redis"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    batch_summary_query = (
        classified.writeStream.foreachBatch(write_batch_summary)
        .outputMode("append")
        .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, "batch_summary"))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    return [
        processed_query,
        invalid_query,
        redis_query,
        batch_summary_query,
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
