import functools
import importlib.util
from pathlib import Path
import tempfile
import threading
import unittest
import uuid
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


MODULE_PATH = Path(__file__).resolve().parents[2] / "services" / "stream-processor" / "job.py"
SPEC = importlib.util.spec_from_file_location("stream_job", MODULE_PATH)
stream_job = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(stream_job)


class BatchSummaryTests(unittest.TestCase):
    def test_summarize_batch_counts_sets_all_expected_fields(self):
        summary = stream_job.summarize_batch_counts(
            batch_id=7,
            valid_count=9,
            invalid_count=3,
            deduplicated_count=2,
            breach_count=4,
            processed_count=6,
            avg_end_to_end_latency_seconds=1.25,
            p95_end_to_end_latency_seconds=2.5,
        )

        self.assertEqual(summary["batch_id"], 7)
        self.assertEqual(summary["valid_count"], 9)
        self.assertEqual(summary["invalid_count"], 3)
        self.assertEqual(summary["deduplicated_count"], 2)
        self.assertEqual(summary["breach_count"], 4)
        self.assertEqual(summary["processed_count"], 6)
        self.assertEqual(summary["avg_end_to_end_latency_seconds"], 1.25)
        self.assertEqual(summary["p95_end_to_end_latency_seconds"], 2.5)


class OutputPathConfigTests(unittest.TestCase):
    def test_breach_window_output_path_defaults_are_exposed(self):
        self.assertEqual(
            stream_job.BREACH_WINDOW_OUTPUT_PATH,
            "/data/output/breach_windows",
        )

    def test_watermark_delay_defaults_are_exposed(self):
        self.assertEqual(stream_job.WATERMARK_DELAY, "10 minutes")


class PersistentMetricStateTests(unittest.TestCase):
    def test_initialize_persistent_stream_metrics_seeds_restart_and_cumulative_counts(self):
        fake_client = Mock()
        fake_client.incr.return_value = 7
        fake_client.get.return_value = "123"
        registry = stream_job.StreamMetricsRegistry()

        with patch.object(stream_job, "metrics_redis_client", return_value=fake_client), patch.object(
            stream_job,
            "STREAM_METRICS_REGISTRY",
            registry,
        ):
            stream_job.initialize_persistent_stream_metrics()

        rendered = registry.render_prometheus()

        self.assertIn("vacciguard_stream_pod_restart_count 7", rendered)
        self.assertIn("vacciguard_stream_processed_events_total 123", rendered)
        self.assertIn("vacciguard_stream_cumulative_processed_events 123", rendered)


class SparkDependencyConfigTests(unittest.TestCase):
    def test_spark_conf_uses_local_jars_when_available(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            jar_one = Path(temp_dir) / "spark-sql-kafka.jar"
            jar_two = Path(temp_dir) / "hadoop-aws.jar"
            jar_one.write_text("", encoding="utf-8")
            jar_two.write_text("", encoding="utf-8")

            with patch.object(
                stream_job,
                "LOCAL_SPARK_JARS",
                f"{jar_one},{jar_two}",
            ), patch.object(stream_job, "SPARK_JARS_PACKAGES", "unused:package:1.0"):
                conf = stream_job.spark_dependency_conf()

        self.assertEqual(conf["spark.jars"], f"{jar_one},{jar_two}")
        self.assertNotIn("spark.jars.packages", conf)

    def test_spark_conf_falls_back_to_runtime_package_resolution(self):
        with patch.object(stream_job, "LOCAL_SPARK_JARS", "/does/not/exist.jar"), patch.object(
            stream_job, "SPARK_JARS_PACKAGES", "org.example:demo:1.0"
        ):
            conf = stream_job.spark_dependency_conf()

        self.assertEqual(conf["spark.jars.packages"], "org.example:demo:1.0")
        self.assertNotIn("spark.jars", conf)


class SparkRuntimeConfigTests(unittest.TestCase):
    def test_spark_runtime_conf_uses_env_driven_parallelism_values(self):
        with patch.object(stream_job, "SPARK_SQL_SHUFFLE_PARTITIONS", "6"), patch.object(
            stream_job, "SPARK_DEFAULT_PARALLELISM", "6"
        ):
            conf = stream_job.spark_runtime_conf()

        self.assertEqual(conf["spark.sql.shuffle.partitions"], "6")
        self.assertEqual(conf["spark.default.parallelism"], "6")

    def test_spark_runtime_conf_keeps_existing_streaming_defaults(self):
        conf = stream_job.spark_runtime_conf()

        self.assertEqual(conf["spark.sql.session.timeZone"], "UTC")
        self.assertEqual(
            conf["spark.hadoop.fs.s3a.aws.credentials.provider"],
            "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
        )
        self.assertEqual(conf["spark.streaming.stopGracefullyOnShutdown"], "true")


class KafkaSourceConfigTests(unittest.TestCase):
    def test_build_stream_uses_max_offsets_per_trigger_when_configured(self):
        reader = Mock()
        stream = Mock()
        reader.format.return_value = reader
        reader.option.return_value = reader
        reader.load.return_value = stream
        stream.select.return_value = stream
        stream.withColumn.return_value = stream

        session = SparkSession.builder.master("local[1]").appName(
            "kafka-source-config-tests"
        ).getOrCreate()
        spark = Mock()
        spark.readStream = reader

        try:
            with patch.object(stream_job, "MAX_OFFSETS_PER_TRIGGER", "400"):
                stream_job.build_stream(spark)
        finally:
            session.stop()

        reader.option.assert_any_call("maxOffsetsPerTrigger", "400")


class BatchSummaryLoggingTests(unittest.TestCase):
    def test_log_batch_summary_formats_message_without_mapping_errors(self):
        summary = stream_job.summarize_batch_counts(
            batch_id=3,
            valid_count=15,
            invalid_count=0,
            deduplicated_count=0,
            breach_count=0,
            processed_count=15,
            avg_end_to_end_latency_seconds=None,
            p95_end_to_end_latency_seconds=None,
        )

        with patch.object(stream_job.log, "info") as mock_log_info:
            stream_job.log_batch_summary(summary)

        mock_log_info.assert_called_once()
        logged_message = mock_log_info.call_args.args[0]
        self.assertIn("Batch 3 summary valid=15 invalid=0 deduplicated=0 breach=0", logged_message)
        self.assertIn("processed=15", logged_message)
        self.assertIn("avg_e2e_latency_s=n/a", logged_message)
        self.assertIn("p95_e2e_latency_s=n/a", logged_message)


class StreamMetricsRegistryTests(unittest.TestCase):
    def test_render_prometheus_is_snapshot_safe_during_concurrent_update(self):
        class BlockingValue:
            def __init__(self, value, ready_event, continue_event):
                self.value = value
                self.ready_event = ready_event
                self.continue_event = continue_event

            def __str__(self):
                self.ready_event.set()
                if not self.continue_event.wait(timeout=1):
                    raise AssertionError("render did not resume in time")
                return str(self.value)

        registry = stream_job.StreamMetricsRegistry()
        ready_event = threading.Event()
        continue_event = threading.Event()
        rendered_holder = {}

        registry._metrics["vacciguard_stream_latest_batch_id"] = BlockingValue(
            -1,
            ready_event,
            continue_event,
        )

        def render_metrics():
            rendered_holder["value"] = registry.render_prometheus()

        render_thread = threading.Thread(target=render_metrics)
        render_thread.start()
        self.assertTrue(ready_event.wait(timeout=1))

        registry.update_batch_metrics(
            batch_id=4,
            processed_count=2,
            invalid_count=3,
            deduplicated_count=1,
            breach_count=0,
            avg_latency_seconds=0.75,
            p95_latency_seconds=1.25,
        )

        continue_event.set()
        render_thread.join(timeout=1)
        self.assertFalse(render_thread.is_alive())

        rendered = rendered_holder["value"]

        self.assertIn("vacciguard_stream_latest_batch_id -1", rendered)
        self.assertIn("vacciguard_stream_latest_batch_timestamp_seconds 0.0", rendered)
        self.assertIn("vacciguard_stream_processed_events_total 0", rendered)
        self.assertIn("vacciguard_stream_invalid_events_total 0", rendered)
        self.assertIn("vacciguard_stream_deduplicated_events_total 0", rendered)
        self.assertIn("vacciguard_stream_breach_events_total 0", rendered)
        self.assertIn("vacciguard_stream_latest_batch_avg_latency_seconds 0.0", rendered)
        self.assertIn("vacciguard_stream_latest_batch_p95_latency_seconds 0.0", rendered)
        self.assertIn("vacciguard_stream_latest_batch_p99_latency_seconds 0.0", rendered)
        self.assertIn("vacciguard_stream_latest_batch_event_time_lag_p95_seconds 0.0", rendered)
        self.assertIn("vacciguard_stream_latest_batch_ingest_to_redis_p95_seconds 0.0", rendered)
        self.assertIn("vacciguard_stream_hot_batch_duration_seconds 0.0", rendered)
        self.assertIn("vacciguard_stream_cold_batch_duration_seconds 0.0", rendered)
        self.assertIn("vacciguard_stream_observed_throughput_eps 0.0", rendered)
        self.assertIn("vacciguard_stream_pod_restart_count 0", rendered)
        self.assertIn("vacciguard_stream_queries_active 0", rendered)
        self.assertIn("vacciguard_stream_cumulative_processed_events 0", rendered)
        self.assertIn("vacciguard_stream_watermark_delay_seconds 0.0", rendered)
        self.assertIn("vacciguard_stream_consumer_lag_records 0", rendered)

    def test_update_batch_metrics_renders_latest_and_total_metrics(self):
        registry = stream_job.StreamMetricsRegistry()

        registry.update_batch_metrics(
            batch_id=3,
            processed_count=5,
            invalid_count=1,
            deduplicated_count=2,
            breach_count=4,
            avg_latency_seconds=1.5,
            p95_latency_seconds=2.0,
            event_time_lag_p95_seconds=9.25,
            watermark_delay_seconds=601.0,
        )
        registry.update_batch_metrics(
            batch_id=4,
            processed_count=2,
            invalid_count=3,
            deduplicated_count=1,
            breach_count=0,
            avg_latency_seconds=0.75,
            p95_latency_seconds=1.25,
            event_time_lag_p95_seconds=5.5,
            watermark_delay_seconds=602.0,
        )
        registry.update_consumer_lag(7)
        registry.update_redis_metrics(ingest_to_redis_p95_seconds=3.25)

        rendered = registry.render_prometheus()

        self.assertIn("vacciguard_stream_latest_batch_id 4", rendered)
        self.assertIn("vacciguard_stream_processed_events_total 7", rendered)
        self.assertIn("vacciguard_stream_invalid_events_total 4", rendered)
        self.assertIn("vacciguard_stream_latest_batch_event_time_lag_p95_seconds 5.5", rendered)
        self.assertIn("vacciguard_stream_latest_batch_ingest_to_redis_p95_seconds 3.25", rendered)
        self.assertIn("vacciguard_stream_watermark_delay_seconds 602.0", rendered)
        self.assertIn("vacciguard_stream_consumer_lag_records 7", rendered)


class QueryPlanningTests(unittest.TestCase):
    def _make_query_builder(self):
        builder = Mock()
        builder.foreachBatch.return_value = builder
        builder.outputMode.return_value = builder
        builder.option.return_value = builder
        builder.trigger.return_value = builder
        builder.start.return_value = Mock(name="query")
        return builder

    def test_start_queries_uses_two_queries_in_baseline_mode(self):
        hot_writer = self._make_query_builder()
        cold_writer = self._make_query_builder()
        hot_df = Mock(writeStream=hot_writer)
        cold_df = Mock(writeStream=cold_writer)
        lookup_df = Mock(name="lookup_df")

        with patch.object(stream_job, "pipeline_mode", return_value="baseline"):
            queries = stream_job.start_queries(hot_df, cold_df, lookup_df)

        self.assertEqual(len(queries), 2)
        hot_callback = hot_writer.foreachBatch.call_args.args[0]
        cold_callback = cold_writer.foreachBatch.call_args.args[0]
        self.assertIs(hot_callback.func, stream_job.write_hot_batch)
        self.assertIs(cold_callback.func, stream_job.write_cold_batch)
        self.assertIs(hot_callback.keywords["lookup_df"], lookup_df)
        self.assertIs(cold_callback.keywords["lookup_df"], lookup_df)

    def test_start_queries_uses_hot_only_query_in_optimized_hot_role(self):
        hot_writer = self._make_query_builder()
        cold_writer = self._make_query_builder()
        hot_df = Mock(writeStream=hot_writer)
        cold_df = Mock(writeStream=cold_writer)
        lookup_df = Mock(name="lookup_df")

        with patch.object(stream_job, "pipeline_mode", return_value="optimized"), patch.object(
            stream_job, "pipeline_service_role", return_value="hot"
        ):
            queries = stream_job.start_queries(hot_df, cold_df, lookup_df)

        self.assertEqual(len(queries), 1)
        hot_callback = hot_writer.foreachBatch.call_args.args[0]
        self.assertIsInstance(hot_callback, functools.partial)
        self.assertIs(hot_callback.func, stream_job.write_hot_batch)
        self.assertIs(hot_callback.keywords["lookup_df"], lookup_df)
        cold_writer.foreachBatch.assert_not_called()

    def test_start_queries_uses_cold_only_query_in_optimized_cold_role(self):
        hot_writer = self._make_query_builder()
        cold_writer = self._make_query_builder()
        hot_df = Mock(writeStream=hot_writer)
        cold_df = Mock(writeStream=cold_writer)
        lookup_df = Mock(name="lookup_df")

        with patch.object(stream_job, "pipeline_mode", return_value="optimized"), patch.object(
            stream_job, "pipeline_service_role", return_value="cold"
        ):
            queries = stream_job.start_queries(hot_df, cold_df, lookup_df)

        self.assertEqual(len(queries), 1)
        cold_callback = cold_writer.foreachBatch.call_args.args[0]
        self.assertIsInstance(cold_callback, functools.partial)
        self.assertIs(cold_callback.func, stream_job.write_cold_batch)
        self.assertIs(cold_callback.keywords["lookup_df"], lookup_df)
        hot_writer.foreachBatch.assert_not_called()

    def test_record_batch_offsets_tracks_latest_partition_offsets(self):
        batch_schema = T.StructType(
            [
                T.StructField("partition", T.IntegerType(), True),
                T.StructField("offset", T.LongType(), True),
            ]
        )
        spark = SparkSession.builder.getOrCreate()
        batch_df = spark.createDataFrame(
            [
                {"partition": 0, "offset": 14},
                {"partition": 0, "offset": 15},
                {"partition": 1, "offset": 9},
            ],
            schema=batch_schema,
        )

        with patch.object(stream_job, "STREAM_METRICS_REGISTRY") as mock_registry:
            stream_job.record_batch_offsets(batch_df)

        mock_registry.update_last_seen_offsets.assert_called_once_with({0: 15, 1: 9})

    def test_poll_consumer_lag_once_uses_recorded_offsets(self):
        fake_consumer = Mock()
        fake_consumer.end_offsets.return_value = {
            ("vacciguard-telemetry", 0): 20,
            ("vacciguard-telemetry", 1): 12,
        }

        with patch.object(stream_job, "STREAM_METRICS_REGISTRY") as mock_registry, patch.object(
            stream_job,
            "TopicPartition",
            side_effect=lambda topic, partition: (topic, partition),
        ):
            mock_registry.get_last_seen_offsets.return_value = {0: 15, 1: 9}

            lag = stream_job.poll_consumer_lag_once(fake_consumer)

        self.assertEqual(lag, 6)
        fake_consumer.end_offsets.assert_called_once()


class MainLifecycleTests(unittest.TestCase):
    def test_main_shuts_down_metrics_server_when_startup_fails(self):
        metrics_server = Mock()
        metrics_server.server_address = ("0.0.0.0", 9108)
        startup_error = RuntimeError("spark bootstrap failed")

        with patch.object(stream_job, "ensure_local_paths"), patch.object(
            stream_job, "ensure_kafka_topic"
        ), patch.object(
            stream_job, "start_metrics_server", return_value=metrics_server
        ), patch.object(
            stream_job, "build_spark", side_effect=startup_error
        ):
            with self.assertRaises(RuntimeError) as exc_info:
                stream_job.main()

        self.assertIs(exc_info.exception, startup_error)
        metrics_server.shutdown.assert_called_once_with()
        metrics_server.server_close.assert_called_once_with()

    def test_main_shuts_down_metrics_server_after_stream_termination(self):
        metrics_server = Mock()
        metrics_server.server_address = ("0.0.0.0", 9108)
        spark = Mock()
        start_queries = Mock(return_value=["query"])

        with patch.object(stream_job, "ensure_local_paths"), patch.object(
            stream_job, "ensure_kafka_topic"
        ), patch.object(
            stream_job, "start_metrics_server", return_value=metrics_server
        ), patch.object(
            stream_job, "build_spark", return_value=spark
        ), patch.object(
            stream_job, "build_stream", return_value="classified"
        ), patch.object(
            stream_job, "build_output_streams", return_value=("processed", "invalid")
        ), patch.object(
            stream_job, "pipeline_mode", return_value="baseline"
        ), patch.object(
            stream_job, "_start_consumer_lag_thread", return_value=Mock()
        ), patch.object(
            stream_job, "start_queries", start_queries
        ):
            stream_job.main()

        spark.sparkContext.setLogLevel.assert_called_once_with(
            stream_job.os.environ.get("SPARK_LOG_LEVEL", "WARN")
        )
        spark.streams.awaitAnyTermination.assert_called_once_with()
        metrics_server.shutdown.assert_called_once_with()
        metrics_server.server_close.assert_called_once_with()

    def test_main_uses_hot_and_cold_streams_in_optimized_hot_role(self):
        metrics_server = Mock()
        metrics_server.server_address = ("0.0.0.0", 9108)
        spark = Mock()
        lookup_df = Mock(name="lookup_df")
        lookup_df.cache.return_value = lookup_df
        lookup_df.count.return_value = 1
        start_queries = Mock(return_value=["query"])

        with patch.object(stream_job, "ensure_local_paths"), patch.object(
            stream_job, "ensure_kafka_topic"
        ), patch.object(
            stream_job, "start_metrics_server", return_value=metrics_server
        ), patch.object(
            stream_job, "build_spark", return_value=spark
        ), patch.object(
            stream_job, "load_lookup", return_value=lookup_df
        ), patch.object(
            stream_job, "build_stream", side_effect=["hot-classified", "cold-classified"]
        ), patch.object(
            stream_job, "pipeline_mode", return_value="optimized"
        ), patch.object(
            stream_job, "pipeline_service_role", return_value="hot"
        ), patch.object(
            stream_job, "_start_consumer_lag_thread", return_value=Mock()
        ), patch.object(
            stream_job, "start_queries", start_queries
        ):
            stream_job.main()

        lookup_df.cache.assert_called_once_with()
        lookup_df.count.assert_called_once_with()
        start_queries.assert_called_once_with("hot-classified", "cold-classified", lookup_df)
        metrics_server.shutdown.assert_called_once_with()
        metrics_server.server_close.assert_called_once_with()

    def test_main_uses_hot_and_cold_streams_in_optimized_cold_role(self):
        metrics_server = Mock()
        metrics_server.server_address = ("0.0.0.0", 9108)
        spark = Mock()
        lookup_df = Mock(name="lookup_df")
        lookup_df.cache.return_value = lookup_df
        lookup_df.count.return_value = 1
        start_queries = Mock(return_value=["query"])

        with patch.object(stream_job, "ensure_local_paths"), patch.object(
            stream_job, "ensure_kafka_topic"
        ), patch.object(
            stream_job, "start_metrics_server", return_value=metrics_server
        ), patch.object(
            stream_job, "build_spark", return_value=spark
        ), patch.object(
            stream_job, "load_lookup", return_value=lookup_df
        ), patch.object(
            stream_job, "build_stream", side_effect=["hot-classified", "cold-classified"]
        ), patch.object(
            stream_job, "pipeline_mode", return_value="optimized"
        ), patch.object(
            stream_job, "pipeline_service_role", return_value="cold"
        ), patch.object(
            stream_job, "_start_consumer_lag_thread", return_value=Mock()
        ), patch.object(
            stream_job, "start_queries", start_queries
        ):
            stream_job.main()

        start_queries.assert_called_once_with("hot-classified", "cold-classified", lookup_df)
        metrics_server.shutdown.assert_called_once_with()
        metrics_server.server_close.assert_called_once_with()


class BatchSummaryBehaviorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("stream-job-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_write_batch_summary_counts_duplicates_without_collecting_event_ids(self):
        batch_schema = T.StructType(
            [
                T.StructField("device_id", T.StringType(), True),
                T.StructField("event_id", T.StringType(), True),
                T.StructField("replay_sent_at", T.StringType(), True),
                T.StructField("temperature_c", T.DoubleType(), True),
                T.StructField("invalid_reason", T.StringType(), True),
            ]
        )
        batch_df = self.spark.createDataFrame(
            [
                {
                    "device_id": "FR-0102",
                    "event_id": "evt-001",
                    "replay_sent_at": None,
                    "temperature_c": 9.0,
                    "invalid_reason": None,
                },
                {
                    "device_id": "FR-0102",
                    "event_id": "evt-001",
                    "replay_sent_at": None,
                    "temperature_c": 9.0,
                    "invalid_reason": None,
                },
                {
                    "device_id": "FR-0102",
                    "event_id": "evt-002",
                    "replay_sent_at": None,
                    "temperature_c": 4.5,
                    "invalid_reason": None,
                },
            ],
            schema=batch_schema,
        )
        lookup_df = self.spark.createDataFrame(
            [
                {
                    "device_id": "FR-0102",
                    "facility_id": "FAC-0002",
                    "facility_name": "Clinic B",
                    "district": "Udaipur",
                    "state": "Rajasthan",
                    "min_temp_c": 2.0,
                    "max_temp_c": 8.0,
                    "storage_type": "refrigerator",
                }
            ],
            schema=stream_job.LOOKUP_SCHEMA,
        )

        with patch.object(stream_job, "log_batch_summary") as mock_log:
            stream_job.write_batch_summary(batch_df, batch_id=12, lookup_df=lookup_df)

        mock_log.assert_called_once()
        summary = mock_log.call_args.args[0]
        self.assertEqual(summary["batch_id"], 12)
        self.assertEqual(summary["valid_count"], 3)
        self.assertEqual(summary["invalid_count"], 0)
        self.assertEqual(summary["deduplicated_count"], 1)
        self.assertEqual(summary["breach_count"], 1)
        self.assertEqual(summary["processed_count"], 2)
        self.assertIsNone(summary["avg_end_to_end_latency_seconds"])
        self.assertIsNone(summary["p95_end_to_end_latency_seconds"])

    def test_write_batch_summary_computes_latency_metrics_from_replay_sent_at(self):
        batch_schema = T.StructType(
            [
                T.StructField("device_id", T.StringType(), True),
                T.StructField("event_id", T.StringType(), True),
                T.StructField("replay_sent_at", T.StringType(), True),
                T.StructField("temperature_c", T.DoubleType(), True),
                T.StructField("invalid_reason", T.StringType(), True),
            ]
        )
        batch_df = self.spark.createDataFrame(
            [
                {
                    "device_id": "FR-0102",
                    "event_id": "evt-001",
                    "replay_sent_at": "2026-04-08T12:00:09Z",
                    "temperature_c": 4.0,
                    "invalid_reason": None,
                },
                {
                    "device_id": "FR-0102",
                    "event_id": "evt-002",
                    "replay_sent_at": "2026-04-08T12:00:08Z",
                    "temperature_c": 4.0,
                    "invalid_reason": None,
                },
            ],
            schema=batch_schema,
        )
        lookup_df = self.spark.createDataFrame(
            [
                {
                    "device_id": "FR-0102",
                    "facility_id": "FAC-0002",
                    "facility_name": "Clinic B",
                    "district": "Udaipur",
                    "state": "Rajasthan",
                    "min_temp_c": 2.0,
                    "max_temp_c": 8.0,
                    "storage_type": "refrigerator",
                }
            ],
            schema=stream_job.LOOKUP_SCHEMA,
        )

        with patch.object(stream_job, "log_batch_summary") as mock_log, patch.object(
            stream_job.F,
            "current_timestamp",
            return_value=stream_job.F.lit("2026-04-08T12:00:10Z").cast("timestamp"),
        ):
            stream_job.write_batch_summary(batch_df, batch_id=13, lookup_df=lookup_df)

        summary = mock_log.call_args.args[0]
        self.assertEqual(summary["processed_count"], 2)
        self.assertEqual(summary["avg_end_to_end_latency_seconds"], 1.5)
        self.assertEqual(summary["p95_end_to_end_latency_seconds"], 2.0)

    def test_write_batch_summary_updates_stream_metrics_registry(self):
        batch_schema = T.StructType(
            [
                T.StructField("device_id", T.StringType(), True),
                T.StructField("event_id", T.StringType(), True),
                T.StructField("replay_sent_at", T.StringType(), True),
                T.StructField("temperature_c", T.DoubleType(), True),
                T.StructField("invalid_reason", T.StringType(), True),
            ]
        )
        batch_df = self.spark.createDataFrame(
            [
                {
                    "device_id": "FR-0102",
                    "event_id": "evt-001",
                    "replay_sent_at": None,
                    "temperature_c": 4.0,
                    "invalid_reason": None,
                },
                {
                    "device_id": "FR-0102",
                    "event_id": "evt-001",
                    "replay_sent_at": None,
                    "temperature_c": 9.5,
                    "invalid_reason": None,
                },
                {
                    "device_id": "FR-0102",
                    "event_id": "evt-002",
                    "replay_sent_at": None,
                    "temperature_c": 4.0,
                    "invalid_reason": "corrupt_json",
                },
            ],
            schema=batch_schema,
        )
        lookup_df = self.spark.createDataFrame(
            [
                {
                    "device_id": "FR-0102",
                    "facility_id": "FAC-0002",
                    "facility_name": "Clinic B",
                    "district": "Udaipur",
                    "state": "Rajasthan",
                    "min_temp_c": 2.0,
                    "max_temp_c": 8.0,
                    "storage_type": "refrigerator",
                }
            ],
            schema=stream_job.LOOKUP_SCHEMA,
        )

        with patch.object(stream_job, "STREAM_METRICS_REGISTRY") as mock_registry, patch.object(
            stream_job.F,
            "current_timestamp",
            return_value=stream_job.F.lit("2026-04-08T12:00:10Z").cast("timestamp"),
        ):
            stream_job.write_batch_summary(batch_df, batch_id=14, lookup_df=lookup_df)

        mock_registry.update_batch_metrics.assert_called_once_with(
            batch_id=14,
            processed_count=1,
            invalid_count=1,
            deduplicated_count=1,
            breach_count=0,
            avg_latency_seconds=None,
            p95_latency_seconds=None,
            event_time_lag_p95_seconds=10.0,
            watermark_delay_seconds=610.0,
        )
        mock_registry.update_consumer_lag.assert_not_called()

    def test_build_breach_windows_aggregates_breach_rate_by_window(self):
        processed = self.spark.createDataFrame(
            [
                {
                    "event_ts": "2026-04-05T10:00:00Z",
                    "facility_id": "FAC-0002",
                    "facility_name": "Clinic B",
                    "breach_status": "safe",
                },
                {
                    "event_ts": "2026-04-05T10:01:00Z",
                    "facility_id": "FAC-0002",
                    "facility_name": "Clinic B",
                    "breach_status": "breach",
                },
                {
                    "event_ts": "2026-04-05T10:03:00Z",
                    "facility_id": "FAC-0002",
                    "facility_name": "Clinic B",
                    "breach_status": "breach",
                },
            ]
        ).withColumn("event_ts", F.to_timestamp("event_ts"))

        rows = (
            stream_job.build_breach_windows(processed)
            .select("total_records", "breach_records", "breach_rate")
            .collect()
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].total_records, 3)
        self.assertEqual(rows[0].breach_records, 2)
        self.assertEqual(rows[0].breach_rate, 2 / 3)


class ProcessedBatchWriteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("processed-batch-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_write_processed_batch_appends_parquet_and_updates_redis(self):
        batch_df = self.spark.createDataFrame(
            [
                {
                    "device_id": "FR-0102",
                    "event_id": "evt-001",
                    "event_time": "2026-04-08T12:00:00Z",
                    "temperature_c": 4.0,
                    "door_open": False,
                    "battery_pct": 88,
                    "facility_id": "FAC-0002",
                    "facility_name": "Clinic B",
                    "district": "Udaipur",
                    "state": "Rajasthan",
                    "storage_type": "refrigerator",
                    "min_temp_c": 2.0,
                    "max_temp_c": 8.0,
                    "breach_status": "safe",
                    "event_ts": "2026-04-08T12:00:00Z",
                    "kafka_ingest_ts": "2026-04-08T12:00:02Z",
                    "offset": 1,
                }
            ]
        ).withColumn("event_ts", F.to_timestamp("event_ts")).withColumn(
            "kafka_ingest_ts", F.to_timestamp("kafka_ingest_ts")
        )

        with tempfile.TemporaryDirectory() as temp_dir, tempfile.TemporaryDirectory() as breach_dir, patch.object(
            stream_job, "PROCESSED_OUTPUT_PATH", temp_dir
        ), patch.object(
            stream_job, "BREACH_WINDOW_OUTPUT_PATH", breach_dir
        ), patch.object(stream_job, "write_latest_state_to_redis") as mock_redis_write:
            stream_job.write_processed_batch(batch_df, batch_id=5)

            parquet_files = list(Path(temp_dir).rglob("*.parquet"))

        self.assertTrue(parquet_files)
        mock_redis_write.assert_called_once()
        self.assertEqual(mock_redis_write.call_args.args[1], 5)

    def test_write_processed_batch_also_writes_breach_windows(self):
        batch_df = self.spark.createDataFrame(
            [
                {
                    "device_id": "FR-0102",
                    "event_id": "evt-001",
                    "event_time": "2026-04-08T12:00:00Z",
                    "temperature_c": 9.0,
                    "door_open": False,
                    "battery_pct": 88,
                    "facility_id": "FAC-0002",
                    "facility_name": "Clinic B",
                    "district": "Udaipur",
                    "state": "Rajasthan",
                    "storage_type": "refrigerator",
                    "min_temp_c": 2.0,
                    "max_temp_c": 8.0,
                    "breach_status": "breach",
                    "event_ts": "2026-04-08T12:00:00Z",
                    "kafka_ingest_ts": "2026-04-08T12:00:02Z",
                    "offset": 1,
                }
            ]
        ).withColumn("event_ts", F.to_timestamp("event_ts")).withColumn(
            "kafka_ingest_ts", F.to_timestamp("kafka_ingest_ts")
        )

        with tempfile.TemporaryDirectory() as processed_dir, tempfile.TemporaryDirectory() as breach_dir, patch.object(
            stream_job, "PROCESSED_OUTPUT_PATH", processed_dir
        ), patch.object(
            stream_job, "BREACH_WINDOW_OUTPUT_PATH", breach_dir
        ), patch.object(stream_job, "write_latest_state_to_redis"):
            stream_job.write_processed_batch(batch_df, batch_id=6)

            breach_files = list(Path(breach_dir).rglob("*.json"))

        self.assertTrue(breach_files)

    def test_write_latest_state_to_redis_updates_ingest_to_redis_metric(self):
        batch_df = self.spark.createDataFrame(
            [
                {
                    "device_id": "FR-0102",
                    "event_id": "evt-001",
                    "event_time": "2026-04-08T12:00:00Z",
                    "replay_sent_at": "2026-04-08T12:00:06Z",
                    "temperature_c": 4.0,
                    "door_open": False,
                    "battery_pct": 88,
                    "facility_id": "FAC-0002",
                    "facility_name": "Clinic B",
                    "district": "Udaipur",
                    "state": "Rajasthan",
                    "storage_type": "refrigerator",
                    "min_temp_c": 2.0,
                    "max_temp_c": 8.0,
                    "breach_status": "safe",
                    "event_ts": "2026-04-08T12:00:00Z",
                    "kafka_ingest_ts": "2026-04-08T12:00:02Z",
                    "offset": 1,
                }
            ]
        ).withColumn("event_ts", F.to_timestamp("event_ts")).withColumn(
            "kafka_ingest_ts", F.to_timestamp("kafka_ingest_ts")
        )

        redis_client = Mock()
        redis_pipeline = Mock()
        redis_client.pipeline.return_value = redis_pipeline
        redis_done_ts = stream_job.datetime.fromisoformat("2026-04-08T12:00:10+00:00").timestamp()

        with patch.object(stream_job, "STREAM_METRICS_REGISTRY") as mock_registry, patch.object(
            stream_job, "redis"
        ) as mock_redis, patch.object(
            stream_job.time,
            "time",
            return_value=redis_done_ts,
        ):
            mock_redis.Redis.return_value = redis_client
            stream_job.write_latest_state_to_redis(batch_df, batch_id=15)

        mock_registry.update_redis_metrics.assert_called_once_with(
            ingest_to_redis_p95_seconds=4.0
        )


class ClassifiedBatchWriteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("classified-batch-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_write_classified_batch_writes_invalid_records_and_logs_summary(self):
        batch_schema = T.StructType(
            [
                T.StructField("raw_payload", T.StringType(), True),
                T.StructField("kafka_ingest_ts", T.TimestampType(), True),
                T.StructField("partition", T.IntegerType(), True),
                T.StructField("offset", T.LongType(), True),
                T.StructField("event_id", T.StringType(), True),
                T.StructField("device_id", T.StringType(), True),
                T.StructField("event_time", T.StringType(), True),
                T.StructField("replay_sent_at", T.StringType(), True),
                T.StructField("temperature_c", T.DoubleType(), True),
                T.StructField("door_open", T.BooleanType(), True),
                T.StructField("battery_pct", T.IntegerType(), True),
                T.StructField("location_lat", T.DoubleType(), True),
                T.StructField("location_lon", T.DoubleType(), True),
                T.StructField("invalid_reason", T.StringType(), True),
            ]
        )
        batch_df = self.spark.createDataFrame(
            [
                {
                    "raw_payload": '{"event_id":"evt-invalid"}',
                    "kafka_ingest_ts": None,
                    "partition": 0,
                    "offset": 1,
                    "event_id": "evt-invalid",
                    "device_id": None,
                    "event_time": None,
                    "replay_sent_at": None,
                    "temperature_c": None,
                    "door_open": None,
                    "battery_pct": None,
                    "location_lat": None,
                    "location_lon": None,
                    "invalid_reason": "missing_device_id",
                }
            ],
            schema=batch_schema,
        )

        with tempfile.TemporaryDirectory() as invalid_dir, patch.object(
            stream_job, "INVALID_OUTPUT_PATH", invalid_dir
        ), patch.object(stream_job, "write_batch_summary") as mock_summary:
            lookup_df = self.spark.createDataFrame(
                [
                    {
                        "device_id": "FR-0102",
                        "facility_id": "FAC-0002",
                        "facility_name": "Clinic B",
                        "district": "Udaipur",
                        "state": "Rajasthan",
                        "min_temp_c": 2.0,
                        "max_temp_c": 8.0,
                        "storage_type": "refrigerator",
                    }
                ],
                schema=stream_job.LOOKUP_SCHEMA,
            )
            stream_job.write_classified_batch(batch_df, batch_id=4, lookup_df=lookup_df)
            invalid_files = list(Path(invalid_dir).rglob("*.json"))

        self.assertTrue(invalid_files)
        mock_summary.assert_called_once()
        self.assertEqual(mock_summary.call_args.args[1], 4)


class OptimizedBatchWriteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("optimized-batch-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_write_optimized_batch_writes_invalid_processed_and_breach_outputs(self):
        lookup_df = self.spark.createDataFrame(
            [
                {
                    "device_id": "FR-0102",
                    "facility_id": "FAC-0002",
                    "facility_name": "Clinic B",
                    "district": "Udaipur",
                    "state": "Rajasthan",
                    "min_temp_c": 2.0,
                    "max_temp_c": 8.0,
                    "storage_type": "refrigerator",
                }
            ],
            schema=stream_job.LOOKUP_SCHEMA,
        )
        batch_df = self.spark.createDataFrame(
            [
                {
                    "raw_payload": '{"event_id":"evt-001"}',
                    "kafka_ingest_ts": "2026-04-08T12:00:02.500Z",
                    "partition": 0,
                    "offset": 1,
                    "event_id": "evt-001",
                    "device_id": "FR-0102",
                    "event_time": "2026-04-08T12:00:00.000Z",
                    "replay_sent_at": "2026-04-08T12:00:06Z",
                    "temperature_c": 4.5,
                    "door_open": False,
                    "battery_pct": 88,
                    "location_lat": 24.5854,
                    "location_lon": 73.7125,
                    "invalid_reason": None,
                    "event_ts": "2026-04-08T12:00:00.000Z",
                },
                {
                    "raw_payload": '{"event_id":"evt-001"}',
                    "kafka_ingest_ts": "2026-04-08T12:00:03.500Z",
                    "partition": 0,
                    "offset": 2,
                    "event_id": "evt-001",
                    "device_id": "FR-0102",
                    "event_time": "2026-04-08T12:00:00.000Z",
                    "replay_sent_at": "2026-04-08T12:00:06Z",
                    "temperature_c": 4.5,
                    "door_open": False,
                    "battery_pct": 88,
                    "location_lat": 24.5854,
                    "location_lon": 73.7125,
                    "invalid_reason": None,
                    "event_ts": "2026-04-08T12:00:00.000Z",
                },
                {
                    "raw_payload": '{"event_id":"evt-002"}',
                    "kafka_ingest_ts": "2026-04-08T12:00:04.500Z",
                    "partition": 0,
                    "offset": 3,
                    "event_id": "evt-002",
                    "device_id": "FR-9999",
                    "event_time": "2026-04-08T12:00:01.000Z",
                    "replay_sent_at": "2026-04-08T12:00:07Z",
                    "temperature_c": 4.5,
                    "door_open": False,
                    "battery_pct": 88,
                    "location_lat": 24.5854,
                    "location_lon": 73.7125,
                    "invalid_reason": None,
                    "event_ts": "2026-04-08T12:00:01.000Z",
                },
                {
                    "raw_payload": '{"event_id":"evt-003"}',
                    "kafka_ingest_ts": "2026-04-08T12:00:05.500Z",
                    "partition": 0,
                    "offset": 4,
                    "event_id": "evt-003",
                    "device_id": None,
                    "event_time": "2026-04-08T12:00:02.000Z",
                    "replay_sent_at": None,
                    "temperature_c": 4.5,
                    "door_open": False,
                    "battery_pct": 88,
                    "location_lat": 24.5854,
                    "location_lon": 73.7125,
                    "invalid_reason": "missing_device_id",
                    "event_ts": "2026-04-08T12:00:02.000Z",
                },
            ]
        ).withColumn("kafka_ingest_ts", F.to_timestamp("kafka_ingest_ts")).withColumn(
            "event_ts", F.to_timestamp("event_ts")
        )

        with tempfile.TemporaryDirectory() as processed_dir, tempfile.TemporaryDirectory() as invalid_dir, tempfile.TemporaryDirectory() as breach_dir, patch.object(
            stream_job, "PROCESSED_OUTPUT_PATH", processed_dir
        ), patch.object(
            stream_job, "INVALID_OUTPUT_PATH", invalid_dir
        ), patch.object(
            stream_job, "BREACH_WINDOW_OUTPUT_PATH", breach_dir
        ), patch.object(
            stream_job, "write_latest_state_to_redis"
        ) as mock_redis_write, patch.object(
            stream_job, "STREAM_METRICS_REGISTRY"
        ) as mock_registry, patch.object(
            stream_job, "log_batch_summary"
        ) as mock_log, patch.object(
            stream_job.F,
            "current_timestamp",
            return_value=stream_job.F.lit("2026-04-08T12:00:10Z").cast("timestamp"),
        ):
            stream_job.write_optimized_batch(batch_df, batch_id=21, lookup_df=lookup_df)
            processed_count = self.spark.read.parquet(processed_dir).count()
            processed_df = self.spark.read.parquet(processed_dir)
            processed_schema = processed_df.schema
            processed_delay = processed_df.select("ingest_delay_seconds").first()[0]
            invalid_count = self.spark.read.json(invalid_dir).count()
            breach_count = self.spark.read.json(breach_dir).count()

        self.assertEqual(processed_count, 1)
        self.assertEqual(processed_schema["ingest_delay_seconds"].dataType, T.DoubleType())
        self.assertAlmostEqual(processed_delay, 2.5, places=6)
        self.assertEqual(invalid_count, 2)
        self.assertGreaterEqual(breach_count, 1)
        mock_redis_write.assert_called_once()
        mock_registry.update_last_seen_offsets.assert_called_once_with({0: 4})
        mock_registry.update_batch_metrics.assert_called_once()
        self.assertEqual(mock_log.call_args.args[0]["processed_count"], 1)


class StreamTransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("stream-transform-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.lookup_df = cls.spark.createDataFrame(
            [
                {
                    "device_id": "FR-0102",
                    "facility_id": "FAC-0002",
                    "facility_name": "Clinic B",
                    "district": "Udaipur",
                    "state": "Rajasthan",
                    "min_temp_c": 2.0,
                    "max_temp_c": 8.0,
                    "storage_type": "refrigerator",
                }
            ],
            schema=stream_job.LOOKUP_SCHEMA,
        )
        cls.input_schema = T.StructType(
            [
                T.StructField("raw_payload", T.StringType(), True),
                T.StructField("kafka_ingest_ts", T.StringType(), True),
                T.StructField("partition", T.IntegerType(), True),
                T.StructField("offset", T.LongType(), True),
                T.StructField("event_id", T.StringType(), True),
                T.StructField("device_id", T.StringType(), True),
                T.StructField("event_time", T.StringType(), True),
                T.StructField("replay_sent_at", T.StringType(), True),
                T.StructField("event_ts", T.StringType(), True),
                T.StructField("temperature_c", T.DoubleType(), True),
                T.StructField("door_open", T.BooleanType(), True),
                T.StructField("battery_pct", T.IntegerType(), True),
                T.StructField("location_lat", T.DoubleType(), True),
                T.StructField("location_lon", T.DoubleType(), True),
                T.StructField("invalid_reason", T.StringType(), True),
            ]
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.input_dir = Path(self.temp_dir.name)

    def tearDown(self):
        for query in list(self.spark.streams.active):
            query.stop()
        self.temp_dir.cleanup()

    def _build_classified_stream(self):
        return (
            self.spark.readStream.schema(self.input_schema)
            .option("maxFilesPerTrigger", 1)
            .json(str(self.input_dir))
            .withColumn("kafka_ingest_ts", F.to_timestamp("kafka_ingest_ts"))
            .withColumn("event_ts", F.to_timestamp("event_ts"))
        )

    def _append_events(self, events):
        self.spark.createDataFrame(events, schema=self.input_schema).coalesce(1).write.mode("append").json(
            str(self.input_dir)
        )

    def test_build_output_streams_deduplicates_repeated_event_ids(self):
        classified = self._build_classified_stream()
        processed, _invalid = stream_job.build_output_streams(
            classified,
            self.lookup_df,
            watermark_delay="10 minutes",
        )

        query_name = f"processed_duplicates_{uuid.uuid4().hex}"
        query = processed.writeStream.format("memory").queryName(query_name).outputMode("append").start()

        try:
            self._append_events(
                [
                    {
                        "raw_payload": '{"event_id":"evt-001"}',
                        "kafka_ingest_ts": "2026-04-05T10:00:05Z",
                        "partition": 0,
                        "offset": 1,
                        "event_id": "evt-001",
                        "device_id": "FR-0102",
                        "event_time": "2026-04-05T10:00:00Z",
                        "replay_sent_at": "2026-04-08T12:00:00Z",
                        "event_ts": "2026-04-05T10:00:00Z",
                        "temperature_c": 4.5,
                        "door_open": False,
                        "battery_pct": 88,
                        "location_lat": 24.5854,
                        "location_lon": 73.7125,
                        "invalid_reason": None,
                    },
                    {
                        "raw_payload": '{"event_id":"evt-001"}',
                        "kafka_ingest_ts": "2026-04-05T10:00:06Z",
                        "partition": 0,
                        "offset": 2,
                        "event_id": "evt-001",
                        "device_id": "FR-0102",
                        "event_time": "2026-04-05T10:00:00Z",
                        "replay_sent_at": "2026-04-08T12:00:00Z",
                        "event_ts": "2026-04-05T10:00:00Z",
                        "temperature_c": 4.5,
                        "door_open": False,
                        "battery_pct": 88,
                        "location_lat": 24.5854,
                        "location_lon": 73.7125,
                        "invalid_reason": None,
                    },
                    {
                        "raw_payload": '{"event_id":"evt-002"}',
                        "kafka_ingest_ts": "2026-04-05T10:00:07Z",
                        "partition": 0,
                        "offset": 3,
                        "event_id": "evt-002",
                        "device_id": "FR-0102",
                        "event_time": "2026-04-05T10:00:01Z",
                        "replay_sent_at": "2026-04-08T12:00:01Z",
                        "event_ts": "2026-04-05T10:00:01Z",
                        "temperature_c": 4.6,
                        "door_open": False,
                        "battery_pct": 87,
                        "location_lat": 24.5854,
                        "location_lon": 73.7125,
                        "invalid_reason": None,
                    },
                ]
            )

            query.processAllAvailable()

            rows = self.spark.sql(f"SELECT event_id FROM {query_name} ORDER BY event_id").collect()
            self.assertEqual([row.event_id for row in rows], ["evt-001", "evt-002"])
        finally:
            query.stop()

    def test_build_output_streams_drops_events_older_than_watermark(self):
        classified = self._build_classified_stream()
        processed, _invalid = stream_job.build_output_streams(
            classified,
            self.lookup_df,
            watermark_delay="1 minute",
        )

        query_name = f"processed_late_{uuid.uuid4().hex}"
        query = processed.writeStream.format("memory").queryName(query_name).outputMode("append").start()

        try:
            self._append_events(
                [
                    {
                        "raw_payload": '{"event_id":"evt-current-1"}',
                        "kafka_ingest_ts": "2026-04-05T10:05:05Z",
                        "partition": 0,
                        "offset": 1,
                        "event_id": "evt-current-1",
                        "device_id": "FR-0102",
                        "event_time": "2026-04-05T10:05:00Z",
                        "replay_sent_at": "2026-04-08T12:05:00Z",
                        "event_ts": "2026-04-05T10:05:00Z",
                        "temperature_c": 4.2,
                        "door_open": False,
                        "battery_pct": 90,
                        "location_lat": 24.5854,
                        "location_lon": 73.7125,
                        "invalid_reason": None,
                    },
                    {
                        "raw_payload": '{"event_id":"evt-current-2"}',
                        "kafka_ingest_ts": "2026-04-05T10:05:15Z",
                        "partition": 0,
                        "offset": 2,
                        "event_id": "evt-current-2",
                        "device_id": "FR-0102",
                        "event_time": "2026-04-05T10:05:10Z",
                        "replay_sent_at": "2026-04-08T12:05:10Z",
                        "event_ts": "2026-04-05T10:05:10Z",
                        "temperature_c": 4.3,
                        "door_open": False,
                        "battery_pct": 89,
                        "location_lat": 24.5854,
                        "location_lon": 73.7125,
                        "invalid_reason": None,
                    },
                ]
            )
            query.processAllAvailable()

            self._append_events(
                [
                    {
                        "raw_payload": '{"event_id":"evt-late"}',
                        "kafka_ingest_ts": "2026-04-05T10:06:00Z",
                        "partition": 0,
                        "offset": 3,
                        "event_id": "evt-late",
                        "device_id": "FR-0102",
                        "event_time": "2026-04-05T10:03:30Z",
                        "replay_sent_at": "2026-04-08T12:03:30Z",
                        "event_ts": "2026-04-05T10:03:30Z",
                        "temperature_c": 4.1,
                        "door_open": False,
                        "battery_pct": 88,
                        "location_lat": 24.5854,
                        "location_lon": 73.7125,
                        "invalid_reason": None,
                    },
                    {
                        "raw_payload": '{"event_id":"evt-on-time"}',
                        "kafka_ingest_ts": "2026-04-05T10:06:05Z",
                        "partition": 0,
                        "offset": 4,
                        "event_id": "evt-on-time",
                        "device_id": "FR-0102",
                        "event_time": "2026-04-05T10:04:30Z",
                        "replay_sent_at": "2026-04-08T12:04:30Z",
                        "event_ts": "2026-04-05T10:04:30Z",
                        "temperature_c": 4.4,
                        "door_open": False,
                        "battery_pct": 87,
                        "location_lat": 24.5854,
                        "location_lon": 73.7125,
                        "invalid_reason": None,
                    },
                ]
            )
            query.processAllAvailable()

            rows = self.spark.sql(f"SELECT event_id FROM {query_name} ORDER BY event_id").collect()
            self.assertEqual(
                [row.event_id for row in rows],
                ["evt-current-1", "evt-current-2", "evt-on-time"],
            )
        finally:
            query.stop()

    def test_breach_window_stream_excludes_late_events_from_window_summary(self):
        classified = self._build_classified_stream()
        processed, _invalid = stream_job.build_output_streams(
            classified,
            self.lookup_df,
            watermark_delay="1 minute",
        )
        breach_windows = stream_job.build_breach_windows(processed)

        query_name = f"breach_windows_{uuid.uuid4().hex}"
        query = (
            breach_windows.writeStream.format("memory")
            .queryName(query_name)
            .outputMode("append")
            .start()
        )

        try:
            self._append_events(
                [
                    {
                        "raw_payload": '{"event_id":"evt-current-safe"}',
                        "kafka_ingest_ts": "2026-04-05T10:05:05Z",
                        "partition": 0,
                        "offset": 1,
                        "event_id": "evt-current-safe",
                        "device_id": "FR-0102",
                        "event_time": "2026-04-05T10:05:00Z",
                        "replay_sent_at": "2026-04-08T12:05:00Z",
                        "event_ts": "2026-04-05T10:05:00Z",
                        "temperature_c": 4.2,
                        "door_open": False,
                        "battery_pct": 90,
                        "location_lat": 24.5854,
                        "location_lon": 73.7125,
                        "invalid_reason": None,
                    },
                    {
                        "raw_payload": '{"event_id":"evt-current-breach"}',
                        "kafka_ingest_ts": "2026-04-05T10:05:15Z",
                        "partition": 0,
                        "offset": 2,
                        "event_id": "evt-current-breach",
                        "device_id": "FR-0102",
                        "event_time": "2026-04-05T10:05:10Z",
                        "replay_sent_at": "2026-04-08T12:05:10Z",
                        "event_ts": "2026-04-05T10:05:10Z",
                        "temperature_c": 9.2,
                        "door_open": False,
                        "battery_pct": 89,
                        "location_lat": 24.5854,
                        "location_lon": 73.7125,
                        "invalid_reason": None,
                    },
                ]
            )
            query.processAllAvailable()

            self._append_events(
                [
                    {
                        "raw_payload": '{"event_id":"evt-late-breach"}',
                        "kafka_ingest_ts": "2026-04-05T10:06:00Z",
                        "partition": 0,
                        "offset": 3,
                        "event_id": "evt-late-breach",
                        "device_id": "FR-0102",
                        "event_time": "2026-04-05T10:03:30Z",
                        "replay_sent_at": "2026-04-08T12:03:30Z",
                        "event_ts": "2026-04-05T10:03:30Z",
                        "temperature_c": 9.5,
                        "door_open": False,
                        "battery_pct": 88,
                        "location_lat": 24.5854,
                        "location_lon": 73.7125,
                        "invalid_reason": None,
                    },
                    {
                        "raw_payload": '{"event_id":"evt-on-time-breach"}',
                        "kafka_ingest_ts": "2026-04-05T10:06:05Z",
                        "partition": 0,
                        "offset": 4,
                        "event_id": "evt-on-time-breach",
                        "device_id": "FR-0102",
                        "event_time": "2026-04-05T10:04:30Z",
                        "replay_sent_at": "2026-04-08T12:04:30Z",
                        "event_ts": "2026-04-05T10:04:30Z",
                        "temperature_c": 9.1,
                        "door_open": False,
                        "battery_pct": 87,
                        "location_lat": 24.5854,
                        "location_lon": 73.7125,
                        "invalid_reason": None,
                    },
                    {
                        "raw_payload": '{"event_id":"evt-watermark-advance"}',
                        "kafka_ingest_ts": "2026-04-05T10:11:35Z",
                        "partition": 0,
                        "offset": 5,
                        "event_id": "evt-watermark-advance",
                        "device_id": "FR-0102",
                        "event_time": "2026-04-05T10:11:30Z",
                        "replay_sent_at": "2026-04-08T12:11:30Z",
                        "event_ts": "2026-04-05T10:11:30Z",
                        "temperature_c": 4.0,
                        "door_open": False,
                        "battery_pct": 86,
                        "location_lat": 24.5854,
                        "location_lon": 73.7125,
                        "invalid_reason": None,
                    },
                ]
            )
            query.processAllAvailable()

            rows = (
                self.spark.sql(
                    "SELECT facility_id, total_records, breach_records, breach_rate "
                    f"FROM {query_name} ORDER BY total_records, breach_records"
                ).collect()
            )

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0].facility_id, "FAC-0002")
            self.assertEqual(rows[0].total_records, 1)
            self.assertEqual(rows[0].breach_records, 1)
            self.assertEqual(rows[0].breach_rate, 1.0)
            self.assertEqual(rows[1].facility_id, "FAC-0002")
            self.assertEqual(rows[1].total_records, 2)
            self.assertEqual(rows[1].breach_records, 1)
            self.assertEqual(rows[1].breach_rate, 0.5)
        finally:
            query.stop()


if __name__ == "__main__":
    unittest.main()
