import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "aws_baseline_metrics.py"
)
SPEC = importlib.util.spec_from_file_location("aws_baseline_metrics", MODULE_PATH)
aws_baseline_metrics = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(aws_baseline_metrics)


class AwsBaselineMetricsTests(unittest.TestCase):
    def test_extract_metrics_prefers_stream_metrics_endpoint_over_log_summaries(self):
        replay_logs = """
2026-04-08T12:00:00Z INFO Loaded 75 events
2026-04-08T12:00:15Z INFO Replay complete: 75 events in 15.0s  avg 5.0 eps
""".strip()
        stream_logs = ""
        stream_metrics_payload = """
vacciguard_stream_processed_events_total 52
vacciguard_stream_invalid_events_total 6
vacciguard_stream_deduplicated_events_total 9
vacciguard_stream_breach_events_total 21
vacciguard_stream_latest_batch_avg_latency_seconds 1.5
vacciguard_stream_latest_batch_p95_latency_seconds 2.75
""".strip()

        metrics = aws_baseline_metrics.extract_metrics(
            replay_logs,
            stream_logs,
            stream_metrics_payload=stream_metrics_payload,
        )

        self.assertEqual(metrics["input_events"], 75)
        self.assertEqual(metrics["throughput_eps"], 5.0)
        self.assertEqual(metrics["processed_events"], 52)
        self.assertEqual(metrics["invalid_events"], 6)
        self.assertEqual(metrics["deduplicated_events"], 9)
        self.assertEqual(metrics["breach_events"], 21)
        self.assertEqual(metrics["avg_end_to_end_latency_seconds"], 1.5)
        self.assertEqual(metrics["p95_end_to_end_latency_seconds"], 2.75)
        self.assertEqual(metrics["stream_metrics_source"], "metrics_endpoint")

    def test_extract_metrics_uses_log_aggregate_latency_with_endpoint_counts(self):
        replay_logs = """
2026-04-08T12:00:00Z INFO Loaded 75 events
2026-04-08T12:00:15Z INFO Replay complete: 75 events in 15.0s  avg 5.0 eps
""".strip()
        stream_logs = """
2026-04-08T12:00:20Z INFO Batch 0 summary valid=45 invalid=2 deduplicated=5 breach=3 processed=40 avg_e2e_latency_s=1.25 p95_e2e_latency_s=2.50
2026-04-08T12:00:30Z INFO Batch 1 summary valid=35 invalid=1 deduplicated=2 breach=4 processed=30 avg_e2e_latency_s=2.00 p95_e2e_latency_s=3.75
""".strip()
        stream_metrics_payload = """
vacciguard_stream_processed_events_total 52
vacciguard_stream_invalid_events_total 6
vacciguard_stream_deduplicated_events_total 9
vacciguard_stream_breach_events_total 21
vacciguard_stream_latest_batch_avg_latency_seconds 9.5
vacciguard_stream_latest_batch_p95_latency_seconds 11.0
""".strip()

        metrics = aws_baseline_metrics.extract_metrics(
            replay_logs,
            stream_logs,
            stream_metrics_payload=stream_metrics_payload,
        )

        self.assertEqual(metrics["processed_events"], 52)
        self.assertEqual(metrics["invalid_events"], 6)
        self.assertEqual(metrics["deduplicated_events"], 9)
        self.assertEqual(metrics["breach_events"], 21)
        self.assertEqual(metrics["avg_end_to_end_latency_seconds"], 1.57)
        self.assertEqual(metrics["p95_end_to_end_latency_seconds"], 3.75)

    def test_extract_metrics_derives_quality_and_runtime_metrics(self):
        replay_logs = """
2026-04-08T12:00:00Z INFO Loaded 100 events
2026-04-08T12:00:15Z INFO Replay complete: 100 events in 10.0s  avg 10.0 eps
""".strip()
        stream_logs = ""
        stream_metrics_payload = """
vacciguard_stream_processed_events_total 80
vacciguard_stream_invalid_events_total 10
vacciguard_stream_deduplicated_events_total 5
vacciguard_stream_breach_events_total 8
vacciguard_stream_latest_batch_avg_latency_seconds 1.5
vacciguard_stream_latest_batch_p95_latency_seconds 2.75
vacciguard_stream_latest_batch_event_time_lag_p95_seconds 12.25
vacciguard_stream_latest_batch_ingest_to_redis_p95_seconds 4.5
vacciguard_stream_watermark_delay_seconds 601.0
vacciguard_stream_consumer_lag_records 17
""".strip()

        metrics = aws_baseline_metrics.extract_metrics(
            replay_logs,
            stream_logs,
            stream_metrics_payload=stream_metrics_payload,
        )

        self.assertEqual(metrics["event_time_lag_p95_seconds"], 12.25)
        self.assertEqual(metrics["ingest_to_redis_p95_seconds"], 4.5)
        self.assertEqual(metrics["watermark_delay_seconds"], 601.0)
        self.assertEqual(metrics["consumer_lag_records"], 17)
        self.assertEqual(metrics["invalid_rate_pct"], 10.0)
        self.assertEqual(metrics["deduplication_rate_pct"], 5.0)
        self.assertEqual(metrics["processed_rate_pct"], 80.0)

    def test_extract_metrics_parses_replay_and_stream_summaries(self):
        replay_logs = """
2026-04-08T12:00:00Z INFO Loaded 75 events
2026-04-08T12:00:15Z INFO Replay complete: 75 events in 15.0s  avg 5.0 eps
""".strip()
        stream_logs = """
2026-04-08T12:00:20Z INFO Batch 0 summary valid=45 invalid=2 deduplicated=5 breach=3 processed=40 avg_e2e_latency_s=1.25 p95_e2e_latency_s=2.50
""".strip()

        metrics = aws_baseline_metrics.extract_metrics(replay_logs, stream_logs)

        self.assertEqual(metrics["input_events"], 75)
        self.assertEqual(metrics["throughput_eps"], 5.0)
        self.assertEqual(metrics["processed_events"], 40)
        self.assertEqual(metrics["invalid_events"], 2)
        self.assertEqual(metrics["deduplicated_events"], 5)
        self.assertEqual(metrics["breach_events"], 3)
        self.assertEqual(metrics["avg_end_to_end_latency_seconds"], 1.25)
        self.assertEqual(metrics["p95_end_to_end_latency_seconds"], 2.5)
        self.assertEqual(metrics["stream_metrics_source"], "stream_logs")

    def test_render_markdown_table_marks_unrun_metrics_explicitly(self):
        table = aws_baseline_metrics.render_markdown_table(
            {
                "avg_end_to_end_latency_seconds": 1.25,
                "p95_end_to_end_latency_seconds": 2.5,
                "throughput_eps": 5.0,
                "input_events": 75,
                "processed_events": 40,
                "invalid_events": 2,
                "deduplicated_events": 5,
                "breach_events": 3,
            }
        )

        self.assertIn("| Avg end-to-end latency | 1.25 s |", table)
        self.assertIn("| P95 latency | 2.50 s |", table)
        self.assertIn("| Throughput | 5.00 events/s |", table)
        self.assertIn("| 10x spike success/failure | Not run |", table)
        self.assertIn("| Recovery time after failure | Not run |", table)
        self.assertIn("| Cost per run | Not run |", table)
        self.assertIn("| Cost per GB processed | Not run |", table)

    def test_render_markdown_table_preserves_workload_metadata(self):
        table = aws_baseline_metrics.render_markdown_table(
            {
                "scenario": "normal",
                "workload_family_version": "evaluation-workload-v1",
                "configured_events_per_second": 10.0,
                "avg_end_to_end_latency_seconds": 1.25,
                "p95_end_to_end_latency_seconds": 2.5,
            }
        )

        self.assertIn("| Workload family version | evaluation-workload-v1 |", table)
        self.assertIn("| Scenario | normal |", table)
        self.assertIn("| Configured replay rate | 10.00 events/s |", table)

    def test_render_markdown_table_includes_artifact_counts_and_metric_source(self):
        table = aws_baseline_metrics.render_markdown_table(
            {
                "stream_metrics_source": "metrics_endpoint",
                "processed_output_objects": 52,
                "invalid_output_objects": 6,
                "breach_window_output_objects": 52,
            }
        )

        self.assertIn("| Stream metrics source | metrics_endpoint |", table)
        self.assertIn("| Processed output objects | 52 |", table)
        self.assertIn("| Invalid output objects | 6 |", table)
        self.assertIn("| Breach window output objects | 52 |", table)

    def test_render_markdown_table_includes_runtime_and_success_metrics(self):
        table = aws_baseline_metrics.render_markdown_table(
            {
                "event_time_lag_p95_seconds": 12.25,
                "ingest_to_redis_p95_seconds": 4.5,
                "watermark_delay_seconds": 601.0,
                "consumer_lag_records": 17,
                "invalid_rate_pct": 10.0,
                "deduplication_rate_pct": 5.0,
                "processed_rate_pct": 80.0,
                "pipeline_success": True,
                "controller_job_success": True,
                "replay_job_success": True,
            }
        )

        self.assertIn("| Event-time lag P95 | 12.25 s |", table)
        self.assertIn("| Ingest-to-Redis P95 | 4.50 s |", table)
        self.assertIn("| Watermark delay | 601.00 s |", table)
        self.assertIn("| Consumer lag | 17 records |", table)
        self.assertIn("| Invalid rate | 10.00% |", table)
        self.assertIn("| Deduplication rate | 5.00% |", table)
        self.assertIn("| Processed rate | 80.00% |", table)
        self.assertIn("| Pipeline success | True |", table)
        self.assertIn("| Controller job success | True |", table)
        self.assertIn("| Replay job success | True |", table)
