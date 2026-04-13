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
