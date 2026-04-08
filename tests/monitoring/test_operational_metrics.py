import importlib.util
from pathlib import Path
import unittest


MODULE_PATH = Path(__file__).resolve().parents[2] / "services" / "stream-processor" / "job.py"
SPEC = importlib.util.spec_from_file_location("stream_job", MODULE_PATH)
stream_job = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(stream_job)


class StreamOperationalMetricsTests(unittest.TestCase):
    def test_stream_metrics_render_prometheus_text(self):
        registry = stream_job.StreamMetricsRegistry()

        registry.update_batch_metrics(
            batch_id=6,
            processed_count=4,
            invalid_count=1,
            deduplicated_count=1,
            breach_count=1,
            avg_latency_seconds=0.5,
            p95_latency_seconds=1.25,
        )
        registry.update_batch_metrics(
            batch_id=7,
            processed_count=11,
            invalid_count=2,
            deduplicated_count=3,
            breach_count=4,
            avg_latency_seconds=1.25,
            p95_latency_seconds=2.75,
        )

        rendered = registry.render_prometheus()

        self.assertIn("vacciguard_stream_latest_batch_id 7", rendered)
        self.assertIn("vacciguard_stream_processed_events_total 15", rendered)
        self.assertIn("vacciguard_stream_invalid_events_total 3", rendered)
        self.assertIn("vacciguard_stream_deduplicated_events_total 4", rendered)
        self.assertIn("vacciguard_stream_breach_events_total 5", rendered)
        self.assertIn("vacciguard_stream_latest_batch_avg_latency_seconds 1.25", rendered)
        self.assertIn("vacciguard_stream_latest_batch_p95_latency_seconds 2.75", rendered)
        self.assertTrue(rendered.endswith("\n"))
