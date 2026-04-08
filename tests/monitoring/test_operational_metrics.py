import importlib.util
import urllib.error
import urllib.request
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

        initial_rendered = registry.render_prometheus()

        self.assertIn("vacciguard_stream_latest_batch_id -1", initial_rendered)
        self.assertIn("vacciguard_stream_latest_batch_timestamp_seconds 0.0", initial_rendered)
        self.assertIn("vacciguard_stream_processed_events_total 0", initial_rendered)
        self.assertIn("vacciguard_stream_invalid_events_total 0", initial_rendered)
        self.assertIn("vacciguard_stream_deduplicated_events_total 0", initial_rendered)
        self.assertIn("vacciguard_stream_breach_events_total 0", initial_rendered)
        self.assertIn("vacciguard_stream_latest_batch_avg_latency_seconds 0.0", initial_rendered)
        self.assertIn("vacciguard_stream_latest_batch_p95_latency_seconds 0.0", initial_rendered)
        self.assertTrue(initial_rendered.endswith("\n"))

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


class StreamMetricsHttpHandlerTests(unittest.TestCase):
    def test_metrics_http_payload_returns_prometheus_text_for_registry(self):
        registry = stream_job.StreamMetricsRegistry()
        registry.update_batch_metrics(
            batch_id=1,
            processed_count=2,
            invalid_count=0,
            deduplicated_count=0,
            breach_count=0,
            avg_latency_seconds=0.5,
            p95_latency_seconds=0.75,
        )

        payload = stream_job.metrics_http_payload(registry)

        self.assertEqual(payload, registry.render_prometheus())
        self.assertIn("vacciguard_stream_processed_events_total 2", payload)
        self.assertIn("vacciguard_stream_latest_batch_id 1", payload)

    def test_metrics_server_serves_metrics_and_404s_other_paths(self):
        registry = stream_job.StreamMetricsRegistry()
        registry.update_batch_metrics(
            batch_id=9,
            processed_count=5,
            invalid_count=1,
            deduplicated_count=1,
            breach_count=1,
            avg_latency_seconds=1.5,
            p95_latency_seconds=2.0,
        )

        server = stream_job.start_metrics_server(0, registry)
        host, port = server.server_address
        metrics_url = f"http://127.0.0.1:{port}/metrics"
        missing_url = f"http://127.0.0.1:{port}/missing"

        try:
            with urllib.request.urlopen(metrics_url, timeout=2) as response:
                payload = response.read().decode("utf-8")
                content_type = response.headers.get("Content-Type")

            self.assertEqual(content_type, "text/plain; version=0.0.4")
            self.assertIn("vacciguard_stream_latest_batch_id 9", payload)
            self.assertIn("vacciguard_stream_processed_events_total 5", payload)

            with self.assertRaises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(missing_url, timeout=2)

            self.assertEqual(exc_info.exception.code, 404)
            exc_info.exception.close()
        finally:
            server.shutdown()
            server.server_close()
