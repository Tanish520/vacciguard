import importlib.util
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, Optional
import unittest
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, relative_path: str, env: Optional[Dict[str, str]] = None):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None

    original_env = {}
    if env:
        for key, value in env.items():
            original_env[key] = os.environ.get(key)
            os.environ[key] = value

    try:
        spec.loader.exec_module(module)
    finally:
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    return module


stream_job = load_module("stream_job", "services/stream-processor/job.py")
replay_producer = load_module(
    "replay_producer",
    "services/replay-producer/producer.py",
    env={
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "WORKLOAD_FILE": "/tmp/workload.ndjson",
    },
)


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


class ReplayOperationalMetricsTests(unittest.TestCase):
    def test_replay_metrics_render_prometheus_text(self):
        registry = replay_producer.ReplayMetricsRegistry()

        initial_rendered = registry.render_prometheus()

        self.assertIn("vacciguard_replay_loaded_events 0", initial_rendered)
        self.assertIn("vacciguard_replay_sent_events_total 0", initial_rendered)
        self.assertIn("vacciguard_replay_configured_rate_events_per_second 0.0", initial_rendered)
        self.assertIn("vacciguard_replay_duration_seconds 0.0", initial_rendered)
        self.assertIn("vacciguard_replay_completion_status 0", initial_rendered)
        self.assertTrue(initial_rendered.endswith("\n"))

        registry.record_loaded_events(4)
        registry.record_sent_event()
        registry.record_sent_event()
        registry.record_completion(duration_seconds=2.5, configured_events_per_second=5.0)

        rendered = registry.render_prometheus()

        self.assertIn("vacciguard_replay_loaded_events 4", rendered)
        self.assertIn("vacciguard_replay_sent_events_total 2", rendered)
        self.assertIn("vacciguard_replay_configured_rate_events_per_second 5.0", rendered)
        self.assertIn("vacciguard_replay_duration_seconds 2.5", rendered)
        self.assertIn("vacciguard_replay_completion_status 1", rendered)
        self.assertTrue(rendered.endswith("\n"))

    def test_replay_metrics_begin_run_resets_terminal_values_and_sets_rate(self):
        registry = replay_producer.ReplayMetricsRegistry()
        registry.record_loaded_events(4)
        registry.record_sent_event()
        registry.record_completion(duration_seconds=2.5, configured_events_per_second=5.0)

        registry.begin_run(configured_events_per_second=8.0)
        rendered = registry.render_prometheus()

        self.assertIn("vacciguard_replay_loaded_events 4", rendered)
        self.assertIn("vacciguard_replay_sent_events_total 1", rendered)
        self.assertIn("vacciguard_replay_configured_rate_events_per_second 8.0", rendered)
        self.assertIn("vacciguard_replay_duration_seconds 0.0", rendered)
        self.assertIn("vacciguard_replay_completion_status 0", rendered)


class ReplayLifecycleTests(unittest.TestCase):
    class FakeClock:
        def __init__(self, *values):
            self._values = list(values)
            self._last = values[-1] if values else 0.0

        def monotonic(self):
            if self._values:
                self._last = self._values.pop(0)
            return self._last

    def test_replay_updates_metrics_during_successful_run(self):
        producer = Mock()
        ack_future_one = Mock()
        ack_future_two = Mock()
        producer.send.side_effect = [ack_future_one, ack_future_two]
        registry = replay_producer.ReplayMetricsRegistry()
        events = [{"event_id": "a"}, {"event_id": "b"}]
        clock = self.FakeClock(100.0, 100.0, 100.2, 100.25, 100.5)

        with patch.object(replay_producer.time, "sleep"), patch.object(
            replay_producer.time, "monotonic", side_effect=clock.monotonic
        ):
            replay_producer.replay(producer, events, 4.0, metrics_registry=registry)

        rendered = registry.render_prometheus()

        self.assertIn("vacciguard_replay_sent_events_total 2", rendered)
        self.assertIn("vacciguard_replay_configured_rate_events_per_second 4.0", rendered)
        self.assertIn("vacciguard_replay_duration_seconds 0.5", rendered)
        self.assertIn("vacciguard_replay_completion_status 1", rendered)
        ack_future_one.get.assert_called_once()
        ack_future_two.get.assert_called_once()
        producer.flush.assert_called_once_with()

    def test_replay_does_not_increment_sent_total_when_delivery_confirmation_fails(self):
        producer = Mock()
        ack_future = Mock()
        ack_future.get.side_effect = RuntimeError("ack failed")
        producer.send.return_value = ack_future
        registry = replay_producer.ReplayMetricsRegistry()
        events = [{"event_id": "a"}]
        clock = self.FakeClock(50.0, 50.0, 50.25)

        with patch.object(replay_producer.time, "monotonic", side_effect=clock.monotonic):
            with self.assertRaises(RuntimeError):
                replay_producer.replay(producer, events, 6.0, metrics_registry=registry)

        rendered = registry.render_prometheus()

        self.assertIn("vacciguard_replay_sent_events_total 0", rendered)
        self.assertIn("vacciguard_replay_configured_rate_events_per_second 6.0", rendered)
        self.assertIn("vacciguard_replay_duration_seconds 0.25", rendered)
        self.assertIn("vacciguard_replay_completion_status 2", rendered)
        producer.flush.assert_not_called()


class ReplayMetricsHttpHandlerTests(unittest.TestCase):
    def test_replay_metrics_http_payload_returns_prometheus_text_for_registry(self):
        registry = replay_producer.ReplayMetricsRegistry()
        registry.record_loaded_events(3)
        registry.record_sent_event()
        registry.record_completion(duration_seconds=1.75, configured_events_per_second=7.5)

        payload = replay_producer.metrics_http_payload(registry)

        self.assertEqual(payload, registry.render_prometheus())
        self.assertIn("vacciguard_replay_loaded_events 3", payload)
        self.assertIn("vacciguard_replay_sent_events_total 1", payload)
        self.assertIn("vacciguard_replay_configured_rate_events_per_second 7.5", payload)
        self.assertIn("vacciguard_replay_completion_status 1", payload)

    def test_replay_metrics_server_serves_metrics_and_404s_other_paths(self):
        registry = replay_producer.ReplayMetricsRegistry()
        registry.record_loaded_events(5)
        registry.record_sent_event()
        registry.record_sent_event()
        registry.record_completion(duration_seconds=3.0, configured_events_per_second=4.0)

        server = replay_producer.start_metrics_server(0, registry)
        host, port = server.server_address
        metrics_url = f"http://127.0.0.1:{port}/metrics"
        missing_url = f"http://127.0.0.1:{port}/missing"

        try:
            with urllib.request.urlopen(metrics_url, timeout=2) as response:
                payload = response.read().decode("utf-8")
                content_type = response.headers.get("Content-Type")

            self.assertEqual(content_type, "text/plain; version=0.0.4")
            self.assertIn("vacciguard_replay_loaded_events 5", payload)
            self.assertIn("vacciguard_replay_sent_events_total 2", payload)
            self.assertIn("vacciguard_replay_configured_rate_events_per_second 4.0", payload)
            self.assertIn("vacciguard_replay_completion_status 1", payload)

            with self.assertRaises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(missing_url, timeout=2)

            self.assertEqual(exc_info.exception.code, 404)
            exc_info.exception.close()
        finally:
            server.shutdown()
            server.server_close()


class ReplayMainLifecycleTests(unittest.TestCase):
    def test_main_drains_metrics_then_shuts_down_server_after_success(self):
        metrics_server = Mock()
        producer = Mock()

        with patch.object(replay_producer, "REPLAY_METRICS_REGISTRY", replay_producer.ReplayMetricsRegistry()), patch.object(
            replay_producer, "REPLAY_METRICS_DRAIN_SECONDS", 3.5
        ), patch.object(
            replay_producer, "start_metrics_server", return_value=metrics_server
        ), patch.object(
            replay_producer, "load_events", return_value=[{"event_id": "a"}]
        ), patch.object(
            replay_producer, "connect", return_value=producer
        ), patch.object(
            replay_producer, "replay"
        ) as mock_replay, patch.object(
            replay_producer.time, "sleep"
        ) as mock_sleep:
            replay_producer.main()

        mock_replay.assert_called_once()
        mock_sleep.assert_called_once_with(3.5)
        producer.close.assert_called_once_with()
        metrics_server.shutdown.assert_called_once_with()
        metrics_server.server_close.assert_called_once_with()

    def test_main_records_failure_and_drains_metrics_server_when_startup_fails(self):
        metrics_server = Mock()
        registry = replay_producer.ReplayMetricsRegistry()
        startup_error = RuntimeError("load failed")

        with patch.object(replay_producer, "REPLAY_METRICS_REGISTRY", registry), patch.object(
            replay_producer, "REPLAY_METRICS_DRAIN_SECONDS", 2.0
        ), patch.object(
            replay_producer, "start_metrics_server", return_value=metrics_server
        ), patch.object(
            replay_producer, "load_events", side_effect=startup_error
        ), patch.object(
            replay_producer.time, "sleep"
        ) as mock_sleep:
            with self.assertRaises(RuntimeError) as exc_info:
                replay_producer.main()

        self.assertIs(exc_info.exception, startup_error)
        rendered = registry.render_prometheus()
        self.assertIn("vacciguard_replay_configured_rate_events_per_second 5.0", rendered)
        self.assertIn("vacciguard_replay_completion_status 2", rendered)
        mock_sleep.assert_called_once_with(2.0)
        metrics_server.shutdown.assert_called_once_with()
        metrics_server.server_close.assert_called_once_with()
