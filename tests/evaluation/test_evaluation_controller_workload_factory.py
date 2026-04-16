import importlib.util
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, relative_path: str):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


workload_factory = load_module(
    "evaluation_controller_workload_factory",
    "services/evaluation-controller/workload_factory.py",
)


class EvaluationControllerWorkloadFactoryTests(unittest.TestCase):
    def test_build_run_scoped_workload_shifts_earliest_event_to_target_time(self):
        target_time = datetime(2026, 4, 16, 1, 2, 3, tzinfo=timezone.utc)

        workload_ndjson, manifest = workload_factory.build_run_scoped_workload(
            scenario="normal",
            target_start_time=target_time,
        )

        events = [json.loads(line) for line in workload_ndjson.splitlines() if line.strip()]
        event_times = [event["event_time"] for event in events]

        self.assertEqual(min(event_times), "2026-04-16T01:02:03Z")
        self.assertEqual(manifest["scenario"], "normal")
        self.assertEqual(manifest["target_eps"], 100.0)
        self.assertEqual(manifest["workload_family_version"], "evaluation-workload-v1")

    def test_build_run_scoped_workload_preserves_expected_event_volume(self):
        workload_ndjson, manifest = workload_factory.build_run_scoped_workload(
            scenario="spike",
            target_start_time=datetime(2026, 4, 16, 1, 2, 3, tzinfo=timezone.utc),
        )

        line_count = len([line for line in workload_ndjson.splitlines() if line.strip()])

        self.assertEqual(line_count, manifest["event_count"])
        self.assertGreater(line_count, 100000)
        self.assertEqual(manifest["target_eps"], 1000.0)

    def test_build_run_scoped_workload_supports_duration_override(self):
        workload_ndjson, manifest = workload_factory.build_run_scoped_workload(
            scenario="spike",
            target_start_time=datetime(2026, 4, 16, 1, 2, 3, tzinfo=timezone.utc),
            duration_minutes=3,
        )

        line_count = len([line for line in workload_ndjson.splitlines() if line.strip()])

        self.assertEqual(manifest["duration_minutes"], 3)
        self.assertEqual(line_count, manifest["event_count"])
        self.assertEqual(line_count, 198000)
        self.assertEqual(manifest["target_eps"], 1000.0)


if __name__ == "__main__":
    unittest.main()
