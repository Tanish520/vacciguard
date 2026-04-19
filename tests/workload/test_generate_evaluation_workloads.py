import importlib.util
import csv
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "generate-evaluation-workloads.py"
)
SPEC = importlib.util.spec_from_file_location("generate_evaluation_workloads", MODULE_PATH)
generator = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(generator)


class EvaluationWorkloadGeneratorTests(unittest.TestCase):
    def test_build_workload_family_metadata_locks_v1_defaults(self):
        metadata = generator.build_workload_family_metadata()

        self.assertEqual(metadata["workload_family_version"], "evaluation-workload-v1")
        self.assertEqual(metadata["devices"], 30)
        self.assertEqual(metadata["duration_minutes"], 12)
        self.assertEqual(metadata["normal_eps"], 100.0)
        self.assertEqual(metadata["spike_eps"], 1000.0)

    def test_generate_normal_scenario_creates_expected_base_volume(self):
        metadata = generator.build_workload_family_metadata()
        events = generator.generate_scenario("normal", metadata)

        self.assertGreaterEqual(len(events), 72000)
        self.assertLess(len(events), 90000)
        self.assertTrue(all("event_id" in event for event in events))

    def test_write_outputs_creates_ndjson_and_manifest(self):
        metadata = generator.build_workload_family_metadata()
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            generator.write_scenario_outputs(output_dir, "normal", metadata)

            manifest = json.loads((output_dir / "normal.manifest.json").read_text())
            lines = (output_dir / "normal.events.ndjson").read_text().strip().splitlines()

        self.assertEqual(manifest["scenario"], "normal")
        self.assertEqual(manifest["workload_family_version"], "evaluation-workload-v1")
        self.assertGreater(len(lines), 70000)

    def test_main_writes_all_v1_scenarios(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            generator.main(["--output-dir", temp_dir])
            names = sorted(path.name for path in Path(temp_dir).iterdir())

        self.assertEqual(
            names,
            [
                "failure-recovery.events.ndjson",
                "failure-recovery.manifest.json",
                "normal.events.ndjson",
                "normal.manifest.json",
                "spike.events.ndjson",
                "spike.manifest.json",
            ],
        )


class EvaluationScenarioMixTests(unittest.TestCase):
    def test_normal_manifest_records_mix_targets(self):
        metadata = generator.build_workload_family_metadata()
        manifest = generator.build_scenario_manifest("normal", metadata, event_count=4500)

        self.assertEqual(manifest["scenario"], "normal")
        self.assertEqual(manifest["mix_targets"]["duplicates_pct"], 0.05)
        self.assertEqual(manifest["mix_targets"]["late_pct"], 0.03)
        self.assertEqual(manifest["mix_targets"]["invalid_pct"], 0.02)

    def test_spike_manifest_uses_10x_rate(self):
        metadata = generator.build_workload_family_metadata()
        manifest = generator.build_scenario_manifest("spike", metadata, event_count=43200)

        self.assertEqual(manifest["target_eps"], 1000.0)

    def test_failure_recovery_manifest_records_fault_model(self):
        metadata = generator.build_workload_family_metadata()
        manifest = generator.build_scenario_manifest("failure-recovery", metadata, event_count=4500)

        self.assertEqual(manifest["fault_model"]["type"], "stream-processor-restart")


class LookupAlignmentTests(unittest.TestCase):
    def test_lookup_template_covers_generated_normal_devices(self):
        metadata = generator.build_workload_family_metadata()
        generated_ids = {
            event["device_id"]
            for event in generator.generate_scenario("normal", metadata)
        }

        lookup_path = (
            Path(__file__).resolve().parents[2]
            / "data"
            / "reference"
            / "device-facility-lookup-template.csv"
        )
        with lookup_path.open(encoding="utf-8") as handle:
            lookup_ids = {row["device_id"] for row in csv.DictReader(handle)}

        self.assertEqual(len(lookup_ids), 30)
        self.assertTrue(generated_ids.issubset(lookup_ids))
