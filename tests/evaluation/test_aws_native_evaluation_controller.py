import importlib.util
import sys
import unittest
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


controller = load_module(
    "evaluation_controller", "services/evaluation-controller/controller.py"
)


class RunContractTests(unittest.TestCase):
    def test_resolve_run_contract_builds_isolated_paths(self):
        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="normal",
            run_id="run-001",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        self.assertEqual(contract.pipeline_target, "baseline")
        self.assertEqual(contract.scenario, "normal")
        self.assertEqual(contract.run_id, "run-001")
        self.assertEqual(contract.kafka_topic, "vacciguard-eval-run-001")
        self.assertEqual(
            contract.s3_prefix,
            "evaluations/baseline/normal/run-001",
        )
        self.assertEqual(
            contract.report_markdown_s3_uri,
            "s3://vacciguard-baseline-data/evaluations/baseline/normal/run-001/report.md",
        )
        self.assertEqual(
            contract.report_json_s3_uri,
            "s3://vacciguard-baseline-data/evaluations/baseline/normal/run-001/report.json",
        )

    def test_resolve_run_contract_rejects_unknown_target(self):
        with self.assertRaises(ValueError):
            controller.resolve_run_contract(
                pipeline_target="dev",
                scenario="normal",
                run_id="run-001",
                workload_family_version="evaluation-workload-v1",
                bucket_name="vacciguard-baseline-data",
            )

    def test_resolve_run_contract_rejects_unsupported_scenario(self):
        with self.assertRaises(ValueError):
            controller.resolve_run_contract(
                pipeline_target="baseline",
                scenario="chaos",
                run_id="run-001",
                workload_family_version="evaluation-workload-v1",
                bucket_name="vacciguard-baseline-data",
            )

    def test_resolve_run_contract_rejects_invalid_run_id(self):
        with self.assertRaises(ValueError):
            controller.resolve_run_contract(
                pipeline_target="baseline",
                scenario="normal",
                run_id="../run-001",
                workload_family_version="evaluation-workload-v1",
                bucket_name="vacciguard-baseline-data",
            )

    def test_report_payload_contains_run_metadata(self):
        contract = controller.resolve_run_contract(
            pipeline_target="optimized",
            scenario="spike",
            run_id="run-002",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        report = controller.build_report_payload(
            contract=contract,
            metrics={"processed_events": 42, "throughput_eps": 8.5},
            status="succeeded",
            failure_reason=None,
        )

        self.assertEqual(report["pipeline_target"], "optimized")
        self.assertEqual(report["scenario"], "spike")
        self.assertEqual(report["status"], "succeeded")
        self.assertEqual(report["processed_events"], 42)
        self.assertEqual(report["throughput_eps"], 8.5)

    def test_report_payload_rejects_reserved_metric_keys(self):
        contract = controller.resolve_run_contract(
            pipeline_target="optimized",
            scenario="spike",
            run_id="run-003",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        with self.assertRaises(ValueError):
            controller.build_report_payload(
                contract=contract,
                metrics={"run_id": "shadow", "processed_events": 42},
                status="succeeded",
                failure_reason=None,
            )
