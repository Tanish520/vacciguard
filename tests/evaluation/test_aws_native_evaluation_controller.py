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

    def test_resolve_run_contract_rejects_run_id_with_underscore(self):
        with self.assertRaises(ValueError):
            controller.resolve_run_contract(
                pipeline_target="baseline",
                scenario="normal",
                run_id="run_001",
                workload_family_version="evaluation-workload-v1",
                bucket_name="vacciguard-baseline-data",
            )

    def test_resolve_run_contract_rejects_run_id_with_trailing_dash(self):
        with self.assertRaises(ValueError):
            controller.resolve_run_contract(
                pipeline_target="baseline",
                scenario="normal",
                run_id="run-001-",
                workload_family_version="evaluation-workload-v1",
                bucket_name="vacciguard-baseline-data",
            )

    def test_resolve_run_contract_rejects_run_id_with_dots(self):
        with self.assertRaises(ValueError):
            controller.resolve_run_contract(
                pipeline_target="baseline",
                scenario="normal",
                run_id="run.001",
                workload_family_version="evaluation-workload-v1",
                bucket_name="vacciguard-baseline-data",
            )

    def test_resolve_run_contract_rejects_run_id_longer_than_label_limit(self):
        with self.assertRaises(ValueError):
            controller.resolve_run_contract(
                pipeline_target="baseline",
                scenario="normal",
                run_id="r" * (controller.MAX_RUN_ID_LENGTH + 1),
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


class ManifestBuilderTests(unittest.TestCase):
    def test_build_workload_configmap_manifest_uses_run_scoped_name(self):
        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="normal",
            run_id="run-003",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        manifest = controller.build_workload_configmap_manifest(
            contract=contract,
            workload_ndjson='{"event_id":"evt-1"}\n',
        )

        self.assertEqual(manifest["kind"], "ConfigMap")
        self.assertEqual(manifest["metadata"]["name"], "vacciguard-workload-run-003")
        self.assertEqual(manifest["data"]["events.ndjson"], '{"event_id":"evt-1"}\n')

    def test_build_manifests_keep_generated_names_within_kubernetes_limit(self):
        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="normal",
            run_id="r" * controller.MAX_RUN_ID_LENGTH,
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        workload_manifest = controller.build_workload_configmap_manifest(
            contract=contract,
            workload_ndjson='{"event_id":"evt-1"}\n',
        )
        replay_manifest = controller.build_replay_job_manifest(
            contract=contract,
            replay_image="repo/replay:tag",
            kafka_bootstrap_servers="kafka:9092",
            workload_configmap_name=workload_manifest["metadata"]["name"],
            target_eps=6.0,
        )

        self.assertLessEqual(
            len(workload_manifest["metadata"]["name"]),
            controller.MAX_KUBERNETES_NAME_LENGTH,
        )
        self.assertLessEqual(
            len(replay_manifest["metadata"]["name"]),
            controller.MAX_KUBERNETES_NAME_LENGTH,
        )

    def test_build_workload_configmap_manifest_rejects_oversized_content(self):
        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="normal",
            run_id="run-003",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        with self.assertRaises(ValueError):
            controller.build_workload_configmap_manifest(
                contract=contract,
                workload_ndjson="x" * (controller.MAX_WORKLOAD_CONFIGMAP_BYTES + 1),
            )

    def test_build_replay_job_manifest_uses_run_specific_name_and_metrics_port(self):
        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="normal",
            run_id="run-003",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        manifest = controller.build_replay_job_manifest(
            contract=contract,
            replay_image="repo/replay:tag",
            kafka_bootstrap_servers="kafka:9092",
            workload_configmap_name="vacciguard-workload-run-003",
            target_eps=6.0,
        )

        template = manifest["spec"]["template"]
        container = manifest["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(manifest["metadata"]["name"], "replay-producer-run-003")
        self.assertEqual(template["metadata"]["labels"]["job_name"], "replay-producer")
        self.assertEqual(container["image"], "repo/replay:tag")
        self.assertEqual(container["env"][1]["value"], "vacciguard-eval-run-003")
        self.assertEqual(container["env"][3]["value"], "6.0")
        self.assertEqual(container["ports"][0]["name"], "metrics")
        self.assertEqual(container["ports"][0]["containerPort"], 9109)
        self.assertEqual(
            manifest["spec"]["template"]["spec"]["volumes"][0]["configMap"]["name"],
            "vacciguard-workload-run-003",
        )

    def test_build_pipeline_config_patch_uses_isolated_paths(self):
        contract = controller.resolve_run_contract(
            pipeline_target="optimized",
            scenario="spike",
            run_id="run-004",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        patch = controller.build_pipeline_config_patch(
            contract=contract,
            app_name="vacciguard-stream-processor",
            kafka_bootstrap_servers="kafka:9092",
            kafka_topic_partitions="6",
            trigger_interval="5 seconds",
            watermark_delay="10 minutes",
            redis_host="redis.example",
            redis_port="6379",
            redis_db="0",
        )

        self.assertEqual(patch["data"]["KAFKA_TOPIC"], "vacciguard-eval-run-004")
        self.assertEqual(
            patch["data"]["PROCESSED_OUTPUT_PATH"],
            "s3a://vacciguard-baseline-data/evaluations/optimized/spike/run-004/processed",
        )
        self.assertEqual(
            patch["data"]["CHECKPOINT_ROOT"],
            "s3a://vacciguard-baseline-data/evaluations/optimized/spike/run-004/checkpoints",
        )
