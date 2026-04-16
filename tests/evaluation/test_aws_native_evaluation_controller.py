import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
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

    def test_render_markdown_report_includes_operational_summary(self):
        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="spike",
            run_id="run-003",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )
        report_payload = controller.build_report_payload(
            contract=contract,
            metrics={
                "input_events": 198000,
                "throughput_eps": 999.4,
                "processed_events": 52,
                "invalid_events": 6,
                "deduplicated_events": 9,
                "breach_events": 52,
                "processed_output_objects": 52,
                "invalid_output_objects": 6,
                "breach_window_output_objects": 52,
                "stream_metrics_source": "metrics_endpoint",
                "event_time_lag_p95_seconds": 12.25,
                "ingest_to_redis_p95_seconds": 4.5,
                "watermark_delay_seconds": 601.0,
                "consumer_lag_records": 17,
                "invalid_rate_pct": 1.82,
                "deduplication_rate_pct": 0.45,
                "processed_rate_pct": 98.03,
                "pipeline_success": True,
                "controller_job_success": True,
                "replay_job_success": True,
            },
            status="succeeded",
            failure_reason=None,
        )

        markdown = controller.render_markdown_report(report_payload)

        self.assertIn("# Evaluation Report: run-003", markdown)
        self.assertIn("## Summary", markdown)
        self.assertIn("- pipeline target: baseline", markdown)
        self.assertIn("- report json: s3://vacciguard-baseline-data/evaluations/baseline/spike/run-003/report.json", markdown)
        self.assertIn("## Metrics", markdown)
        self.assertIn("| Stream metrics source | metrics_endpoint |", markdown)
        self.assertIn("| Ingest-to-Redis P95 | 4.50 s |", markdown)
        self.assertIn("| Consumer lag | 17 records |", markdown)
        self.assertIn("| Pipeline success | True |", markdown)
        self.assertIn("| Processed output objects | 52 |", markdown)


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

    def test_build_replay_job_manifest_supports_s3_workload_uri(self):
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
            workload_configmap_name=None,
            workload_runtime_path="s3://vacciguard-baseline-data/evaluations/run-003/workloads/normal.events.ndjson",
            target_eps=6.0,
        )

        container = manifest["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(
            container["env"][2]["value"],
            "s3://vacciguard-baseline-data/evaluations/run-003/workloads/normal.events.ndjson",
        )
        self.assertNotIn("volumes", manifest["spec"]["template"]["spec"])
        self.assertNotIn("volumeMounts", container)

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


class ControllerMainTests(unittest.TestCase):
    def test_main_module_loads_without_controller_test_shim(self):
        sys.modules.pop("controller", None)
        controller_main = load_module(
            "evaluation_controller_main_bootstrap",
            "services/evaluation-controller/main.py",
        )

        self.assertTrue(hasattr(controller_main, "main"))

    def test_load_workload_inputs_honors_duration_override_env(self):
        controller_main = load_module(
            "evaluation_controller_main_duration_override",
            "services/evaluation-controller/main.py",
        )
        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="spike",
            run_id="run-005",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        with mock.patch.dict(
            os.environ, {"WORKLOAD_DURATION_MINUTES": "3"}, clear=False
        ):
            _, manifest = controller_main.load_workload_inputs(contract)

        self.assertEqual(manifest["duration_minutes"], 3)
        self.assertEqual(manifest["event_count"], 198000)

    def test_main_writes_report_payload_when_orchestration_succeeds(self):
        controller_main = load_module(
            "evaluation_controller_main", "services/evaluation-controller/main.py"
        )

        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="normal",
            run_id="run-005",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        with mock.patch.object(
            controller_main, "load_runtime_inputs", return_value=contract
        ), mock.patch.dict(
            os.environ, {"EVALUATION_CONTROLLER_MODE": "orchestrate"}, clear=False
        ), mock.patch.object(
            controller_main, "run_orchestration", return_value={"processed_events": 10}
        ), mock.patch.object(controller_main, "upload_reports") as mock_upload:
            controller_main.main()

        mock_upload.assert_called_once()

    def test_main_writes_bootstrap_report_for_default_orchestration(self):
        controller_main = load_module(
            "evaluation_controller_main_bootstrap",
            "services/evaluation-controller/main.py",
        )

        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="normal",
            run_id="run-006",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        with mock.patch.object(
            controller_main, "load_runtime_inputs", return_value=contract
        ), mock.patch.object(
            controller_main, "upload_reports"
        ) as mock_upload:
            controller_main.main()

        mock_upload.assert_called_once()
        report_payload = mock_upload.call_args.kwargs["report_payload"]
        self.assertEqual(report_payload["status"], "bootstrap")
        self.assertEqual(report_payload["controller_mode"], "bootstrap")
        self.assertEqual(report_payload["processed_events"], 0)

    def test_main_propagates_orchestration_failure_without_upload(self):
        controller_main = load_module(
            "evaluation_controller_main_failure",
            "services/evaluation-controller/main.py",
        )

        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="normal",
            run_id="run-008",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        with mock.patch.object(
            controller_main, "load_runtime_inputs", return_value=contract
        ), mock.patch.dict(
            os.environ, {"EVALUATION_CONTROLLER_MODE": "orchestrate"}, clear=False
        ), mock.patch.object(
            controller_main,
            "run_orchestration",
            side_effect=RuntimeError("boom"),
        ), mock.patch.object(controller_main, "upload_reports") as mock_upload:
            with self.assertRaises(RuntimeError):
                controller_main.main()

        mock_upload.assert_not_called()

    def test_upload_reports_writes_markdown_and_json_to_s3(self):
        controller_main = load_module(
            "evaluation_controller_main_upload",
            "services/evaluation-controller/main.py",
        )

        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="normal",
            run_id="run-007",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )
        report_payload = controller.build_report_payload(
            contract=contract,
            metrics={"processed_events": 10},
            status="succeeded",
            failure_reason=None,
        )

        with mock.patch("boto3.client") as mock_client_factory:
            mock_s3 = mock.Mock()
            mock_client_factory.return_value = mock_s3

            controller_main.upload_reports(
                contract=contract,
                report_payload=report_payload,
            )

        self.assertEqual(mock_s3.put_object.call_count, 2)
        markdown_call = mock_s3.put_object.call_args_list[0].kwargs
        json_call = mock_s3.put_object.call_args_list[1].kwargs
        self.assertEqual(markdown_call["Bucket"], "vacciguard-baseline-data")
        self.assertEqual(
            markdown_call["Key"],
            "evaluations/baseline/normal/run-007/report.md",
        )
        self.assertEqual(markdown_call["ContentType"], "text/markdown")
        self.assertEqual(json_call["Bucket"], "vacciguard-baseline-data")
        self.assertEqual(
            json_call["Key"],
            "evaluations/baseline/normal/run-007/report.json",
        )
        self.assertEqual(json_call["ContentType"], "application/json")

    def test_service_files_import_in_isolated_runtime_layout(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            shutil.copy(
                ROOT / "services/evaluation-controller/main.py",
                temp_root / "main.py",
            )
            shutil.copy(
                ROOT / "services/evaluation-controller/controller.py",
                temp_root / "controller.py",
            )
            shutil.copy(
                ROOT / "services/evaluation-controller/aws_baseline_metrics.py",
                temp_root / "aws_baseline_metrics.py",
            )
            shutil.copy(
                ROOT / "services/evaluation-controller/workload_factory.py",
                temp_root / "workload_factory.py",
            )

            subprocess.run(
                [
                    sys.executable,
                    "-c",
                    (
                        "import importlib.util, pathlib; "
                        "path = pathlib.Path('main.py').resolve(); "
                        "spec = importlib.util.spec_from_file_location('isolated_main', path); "
                        "module = importlib.util.module_from_spec(spec); "
                        "spec.loader.exec_module(module)"
                    ),
                ],
                cwd=temp_root,
                check=True,
                capture_output=True,
                text=True,
                env={**os.environ, "PYTHONPATH": ""},
            )


class OrchestrationFlowTests(unittest.TestCase):
    def test_extract_metrics_from_logs_merges_metadata(self):
        with mock.patch.object(
            controller.aws_baseline_metrics,
            "extract_metrics",
            return_value={"processed_events": 27},
        ) as mock_extract:
            metrics = controller.extract_metrics_from_logs(
                "replay",
                "stream",
                "prometheus",
                {"processed_output_objects": 52},
                {
                    "pipeline_target": "baseline",
                    "scenario": "normal",
                    "workload_family_version": "evaluation-workload-v1",
                },
            )

        mock_extract.assert_called_once_with(
            "replay",
            "stream",
            stream_metrics_payload="prometheus",
        )
        self.assertEqual(metrics["processed_events"], 27)
        self.assertEqual(metrics["processed_output_objects"], 52)
        self.assertEqual(metrics["pipeline_target"], "baseline")
        self.assertEqual(metrics["scenario"], "normal")
        self.assertEqual(
            metrics["workload_family_version"], "evaluation-workload-v1"
        )

    def test_run_orchestration_collects_metrics_and_returns_report_fields(self):
        controller_main = load_module(
            "evaluation_controller_main_orchestration",
            "services/evaluation-controller/main.py",
        )

        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="normal",
            run_id="run-006",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        with mock.patch.object(controller_main, "reset_redis_state"), \
            mock.patch.object(controller_main, "patch_pipeline_config"), \
            mock.patch.object(controller_main, "restart_stream_processor"), \
            mock.patch.object(controller_main, "wait_for_stream_ready"), \
            mock.patch.object(controller_main, "launch_replay_job"), \
            mock.patch.object(controller_main, "wait_for_replay_completion"), \
            mock.patch.object(
                controller_main,
                "load_workload_inputs",
                return_value=(
                    "workload",
                    {
                        "target_eps": 1000.0,
                        "duration_minutes": 3,
                        "event_count": 198000,
                    },
                ),
            ), \
            mock.patch.object(
                controller_main, "collect_replay_logs", return_value="replay"
            ), \
            mock.patch.object(
                controller_main,
                "wait_for_stream_metrics_settlement",
                return_value="prometheus",
            ), \
            mock.patch.object(
                controller_main, "collect_stream_logs", return_value="stream"
            ), \
            mock.patch.object(
                controller_main,
                "summarize_s3_outputs",
                return_value={
                    "processed_output_objects": 52,
                    "invalid_output_objects": 6,
                    "breach_window_output_objects": 52,
                },
            ), \
            mock.patch.object(
                controller_main.controller,
                "extract_metrics_from_logs",
                return_value={"processed_events": 27},
            ) as mock_extract:
            metrics = controller_main.run_orchestration(contract)

        mock_extract.assert_called_once_with(
            "replay",
            "stream",
            "prometheus",
            {
                "processed_output_objects": 52,
                "invalid_output_objects": 6,
                "breach_window_output_objects": 52,
            },
            {
                "pipeline_target": "baseline",
                "scenario": "normal",
                "workload_family_version": "evaluation-workload-v1",
                "configured_events_per_second": 1000.0,
                "workload_duration_minutes": 3,
                "pipeline_success": True,
                "controller_job_success": True,
                "replay_job_success": True,
            },
        )
        self.assertEqual(metrics["processed_events"], 27)
