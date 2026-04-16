from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]

class AwsNativeEvaluationManifestTests(unittest.TestCase):
    def test_base_kustomization_includes_evaluation_controller_resources(self):
        raw = (
            ROOT / "infra/kubernetes/base/kustomization.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn("serviceaccount-evaluation-controller.yaml", raw)
        self.assertIn("role-evaluation-controller.yaml", raw)
        self.assertIn("rolebinding-evaluation-controller.yaml", raw)
        self.assertIn("job-evaluation-controller.yaml", raw)

    def test_evaluation_controller_job_template_is_dormant_and_pinned(self):
        raw = (
            ROOT / "infra/kubernetes/base/job-evaluation-controller.yaml"
        ).read_text(encoding="utf-8")

        self.assertIn("name: evaluation-controller", raw)
        self.assertIn("suspend: true", raw)
        self.assertIn("- name: PIPELINE_TARGET", raw)
        self.assertIn("- name: SCENARIO", raw)
        self.assertIn("- name: RUN_ID", raw)
        self.assertIn("- name: WORKLOAD_FAMILY_VERSION", raw)
        self.assertIn("- name: EVALUATION_CONTROLLER_MODE", raw)
        self.assertIn("value: orchestrate", raw)
        self.assertIn("value: vacciguard-tanish-baseline-ap-south-1-data", raw)
        self.assertIn(
            "image: 347038623570.dkr.ecr.ap-south-1.amazonaws.com/vacciguard-evaluation-controller:reporting-20260415t221504z-amd64",
            raw,
        )
        self.assertNotIn(":latest", raw)

    def test_submit_helper_uses_local_template_and_run_scoped_job_name(self):
        raw = (ROOT / "scripts/run-aws-evaluation-controller.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn('JOB_NAME="evaluation-controller-', raw)
        self.assertIn(
            "kubectl create --dry-run=client -f infra/kubernetes/base/job-evaluation-controller.yaml -o json",
            raw,
        )
        self.assertIn('template["metadata"]["namespace"] = "vacciguard"', raw)
        self.assertIn('"WORKLOAD_DURATION_MINUTES"', raw)
        self.assertIn('kubectl wait \\', raw)
        self.assertIn('--for=condition=Ready', raw)
        self.assertIn('--pod-running-timeout="$POD_RUNNING_TIMEOUT"', raw)
        self.assertNotIn("--from=job/evaluation-controller", raw)

    def test_build_and_push_helper_targets_linux_amd64(self):
        raw = (
            ROOT / "scripts/build-and-push-evaluation-controller.sh"
        ).read_text(encoding="utf-8")

        self.assertIn('DOCKER_PLATFORM="${DOCKER_PLATFORM:-linux/amd64}"', raw)
        self.assertIn("docker buildx build", raw)
        self.assertIn("services/evaluation-controller/Dockerfile", raw)
        self.assertIn("vacciguard-evaluation-controller", raw)

    def test_baseline_overlay_is_tuned_for_lower_latency(self):
        configmap_raw = (
            ROOT / "infra/kubernetes/baseline/configmap-pipeline.yaml"
        ).read_text(encoding="utf-8")
        stream_patch_raw = (
            ROOT / "infra/kubernetes/baseline/patch-stream-processor-resources.yaml"
        ).read_text(encoding="utf-8")
        kafka_patch_raw = (
            ROOT / "infra/kubernetes/baseline/patch-kafka-resources.yaml"
        ).read_text(encoding="utf-8")

        self.assertIn("TRIGGER_INTERVAL: 2 seconds", configmap_raw)
        self.assertIn('SPARK_SQL_SHUFFLE_PARTITIONS: "8"', configmap_raw)
        self.assertIn('SPARK_DEFAULT_PARALLELISM: "8"', configmap_raw)
        self.assertIn("PIPELINE_MODE: baseline", configmap_raw)
        self.assertIn('cpu: "1500m"', stream_patch_raw)
        self.assertIn('memory: "2560Mi"', stream_patch_raw)
        self.assertIn('cpu: "2000m"', stream_patch_raw)
        self.assertIn('memory: "4096Mi"', stream_patch_raw)
        self.assertIn('cpu: "750m"', kafka_patch_raw)
        self.assertIn('memory: "1536Mi"', kafka_patch_raw)
        self.assertIn('cpu: "1500m"', kafka_patch_raw)
        self.assertIn('memory: "3Gi"', kafka_patch_raw)
