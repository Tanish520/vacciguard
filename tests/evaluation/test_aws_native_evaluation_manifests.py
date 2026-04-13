import json
from pathlib import Path
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[2]


def render_manifest(path: str) -> dict[str, object]:
    result = subprocess.run(
        [
            "kubectl",
            "create",
            "--dry-run=client",
            "-f",
            str(ROOT / path),
            "-o",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


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
        manifest = render_manifest("infra/kubernetes/base/job-evaluation-controller.yaml")

        self.assertEqual(manifest["metadata"]["name"], "evaluation-controller")
        self.assertTrue(manifest["spec"]["suspend"])
        env_names = {
            item["name"] for item in manifest["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        self.assertIn("PIPELINE_TARGET", env_names)
        self.assertIn("SCENARIO", env_names)
        self.assertIn("RUN_ID", env_names)
        self.assertIn("WORKLOAD_FAMILY_VERSION", env_names)
        image = manifest["spec"]["template"]["spec"]["containers"][0]["image"]
        self.assertNotIn(":latest", image)

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
        self.assertNotIn("--from=job/evaluation-controller", raw)
