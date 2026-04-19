import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]


class AwsManagedObservabilityManifestTests(unittest.TestCase):
    def test_readmes_and_kustomization_exist(self):
        self.assertTrue((ROOT / "infra/monitoring/aws-managed/README.md").exists())
        self.assertTrue((ROOT / "infra/monitoring/aws-managed/grafana/README.md").exists())
        self.assertTrue((ROOT / "infra/kubernetes/aws-observability/kustomization.yaml").exists())

    def test_readmes_mentions_managed_stack(self):
        monitoring_readme = (ROOT / "infra/monitoring/aws-managed/README.md").read_text(encoding="utf-8")
        grafana_readme = (ROOT / "infra/monitoring/aws-managed/grafana/README.md").read_text(encoding="utf-8")
        terraform_readme = (ROOT / "infra/terraform/README.md").read_text(encoding="utf-8")

        for raw in (monitoring_readme, grafana_readme):
            self.assertIn("AMP", raw)
            self.assertIn("CloudWatch", raw)
        self.assertIn("Amazon Managed Grafana", monitoring_readme)
        self.assertIn("ap-southeast-1", monitoring_readme)
        self.assertIn("ap-southeast-1", grafana_readme)
        self.assertIn("ap-southeast-1", terraform_readme)
        self.assertIn("IAM Identity Center", terraform_readme)

    def test_collector_namespace_and_irsa_are_fixed(self):
        namespace_raw = (ROOT / "infra/kubernetes/aws-observability/namespace.yaml").read_text(encoding="utf-8")
        serviceaccount_raw = (ROOT / "infra/kubernetes/aws-observability/serviceaccount-adot-collector.yaml").read_text(encoding="utf-8")
        rolebinding_raw = (ROOT / "infra/kubernetes/aws-observability/rolebinding-adot-collector.yaml").read_text(encoding="utf-8")
        configmap_raw = (ROOT / "infra/kubernetes/aws-observability/configmap-collector.yaml").read_text(encoding="utf-8")

        self.assertIn("name: observability", namespace_raw)
        self.assertIn("name: adot-collector", serviceaccount_raw)
        self.assertIn("observability", serviceaccount_raw)
        self.assertIn("eks.amazonaws.com/role-arn", serviceaccount_raw)
        self.assertIn("system:serviceaccount:observability:adot-collector", rolebinding_raw)
        self.assertIn("__meta_kubernetes_pod_label_pipeline_target", configmap_raw)
        self.assertIn("target_label: pipeline_target", configmap_raw)

    def test_terraform_observability_mentions_amp_and_remote_write(self):
        terraform_raw = (ROOT / "infra/terraform/observability.tf").read_text(encoding="utf-8")

        self.assertIn("aws_prometheus_workspace", terraform_raw)
        self.assertIn("aws_grafana_workspace", terraform_raw)
        self.assertIn("amazon-cloudwatch-observability", terraform_raw)
        self.assertIn("aps:RemoteWrite", terraform_raw)
        self.assertIn("system:serviceaccount:observability:adot-collector", terraform_raw)

    def test_dashboard_has_expected_panels(self):
        dashboard = json.loads(
            (ROOT / "infra/monitoring/aws-managed/grafana/dashboards/baseline-vs-optimized.json").read_text(
                encoding="utf-8"
            )
        )

        self.assertIn("VacciGuard Baseline vs Optimized", dashboard["title"])
        titles = {panel["title"] for panel in dashboard["panels"]}
        expected_titles = {
            "Avg Latency",
            "P95 Latency",
            "Ingest-to-Redis P95",
            "Processed Events",
            "Invalid Rate",
            "Dedup Rate",
            "Breach Rate",
            "Consumer Lag",
        }
        self.assertTrue(expected_titles.issubset(titles))

        all_exprs = [
            target["expr"]
            for panel in dashboard["panels"]
            for target in panel.get("targets", [])
            if "expr" in target
        ]
        self.assertTrue(any("pipeline_target" in expr for expr in all_exprs))
        self.assertTrue(any("vacciguard_stream_latest_batch_avg_latency_seconds" in expr for expr in all_exprs))
        self.assertTrue(any("vacciguard_stream_latest_batch_p95_latency_seconds" in expr for expr in all_exprs))
        self.assertTrue(any("vacciguard_stream_latest_batch_ingest_to_redis_p95_seconds" in expr for expr in all_exprs))
        self.assertTrue(any("vacciguard_stream_consumer_lag_records" in expr for expr in all_exprs))
