import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class MonitoringManifestTests(unittest.TestCase):
    def test_aws_baseline_foundation_mentions_monitoring_stack(self):
        raw = (ROOT / "docs/aws-baseline-foundation.md").read_text(encoding="utf-8")
        self.assertIn("Monitoring Stack", raw)
        self.assertIn("infra/monitoring/prometheus", raw)
        self.assertIn("infra/monitoring/grafana", raw)

    def test_monitoring_overview_readme_mentions_stack_split(self):
        raw = (ROOT / "infra/monitoring/README.md").read_text(encoding="utf-8")
        self.assertIn("CloudWatch", raw)
        self.assertIn("Prometheus", raw)
        self.assertIn("Grafana", raw)

    def test_prometheus_and_grafana_kustomizations_exist(self):
        self.assertTrue((ROOT / "infra/monitoring/prometheus/kustomization.yaml").exists())
        self.assertTrue((ROOT / "infra/monitoring/grafana/kustomization.yaml").exists())

    def test_prometheus_config_scrapes_monitoring_targets(self):
        raw = (ROOT / "infra/monitoring/prometheus/configmap-prometheus.yaml").read_text(encoding="utf-8")
        self.assertIn("kubernetes-pods", raw)
        self.assertIn("kubernetes-nodes", raw)
        self.assertIn("vacciguard", raw)
        self.assertIn("^(vacciguard|monitoring)$", raw)

    def test_baseline_dashboard_mentions_expected_panels(self):
        dashboard_path = ROOT / "infra/monitoring/grafana/configmap-dashboard-baseline-overview.yaml"
        raw = dashboard_path.read_text(encoding="utf-8")
        self.assertIn("VacciGuard Baseline Overview", raw)
        self.assertIn("Pod Restarts", raw)
        self.assertIn("CPU Usage", raw)
        self.assertIn("Memory Usage", raw)

    def test_grafana_datasource_points_to_prometheus_service(self):
        raw = (ROOT / "infra/monitoring/grafana/configmap-datasource.yaml").read_text(encoding="utf-8")
        self.assertIn("http://prometheus.monitoring.svc.cluster.local:9090", raw)

    def test_grafana_bundle_declares_monitoring_namespace(self):
        kustomization_raw = (ROOT / "infra/monitoring/grafana/kustomization.yaml").read_text(encoding="utf-8")
        self.assertIn("namespace.yaml", kustomization_raw)

        namespace_raw = (ROOT / "infra/monitoring/grafana/namespace.yaml").read_text(encoding="utf-8")
        self.assertIn("kind: Namespace", namespace_raw)
        self.assertIn("name: monitoring", namespace_raw)

    def test_cloudwatch_readme_exists(self):
        self.assertTrue((ROOT / "infra/monitoring/cloudwatch/README.md").exists())
