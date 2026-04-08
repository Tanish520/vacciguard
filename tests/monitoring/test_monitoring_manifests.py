import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class MonitoringManifestTests(unittest.TestCase):
    def test_monitoring_overview_readme_mentions_stack_split(self):
        raw = (ROOT / "infra/monitoring/README.md").read_text(encoding="utf-8")
        self.assertIn("CloudWatch", raw)
        self.assertIn("Prometheus", raw)
        self.assertIn("Grafana", raw)

    def test_prometheus_and_grafana_kustomizations_exist(self):
        self.assertTrue((ROOT / "infra/monitoring/prometheus/kustomization.yaml").exists())
        self.assertTrue((ROOT / "infra/monitoring/grafana/kustomization.yaml").exists())

    def test_baseline_dashboard_mentions_expected_panels(self):
        dashboard_path = ROOT / "infra/monitoring/grafana/configmap-dashboard-baseline-overview.yaml"
        raw = dashboard_path.read_text(encoding="utf-8")
        self.assertIn("VacciGuard Baseline Overview", raw)
        self.assertIn("Pod Restarts", raw)
        self.assertIn("CPU Usage", raw)
        self.assertIn("Memory Usage", raw)

    def test_cloudwatch_readme_exists(self):
        self.assertTrue((ROOT / "infra/monitoring/cloudwatch/README.md").exists())
