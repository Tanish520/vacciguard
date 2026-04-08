import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def load_embedded_prometheus_config() -> str:
    raw = (ROOT / "infra/monitoring/prometheus/configmap-prometheus.yaml").read_text(encoding="utf-8")
    lines = raw.splitlines()
    start = lines.index("  prometheus.yml: |") + 1
    return "\n".join(line[4:] for line in lines[start:] if line.startswith("    "))


def parse_prometheus_scrape_jobs():
    jobs = []
    current_job = None
    current_relabel = None

    for line in load_embedded_prometheus_config().splitlines():
        if line.startswith("  - job_name: "):
            current_job = {
                "job_name": line.split(": ", 1)[1],
                "metrics_path": None,
                "relabel_configs": [],
            }
            jobs.append(current_job)
            current_relabel = None
            continue

        if current_job is None:
            continue

        if line.startswith("    metrics_path: "):
            current_job["metrics_path"] = line.split(": ", 1)[1]
            continue

        if line.startswith("      - source_labels: "):
            current_relabel = {
                "source_labels": parse_inline_list(line.split(": ", 1)[1]),
                "action": None,
                "regex": None,
            }
            current_job["relabel_configs"].append(current_relabel)
            continue

        if current_relabel is None:
            continue

        if line.startswith("        action: "):
            current_relabel["action"] = line.split(": ", 1)[1]
            continue

        if line.startswith("        regex: "):
            current_relabel["regex"] = line.split(": ", 1)[1]

    return jobs


def parse_inline_list(raw: str) -> list[str]:
    stripped = raw.strip()
    if not stripped.startswith("[") or not stripped.endswith("]"):
        raise AssertionError(f"Expected inline list, got: {raw}")
    body = stripped[1:-1].strip()
    if not body:
        return []
    return [item.strip() for item in body.split(",")]


def relabel_config(job: dict[str, object], source_labels: list[str]) -> dict[str, object]:
    for config in job["relabel_configs"]:
        if config["source_labels"] == source_labels:
            return config
    raise AssertionError(f"Missing relabel config for source_labels={source_labels} in {job['job_name']}")


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
        jobs = {job["job_name"]: job for job in parse_prometheus_scrape_jobs()}

        self.assertIn("kubernetes-nodes", jobs)
        self.assertNotIn("kubernetes-pods", jobs)
        self.assertIn("stream-processor-metrics", jobs)
        self.assertIn("replay-producer-metrics", jobs)
        self.assertEqual(jobs["stream-processor-metrics"]["metrics_path"], "/metrics")
        self.assertEqual(jobs["replay-producer-metrics"]["metrics_path"], "/metrics")

        stream_phase = relabel_config(
            jobs["stream-processor-metrics"], ["__meta_kubernetes_pod_phase"]
        )
        self.assertEqual(stream_phase["action"], "keep")
        self.assertEqual(stream_phase["regex"], "Running")

        stream_port = relabel_config(
            jobs["stream-processor-metrics"], ["__meta_kubernetes_pod_container_port_name"]
        )
        self.assertEqual(stream_port["action"], "keep")
        self.assertEqual(stream_port["regex"], "metrics")

        stream_app = relabel_config(jobs["stream-processor-metrics"], ["__meta_kubernetes_pod_label_app"])
        self.assertEqual(stream_app["action"], "keep")
        self.assertEqual(stream_app["regex"], "stream-processor")

        replay_phase = relabel_config(
            jobs["replay-producer-metrics"], ["__meta_kubernetes_pod_phase"]
        )
        self.assertEqual(replay_phase["action"], "keep")
        self.assertEqual(replay_phase["regex"], "Running")

        replay_port = relabel_config(
            jobs["replay-producer-metrics"], ["__meta_kubernetes_pod_container_port_name"]
        )
        self.assertEqual(replay_port["action"], "keep")
        self.assertEqual(replay_port["regex"], "metrics")

        replay_job = relabel_config(
            jobs["replay-producer-metrics"], ["__meta_kubernetes_pod_label_job_name"]
        )
        self.assertEqual(replay_job["action"], "keep")
        self.assertEqual(replay_job["regex"], "replay-producer")

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
