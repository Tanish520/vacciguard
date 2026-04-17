import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def load_embedded_prometheus_config() -> str:
    raw = (ROOT / "infra/monitoring/prometheus/configmap-prometheus.yaml").read_text(encoding="utf-8")
    lines = raw.splitlines()
    start = lines.index("  prometheus.yml: |") + 1
    return "\n".join(line[4:] for line in lines[start:] if line.startswith("    "))


def load_embedded_prometheus_alert_rules() -> str:
    raw = (ROOT / "infra/monitoring/prometheus/configmap-prometheus.yaml").read_text(encoding="utf-8")
    lines = raw.splitlines()
    start = lines.index("  vacciguard-sla-alerts.yml: |") + 1
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
                "target_label": None,
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
            continue

        if line.startswith("        target_label: "):
            current_relabel["target_label"] = line.split(": ", 1)[1]

    return jobs


def load_embedded_grafana_dashboard() -> dict[str, object]:
    raw = (
        ROOT / "infra/monitoring/grafana/configmap-dashboard-baseline-overview.yaml"
    ).read_text(encoding="utf-8")
    lines = raw.splitlines()
    start = lines.index("  baseline-overview.json: |") + 1
    payload = "\n".join(line[4:] for line in lines[start:] if line.startswith("    "))
    return json.loads(payload)


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


def pod_target_relabel(job: dict[str, object]) -> dict[str, object]:
    for config in job["relabel_configs"]:
        if (
            config["source_labels"] == ["__meta_kubernetes_pod_name"]
            and config["target_label"] == "pod"
        ):
            return config
    raise AssertionError(f"Missing pod target relabel in {job['job_name']}")


def replay_completion_status_target_expr(dashboard: dict[str, object]) -> str:
    for panel in dashboard["panels"]:
        if panel.get("title") != "Replay Completion":
            continue
        exprs = [target["expr"] for target in panel.get("targets", []) if "expr" in target]
        if len(exprs) != 1:
            raise AssertionError("Replay Completion panel must define exactly one expression")
        return exprs[0]
    raise AssertionError("Replay Completion panel not found")


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
        stream_pod = pod_target_relabel(jobs["stream-processor-metrics"])
        self.assertIsNone(stream_pod["action"])
        self.assertIsNone(stream_pod["regex"])

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
        replay_pod = pod_target_relabel(jobs["replay-producer-metrics"])
        self.assertIsNone(replay_pod["action"])
        self.assertIsNone(replay_pod["regex"])

    def test_prometheus_config_loads_sla_alert_rules(self):
        raw = load_embedded_prometheus_config()
        self.assertIn("rule_files:", raw)
        self.assertIn("/etc/prometheus/vacciguard-sla-alerts.yml", raw)

    def test_prometheus_alert_rules_cover_latency_and_lag(self):
        raw = load_embedded_prometheus_alert_rules()
        self.assertIn("groups:", raw)
        self.assertIn("name: vacciguard_sla", raw)
        self.assertIn("alert: HighLatency", raw)
        self.assertIn("vacciguard_stream_latest_batch_avg_latency_seconds > 5", raw)
        self.assertIn("alert: ConsumerLagBuilding", raw)
        self.assertIn("vacciguard_stream_consumer_lag_records > 1000", raw)

    def test_baseline_dashboard_uses_real_pipeline_metrics(self):
        dashboard = load_embedded_grafana_dashboard()
        self.assertEqual(dashboard["title"], "VacciGuard Baseline Overview")

        panels = dashboard["panels"]
        self.assertGreaterEqual(len(panels), 12)

        panel_queries = {
            panel["title"]: [
                target["expr"] for target in panel.get("targets", []) if "expr" in target
            ]
            for panel in panels
        }
        expected_panel_queries = {
            "Avg Latency (s)": "avg_over_time(vacciguard_stream_latest_batch_avg_latency_seconds[$__range])",
            "P95 Latency (s)": "avg_over_time(vacciguard_stream_latest_batch_p95_latency_seconds[$__range])",
            "P99 Latency (s)": "avg_over_time(vacciguard_stream_latest_batch_p99_latency_seconds[$__range])",
            "Throughput (eps)": "max_over_time(vacciguard_stream_observed_throughput_eps[$__range])",
            "Consumer Lag": "max_over_time(vacciguard_stream_consumer_lag_records[$__range])",
            "Alerts Fired (run)": "sum(changes(ALERTS{alertstate=\"firing\", alertname=~\"HighLatency|ConsumerLagBuilding\"}[$__range]))",
            "Cost / GB (est.)": "((0.196 * max_over_time(vacciguard_replay_duration_seconds[$__range]) / 3600) + 0.0005) / ((max_over_time(vacciguard_replay_sent_events_total[$__range]) * 200) / 1000000000)",
            "Ingest-to-Redis P95 (s)": "avg_over_time(vacciguard_stream_latest_batch_ingest_to_redis_p95_seconds[$__range])",
            "Processed Events": "sum(vacciguard_stream_processed_events_total)",
            "Invalid Rate (%)": "100 * sum(vacciguard_stream_invalid_events_total) / clamp_min(sum(vacciguard_stream_processed_events_total) + sum(vacciguard_stream_invalid_events_total), 1)",
            "Dedup Rate (%)": "100 * sum(vacciguard_stream_deduplicated_events_total) / clamp_min(sum(vacciguard_stream_processed_events_total) + sum(vacciguard_stream_invalid_events_total), 1)",
            "Breach Rate (%)": "100 * sum(vacciguard_stream_breach_events_total) / clamp_min(sum(vacciguard_stream_processed_events_total), 1)",
            "Live Alerts": "ALERTS{alertname=~\"HighLatency|ConsumerLagBuilding\"}",
        }
        for panel_title, expected_expr in expected_panel_queries.items():
            self.assertIn(panel_title, panel_queries)
            self.assertIn(expected_expr, panel_queries[panel_title])

        for panel in panels:
            for target in panel.get("targets", []):
                self.assertEqual(target.get("datasource"), "Prometheus")

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

    def test_aws_managed_readme_calls_out_label_based_comparison(self):
        raw = (ROOT / "infra/monitoring/aws-managed/README.md").read_text(encoding="utf-8")
        self.assertIn("baseline and optimized runs on the same panels", raw)
        self.assertIn("pipeline_target", raw)

    def test_aws_managed_dashboard_groups_series_by_pipeline_target(self):
        dashboard = json.loads(
            (ROOT / "infra/monitoring/aws-managed/grafana/dashboards/baseline-vs-optimized.json").read_text(
                encoding="utf-8"
            )
        )

        self.assertEqual(dashboard["title"], "VacciGuard Baseline vs Optimized")
        for panel in dashboard["panels"]:
            self.assertEqual(panel.get("datasource"), "AMP")
            exprs = [target["expr"] for target in panel.get("targets", []) if "expr" in target]
            self.assertTrue(any("pipeline_target" in expr for expr in exprs))
