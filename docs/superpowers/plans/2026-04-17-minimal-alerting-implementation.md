# Minimal Alerting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the smallest possible Prometheus-based SLA alerting setup so VacciGuard can detect latency and backlog violations live.

**Architecture:** Keep the existing in-cluster Prometheus stack and add one alert rules file to the current Prometheus ConfigMap. Prometheus will evaluate the rules against application metrics it already scrapes; no new notification channel is required for this minimum version. Validation will prove the rules are loaded, the monitoring manifests still render cleanly, and the latency alert can enter a firing state during a spike run.

**Tech Stack:** Kubernetes, Prometheus, YAML, Python `unittest`, `kubectl`, `kustomize`

---

### Task 1: Add Prometheus SLA alert rules to the existing monitoring config

**Files:**
- Modify: `infra/monitoring/prometheus/configmap-prometheus.yaml`
- Modify: `tests/monitoring/test_monitoring_manifests.py`

- [ ] **Step 1: Write the failing test**

Add a test that loads the embedded Prometheus config and asserts that it declares a `rule_files` entry and the new alert rule file name:

```python
def test_prometheus_config_loads_sla_alert_rules(self):
    raw = load_embedded_prometheus_config()
    self.assertIn("rule_files:", raw)
    self.assertIn("/etc/prometheus/vacciguard-sla-alerts.yml", raw)
```

Add a second test that reads the embedded alert rule payload from the ConfigMap and checks both alert names and expressions:

```python
def load_embedded_prometheus_alert_rules() -> str:
    raw = (ROOT / "infra/monitoring/prometheus/configmap-prometheus.yaml").read_text(encoding="utf-8")
    lines = raw.splitlines()
    start = lines.index("  vacciguard-sla-alerts.yml: |") + 1
    return "\n".join(line[4:] for line in lines[start:] if line.startswith("    "))


def test_prometheus_alert_rules_cover_latency_and_lag(self):
    raw = load_embedded_prometheus_alert_rules()
    self.assertIn("HighLatency", raw)
    self.assertIn("vacciguard_stream_latest_batch_avg_latency_seconds > 5", raw)
    self.assertIn("ConsumerLagBuilding", raw)
    self.assertIn("vacciguard_stream_consumer_lag_records > 1000", raw)
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run:
```bash
pytest -q tests/monitoring/test_monitoring_manifests.py
```

Expected:
- the new alert-rule tests fail because the ConfigMap does not yet define `rule_files` or `vacciguard-sla-alerts.yml`

- [ ] **Step 3: Add the minimal implementation**

Update `infra/monitoring/prometheus/configmap-prometheus.yaml` so it keeps the existing `prometheus.yml` and adds a second key with the alert rules:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
  namespace: monitoring
data:
  prometheus.yml: |
    global:
      scrape_interval: 15s
      evaluation_interval: 15s
    rule_files:
      - /etc/prometheus/vacciguard-sla-alerts.yml
    scrape_configs:
      - job_name: kubernetes-nodes
        kubernetes_sd_configs:
          - role: node
      - job_name: stream-processor-metrics
        metrics_path: /metrics
        kubernetes_sd_configs:
          - role: pod
        relabel_configs:
          - source_labels: [__meta_kubernetes_namespace]
            action: keep
            regex: vacciguard
          - source_labels: [__meta_kubernetes_pod_phase]
            action: keep
            regex: Running
          - source_labels: [__meta_kubernetes_pod_container_port_name]
            action: keep
            regex: metrics
          - source_labels: [__meta_kubernetes_pod_label_app]
            action: keep
            regex: stream-processor
          - source_labels: [__meta_kubernetes_pod_name]
            target_label: pod
      - job_name: replay-producer-metrics
        metrics_path: /metrics
        kubernetes_sd_configs:
          - role: pod
        relabel_configs:
          - source_labels: [__meta_kubernetes_namespace]
            action: keep
            regex: vacciguard
          - source_labels: [__meta_kubernetes_pod_phase]
            action: keep
            regex: Running
          - source_labels: [__meta_kubernetes_pod_container_port_name]
            action: keep
            regex: metrics
          - source_labels: [__meta_kubernetes_pod_label_job_name]
            action: keep
            regex: replay-producer
          - source_labels: [__meta_kubernetes_pod_name]
            target_label: pod
  vacciguard-sla-alerts.yml: |
    groups:
      - name: vacciguard_sla
        rules:
          - alert: HighLatency
            expr: vacciguard_stream_latest_batch_avg_latency_seconds > 5
            for: 1m
            labels:
              severity: warning
              component: stream-processor
            annotations:
              summary: "VacciGuard latency SLA breached"
              description: "Average end-to-end latency has exceeded 5 seconds for more than 1 minute."
          - alert: ConsumerLagBuilding
            expr: vacciguard_stream_consumer_lag_records > 1000
            for: 2m
            labels:
              severity: warning
              component: stream-processor
            annotations:
              summary: "VacciGuard Kafka consumer lag is growing"
              description: "Consumer lag has exceeded 1000 records for more than 2 minutes."
```

Keep the deployment unchanged: `infra/monitoring/prometheus/deployment-prometheus.yaml` already mounts the ConfigMap at `/etc/prometheus`, so the new rule file will appear automatically.

- [ ] **Step 4: Run the tests and the manifest render check**

Run:
```bash
pytest -q tests/monitoring/test_monitoring_manifests.py
kubectl kustomize infra/monitoring
```

Expected:
- the monitoring tests pass
- `kubectl kustomize infra/monitoring` renders without errors

- [ ] **Step 5: Commit the alerting change**

```bash
git add infra/monitoring/prometheus/configmap-prometheus.yaml tests/monitoring/test_monitoring_manifests.py
git commit -m "feat: add minimal Prometheus SLA alert rules"
```

### Task 2: Deploy the rules and prove the latency alert can fire

**Files:**
- No code changes expected if Task 1 reused the existing ConfigMap mount

- [ ] **Step 1: Apply the monitoring manifests**

Run:
```bash
kubectl apply -k infra/monitoring/prometheus
```

Expected:
- Prometheus reloads with the new rule file mounted from the ConfigMap

- [ ] **Step 2: Verify Prometheus sees the rules**

Run:
```bash
kubectl -n monitoring port-forward svc/prometheus 9090:9090
```

Then check:
```bash
curl -s http://localhost:9090/api/v1/rules | jq '.data.groups[] | select(.name=="vacciguard_sla")'
```

Expected:
- the `vacciguard_sla` group exists
- both `HighLatency` and `ConsumerLagBuilding` appear in the output

- [ ] **Step 3: Prove the alert can enter firing state**

Run the baseline spike scenario, which already produces latency above 5 seconds:

```bash
WORKLOAD_DURATION_MINUTES=5 \
bash scripts/run-aws-baseline-evaluation.sh \
  spike-$(date -u +%Y%m%dT%H%M%SZ) \
  spike
```

While that run is active, query Prometheus alerts:

```bash
curl -s http://localhost:9090/api/v1/alerts | jq '.data.alerts[] | select(.labels.alertname=="HighLatency")'
```

Expected:
- the alert appears in `firing` state during the spike run
- the evidence is enough to show live SLA violation detection without adding a separate notification stack

- [ ] **Step 4: Record the result in the monitoring notes**

Update `infra/monitoring/README.md` with a short note that says Prometheus now evaluates VacciGuard SLA alert rules for latency and consumer lag, and that the existing monitoring stack is sufficient for live SLA detection.

- [ ] **Step 5: Commit the validation note**

```bash
git add infra/monitoring/README.md
git commit -m "docs: note Prometheus SLA alert validation"
```
