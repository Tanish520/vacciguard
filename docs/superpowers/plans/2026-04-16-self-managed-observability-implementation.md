# Self-Managed Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move AWS evaluation runs back to the existing in-cluster Prometheus/Grafana stack so baseline and optimized runs can be compared on a live dashboard without AWS Managed Grafana, region, or Identity Center blockers.

**Architecture:** Keep the current self-managed monitoring stack as the canonical observability path for both local development and AWS EKS evaluations. Prometheus remains the metrics backend in-cluster, Grafana remains the dashboard surface in-cluster, and the dashboard groups series by `pipeline_target` so baseline and optimized runs appear on the same panels. CloudWatch stays available for logs and AWS infrastructure checks, but it is not the primary dashboard path.

**Tech Stack:** Python 3.11, Kubernetes, Prometheus, Grafana, Bash, `kubectl kustomize`, `unittest`

---

## File Structure

- Modify: `infra/monitoring/README.md`
  - make the self-managed Prometheus/Grafana stack the canonical path for AWS evaluation runs
- Modify: `infra/kubernetes/README.md`
  - explain that AWS evaluation runs use the same in-cluster monitoring stack as local development
- Modify: `docs/aws-baseline-foundation.md`
  - update the AWS runbook to point at the in-cluster dashboard instead of AWS Managed Grafana
- Modify: `infra/terraform/README.md`
  - remove or demote the AWS Managed Grafana guidance so it no longer reads like the primary path
- Modify: `infra/monitoring/aws-managed/README.md`
  - mark the AWS-managed path as archival/reference only
- Modify: `infra/monitoring/aws-managed/grafana/README.md`
  - mark the AWS-managed dashboard docs as archival/reference only
- Modify: `infra/monitoring/prometheus/configmap-prometheus.yaml`
  - carry the `pipeline_target` label through Prometheus scrape relabeling
- Modify: `infra/monitoring/grafana/configmap-dashboard-baseline-overview.yaml`
  - turn the existing dashboard into a baseline-vs-optimized comparison dashboard with live series
- Modify: `tests/monitoring/test_monitoring_manifests.py`
  - update the monitoring assertions for the self-managed comparison dashboard and the `pipeline_target` relabeling
- Modify: `tests/monitoring/test_operational_metrics.py`
  - assert the stream metrics registry still exposes the latency, ingest, and consumer-lag metrics the dashboard needs
- Delete: `tests/monitoring/test_aws_managed_observability_manifests.py`
  - remove the AWS-managed observability test file now that the self-managed path is canonical
- Modify: `scripts/run-aws-baseline-evaluation.sh`
  - preflight the in-cluster monitoring stack before each AWS baseline run
- Modify: `scripts/run-aws-evaluation-controller.sh`
  - preflight the in-cluster monitoring stack before each controller-driven AWS run
- Modify: `tests/evaluation/test_aws_native_evaluation_manifests.py`
  - verify the run scripts still invoke the self-managed monitoring preflight

## Task 1: Re-center the docs and tests on the self-managed stack

This task makes the repo say what the project now does: AWS evaluations use the same Prometheus/Grafana stack that the local environment already uses.

**Files:**
- Modify: `infra/monitoring/README.md`
- Modify: `infra/kubernetes/README.md`
- Modify: `docs/aws-baseline-foundation.md`
- Modify: `infra/terraform/README.md`
- Modify: `infra/monitoring/aws-managed/README.md`
- Modify: `infra/monitoring/aws-managed/grafana/README.md`
- Modify: `tests/monitoring/test_monitoring_manifests.py`
- Delete: `tests/monitoring/test_aws_managed_observability_manifests.py`

- [ ] **Step 1: Write the failing tests**

Extend `tests/monitoring/test_monitoring_manifests.py` so it checks that:

- the monitoring README names the self-managed stack as the AWS evaluation path
- the Kubernetes README says AWS runs use the same in-cluster Prometheus/Grafana setup as local development
- the AWS baseline foundation doc points readers at the in-cluster dashboard path rather than AWS Managed Grafana
- the AWS-managed docs read as archival/reference material instead of the primary flow

Example assertions:

```python
def test_self_managed_stack_is_canonical_for_aws_runs(self):
    monitoring_readme = (ROOT / "infra/monitoring/README.md").read_text(encoding="utf-8")
    kubernetes_readme = (ROOT / "infra/kubernetes/README.md").read_text(encoding="utf-8")
    aws_foundation = (ROOT / "docs/aws-baseline-foundation.md").read_text(encoding="utf-8")
    aws_managed_readme = (ROOT / "infra/monitoring/aws-managed/README.md").read_text(encoding="utf-8")

    self.assertIn("Prometheus plus Grafana for in-cluster metrics and dashboards", monitoring_readme)
    self.assertIn("AWS evaluation runs", monitoring_readme)
    self.assertIn("self-managed", monitoring_readme)
    self.assertIn("same in-cluster monitoring stack", kubernetes_readme)
    self.assertIn("VacciGuard Baseline Overview", aws_foundation)
    self.assertNotIn("Amazon Managed Grafana is provisioned", aws_foundation)
    self.assertIn("archival", aws_managed_readme)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
python3 -m unittest tests.monitoring.test_monitoring_manifests -v
```

Expected: FAIL because the docs still describe the AWS-managed path as current.

- [ ] **Step 3: Implement the doc changes**

Update the docs so the self-managed stack is the canonical AWS path:

- `infra/monitoring/README.md` should say AWS evaluation runs use the in-cluster Prometheus/Grafana stack and CloudWatch stays optional for logs and alarms.
- `infra/kubernetes/README.md` should say the AWS overlay reuses the same monitoring manifests as local development.
- `docs/aws-baseline-foundation.md` should point at the self-managed dashboard instead of AWS Managed Grafana.
- `infra/terraform/README.md` should stop presenting AWS Managed Grafana as the primary observability route.
- `infra/monitoring/aws-managed/README.md` and `infra/monitoring/aws-managed/grafana/README.md` should explicitly label the AWS-managed path as historical/reference only.
- `tests/monitoring/test_aws_managed_observability_manifests.py` should be deleted once the self-managed tests replace it.

Keep the CloudWatch guidance, but demote it to logs and infra visibility instead of the primary dashboard path.

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
python3 -m unittest tests.monitoring.test_monitoring_manifests -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add infra/monitoring/README.md infra/kubernetes/README.md docs/aws-baseline-foundation.md infra/terraform/README.md infra/monitoring/aws-managed/README.md infra/monitoring/aws-managed/grafana/README.md tests/monitoring/test_monitoring_manifests.py tests/monitoring/test_aws_managed_observability_manifests.py
git commit -m "docs: center aws runs on self-managed observability"
```

## Task 2: Make the in-cluster dashboard compare baseline and optimized runs

This task turns the existing Grafana dashboard into the live comparison view the project needs in AWS.

**Files:**
- Modify: `infra/monitoring/prometheus/configmap-prometheus.yaml`
- Modify: `infra/monitoring/grafana/configmap-dashboard-baseline-overview.yaml`
- Modify: `tests/monitoring/test_monitoring_manifests.py`
- Modify: `tests/monitoring/test_operational_metrics.py`

- [ ] **Step 1: Write the failing tests**

Update the monitoring tests so they require:

- Prometheus scrape relabeling preserves `pipeline_target` for both `stream-processor-metrics` and `replay-producer-metrics`
- the Grafana dashboard title changes to `VacciGuard Baseline vs Optimized`
- the dashboard panels include the full comparison set:
  - average latency
  - P95 latency
  - ingest-to-Redis P95
  - processed events
  - invalid events
  - deduplicated events
  - breach events
  - consumer lag
- the panel queries group series by `pipeline_target`

Example assertions:

```python
def test_prometheus_scrape_jobs_preserve_pipeline_target(self):
    jobs = {job["job_name"]: job for job in parse_prometheus_scrape_jobs()}

    stream_pipeline = relabel_config(
        jobs["stream-processor-metrics"], ["__meta_kubernetes_pod_label_pipeline_target"]
    )
    replay_pipeline = relabel_config(
        jobs["replay-producer-metrics"], ["__meta_kubernetes_pod_label_pipeline_target"]
    )

    self.assertEqual(stream_pipeline["target_label"], "pipeline_target")
    self.assertEqual(replay_pipeline["target_label"], "pipeline_target")

def test_dashboard_has_comparison_panels(self):
    dashboard = load_embedded_grafana_dashboard()
    self.assertEqual(dashboard["title"], "VacciGuard Baseline vs Optimized")

    titles = {panel["title"] for panel in dashboard["panels"]}
    expected = {
        "Baseline vs Optimized Avg Latency",
        "Baseline vs Optimized P95 Latency",
        "Ingest-to-Redis P95",
        "Processed Events",
        "Invalid Events",
        "Deduplicated Events",
        "Breach Events",
        "Consumer Lag",
    }
    self.assertTrue(expected.issubset(titles))

    exprs = [
        target["expr"]
        for panel in dashboard["panels"]
        for target in panel.get("targets", [])
        if "expr" in target
    ]
    self.assertTrue(any("pipeline_target" in expr for expr in exprs))
    self.assertTrue(any("vacciguard_stream_latest_batch_ingest_to_redis_p95_seconds" in expr for expr in exprs))
    self.assertTrue(any("vacciguard_stream_consumer_lag_records" in expr for expr in exprs))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
python3 -m unittest tests.monitoring.test_monitoring_manifests tests.monitoring.test_operational_metrics -v
```

Expected: FAIL because the current Prometheus config does not yet relabel `pipeline_target` and the dashboard still reads as the baseline-only view.

- [ ] **Step 3: Implement the dashboard and scrape changes**

Update the manifests so the dashboard can compare live baseline and optimized runs:

- `infra/monitoring/prometheus/configmap-prometheus.yaml` should add a relabel rule that copies `__meta_kubernetes_pod_label_pipeline_target` into the `pipeline_target` label for both stream and replay jobs.
- `infra/monitoring/grafana/configmap-dashboard-baseline-overview.yaml` should become the comparison dashboard and use `sum by (pipeline_target)` or `avg by (pipeline_target)` queries for each panel.
- `infra/monitoring/grafana/configmap-dashboard-baseline-overview.yaml` should keep the Prometheus datasource and continue to show the existing stream and replay metrics, plus consumer lag and ingest-to-Redis latency.

Keep the dashboard file path the same so the existing Grafana provider keeps loading it without extra wiring.

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
python3 -m unittest tests.monitoring.test_monitoring_manifests tests.monitoring.test_operational_metrics -v
kubectl kustomize infra/monitoring/prometheus >/tmp/vacciguard-prometheus.yaml
kubectl kustomize infra/monitoring/grafana >/tmp/vacciguard-grafana.yaml
```

Expected:

- unit tests pass
- both kustomizations render cleanly

- [ ] **Step 5: Commit**

```bash
git add infra/monitoring/prometheus/configmap-prometheus.yaml infra/monitoring/grafana/configmap-dashboard-baseline-overview.yaml tests/monitoring/test_monitoring_manifests.py tests/monitoring/test_operational_metrics.py
git commit -m "feat: add self-managed baseline vs optimized dashboard"
```

## Task 3: Make the AWS evaluation scripts bring up the monitoring stack before each run

This task makes sure the live dashboard is actually available before the AWS evaluation starts, so the run data shows up while the workload is still fresh.

**Files:**
- Modify: `scripts/run-aws-baseline-evaluation.sh`
- Modify: `scripts/run-aws-evaluation-controller.sh`
- Modify: `tests/evaluation/test_aws_native_evaluation_manifests.py`

- [ ] **Step 1: Write the failing tests**

Extend `tests/evaluation/test_aws_native_evaluation_manifests.py` so it checks that both run scripts preflight the self-managed monitoring stack:

```python
def test_run_scripts_apply_the_monitoring_stack(self):
    baseline_raw = (ROOT / "scripts/run-aws-baseline-evaluation.sh").read_text(encoding="utf-8")
    controller_raw = (ROOT / "scripts/run-aws-evaluation-controller.sh").read_text(encoding="utf-8")

    for raw in (baseline_raw, controller_raw):
        self.assertIn("kubectl apply -k infra/monitoring/prometheus", raw)
        self.assertIn("kubectl apply -k infra/monitoring/grafana", raw)
        self.assertIn("kubectl rollout status deployment/prometheus -n monitoring", raw)
        self.assertIn("kubectl rollout status deployment/grafana -n monitoring", raw)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_manifests -v
```

Expected: FAIL because the current scripts do not yet preflight the monitoring stack.

- [ ] **Step 3: Implement the preflight in the scripts**

Add a shared preflight block to both scripts that:

- applies `infra/monitoring/prometheus`
- applies `infra/monitoring/grafana`
- waits for the Prometheus and Grafana deployments to become ready

The resulting shell flow should look like:

```bash
kubectl apply -k infra/monitoring/prometheus >/dev/null
kubectl apply -k infra/monitoring/grafana >/dev/null
kubectl rollout status deployment/prometheus -n monitoring --timeout=5m >/dev/null
kubectl rollout status deployment/grafana -n monitoring --timeout=5m >/dev/null
```

This keeps the AWS run path simple: the metrics endpoints are already in the cluster, and the dashboard is guaranteed to exist before the workload starts.

- [ ] **Step 4: Run the tests and a short live smoke run**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_manifests -v
kubectl kustomize infra/monitoring/prometheus >/tmp/vacciguard-prometheus.yaml
kubectl kustomize infra/monitoring/grafana >/tmp/vacciguard-grafana.yaml
WORKLOAD_DURATION_MINUTES=1 bash scripts/run-aws-baseline-evaluation.sh obs-smoke normal
```

Expected:

- the test passes
- the monitoring manifests render cleanly
- the smoke run completes and produces live Prometheus series for the dashboard

- [ ] **Step 5: Commit**

```bash
git add scripts/run-aws-baseline-evaluation.sh scripts/run-aws-evaluation-controller.sh tests/evaluation/test_aws_native_evaluation_manifests.py
git commit -m "feat: preflight self-managed monitoring for aws runs"
```

## Verification Before Hand-Off

Before calling this work done, run the full relevant suite and confirm the dashboard stack still renders:

```bash
python3 -m unittest tests.monitoring.test_monitoring_manifests tests.monitoring.test_operational_metrics tests.evaluation.test_aws_native_evaluation_manifests -v
kubectl kustomize infra/monitoring/prometheus
kubectl kustomize infra/monitoring/grafana
```

If you want to verify the live end state, run one short AWS baseline or optimized smoke evaluation and open the in-cluster Grafana dashboard. The success condition is that the baseline and optimized panels both show live data during the same run window.

Expected outcome:

- self-managed monitoring is the canonical AWS path
- the Grafana dashboard compares baseline and optimized runs on the same panels
- the dashboard shows live metric data, not just static panel definitions
- AWS Managed Grafana is no longer required for the observability workflow
