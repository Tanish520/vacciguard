# AWS-Managed Observability and Optimized Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the AWS EKS runs for both baseline and optimized pipelines to AWS-managed observability, then land the optimized stream-processing path that should beat the baseline on latency.

**Architecture:** Keep local Prometheus and Grafana for developer smoke tests, but make the AWS evaluation path use Amazon Managed Service for Prometheus, Amazon Managed Grafana, and CloudWatch/Container Insights. This plan assumes the AWS account already has IAM Identity Center enabled, because Amazon Managed Grafana requires it. The stream processor keeps one image and one codebase, but it switches behavior with `PIPELINE_MODE=baseline|optimized`: baseline preserves the current two-query shape, while optimized uses one `foreachBatch` callback and within-batch deduplication. The evaluation controller remains the run orchestrator and report writer for both pipeline targets.

**Tech Stack:** Python 3.11, Apache Spark Structured Streaming, Kubernetes, Terraform, Amazon EKS, Amazon Managed Service for Prometheus, Amazon Managed Grafana, Amazon CloudWatch, boto3, `kubectl kustomize`, `unittest`

---

## File Structure

- Create: `infra/terraform/observability.tf`
  - AMP workspace, Amazon Managed Grafana workspace, CloudWatch/Container Insights wiring, and IAM permissions for managed observability
- Modify: `infra/terraform/iam.tf`
  - add IAM roles and policies for the collector, Grafana access, and EKS add-ons
- Modify: `infra/terraform/variables.tf`
  - add knobs for workspace names, dashboard name, namespace, and observability mode
- Modify: `infra/terraform/outputs.tf`
  - expose AMP workspace ARN/URL, Grafana workspace ID/URL, and CloudWatch log group names
- Modify: `infra/terraform/main.tf`
  - wire the observability resources into the existing EKS baseline stack
- Modify: `infra/terraform/README.md`
  - explain how the managed observability layer is provisioned and used
- Create: `infra/kubernetes/aws-observability/namespace.yaml`
  - dedicated namespace for the managed metrics collector
- Create: `infra/kubernetes/aws-observability/kustomization.yaml`
  - AWS-specific EKS monitoring bundle for the metrics collector and CloudWatch-aware settings
- Create: `infra/kubernetes/aws-observability/serviceaccount-adot-collector.yaml`
  - service account for the metrics collector with IRSA
- Create: `infra/kubernetes/aws-observability/role-adot-collector.yaml`
  - namespace-scoped RBAC for scraping and endpoint discovery
- Create: `infra/kubernetes/aws-observability/rolebinding-adot-collector.yaml`
  - bind the collector role to its service account
- Create: `infra/kubernetes/aws-observability/deployment-adot-collector.yaml`
  - collector deployment that remote-writes Prometheus metrics to AMP
- Create: `infra/kubernetes/aws-observability/configmap-collector.yaml`
  - scrape config and pipeline metadata for baseline and optimized runs
- Create: `infra/kubernetes/aws-observability/service-collector.yaml`
  - service exposing the collector health endpoint
- Create: `infra/monitoring/aws-managed/README.md`
  - AWS-managed observability runbook and dashboard guidance
- Create: `infra/monitoring/aws-managed/grafana/README.md`
  - dashboard import and panel conventions
- Create: `infra/monitoring/aws-managed/grafana/dashboards/baseline-vs-optimized.json`
  - Grafana dashboard JSON for direct baseline vs optimized comparisons
- Modify: `infra/monitoring/README.md`
  - preserve local dev monitoring notes and explain the AWS-managed split
- Modify: `infra/kubernetes/README.md`
  - document the AWS observability overlay and how it differs from local dev
- Modify: `services/stream-processor/job.py`
  - add the `PIPELINE_MODE` switch and keep emitted metrics compatible with the AWS dashboard
- Modify: `infra/kubernetes/base/configmap-pipeline.yaml`
  - add the default `PIPELINE_MODE: baseline`
- Modify: `infra/kubernetes/baseline/configmap-pipeline.yaml`
  - keep baseline runtime values explicit
- Modify: `infra/kubernetes/optimized/configmap-pipeline.yaml`
  - set optimized runtime values and `PIPELINE_MODE: optimized`
- Modify: `services/evaluation-controller/controller.py`
  - thread the pipeline mode into the run patch and report metadata
- Modify: `services/evaluation-controller/main.py`
  - keep the evaluation controller orchestration path aligned with the selected pipeline mode
- Modify: `scripts/run-aws-evaluation-controller.sh`
  - pass `PIPELINE_MODE` and `WORKLOAD_DURATION_MINUTES` when provided
- Modify: `scripts/run-aws-baseline-evaluation.sh`
  - keep baseline runs defaulted to baseline mode
- Modify: `tests/monitoring/test_monitoring_manifests.py`
  - update the local monitoring assertions and add coverage for the AWS-managed docs/manifests
- Create: `tests/monitoring/test_aws_managed_observability_manifests.py`
  - verify AMP/AMG/CloudWatch-related manifests, dashboard panels, and wiring
- Modify: `tests/stream/test_job.py`
  - update stream-processing tests for the `PIPELINE_MODE` split and metric-shape changes
- Modify: `tests/evaluation/test_aws_native_evaluation_controller.py`
  - verify evaluation controller still orchestrates baseline and optimized runs cleanly

## Task 1: Provision the AWS-managed observability foundation

This task adds the AWS-side metrics, dashboard, and log plumbing that both baseline and optimized EKS runs will share.

**Prerequisite:** confirm IAM Identity Center is enabled in the AWS account before starting Terraform. Amazon Managed Grafana depends on it. If the account does not have Identity Center yet, stop here and enable it before continuing.

**Files:**
- Create: `infra/terraform/observability.tf`
- Modify: `infra/terraform/iam.tf`
- Modify: `infra/terraform/variables.tf`
- Modify: `infra/terraform/outputs.tf`
- Modify: `infra/terraform/main.tf`
- Modify: `infra/terraform/README.md`
- Create: `infra/kubernetes/aws-observability/namespace.yaml`
- Create: `infra/kubernetes/aws-observability/kustomization.yaml`
- Create: `infra/kubernetes/aws-observability/serviceaccount-adot-collector.yaml`
- Create: `infra/kubernetes/aws-observability/role-adot-collector.yaml`
- Create: `infra/kubernetes/aws-observability/rolebinding-adot-collector.yaml`
- Create: `infra/kubernetes/aws-observability/deployment-adot-collector.yaml`
- Create: `infra/kubernetes/aws-observability/configmap-collector.yaml`
- Create: `infra/kubernetes/aws-observability/service-collector.yaml`
- Create: `infra/monitoring/aws-managed/README.md`
- Create: `infra/monitoring/aws-managed/grafana/README.md`
- Create: `infra/monitoring/aws-managed/grafana/dashboards/baseline-vs-optimized.json`
- Modify: `infra/monitoring/README.md`
- Modify: `infra/kubernetes/README.md`
- Create: `tests/monitoring/test_aws_managed_observability_manifests.py`

- [ ] **Step 1: Write the failing tests**

Add `tests/monitoring/test_aws_managed_observability_manifests.py` with checks that:
- the AWS-managed monitoring README exists and mentions AMP, AMG, and CloudWatch
- the new `infra/kubernetes/aws-observability` kustomization exists
- the collector namespace is fixed to `observability`
- the collector service account subject is `system:serviceaccount:observability:adot-collector`
- the IAM policy includes `aps:RemoteWrite`
- the Grafana dashboard JSON contains panels for:
  - average latency
  - p95 latency
  - ingest-to-Redis p95
  - processed events
  - invalid events
  - deduplicated events
  - breach events
  - consumer lag
- the dashboard title clearly distinguishes baseline vs optimized comparison

Example assertions:

```python
def test_aws_managed_dashboard_has_comparison_panels(self):
    dashboard = json.loads(
        (ROOT / "infra/monitoring/aws-managed/grafana/dashboards/baseline-vs-optimized.json").read_text()
    )
    titles = {panel["title"] for panel in dashboard["panels"]}
    self.assertIn("Baseline vs Optimized Latency", titles)
    self.assertIn("Ingest-to-Redis P95", titles)
    self.assertIn("Consumer Lag", titles)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
python3 -m unittest tests.monitoring.test_aws_managed_observability_manifests -v
```

Expected: FAIL because the AWS-managed observability files do not exist yet.

- [ ] **Step 3: Implement the Terraform and Kubernetes wiring**

Add the new Terraform observability resources and the AWS-specific Kubernetes collector bundle.

The Terraform work should:
- create or configure an AMP workspace
- create an Amazon Managed Grafana workspace in the IAM Identity Center-enabled account
- expose the AMP workspace ARN and query endpoint
- add IAM permissions for the collector to remote-write metrics with `aps:RemoteWrite`
- add CloudWatch log group outputs for controller, replay, and stream logs

The Kubernetes bundle should:
- create and use the dedicated `observability` namespace
- run the collector with IRSA bound to `system:serviceaccount:observability:adot-collector`
- discover the stream processor and replay producer metrics endpoints
- remote-write the Prometheus metrics into AMP
- keep the collector narrowly scoped to the evaluation workloads, not the local compose flow

Keep local Prometheus and Grafana manifests untouched for developer use. This task only changes the AWS path.

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
python3 -m unittest tests.monitoring.test_aws_managed_observability_manifests -v
kubectl kustomize infra/kubernetes/aws-observability >/tmp/vacciguard-aws-observability.yaml
terraform -chdir=infra/terraform validate
```

Expected:
- unit tests pass
- `kubectl kustomize` renders valid YAML
- Terraform validation succeeds

- [ ] **Step 5: Commit**

```bash
git add infra/terraform/observability.tf infra/terraform/iam.tf infra/terraform/variables.tf infra/terraform/outputs.tf infra/terraform/main.tf infra/terraform/README.md infra/kubernetes/aws-observability infra/monitoring/aws-managed infra/monitoring/README.md infra/kubernetes/README.md tests/monitoring/test_aws_managed_observability_manifests.py
git commit -m "feat: add aws-managed observability for evaluation runs"
```

## Task 2: Refactor the stream processor for the optimized hot path

This is the pipeline rewrite that is supposed to reduce latency under normal load. It should preserve business behavior while changing where and when the work happens.

**Decision:** keep one image and one code path in `services/stream-processor/job.py`, but branch the runtime behavior with `PIPELINE_MODE=baseline|optimized`. Baseline remains the reference implementation. Optimized bypasses the duplicate batch-summary path and uses one `foreachBatch` callback.

**Files:**
- Modify: `services/stream-processor/job.py`
- Modify: `infra/kubernetes/base/configmap-pipeline.yaml`
- Modify: `infra/kubernetes/baseline/configmap-pipeline.yaml`
- Modify: `infra/kubernetes/optimized/configmap-pipeline.yaml`
- Modify: `infra/kubernetes/optimized/kustomization.yaml`
- Modify: `tests/stream/test_job.py`

- [ ] **Step 1: Write the failing tests**

Add or update tests in `tests/stream/test_job.py` that verify:
- `PIPELINE_MODE=baseline` keeps the current two-query baseline shape
- `PIPELINE_MODE=optimized` switches to one `foreachBatch` query for the classified stream
- the optimized batch callback keeps the same invalid/processed/breach outputs
- the lookup DataFrame can be cached once and passed into the callback
- the consumer lag metric is updated by the background poller, not by the batch callback
- the optimized configmap sets the runtime values explicitly

Example test shape:

```python
def test_start_queries_uses_single_foreach_batch_query_when_optimized(self):
    with patch.dict(os.environ, {"PIPELINE_MODE": "optimized"}):
        queries = stream_job.start_queries(processed_df, invalid_df, classified_df, lookup_df)
    self.assertEqual(len(queries), 1)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
python3 -m unittest tests.stream.test_job -v
```

Expected: FAIL because the current stream job still uses the baseline architecture for every mode.

- [ ] **Step 3: Implement the optimized stream job**

Change `services/stream-processor/job.py` so the runtime mode is explicit:
- default to `PIPELINE_MODE=baseline`
- keep the current `processed` and `classified` side-effect queries in baseline mode
- add an optimized branch that uses one `foreachBatch` callback for the classified stream
- persist the batch frame only when needed in the optimized branch
- deduplicate within the batch before enrichment in the optimized branch
- write invalid rows, processed parquet, breach windows, and Redis state from the same callback in the optimized branch
- keep consumer lag polling off the hot path
- keep the Prometheus metric names stable enough for the new dashboard

Update the configmaps so the runtime values are explicit:
- `infra/kubernetes/base/configmap-pipeline.yaml` sets `PIPELINE_MODE: baseline`
- `infra/kubernetes/baseline/configmap-pipeline.yaml` keeps `PIPELINE_MODE: baseline`
- `infra/kubernetes/optimized/configmap-pipeline.yaml` sets:
  - `PIPELINE_MODE: optimized`
  - `TRIGGER_INTERVAL: 1 seconds`
  - `WATERMARK_DELAY: 30 seconds`
  - `CHECKPOINT_ROOT: /data/checkpoints`

Keep the core behavior the same:
- corrupt JSON is still invalid
- missing device and invalid event-time records are still invalid
- unknown devices are still invalid
- valid records still go to Redis and the cold outputs
- breach classification still uses the facility thresholds from the lookup file

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
python3 -m unittest tests.stream.test_job -v
kubectl kustomize infra/kubernetes/optimized >/tmp/vacciguard-optimized.yaml
```

Expected:
- stream job tests pass
- optimized manifest renders cleanly

- [ ] **Step 5: Commit**

```bash
git add services/stream-processor/job.py infra/kubernetes/base/configmap-pipeline.yaml infra/kubernetes/baseline/configmap-pipeline.yaml infra/kubernetes/optimized/configmap-pipeline.yaml infra/kubernetes/optimized/kustomization.yaml tests/stream/test_job.py
git commit -m "feat: optimize stream processor hot path"
```

## Task 3: Make the evaluation controller and run scripts align with managed observability

This task keeps the AWS evaluation flow honest and repeatable across both pipeline targets while the new observability stack is in use.

**Files:**
- Modify: `services/evaluation-controller/controller.py`
- Modify: `services/evaluation-controller/main.py`
- Modify: `scripts/run-aws-evaluation-controller.sh`
- Modify: `scripts/run-aws-baseline-evaluation.sh`
- Modify: `tests/evaluation/test_aws_native_evaluation_controller.py`

- [ ] **Step 1: Write the failing tests**

Extend `tests/evaluation/test_aws_native_evaluation_controller.py` so it checks:
- the controller still resolves baseline and optimized run contracts correctly
- the duration override is honored for 5-minute runs
- the pipeline config patch includes `PIPELINE_MODE`
- the report payload includes enough run metadata to distinguish baseline from optimized
- the AWS evaluation wrapper passes through the selected pipeline mode without breaking the old baseline flow

Example assertions:

```python
def test_build_pipeline_config_patch_sets_pipeline_mode(self):
    contract = controller.resolve_run_contract(
        pipeline_target="optimized",
        scenario="normal",
        run_id="run-123",
        workload_family_version="evaluation-workload-v1",
        bucket_name="vacciguard-data",
    )
    patch = controller.build_pipeline_config_patch(
        contract=contract,
        app_name="vacciguard-stream-processor",
        kafka_bootstrap_servers="kafka:9092",
        kafka_topic_partitions="8",
        trigger_interval="1 seconds",
        watermark_delay="30 seconds",
        redis_host="redis",
        redis_port="6379",
        redis_db="0",
        pipeline_mode="optimized",
    )
    self.assertEqual(patch["data"]["PIPELINE_MODE"], "optimized")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller -v
```

Expected: FAIL where the new pipeline mode handling or run contract handling is not yet implemented.

- [ ] **Step 3: Implement the orchestration updates**

Adjust the evaluation controller and run scripts so they:
- keep producing the formal S3 report for baseline and optimized runs
- capture enough metadata for the dashboard to group runs by pipeline target and run ID
- keep the 5-minute duration override working for normal-load comparisons
- pass `PIPELINE_MODE=baseline` for baseline runs and `PIPELINE_MODE=optimized` for optimized runs
- preserve the existing baseline evaluation path for backward compatibility

Update `services/evaluation-controller/controller.py` so `build_pipeline_config_patch()` emits the pipeline mode and checkpoint settings that match the selected run.

Update the run scripts so the mode is explicit:
- `scripts/run-aws-baseline-evaluation.sh` defaults to baseline mode
- `scripts/run-aws-evaluation-controller.sh` forwards `PIPELINE_MODE` and `WORKLOAD_DURATION_MINUTES` when provided

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/evaluation-controller/main.py services/evaluation-controller/controller.py scripts/run-aws-evaluation-controller.sh scripts/run-aws-baseline-evaluation.sh tests/evaluation/test_aws_native_evaluation_controller.py
git commit -m "feat: align evaluation controller with aws-managed observability"
```

## Task 4: Build the baseline-vs-optimized dashboard and finish the docs

This task makes the AWS observability story usable: the dashboards should show a direct comparison, and the docs should tell the next engineer how to use the system without guessing.

**Files:**
- Create: `infra/monitoring/aws-managed/grafana/dashboards/baseline-vs-optimized.json`
- Modify: `infra/monitoring/aws-managed/README.md`
- Modify: `infra/monitoring/aws-managed/grafana/README.md`
- Modify: `docs/aws-baseline-foundation.md`
- Modify: `infra/monitoring/README.md`
- Modify: `infra/terraform/README.md`
- Modify: `infra/kubernetes/README.md`
- Modify: `tests/monitoring/test_monitoring_manifests.py`

- [ ] **Step 1: Write the failing tests**

Update `tests/monitoring/test_monitoring_manifests.py` so it checks:
- the AWS-managed docs mention AMP, AMG, and CloudWatch clearly
- the new dashboard JSON exists
- the dashboard has panels for the baseline-vs-optimized comparison
- the panel queries reference the same core metrics used by the stream processor and replay producer
- the dashboard does not depend on the old in-cluster Prometheus service for AWS runs

Example panel expectations:

```python
expected_titles = {
    "Baseline vs Optimized Avg Latency",
    "Baseline vs Optimized P95 Latency",
    "Ingest-to-Redis P95",
    "Processed Events",
    "Invalid Events",
    "Deduplicated Events",
    "Breach Events",
    "Consumer Lag",
}
self.assertTrue(expected_titles.issubset({panel["title"] for panel in dashboard["panels"]}))
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
python3 -m unittest tests.monitoring.test_monitoring_manifests -v
```

Expected: FAIL until the AWS-managed dashboard/docs are added.

- [ ] **Step 3: Implement the dashboard and doc updates**

Build a Grafana dashboard that compares baseline and optimized runs on the same screen:
- top row: average latency, p95 latency, ingest-to-Redis p95
- middle row: processed, invalid, deduplicated, and breach counts
- bottom row: consumer lag, replay throughput, and run success signals

Use the dashboard to answer:
- did the optimized path reduce latency enough?
- did the optimization change invalid or dedup rates?
- did Kafka lag stay at zero?
- did the run remain stable under the same load?

Update the docs so they explain:
- local dev still uses the in-cluster Prometheus/Grafana path
- AWS EKS runs use AMP, AMG, and CloudWatch
- baseline and optimized runs share the same observability story
- the evaluation controller writes the formal report to S3, while the managed stack provides live visibility

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
python3 -m unittest tests.monitoring.test_monitoring_manifests -v
python3 -m json.tool infra/monitoring/aws-managed/grafana/dashboards/baseline-vs-optimized.json >/dev/null
```

Expected:
- monitoring tests pass
- AWS-managed dashboard JSON is valid

- [ ] **Step 5: Commit**

```bash
git add infra/monitoring/aws-managed infra/monitoring/README.md docs/aws-baseline-foundation.md infra/terraform/README.md infra/kubernetes/README.md tests/monitoring/test_monitoring_manifests.py
git commit -m "feat: add aws-managed observability dashboard and docs"
```

## Verification Before Hand-Off

Before calling this work done, run the full relevant suite:

```bash
python3 -m unittest tests.stream.test_job -v
python3 -m unittest tests.monitoring.test_monitoring_manifests -v
python3 -m unittest tests.monitoring.test_aws_managed_observability_manifests -v
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller -v
terraform -chdir=infra/terraform validate
kubectl kustomize infra/kubernetes/aws-observability
kubectl kustomize infra/kubernetes/optimized
python3 -m json.tool infra/monitoring/aws-managed/grafana/dashboards/baseline-vs-optimized.json >/dev/null
```

Expected outcome:
- all tests pass
- both Kubernetes overlays render cleanly
- Terraform validates
- the AWS-managed dashboard and the optimized pipeline can be deployed together without breaking the baseline flow
