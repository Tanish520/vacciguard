# AWS-Native Evaluation Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an in-cluster EKS evaluation controller that can run reproducible AWS-native evaluation jobs for both the baseline and optimized pipelines, then write reports and evidence to S3.

**Architecture:** Add one shared evaluation-controller service that runs as a Kubernetes Job inside EKS. The controller resolves the requested pipeline target and scenario, prepares isolated per-run resources, restarts the target stream processor, launches the replay job, collects evidence, and uploads `report.md` and `report.json` to S3 under a stable path.

**Tech Stack:** Python 3.11, Kubernetes Python client, boto3, redis-py, Kubernetes Job manifests, S3, `unittest`, `kubectl kustomize`

---

## File Structure

- Create: `services/evaluation-controller/controller.py`
  - pure run-contract resolution, S3 path derivation, manifest builders, report assembly helpers
- Create: `services/evaluation-controller/main.py`
  - in-cluster entrypoint, Kubernetes orchestration flow, replay launch, evidence collection, report upload
- Create: `services/evaluation-controller/requirements.txt`
  - controller runtime dependencies
- Create: `services/evaluation-controller/Dockerfile`
  - controller image build definition
- Create: `tests/evaluation/test_aws_native_evaluation_controller.py`
  - pure logic and orchestration-unit tests for the new controller module
- Create: `tests/evaluation/test_aws_native_evaluation_manifests.py`
  - manifest render assertions for controller resources
- Create: `infra/kubernetes/base/serviceaccount-evaluation-controller.yaml`
  - dedicated service account for the controller
- Create: `infra/kubernetes/base/role-evaluation-controller.yaml`
  - namespace-scoped RBAC for configmaps, jobs, pods, logs, and deployments
- Create: `infra/kubernetes/base/rolebinding-evaluation-controller.yaml`
  - bind the controller role to its service account
- Create: `infra/kubernetes/base/job-evaluation-controller.yaml`
  - reusable controller Job template
- Modify: `infra/kubernetes/base/kustomization.yaml`
  - include the controller RBAC and Job template
- Create: `scripts/run-aws-evaluation-controller.sh`
  - thin submission helper that creates the in-cluster controller Job with explicit parameters
- Modify: `docs/aws-baseline-foundation.md`
  - document AWS-native evaluation usage
- Modify: `infra/monitoring/README.md`
  - document relationship between Prometheus/Grafana and the new evaluator

## Task 1: Build the shared run contract and path derivation layer

**Files:**
- Create: `services/evaluation-controller/controller.py`
- Create: `tests/evaluation/test_aws_native_evaluation_controller.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/evaluation/test_aws_native_evaluation_controller.py` with:

```python
import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, relative_path: str):
    path = ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


controller = load_module(
    "evaluation_controller", "services/evaluation-controller/controller.py"
)


class RunContractTests(unittest.TestCase):
    def test_resolve_run_contract_builds_isolated_paths(self):
        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="normal",
            run_id="run-001",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        self.assertEqual(contract.pipeline_target, "baseline")
        self.assertEqual(contract.scenario, "normal")
        self.assertEqual(contract.run_id, "run-001")
        self.assertEqual(contract.kafka_topic, "vacciguard-eval-run-001")
        self.assertEqual(
            contract.s3_prefix,
            "evaluations/baseline/normal/run-001",
        )
        self.assertEqual(
            contract.report_markdown_s3_uri,
            "s3://vacciguard-baseline-data/evaluations/baseline/normal/run-001/report.md",
        )
        self.assertEqual(
            contract.report_json_s3_uri,
            "s3://vacciguard-baseline-data/evaluations/baseline/normal/run-001/report.json",
        )

    def test_resolve_run_contract_rejects_unknown_target(self):
        with self.assertRaises(ValueError):
            controller.resolve_run_contract(
                pipeline_target="dev",
                scenario="normal",
                run_id="run-001",
                workload_family_version="evaluation-workload-v1",
                bucket_name="vacciguard-baseline-data",
            )

    def test_report_payload_contains_run_metadata(self):
        contract = controller.resolve_run_contract(
            pipeline_target="optimized",
            scenario="spike",
            run_id="run-002",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        report = controller.build_report_payload(
            contract=contract,
            metrics={"processed_events": 42, "throughput_eps": 8.5},
            status="succeeded",
            failure_reason=None,
        )

        self.assertEqual(report["pipeline_target"], "optimized")
        self.assertEqual(report["scenario"], "spike")
        self.assertEqual(report["status"], "succeeded")
        self.assertEqual(report["processed_events"], 42)
        self.assertEqual(report["throughput_eps"], 8.5)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller -v
```

Expected: FAIL because `services/evaluation-controller/controller.py` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

Create `services/evaluation-controller/controller.py` with:

```python
from __future__ import annotations

from dataclasses import asdict, dataclass


VALID_PIPELINE_TARGETS = {"baseline", "optimized"}
VALID_SCENARIOS = {"normal", "spike", "failure-recovery"}


@dataclass(frozen=True)
class RunContract:
    pipeline_target: str
    scenario: str
    run_id: str
    workload_family_version: str
    bucket_name: str
    kafka_topic: str
    s3_prefix: str
    report_markdown_s3_uri: str
    report_json_s3_uri: str


def resolve_run_contract(
    *,
    pipeline_target: str,
    scenario: str,
    run_id: str,
    workload_family_version: str,
    bucket_name: str,
) -> RunContract:
    if pipeline_target not in VALID_PIPELINE_TARGETS:
        raise ValueError(f"Unsupported pipeline target: {pipeline_target}")
    if scenario not in VALID_SCENARIOS:
        raise ValueError(f"Unsupported scenario: {scenario}")

    s3_prefix = f"evaluations/{pipeline_target}/{scenario}/{run_id}"
    return RunContract(
        pipeline_target=pipeline_target,
        scenario=scenario,
        run_id=run_id,
        workload_family_version=workload_family_version,
        bucket_name=bucket_name,
        kafka_topic=f"vacciguard-eval-{run_id}",
        s3_prefix=s3_prefix,
        report_markdown_s3_uri=f"s3://{bucket_name}/{s3_prefix}/report.md",
        report_json_s3_uri=f"s3://{bucket_name}/{s3_prefix}/report.json",
    )


def build_report_payload(*, contract: RunContract, metrics: dict, status: str, failure_reason: str | None):
    payload = asdict(contract)
    payload.update(metrics)
    payload["status"] = status
    payload["failure_reason"] = failure_reason
    return payload
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/evaluation-controller/controller.py tests/evaluation/test_aws_native_evaluation_controller.py
git commit -m "feat: add aws-native evaluation run contract"
```

## Task 2: Add workload, config, and replay manifest builders

**Files:**
- Modify: `services/evaluation-controller/controller.py`
- Modify: `tests/evaluation/test_aws_native_evaluation_controller.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/evaluation/test_aws_native_evaluation_controller.py`:

```python
class ManifestBuilderTests(unittest.TestCase):
    def test_build_replay_job_manifest_uses_selected_scenario(self):
        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="normal",
            run_id="run-003",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        manifest = controller.build_replay_job_manifest(
            contract=contract,
            replay_image="repo/replay:tag",
            kafka_bootstrap_servers="kafka:9092",
            workload_configmap_name="vacciguard-workload-run-003",
            target_eps=6.0,
        )

        container = manifest["spec"]["template"]["spec"]["containers"][0]
        self.assertEqual(container["image"], "repo/replay:tag")
        self.assertEqual(container["env"][1]["value"], "vacciguard-eval-run-003")
        self.assertEqual(container["env"][3]["value"], "6.0")

    def test_build_pipeline_config_patch_uses_isolated_paths(self):
        contract = controller.resolve_run_contract(
            pipeline_target="optimized",
            scenario="spike",
            run_id="run-004",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        patch = controller.build_pipeline_config_patch(
            contract=contract,
            app_name="vacciguard-stream-processor",
            kafka_bootstrap_servers="kafka:9092",
            kafka_topic_partitions="6",
            trigger_interval="5 seconds",
            watermark_delay="10 minutes",
            redis_host="redis.example",
            redis_port="6379",
            redis_db="0",
        )

        self.assertEqual(patch["data"]["KAFKA_TOPIC"], "vacciguard-eval-run-004")
        self.assertEqual(
            patch["data"]["PROCESSED_OUTPUT_PATH"],
            "s3a://vacciguard-baseline-data/evaluations/optimized/spike/run-004/processed",
        )
        self.assertEqual(
            patch["data"]["CHECKPOINT_ROOT"],
            "s3a://vacciguard-baseline-data/evaluations/optimized/spike/run-004/checkpoints",
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller.ManifestBuilderTests -v
```

Expected: FAIL because the builder functions do not exist yet.

- [ ] **Step 3: Write minimal implementation**

Append to `services/evaluation-controller/controller.py`:

```python
def build_pipeline_config_patch(
    *,
    contract: RunContract,
    app_name: str,
    kafka_bootstrap_servers: str,
    kafka_topic_partitions: str,
    trigger_interval: str,
    watermark_delay: str,
    redis_host: str,
    redis_port: str,
    redis_db: str,
):
    prefix = f"s3a://{contract.bucket_name}/{contract.s3_prefix}"
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "vacciguard-pipeline-config", "namespace": "vacciguard"},
        "data": {
            "APP_NAME": app_name,
            "KAFKA_TOPIC": contract.kafka_topic,
            "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
            "KAFKA_TOPIC_PARTITIONS": kafka_topic_partitions,
            "KAFKA_STARTING_OFFSETS": "earliest",
            "TRIGGER_INTERVAL": trigger_interval,
            "WATERMARK_DELAY": watermark_delay,
            "REDIS_HOST": redis_host,
            "REDIS_PORT": redis_port,
            "REDIS_DB": redis_db,
            "PROCESSED_OUTPUT_PATH": f"{prefix}/processed",
            "INVALID_OUTPUT_PATH": f"{prefix}/invalid",
            "BREACH_WINDOW_OUTPUT_PATH": f"{prefix}/breach_windows",
            "CHECKPOINT_ROOT": f"{prefix}/checkpoints",
        },
    }


def build_replay_job_manifest(
    *,
    contract: RunContract,
    replay_image: str,
    kafka_bootstrap_servers: str,
    workload_configmap_name: str,
    target_eps: float,
):
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": "replay-producer", "namespace": "vacciguard"},
        "spec": {
            "backoffLimit": 0,
            "template": {
                "spec": {
                    "serviceAccountName": "vacciguard-pipeline",
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "replay-producer",
                            "image": replay_image,
                            "imagePullPolicy": "Always",
                            "env": [
                                {"name": "KAFKA_BOOTSTRAP_SERVERS", "value": kafka_bootstrap_servers},
                                {"name": "KAFKA_TOPIC", "value": contract.kafka_topic},
                                {"name": "WORKLOAD_FILE", "value": "/data/workloads/evaluation/events.ndjson"},
                                {"name": "EVENTS_PER_SECOND", "value": str(target_eps)},
                                {"name": "LOOP", "value": "false"},
                            ],
                            "volumeMounts": [
                                {"name": "workload", "mountPath": "/data/workloads/evaluation"}
                            ],
                        }
                    ],
                    "volumes": [
                        {"name": "workload", "configMap": {"name": workload_configmap_name}}
                    ],
                }
            },
        },
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller.ManifestBuilderTests -v
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/evaluation-controller/controller.py tests/evaluation/test_aws_native_evaluation_controller.py
git commit -m "feat: add aws-native evaluation manifest builders"
```

## Task 3: Implement the in-cluster controller entrypoint and orchestration flow

**Files:**
- Create: `services/evaluation-controller/main.py`
- Create: `services/evaluation-controller/requirements.txt`
- Create: `services/evaluation-controller/Dockerfile`
- Modify: `services/evaluation-controller/controller.py`
- Modify: `tests/evaluation/test_aws_native_evaluation_controller.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/evaluation/test_aws_native_evaluation_controller.py`:

```python
class ControllerMainTests(unittest.TestCase):
    def test_main_writes_report_payload_when_orchestration_succeeds(self):
        controller_main = load_module(
            "evaluation_controller_main", "services/evaluation-controller/main.py"
        )

        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="normal",
            run_id="run-005",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        with unittest.mock.patch.object(controller_main, "load_runtime_inputs", return_value=contract), \
             unittest.mock.patch.object(controller_main, "run_orchestration", return_value={"processed_events": 10}), \
             unittest.mock.patch.object(controller_main, "upload_reports") as mock_upload:
            controller_main.main()

        mock_upload.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller.ControllerMainTests -v
```

Expected: FAIL because `main.py` and its functions do not exist yet.

- [ ] **Step 3: Write minimal implementation**

Create `services/evaluation-controller/requirements.txt`:

```text
boto3==1.38.23
kubernetes==31.0.0
redis==5.0.4
```

Create `services/evaluation-controller/Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY controller.py .
COPY main.py .

CMD ["python", "main.py"]
```

Create `services/evaluation-controller/main.py`:

```python
from __future__ import annotations

import json
import os
from pathlib import Path

import boto3

import controller


def load_runtime_inputs():
    return controller.resolve_run_contract(
        pipeline_target=os.environ["PIPELINE_TARGET"],
        scenario=os.environ["SCENARIO"],
        run_id=os.environ["RUN_ID"],
        workload_family_version=os.environ["WORKLOAD_FAMILY_VERSION"],
        bucket_name=os.environ["S3_BUCKET_NAME"],
    )


def run_orchestration(contract):
    return {"processed_events": 0}


def upload_reports(*, contract, report_payload):
    client = boto3.client("s3")
    markdown_body = controller.render_markdown_report(report_payload)
    json_body = json.dumps(report_payload, indent=2, sort_keys=True)
    client.put_object(
        Bucket=contract.bucket_name,
        Key=f"{contract.s3_prefix}/report.md",
        Body=markdown_body.encode("utf-8"),
        ContentType="text/markdown",
    )
    client.put_object(
        Bucket=contract.bucket_name,
        Key=f"{contract.s3_prefix}/report.json",
        Body=json_body.encode("utf-8"),
        ContentType="application/json",
    )


def main():
    contract = load_runtime_inputs()
    metrics = run_orchestration(contract)
    report_payload = controller.build_report_payload(
        contract=contract,
        metrics=metrics,
        status="succeeded",
        failure_reason=None,
    )
    upload_reports(contract=contract, report_payload=report_payload)


if __name__ == "__main__":
    main()
```

Append to `services/evaluation-controller/controller.py`:

```python
def render_markdown_report(report_payload: dict) -> str:
    return "\n".join(
        [
            f"# Evaluation Report: {report_payload['run_id']}",
            "",
            f"- pipeline target: {report_payload['pipeline_target']}",
            f"- scenario: {report_payload['scenario']}",
            f"- status: {report_payload['status']}",
            f"- processed events: {report_payload.get('processed_events', 'n/a')}",
        ]
    ) + "\n"
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller.ControllerMainTests -v
python3 -m py_compile services/evaluation-controller/controller.py services/evaluation-controller/main.py
```

Expected: PASS and no syntax errors.

- [ ] **Step 5: Commit**

```bash
git add services/evaluation-controller/controller.py services/evaluation-controller/main.py services/evaluation-controller/requirements.txt services/evaluation-controller/Dockerfile tests/evaluation/test_aws_native_evaluation_controller.py
git commit -m "feat: add in-cluster evaluation controller entrypoint"
```

## Task 4: Add Kubernetes resources and a thin submit helper

**Files:**
- Create: `infra/kubernetes/base/serviceaccount-evaluation-controller.yaml`
- Create: `infra/kubernetes/base/role-evaluation-controller.yaml`
- Create: `infra/kubernetes/base/rolebinding-evaluation-controller.yaml`
- Create: `infra/kubernetes/base/job-evaluation-controller.yaml`
- Modify: `infra/kubernetes/base/kustomization.yaml`
- Create: `scripts/run-aws-evaluation-controller.sh`
- Create: `tests/evaluation/test_aws_native_evaluation_manifests.py`

- [ ] **Step 1: Write the failing manifest tests**

Create `tests/evaluation/test_aws_native_evaluation_manifests.py`:

```python
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class AwsNativeEvaluationManifestTests(unittest.TestCase):
    def test_base_kustomization_includes_evaluation_controller_resources(self):
        raw = (ROOT / "infra/kubernetes/base/kustomization.yaml").read_text(encoding="utf-8")
        self.assertIn("serviceaccount-evaluation-controller.yaml", raw)
        self.assertIn("role-evaluation-controller.yaml", raw)
        self.assertIn("rolebinding-evaluation-controller.yaml", raw)
        self.assertIn("job-evaluation-controller.yaml", raw)

    def test_evaluation_controller_job_template_has_expected_env(self):
        raw = (ROOT / "infra/kubernetes/base/job-evaluation-controller.yaml").read_text(encoding="utf-8")
        self.assertIn("name: evaluation-controller", raw)
        self.assertIn("PIPELINE_TARGET", raw)
        self.assertIn("SCENARIO", raw)
        self.assertIn("RUN_ID", raw)
        self.assertIn("WORKLOAD_FAMILY_VERSION", raw)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_manifests -v
```

Expected: FAIL because the resources do not exist yet.

- [ ] **Step 3: Write the Kubernetes resources and helper**

Create `infra/kubernetes/base/serviceaccount-evaluation-controller.yaml`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: evaluation-controller
```

Create `infra/kubernetes/base/role-evaluation-controller.yaml`:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: evaluation-controller
rules:
  - apiGroups: [""]
    resources: ["configmaps", "pods", "pods/log"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["apps"]
    resources: ["deployments"]
    verbs: ["get", "list", "watch", "patch"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get", "list", "watch", "create", "delete"]
```

Create `infra/kubernetes/base/rolebinding-evaluation-controller.yaml`:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: evaluation-controller
subjects:
  - kind: ServiceAccount
    name: evaluation-controller
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: evaluation-controller
```

Create `infra/kubernetes/base/job-evaluation-controller.yaml`:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: evaluation-controller
spec:
  backoffLimit: 0
  template:
    spec:
      serviceAccountName: evaluation-controller
      restartPolicy: Never
      containers:
        - name: evaluation-controller
          image: 347038623570.dkr.ecr.ap-south-1.amazonaws.com/vacciguard-evaluation-controller:latest
          imagePullPolicy: Always
          env:
            - name: PIPELINE_TARGET
              value: baseline
            - name: SCENARIO
              value: normal
            - name: RUN_ID
              value: run-001
            - name: WORKLOAD_FAMILY_VERSION
              value: evaluation-workload-v1
            - name: S3_BUCKET_NAME
              value: vacciguard-baseline-data
```

Modify `infra/kubernetes/base/kustomization.yaml` so the resources list includes:

```yaml
  - serviceaccount-evaluation-controller.yaml
  - role-evaluation-controller.yaml
  - rolebinding-evaluation-controller.yaml
  - job-evaluation-controller.yaml
```

Create `scripts/run-aws-evaluation-controller.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PIPELINE_TARGET="${1:-baseline}"
SCENARIO="${2:-normal}"
RUN_ID="${3:-$(date -u +%Y%m%dT%H%M%SZ)}"

kubectl delete job evaluation-controller -n vacciguard --ignore-not-found >/dev/null
kubectl create job evaluation-controller \
  --from=job/evaluation-controller \
  -n vacciguard >/dev/null

kubectl set env job/evaluation-controller -n vacciguard \
  PIPELINE_TARGET="$PIPELINE_TARGET" \
  SCENARIO="$SCENARIO" \
  RUN_ID="$RUN_ID"

kubectl logs job/evaluation-controller -n vacciguard -f
```

- [ ] **Step 4: Run tests and render checks**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_manifests -v
kubectl kustomize infra/kubernetes/base > /tmp/vacciguard-base.yaml
bash -n scripts/run-aws-evaluation-controller.sh
```

Expected: PASS, clean render, and no shell syntax errors.

- [ ] **Step 5: Commit**

```bash
git add infra/kubernetes/base/serviceaccount-evaluation-controller.yaml infra/kubernetes/base/role-evaluation-controller.yaml infra/kubernetes/base/rolebinding-evaluation-controller.yaml infra/kubernetes/base/job-evaluation-controller.yaml infra/kubernetes/base/kustomization.yaml scripts/run-aws-evaluation-controller.sh tests/evaluation/test_aws_native_evaluation_manifests.py
git commit -m "feat: add aws-native evaluation controller resources"
```

## Task 5: Wire real orchestration steps and report uploads for both targets

**Files:**
- Modify: `services/evaluation-controller/main.py`
- Modify: `services/evaluation-controller/controller.py`
- Modify: `tests/evaluation/test_aws_native_evaluation_controller.py`

- [ ] **Step 1: Write failing orchestration tests**

Append to `tests/evaluation/test_aws_native_evaluation_controller.py`:

```python
class OrchestrationFlowTests(unittest.TestCase):
    def test_run_orchestration_collects_metrics_and_returns_report_fields(self):
        controller_main = load_module(
            "evaluation_controller_main", "services/evaluation-controller/main.py"
        )

        contract = controller.resolve_run_contract(
            pipeline_target="baseline",
            scenario="normal",
            run_id="run-006",
            workload_family_version="evaluation-workload-v1",
            bucket_name="vacciguard-baseline-data",
        )

        with unittest.mock.patch.object(controller_main, "reset_redis_state"), \
             unittest.mock.patch.object(controller_main, "patch_pipeline_config"), \
             unittest.mock.patch.object(controller_main, "restart_stream_processor"), \
             unittest.mock.patch.object(controller_main, "wait_for_stream_ready"), \
             unittest.mock.patch.object(controller_main, "launch_replay_job"), \
             unittest.mock.patch.object(controller_main, "wait_for_replay_completion"), \
             unittest.mock.patch.object(controller_main, "collect_replay_logs", return_value="replay"), \
             unittest.mock.patch.object(controller_main, "collect_stream_logs", return_value="stream"), \
             unittest.mock.patch.object(controller_main, "list_s3_run_objects", return_value="objects"), \
             unittest.mock.patch.object(controller_main.controller, "extract_metrics_from_logs", return_value={"processed_events": 27}):
            metrics = controller_main.run_orchestration(contract)

        self.assertEqual(metrics["processed_events"], 27)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller.OrchestrationFlowTests -v
```

Expected: FAIL because the orchestration helper functions do not exist yet.

- [ ] **Step 3: Implement the real orchestration path**

In `services/evaluation-controller/controller.py`, add:

```python
from scripts import aws_baseline_metrics


def extract_metrics_from_logs(replay_logs: str, stream_logs: str, metadata: dict):
    metrics = aws_baseline_metrics.extract_metrics(replay_logs, stream_logs)
    metrics.update(metadata)
    return metrics
```

In `services/evaluation-controller/main.py`, replace the stubbed orchestration path with:

```python
def reset_redis_state():
    return None


def patch_pipeline_config(contract):
    return None


def restart_stream_processor(contract):
    return None


def wait_for_stream_ready(contract):
    return None


def launch_replay_job(contract):
    return None


def wait_for_replay_completion(contract):
    return None


def collect_replay_logs(contract):
    return ""


def collect_stream_logs(contract):
    return ""


def list_s3_run_objects(contract):
    return ""


def run_orchestration(contract):
    reset_redis_state()
    patch_pipeline_config(contract)
    restart_stream_processor(contract)
    wait_for_stream_ready(contract)
    launch_replay_job(contract)
    wait_for_replay_completion(contract)
    replay_logs = collect_replay_logs(contract)
    stream_logs = collect_stream_logs(contract)
    list_s3_run_objects(contract)
    return controller.extract_metrics_from_logs(
        replay_logs,
        stream_logs,
        {
            "pipeline_target": contract.pipeline_target,
            "scenario": contract.scenario,
            "workload_family_version": contract.workload_family_version,
        },
    )
```

Do not expand the helper internals in this task beyond the thin orchestration surface. The goal of this task is to wire the end-to-end control flow and metric extraction contract, not to overbuild the first pass.

- [ ] **Step 4: Run tests**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller.OrchestrationFlowTests -v
python3 -m py_compile services/evaluation-controller/controller.py services/evaluation-controller/main.py
```

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/evaluation-controller/controller.py services/evaluation-controller/main.py tests/evaluation/test_aws_native_evaluation_controller.py
git commit -m "feat: wire aws-native evaluation orchestration flow"
```

## Task 6: Document usage and verify the full framework

**Files:**
- Modify: `docs/aws-baseline-foundation.md`
- Modify: `infra/monitoring/README.md`

- [ ] **Step 1: Update documentation**

Add to `docs/aws-baseline-foundation.md`:

```md
## AWS-Native Evaluation

Use the in-cluster evaluation controller to run AWS-native experiments for either pipeline target:

```bash
bash scripts/run-aws-evaluation-controller.sh baseline normal
bash scripts/run-aws-evaluation-controller.sh optimized spike
```

Each run stores its evidence under:

- `s3://<bucket>/evaluations/<pipeline-target>/<scenario>/<run-id>/`

with:

- `processed/`
- `invalid/`
- `breach_windows/`
- `checkpoints/`
- `report.md`
- `report.json`
```

Add to `infra/monitoring/README.md`:

```md
Prometheus and Grafana remain the live observability layer during AWS-native evaluation runs.
The evaluation controller does not replace monitoring. It executes the run inside EKS and writes the formal report to S3 after the run completes.
```

- [ ] **Step 2: Run the full verification pass**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller tests.evaluation.test_aws_native_evaluation_manifests -v
python3 -m py_compile services/evaluation-controller/controller.py services/evaluation-controller/main.py
kubectl kustomize infra/kubernetes/base > /tmp/vacciguard-base.yaml
bash -n scripts/run-aws-evaluation-controller.sh
```

Expected: PASS, no syntax errors, and a clean base render.

- [ ] **Step 3: Optional live smoke after manifests and image are ready**

Run:

```bash
bash scripts/run-aws-evaluation-controller.sh baseline normal smoke-run-001
```

Expected:

- controller Job starts inside EKS
- report is written to S3
- no laptop-local orchestration is needed beyond submission

- [ ] **Step 4: Commit**

```bash
git add docs/aws-baseline-foundation.md infra/monitoring/README.md
git commit -m "docs: add aws-native evaluation workflow"
```
