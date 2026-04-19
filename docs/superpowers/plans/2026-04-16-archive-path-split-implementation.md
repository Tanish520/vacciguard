# Archive Path Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split VacciGuard's optimized stream processor into a hot Redis path and a separate S3 archive path so S3 work no longer blocks alert latency.

**Architecture:** Keep the current Kafka source and transformation logic, but move side effects into two role-specific callbacks. The hot deployment will only build the latest device state and write Redis. The archive deployment will reuse the same validated/enriched DataFrame and write processed, invalid, and breach outputs to S3. Both services stay in the optimized namespace, but they get separate deployments, separate checkpoint roots, and separate readiness/metrics handling so the hot path can be measured independently.

**Tech Stack:** Python 3.11, PySpark Structured Streaming, Kubernetes, Kustomize, `unittest`, AWS evaluation controller, Prometheus metrics.

---

### Task 1: Split the stream processor into hot and archive roles

**Files:**
- Modify: `services/stream-processor/job.py`
- Modify: `tests/stream/test_job.py`

- [ ] **Step 1: Write the failing tests**

Add tests that prove the two roles have different side effects and that query wiring switches by role.

```python
def test_hot_role_writes_redis_only(self):
    with patch.object(stream_job, "pipeline_role", return_value="hot"), \
         patch.object(stream_job, "write_latest_state_to_redis") as mock_redis, \
         patch.object(stream_job, "write_invalid_output_batch") as mock_invalid, \
         patch.object(stream_job, "write_processed_output_batch") as mock_processed, \
         patch.object(stream_job, "write_breach_windows_batch") as mock_breach:
        stream_job.write_hot_batch(self.classified, batch_id=7, lookup_df=self.lookup_df)

    mock_redis.assert_called_once()
    mock_invalid.assert_not_called()
    mock_processed.assert_not_called()
    mock_breach.assert_not_called()


def test_archive_role_writes_s3_only(self):
    with patch.object(stream_job, "pipeline_role", return_value="archive"), \
         patch.object(stream_job, "write_latest_state_to_redis") as mock_redis, \
         patch.object(stream_job, "write_invalid_output_batch") as mock_invalid, \
         patch.object(stream_job, "write_processed_output_batch") as mock_processed, \
         patch.object(stream_job, "write_breach_windows_batch") as mock_breach:
        stream_job.write_archive_batch(self.classified, batch_id=7, lookup_df=self.lookup_df)

    mock_redis.assert_not_called()
    mock_invalid.assert_called_once()
    mock_processed.assert_called_once()
    mock_breach.assert_called_once()
```

Add one query-selection test so the optimized entrypoint routes by role instead of always using the combined callback.

```python
def test_start_queries_uses_pipeline_role_for_optimized_mode(self):
    with patch.object(stream_job, "pipeline_mode", return_value="optimized"), \
         patch.object(stream_job, "pipeline_role", return_value="hot"), \
         patch.object(stream_job.functools, "partial") as mock_partial:
        stream_job.start_queries(None, None, self.classified, self.lookup_df)

    mock_partial.assert_called_once()
```

- [ ] **Step 2: Run the targeted tests and confirm they fail for the right reason**

Run:

```bash
python3 -m unittest tests.stream.test_job.OptimizedBatchWriteTests.test_hot_role_writes_redis_only tests.stream.test_job.OptimizedBatchWriteTests.test_archive_role_writes_s3_only tests.stream.test_job.OptimizedBatchWriteTests.test_start_queries_uses_pipeline_role_for_optimized_mode -v
```

Expected:
- fail because `PIPELINE_ROLE`, `write_hot_batch`, and `write_archive_batch` are not wired yet.

- [ ] **Step 3: Implement the split inside `job.py`**

Introduce a role helper and two role-specific callbacks that both reuse `build_output_streams()`:

```python
PIPELINE_ROLE = os.environ.get("PIPELINE_ROLE", "hot")


def pipeline_role() -> str:
    return os.environ.get("PIPELINE_ROLE", PIPELINE_ROLE).strip().lower()


def write_hot_batch(batch_df: DataFrame, batch_id: int, lookup_df: DataFrame) -> None:
    processed_batch, _invalid_batch = build_output_streams(batch_df, lookup_df)
    write_latest_state_to_redis(processed_batch, batch_id)


def write_archive_batch(batch_df: DataFrame, batch_id: int, lookup_df: DataFrame) -> None:
    processed_batch, invalid_batch = build_output_streams(batch_df, lookup_df)
    write_invalid_output_batch(invalid_batch, batch_id)
    write_processed_output_batch(processed_batch, batch_id)
    write_breach_windows_batch(processed_batch, batch_id)
```

Refactor the S3 helpers so the archive path owns them and the hot path never touches them:

```python
def write_invalid_output_batch(invalid_batch: DataFrame, batch_id: int) -> None:
    if invalid_batch.rdd.isEmpty():
        log.info("Batch %s: no invalid records to append", batch_id)
        return
    invalid_batch.write.mode("append").json(INVALID_OUTPUT_PATH)


def write_processed_output_batch(processed_batch: DataFrame, batch_id: int) -> None:
    if processed_batch.rdd.isEmpty():
        log.info("Batch %s: no processed records to append", batch_id)
        return
    processed_batch.write.mode("append").parquet(PROCESSED_OUTPUT_PATH)


def write_archive_batch(batch_df: DataFrame, batch_id: int, lookup_df: DataFrame) -> None:
    processed_batch, invalid_batch = build_output_streams(batch_df, lookup_df)
    write_invalid_output_batch(invalid_batch, batch_id)
    write_processed_output_batch(processed_batch, batch_id)
    write_breach_windows_batch(processed_batch, batch_id)
```

Update `start_queries()` so optimized mode returns one query for the hot role or one query for the archive role, instead of one mixed query that does both jobs:

```python
def start_queries(processed, invalid, classified, lookup_df):
    if pipeline_mode() == "optimized":
        if pipeline_role() == "hot":
            query = classified.writeStream.foreachBatch(
                functools.partial(write_hot_batch, lookup_df=lookup_df)
            )
        elif pipeline_role() == "archive":
            query = classified.writeStream.foreachBatch(
                functools.partial(write_archive_batch, lookup_df=lookup_df)
            )
        else:
            raise ValueError(f"Unsupported PIPELINE_ROLE={pipeline_role()}")

        return [
            query.outputMode("append")
            .option("checkpointLocation", os.path.join(CHECKPOINT_ROOT, f"{pipeline_role()}_side_effects"))
            .trigger(processingTime=TRIGGER_INTERVAL)
            .start()
        ]
```

- [ ] **Step 4: Run the focused tests and make sure they pass**

Run:

```bash
python3 -m unittest tests.stream.test_job.OptimizedBatchWriteTests.test_hot_role_writes_redis_only tests.stream.test_job.OptimizedBatchWriteTests.test_archive_role_writes_s3_only tests.stream.test_job.OptimizedBatchWriteTests.test_start_queries_uses_pipeline_role_for_optimized_mode -v
```

Expected:
- pass

- [ ] **Step 5: Commit**

```bash
git add services/stream-processor/job.py tests/stream/test_job.py
git commit -m "feat: split optimized stream roles"
```

---

### Task 2: Add separate hot and archive Kubernetes deployments

**Files:**
- Modify: `infra/kubernetes/optimized/kustomization.yaml`
- Modify: `infra/kubernetes/optimized/patch-stream-processor-resources.yaml`
- Add: `infra/kubernetes/optimized/deployment-stream-processor-archive.yaml`
- Modify: `tests/evaluation/test_aws_native_evaluation_manifests.py`

- [ ] **Step 1: Write the failing manifest test**

Add a render check that proves the optimized overlay now contains both deployments and both role labels:

```python
def test_optimized_overlay_renders_hot_and_archive_deployments(self):
    rendered = subprocess.check_output(
        ["kubectl", "kustomize", "infra/kubernetes/optimized"],
        text=True,
    )
    self.assertIn("name: stream-processor", rendered)
    self.assertIn("name: stream-processor-archive", rendered)
    self.assertIn("stream_role: hot", rendered)
    self.assertIn("stream_role: archive", rendered)
    self.assertIn("PIPELINE_ROLE", rendered)
```

- [ ] **Step 2: Run the test and confirm it fails before the manifest split**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_manifests.AwsNativeEvaluationManifestTests.test_optimized_overlay_renders_hot_and_archive_deployments -v
```

Expected:
- fail because only one optimized deployment exists today.

- [ ] **Step 3: Implement the split manifest layout**

Keep the optimized overlay patch for the hot path and add a second deployment resource for the archive path.

Hot deployment patch:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stream-processor
spec:
  template:
    metadata:
      labels:
        app: stream-processor
        stream_role: hot
        pipeline_target: optimized
    spec:
      containers:
        - name: stream-processor
          env:
            - name: PIPELINE_ROLE
              value: hot
            - name: CHECKPOINT_ROOT
              value: /data/checkpoints/hot
```

Archive deployment resource:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stream-processor-archive
  namespace: vacciguard-optimized
spec:
  replicas: 1
  selector:
    matchLabels:
      app: stream-processor-archive
      stream_role: archive
  template:
    metadata:
      labels:
        app: stream-processor-archive
        stream_role: archive
        pipeline_target: optimized
    spec:
      serviceAccountName: vacciguard-pipeline
      containers:
        - name: stream-processor
          image: 347038623570.dkr.ecr.ap-south-1.amazonaws.com/vacciguard-stream-processor:metrics-5m-20260415t222355z-amd64
          imagePullPolicy: Always
          ports:
            - name: metrics
              containerPort: 9108
          volumeMounts:
            - name: lookup-data
              mountPath: /data/reference
          envFrom:
            - configMapRef:
                name: vacciguard-pipeline-config
          env:
            - name: PIPELINE_ROLE
              value: archive
            - name: CHECKPOINT_ROOT
              value: /data/checkpoints/archive
      volumes:
        - name: lookup-data
          configMap:
            name: vacciguard-lookup-data
```

Update `infra/kubernetes/optimized/kustomization.yaml` to include the archive deployment resource alongside the existing overlay resources.

- [ ] **Step 4: Run the render test and confirm both deployments appear**

Run:

```bash
kubectl kustomize infra/kubernetes/optimized
```

Expected:
- `stream-processor` renders with `stream_role: hot`
- `stream-processor-archive` renders with `stream_role: archive`

- [ ] **Step 5: Commit**

```bash
git add infra/kubernetes/optimized/kustomization.yaml infra/kubernetes/optimized/patch-stream-processor-resources.yaml infra/kubernetes/optimized/deployment-stream-processor-archive.yaml tests/evaluation/test_aws_native_evaluation_manifests.py
git commit -m "feat: add archive stream deployment"
```

---

### Task 3: Teach the evaluation controller to manage hot and archive pods separately

**Files:**
- Modify: `services/evaluation-controller/main.py`
- Modify: `tests/evaluation/test_aws_native_evaluation_controller.py`

- [ ] **Step 1: Write the failing controller tests**

Add a test that proves the controller targets the hot pod for metric scraping and can wait for the archive pod separately.

```python
from types import SimpleNamespace


def ready_pod(name: str):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, creation_timestamp=None),
        status=SimpleNamespace(
            conditions=[SimpleNamespace(type="Ready", status="True")],
            pod_ip="127.0.0.1",
        ),
    )


def test_collect_stream_metrics_payload_targets_hot_role(self):
    with patch.object(controller, "latest_stream_pod") as mock_latest:
        mock_latest.return_value = ready_pod("stream-processor-hot")
        controller.collect_stream_metrics_payload(contract, role="hot")

    mock_latest.assert_called_once_with("hot")


def test_wait_for_stream_ready_checks_hot_and_archive_roles(self):
    with patch.object(controller, "latest_stream_pod") as mock_latest, \
         patch.object(controller.time, "time", side_effect=[0, 10, 20]), \
         patch.object(controller.time, "sleep"):
        mock_latest.return_value = ready_pod("stream-processor-hot")
        controller.wait_for_stream_ready(contract, role="hot")


def test_wait_for_stream_ready_checks_archive_role_separately(self):
    with patch.object(controller, "latest_stream_pod") as mock_latest, \
         patch.object(controller.time, "time", side_effect=[0, 10, 20]), \
         patch.object(controller.time, "sleep"):
        mock_latest.return_value = ready_pod("stream-processor-archive")
        controller.wait_for_stream_ready(contract, role="archive")

    mock_latest.assert_called_once_with("archive")
```

- [ ] **Step 2: Run the controller tests and confirm they fail before the role split**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller -v
```

Expected:
- fail because the controller still assumes a single stream processor pod.

- [ ] **Step 3: Implement role-aware controller helpers**

Add a generic selector helper and use it everywhere the controller reaches into the streaming service:

```python
def stream_selector(role: str) -> str:
    return f"app=stream-processor,stream_role={role}"


def latest_stream_pod(role: str = "hot"):
    pods = core_api().list_namespaced_pod(
        NAMESPACE,
        label_selector=stream_selector(role),
    ).items
    if not pods:
        raise RuntimeError(f"No stream-processor pods found for role={role}")
    return max(pods, key=lambda pod: pod.metadata.creation_timestamp or datetime.min.replace(tzinfo=timezone.utc))
```

Update readiness and orchestration so the controller explicitly waits for both roles before launching the replay:

```python
def wait_for_stream_ready(contract: controller.RunContract, role: str = "hot") -> None:
    ...
    pod = latest_stream_pod(role)
    ...


def restart_stream_processors(contract: controller.RunContract) -> None:
    restart_stream_processor("stream-processor")
    restart_stream_processor("stream-processor-archive")
```

Keep metric scraping pointed at the hot pod, since that is the latency-sensitive service:

```python
def collect_stream_metrics_payload(contract: controller.RunContract, role: str = "hot") -> str:
    pod = latest_stream_pod(role)
    ...
```

- [ ] **Step 4: Run the controller tests and verify they pass**

Run:

```bash
python3 -m unittest tests.evaluation.test_aws_native_evaluation_controller -v
```

Expected:
- pass

- [ ] **Step 5: Commit**

```bash
git add services/evaluation-controller/main.py tests/evaluation/test_aws_native_evaluation_controller.py
git commit -m "feat: orchestrate split stream roles"
```

---

### Task 4: Validate the split architecture with a normal 5-minute run

**Files:**
- Verify: `services/stream-processor/job.py`
- Verify: `infra/kubernetes/optimized/*`
- Verify: `services/evaluation-controller/main.py`

- [ ] **Step 1: Run the targeted unit and manifest checks**

Run:

```bash
python3 -m unittest tests.stream.test_job -v
python3 -m unittest tests.evaluation.test_aws_native_evaluation_manifests tests.evaluation.test_aws_native_evaluation_controller -v
kubectl kustomize infra/kubernetes/optimized
```

Expected:
- all tests pass
- the rendered overlay includes both hot and archive deployments

- [ ] **Step 2: Run the optimized normal workload for 5 minutes**

Run:

```bash
WORKLOAD_DURATION_MINUTES=5 bash scripts/run-aws-evaluation-controller.sh optimized normal 20260416-archive-split-normal
```

Expected:
- pipeline succeeds
- hot-path latency is lower than the current single-callback optimized run
- S3 outputs are still complete and correctly counted

- [ ] **Step 3: Compare the report to the previous optimized normal run**

Check these fields in the new report:

- `avg_end_to_end_latency_seconds`
- `p95_end_to_end_latency_seconds`
- `ingest_to_redis_p95_seconds`
- `processed_events`
- `invalid_events`
- `deduplicated_events`
- `breach_events`
- `consumer_lag_records`
- `processed_output_objects`
- `invalid_output_objects`
- `breach_window_output_objects`

Use the previous optimized normal run as the comparison baseline:

- `20260416-optimized-5m-normal-run4`

- [ ] **Step 4: Commit**

```bash
git add services/stream-processor/job.py infra/kubernetes/optimized/kustomization.yaml infra/kubernetes/optimized/patch-stream-processor-resources.yaml infra/kubernetes/optimized/deployment-stream-processor-archive.yaml tests/stream/test_job.py tests/evaluation/test_aws_native_evaluation_controller.py tests/evaluation/test_aws_native_evaluation_manifests.py services/evaluation-controller/main.py
git commit -m "docs: validate archive split implementation"
```

## Self-Review Notes

- The plan keeps the current Kafka source and transformation logic intact.
- The hot path no longer owns S3 writes.
- The archive path no longer owns Redis writes.
- The Kubernetes changes are split by role, not by unrelated refactors.
- The controller changes are limited to orchestration and pod selection.
- The validation step includes both unit coverage and a real 5-minute normal evaluation run.
