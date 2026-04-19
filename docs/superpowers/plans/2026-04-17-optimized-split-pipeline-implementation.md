# Optimized Split Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current single-process optimized pipeline with split `optimized-hot` and `optimized-cold` services while keeping Redis/S3/report outputs compatible and making the hot path viable for spike-load latency targets.

**Architecture:** The optimized overlay will stop inheriting a single stream-processor deployment and instead run two deployments from the same image with different roles and config. The evaluation controller will orchestrate both services as one optimized target, scrape hot-owned SLA metrics and cold-owned archive metrics separately, then merge them into one unchanged external report shape.

**Tech Stack:** Python, PySpark Structured Streaming, Redis, Kafka, Kubernetes manifests via Kustomize, pytest/unittest

---

## File Structure

**Create:**
- `infra/kubernetes/optimized/configmap-pipeline-hot.yaml`
- `infra/kubernetes/optimized/configmap-pipeline-cold.yaml`
- `infra/kubernetes/optimized/deployment-stream-processor-hot.yaml`
- `infra/kubernetes/optimized/deployment-stream-processor-cold.yaml`
- `infra/kubernetes/optimized/patch-optimized-hot-resources.yaml`
- `infra/kubernetes/optimized/patch-optimized-cold-resources.yaml`
- `docs/superpowers/plans/2026-04-17-optimized-split-pipeline-implementation.md`

**Modify:**
- `infra/kubernetes/optimized/kustomization.yaml`
- `services/evaluation-controller/main.py`
- `services/evaluation-controller/controller.py`
- `services/evaluation-controller/aws_baseline_metrics.py`
- `services/stream-processor/job.py`
- `tests/evaluation/test_aws_native_evaluation_manifests.py`
- `tests/evaluation/test_aws_native_evaluation_controller.py`
- `tests/stream/test_job.py`

**Testing note:**
- Local Spark tests currently fail in this environment when the driver and worker use different Python minor versions. For stream tests, run them with the same interpreter for both:

```bash
PYSPARK_PYTHON=$(which python3) PYSPARK_DRIVER_PYTHON=$(which python3) pytest tests/stream/test_job.py -q
```

### Task 1: Split The Optimized Kubernetes Overlay

**Files:**
- Create: `infra/kubernetes/optimized/configmap-pipeline-hot.yaml`
- Create: `infra/kubernetes/optimized/configmap-pipeline-cold.yaml`
- Create: `infra/kubernetes/optimized/deployment-stream-processor-hot.yaml`
- Create: `infra/kubernetes/optimized/deployment-stream-processor-cold.yaml`
- Create: `infra/kubernetes/optimized/patch-optimized-hot-resources.yaml`
- Create: `infra/kubernetes/optimized/patch-optimized-cold-resources.yaml`
- Modify: `infra/kubernetes/optimized/kustomization.yaml`
- Test: `tests/evaluation/test_aws_native_evaluation_manifests.py`

- [ ] **Step 1: Write the failing manifest test**

```python
def test_optimized_overlay_defines_split_hot_and_cold_services(self):
    kustomization_raw = (
        ROOT / "infra/kubernetes/optimized/kustomization.yaml"
    ).read_text(encoding="utf-8")
    hot_configmap_raw = (
        ROOT / "infra/kubernetes/optimized/configmap-pipeline-hot.yaml"
    ).read_text(encoding="utf-8")
    cold_configmap_raw = (
        ROOT / "infra/kubernetes/optimized/configmap-pipeline-cold.yaml"
    ).read_text(encoding="utf-8")

    self.assertIn("deployment-stream-processor-hot.yaml", kustomization_raw)
    self.assertIn("deployment-stream-processor-cold.yaml", kustomization_raw)
    self.assertIn("PIPELINE_MODE: optimized", hot_configmap_raw)
    self.assertIn("PIPELINE_SERVICE_ROLE: hot", hot_configmap_raw)
    self.assertIn('MAX_OFFSETS_PER_TRIGGER: "2000"', hot_configmap_raw)
    self.assertIn("PIPELINE_SERVICE_ROLE: cold", cold_configmap_raw)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/evaluation/test_aws_native_evaluation_manifests.py -q
```

Expected:
- FAIL because the optimized overlay does not yet define separate hot/cold deployments or role-specific config maps.

- [ ] **Step 3: Implement the optimized split overlay**

Create the hot config map with explicit hot-path settings:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: vacciguard-pipeline-config-hot
  namespace: vacciguard
data:
  PIPELINE_MODE: optimized
  PIPELINE_SERVICE_ROLE: hot
  MAX_OFFSETS_PER_TRIGGER: "2000"
  TRIGGER_INTERVAL: 1 seconds
  WATERMARK_DELAY: 30 seconds
  CHECKPOINT_ROOT: /data/checkpoints/hot
```

Create the cold config map with slower archival settings:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: vacciguard-pipeline-config-cold
  namespace: vacciguard
data:
  PIPELINE_MODE: optimized
  PIPELINE_SERVICE_ROLE: cold
  COLD_TRIGGER_INTERVAL: 30 seconds
  CHECKPOINT_ROOT: /data/checkpoints/cold
```

Point `infra/kubernetes/optimized/kustomization.yaml` at the two deployments and resource patches instead of relying on the old single optimized patch set:

```yaml
resources:
  - ../baseline
  - configmap-pipeline-hot.yaml
  - configmap-pipeline-cold.yaml
  - deployment-stream-processor-hot.yaml
  - deployment-stream-processor-cold.yaml
patches:
  - path: patch-kafka-resources.yaml
  - path: patch-optimized-hot-resources.yaml
  - path: patch-optimized-cold-resources.yaml
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/evaluation/test_aws_native_evaluation_manifests.py -q
```

Expected:
- PASS with the new optimized hot/cold manifest assertions and no regressions in the existing overlay checks.

- [ ] **Step 5: Commit**

```bash
git add \
  infra/kubernetes/optimized/configmap-pipeline-hot.yaml \
  infra/kubernetes/optimized/configmap-pipeline-cold.yaml \
  infra/kubernetes/optimized/deployment-stream-processor-hot.yaml \
  infra/kubernetes/optimized/deployment-stream-processor-cold.yaml \
  infra/kubernetes/optimized/patch-optimized-hot-resources.yaml \
  infra/kubernetes/optimized/patch-optimized-cold-resources.yaml \
  infra/kubernetes/optimized/kustomization.yaml \
  tests/evaluation/test_aws_native_evaluation_manifests.py
git commit -m "feat: split optimized kubernetes overlay"
```

### Task 2: Teach The Evaluation Controller About Dual Optimized Services

**Files:**
- Modify: `services/evaluation-controller/main.py`
- Modify: `services/evaluation-controller/controller.py`
- Modify: `services/evaluation-controller/aws_baseline_metrics.py`
- Test: `tests/evaluation/test_aws_native_evaluation_controller.py`

- [ ] **Step 1: Write the failing controller test**

```python
def test_optimized_run_collects_hot_latency_and_cold_counts_separately(self):
    contract = controller.resolve_run_contract(
        pipeline_target="optimized",
        scenario="spike",
        run_id="run-optimized-split",
        workload_family_version="evaluation-workload-v1",
        bucket_name="vacciguard-baseline-data",
    )

    hot_metrics = {
        "avg_end_to_end_latency_seconds": 3.4,
        "p95_end_to_end_latency_seconds": 4.1,
        "ingest_to_redis_p95_seconds": 4.0,
    }
    cold_metrics = {
        "processed_events": 323000,
        "invalid_events": 6000,
        "deduplicated_events": 700,
        "breach_events": 4900,
    }

    merged = controller.merge_optimized_metrics(hot_metrics, cold_metrics)

    self.assertEqual(merged["avg_end_to_end_latency_seconds"], 3.4)
    self.assertEqual(merged["processed_events"], 323000)
    self.assertNotIn("hot_metrics", merged)
    self.assertNotIn("cold_metrics", merged)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/evaluation/test_aws_native_evaluation_controller.py -q
```

Expected:
- FAIL because the controller still assumes one optimized stream-processor and has no hot/cold metric merge helper.

- [ ] **Step 3: Implement controller support for optimized-hot and optimized-cold**

Add a role-aware merge helper in `services/evaluation-controller/controller.py`:

```python
def merge_optimized_metrics(
    hot_metrics: dict[str, object],
    cold_metrics: dict[str, object],
) -> dict[str, object]:
    merged = dict(cold_metrics)
    for key in (
        "avg_end_to_end_latency_seconds",
        "p95_end_to_end_latency_seconds",
        "p99_end_to_end_latency_seconds",
        "ingest_to_redis_p95_seconds",
        "hot_batch_duration_seconds",
        "observed_throughput_eps",
    ):
        if key in hot_metrics:
            merged[key] = hot_metrics[key]
    return merged
```

Then update `services/evaluation-controller/main.py` so optimized orchestration:

```python
- restarts both optimized deployments
- waits for both deployments to become ready
- scrapes hot metrics from the optimized-hot pod
- scrapes cold metrics from the optimized-cold pod
- merges them before report generation
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/evaluation/test_aws_native_evaluation_controller.py -q
```

Expected:
- PASS with the new optimized split-controller behavior and no regression in baseline report generation.

- [ ] **Step 5: Commit**

```bash
git add \
  services/evaluation-controller/main.py \
  services/evaluation-controller/controller.py \
  services/evaluation-controller/aws_baseline_metrics.py \
  tests/evaluation/test_aws_native_evaluation_controller.py
git commit -m "feat: orchestrate split optimized services"
```

### Task 3: Add Service Roles To The Stream Processor Entry Point

**Files:**
- Modify: `services/stream-processor/job.py`
- Test: `tests/stream/test_job.py`

- [ ] **Step 1: Write the failing service-role tests**

```python
def test_start_queries_uses_hot_only_query_in_optimized_hot_role(self):
    with patch.object(stream_job, "pipeline_mode", return_value="optimized"), patch.object(
        stream_job, "pipeline_service_role", return_value="hot"
    ):
        queries = stream_job.start_queries(classified_df, classified_df, lookup_df)
    self.assertEqual(len(queries), 1)

def test_start_queries_uses_cold_only_query_in_optimized_cold_role(self):
    with patch.object(stream_job, "pipeline_mode", return_value="optimized"), patch.object(
        stream_job, "pipeline_service_role", return_value="cold"
    ):
        queries = stream_job.start_queries(classified_df, classified_df, lookup_df)
    self.assertEqual(len(queries), 1)
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
PYSPARK_PYTHON=$(which python3) PYSPARK_DRIVER_PYTHON=$(which python3) pytest tests/stream/test_job.py -q
```

Expected:
- FAIL because optimized mode still starts the legacy single optimized query and has no service-role split.

- [ ] **Step 3: Implement `PIPELINE_SERVICE_ROLE` and role-based startup**

Add a role helper in `services/stream-processor/job.py`:

```python
def pipeline_service_role() -> str:
    return os.environ.get("PIPELINE_SERVICE_ROLE", "monolith").strip().lower()
```

Update `start_queries()` so optimized mode routes by role:

```python
if pipeline_mode() == "optimized" and pipeline_service_role() == "hot":
    return [hot_query]
if pipeline_mode() == "optimized" and pipeline_service_role() == "cold":
    return [cold_query]
```

Update `main()` so optimized builds two classified streams but each service only starts the query it owns.

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
PYSPARK_PYTHON=$(which python3) PYSPARK_DRIVER_PYTHON=$(which python3) pytest tests/stream/test_job.py -q
```

Expected:
- PASS for the new optimized hot/cold role tests and no regression in baseline query startup behavior.

- [ ] **Step 5: Commit**

```bash
git add services/stream-processor/job.py tests/stream/test_job.py
git commit -m "feat: add optimized stream processor roles"
```

### Task 4: Rewrite The Optimized Hot Path To Remove Driver Bottlenecks

**Files:**
- Modify: `services/stream-processor/job.py`
- Test: `tests/stream/test_job.py`

- [ ] **Step 1: Write the failing hot-path tests**

```python
def test_reduce_latest_state_rows_keeps_latest_record_per_device(self):
    reduced = stream_job.reduce_latest_state_rows(batch_df)
    rows = {row["device_id"]: row["event_id"] for row in reduced.collect()}
    self.assertEqual(rows["dev-1"], "evt-2")

def test_write_latest_state_to_redis_uses_partition_writer(self):
    batch_df = Mock()
    reduced_df = Mock()
    batch_df.transform.return_value = reduced_df

    with patch.object(stream_job, "reduce_latest_state_rows", return_value=reduced_df), patch.object(
        stream_job, "_write_latest_state_partition"
    ) as mock_partition_writer:
        stream_job.write_latest_state_to_redis(batch_df, batch_id=7)

    reduced_df.foreachPartition.assert_called_once()
    mock_partition_writer.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
PYSPARK_PYTHON=$(which python3) PYSPARK_DRIVER_PYTHON=$(which python3) pytest tests/stream/test_job.py -q
```

Expected:
- FAIL because the hot path still uses `row_number()` plus `toLocalIterator()` on the driver.

- [ ] **Step 3: Implement grouped latest-state reduction and partition-local Redis writes**

Replace the windowed latest-row logic with a grouped reduction:

```python
def reduce_latest_state_rows(batch_df: DataFrame) -> DataFrame:
    latest_struct = F.max(
        F.struct(
            F.col("event_ts"),
            F.col("kafka_ingest_ts"),
            F.col("offset"),
            F.struct(*[F.col(name) for name in batch_df.columns]).alias("payload"),
        )
    ).alias("latest")

    return (
        batch_df.groupBy("device_id")
        .agg(latest_struct)
        .select("latest.payload.*")
    )
```

Replace the driver loop with partition-local writes:

```python
def _write_latest_state_partition(rows):
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    pipeline = client.pipeline(transaction=False)
    for row in rows:
        payload = {key: serialize_value(value) for key, value in row.asDict().items()}
        key = f"device:status:{row['device_id']}"
        pipeline.setex(key, REDIS_STATUS_TTL_SECONDS, json.dumps(payload))
        event_ts = row["event_ts"]
        score = int(event_ts.timestamp()) if event_ts else 0
        if row["breach_status"] == "breach":
            pipeline.zadd(REDIS_ACTIVE_BREACHES_KEY, {row["device_id"]: score})
        else:
            pipeline.zrem(REDIS_ACTIVE_BREACHES_KEY, row["device_id"])
    pipeline.execute()
```

Then in `write_latest_state_to_redis()`:

```python
latest_rows = reduce_latest_state_rows(batch_df)
latest_rows.foreachPartition(_write_latest_state_partition)
```

Keep hot-owned latency publication inside the hot role only.

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
PYSPARK_PYTHON=$(which python3) PYSPARK_DRIVER_PYTHON=$(which python3) pytest tests/stream/test_job.py -q
```

Expected:
- PASS for the new hot-path optimization tests and no regression in existing stream output behavior.

- [ ] **Step 5: Commit**

```bash
git add services/stream-processor/job.py tests/stream/test_job.py
git commit -m "feat: optimize hot path latest-state writes"
```

### Task 5: Verify End-To-End Optimized Behavior

**Files:**
- Modify: `services/evaluation-controller/main.py`
- Modify: `services/stream-processor/job.py`
- Modify: `infra/kubernetes/optimized/kustomization.yaml`
- Test: `tests/evaluation/test_aws_native_evaluation_manifests.py`
- Test: `tests/evaluation/test_aws_native_evaluation_controller.py`
- Test: `tests/stream/test_job.py`

- [ ] **Step 1: Run the focused automated checks**

Run:

```bash
pytest tests/evaluation/test_aws_native_evaluation_manifests.py -q
pytest tests/evaluation/test_aws_native_evaluation_controller.py -q
PYSPARK_PYTHON=$(which python3) PYSPARK_DRIVER_PYTHON=$(which python3) pytest tests/stream/test_job.py -q
```

Expected:
- All targeted manifest, controller, and stream tests PASS in the worktree.

- [ ] **Step 2: Run an optimized normal-load evaluation**

Run:

```bash
WORKLOAD_DURATION_MINUTES=5 bash scripts/run-aws-evaluation-controller.sh optimized normal optimized-normal-$(date -u +%Y%m%dT%H%M%SZ)
```

Expected:
- optimized report succeeds
- normal-load latency remains under `5s`
- processed / invalid / breach outputs still appear in the usual report fields

- [ ] **Step 3: Run an optimized spike-load evaluation**

Run:

```bash
WORKLOAD_DURATION_MINUTES=5 bash scripts/run-aws-evaluation-controller.sh optimized spike optimized-spike-$(date -u +%Y%m%dT%H%M%SZ)
```

Expected:
- optimized report succeeds
- spike latency is materially lower than the old single-process optimized path
- report schema remains unchanged

- [ ] **Step 4: Commit final implementation adjustments**

```bash
git add \
  infra/kubernetes/optimized \
  services/evaluation-controller \
  services/stream-processor/job.py \
  tests/evaluation/test_aws_native_evaluation_manifests.py \
  tests/evaluation/test_aws_native_evaluation_controller.py \
  tests/stream/test_job.py
git commit -m "feat: ship split optimized pipeline"
```
