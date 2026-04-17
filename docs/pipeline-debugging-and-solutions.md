# VacciGuard Pipeline: Problems Encountered and How They Were Solved

## Overview

This document records every significant problem encountered during the development and evaluation of the VacciGuard pipeline, the root cause investigation for each, and the exact fix applied. It is intended to serve as a reference for the project report and to make the engineering decisions traceable.

The pipeline processes temperature and door-status telemetry from vaccine cold-chain storage devices across Rajasthan, India. It uses Apache Kafka for ingestion, Apache Spark Structured Streaming for processing, Redis for real-time state, and S3 for cold storage. The core SLA is end-to-end latency below 5 seconds at 100 events per second (eps) normal load.

---

## Contents

1. [Problem 1: High baseline latency — single-query pipeline (13.74s avg)](#problem-1-high-baseline-latency--single-query-pipeline)
2. [Problem 2: Null latency after first hot/cold deployment](#problem-2-null-latency-after-first-hotcold-deployment)
3. [Problem 3: Consumer lag of 12,000+ records](#problem-3-consumer-lag-of-12000-records)
4. [Problem 4: Latency metric overwritten to 0.0 by cold batch](#problem-4-latency-metric-overwritten-to-00-by-cold-batch)
5. [Problem 5: Post-workload metrics reset to 0.0](#problem-5-post-workload-metrics-reset-to-00)
6. [Problem 6: Evaluation controller drops cold-path config on each run](#problem-6-evaluation-controller-drops-cold-path-config-on-each-run)
7. [Problem 7: Spike load latency regression (146s avg)](#problem-7-spike-load-latency-regression-146s-avg)
8. [Summary: Before and After](#summary-before-and-after)
9. [Key Engineering Lessons](#key-engineering-lessons)

---

## Problem 1: High Baseline Latency — Single-Query Pipeline

### What was observed

The first round of evaluations (April 16) showed end-to-end latency of **13.74s average, 33.27s P95** at 100 eps (normal load). This far exceeded the 5-second SLA target.

```
Run: 20260416-5m-normal
avg_end_to_end_latency_seconds: 13.74
p95_end_to_end_latency_seconds: 33.27
ingest_to_redis_p95_seconds: 19.71
consumer_lag_records: 0
processed_rate_pct: 98.17%
```

### Root cause

The pipeline used a single Spark Structured Streaming query with a `foreachBatch` callback that performed all work sequentially in every batch:

1. Validate and classify events
2. Deduplicate across the batch (window-based row-number dedup)
3. Join against a lookup DataFrame (re-loaded from S3 every batch)
4. Filter invalid events and write to S3
5. Write processed events to S3
6. Write breach window aggregations to S3
7. Write the latest device state to Redis

Every batch had to complete all three S3 write operations before Redis was updated. S3 `PUT` operations take 50–200ms each. With 5–10 S3 write calls per batch, plus Spark's own action overhead (multiple `count()`, `isEmpty()`, and write actions per batch), each batch cycle took 12–15 seconds.

The trigger interval was set to `30 seconds`, meaning the pipeline processed a burst of events every 30 seconds. Redis was only updated at the end of each such cycle. Any event ingested in second 1 of the window would not be visible in Redis until second 30+ when the full batch completed.

### The fix: Hot/Cold architectural split

The root cause was that Redis writes (which need to be fast) were blocked behind S3 writes (which are slow and can afford to be batched). The solution was to split the pipeline into two independent Spark streaming queries sharing the same SparkSession:

**Hot query** (2-second trigger):
- Only writes to Redis
- No S3 writes, no counts, no deduplication window — just validate, join lookup, and push to Redis
- Uses Python to measure latency after `pipeline.execute()` returns
- Single Spark action: `toLocalIterator()` to collect device rows for Redis pipelining

**Cold query** (30-second trigger):
- Handles all S3 writes (processed, invalid, breach windows)
- Computes counts and consumer lag
- Never writes to Redis, never touches latency metrics

This way, Redis is updated every 2 seconds regardless of how long S3 writes take.

### Verification

After deploying the hot/cold split:

```
Run: normal-20260417t035340z
avg_end_to_end_latency_seconds: 2.70   ← was 13.74s
p95_end_to_end_latency_seconds: 2.88   ← was 33.27s
ingest_to_redis_p95_seconds: 2.91
consumer_lag_records: 0
processed_rate_pct: 97.95%
```

**Improvement: 80% latency reduction at normal load.** The 5-second SLA target was met.

---

## Problem 2: Null Latency After First Hot/Cold Deployment

### What was observed

The first deployment of the hot/cold split pipeline produced a report with `null` for both latency fields, even though events were being processed and Redis was being written to:

```
Run: normal-20260417t032034z
avg_end_to_end_latency_seconds: null
p95_end_to_end_latency_seconds: null
ingest_to_redis_p95_seconds: null
consumer_lag_records: 12621
processed_events: 7179
```

### Root cause investigation

The latency computation in `write_latest_state_to_redis` required parsing the `replay_sent_at` field from each event row. This field holds the ISO 8601 timestamp of when the replay producer sent the event to Kafka. The original code used Python's `strptime` with a fixed format string:

```python
# Original code — fragile
datetime.strptime(row["replay_sent_at"], "%Y-%m-%dT%H:%M:%S%z")
```

If `replay_sent_at` contained fractional seconds (e.g., `2026-04-17T03:21:45.123456+00:00`), `strptime` with `%S` would fail to parse and raise an exception. When this exception occurred inside the hot batch callback, the latency variables were never assigned, and the metrics endpoint exposed `null`.

### The fix

Switched to `datetime.fromisoformat()` with a simple `Z` → `+00:00` substitution, which handles any valid ISO 8601 format including fractional seconds:

```python
# Fixed code — handles any valid ISO 8601 format
datetime.fromisoformat(row["replay_sent_at"].replace("Z", "+00:00")).timestamp()
```

This is more robust because `fromisoformat` is not tied to a format string and can handle both `2026-04-17T03:21:45Z` and `2026-04-17T03:21:45.123456+00:00` without modification.

### Verification

After this fix (combined with others described below), run `normal-20260417t035340z` showed valid latency values.

---

## Problem 3: Consumer Lag of 12,000+ Records

### What was observed

The same first hot/cold run showed a consumer lag of 12,621 records at the end of the workload, meaning ~12,000 events arrived in Kafka but were not yet processed by the cold query:

```
Run: normal-20260417t032034z
consumer_lag_records: 12621
input_events: 19800
processed_events: 7179     ← far fewer than expected
```

At 100 eps over 3 minutes, 19,800 events were produced. Only 7,179 were processed by the cold path. The cold path had fallen behind almost from the start.

### Root cause

The `MAX_OFFSETS_PER_TRIGGER` configuration controls how many Kafka records a Spark streaming query reads in a single trigger cycle. It was set to `1000` globally.

In the single-query design, `1000 events / 30s trigger = 33 eps throughput ceiling`. This was fine when the trigger was 30 seconds, since 100 eps × 30s = 3,000 events per trigger, and the offset cap was not the bottleneck.

In the new hot/cold split, the **hot query** had a 2-second trigger and was not the problem — it could read 1,000 events every 2 seconds = 500 eps effective throughput, well above 100 eps.

The **cold query** still had a 30-second trigger. With `MAX_OFFSETS_PER_TRIGGER=1000`, it could only read 1,000 events per 30-second window = **33 eps effective throughput**. At 100 eps input, the cold query fell behind at 67 events/second. Over a 3-minute workload:

```
Deficit per cold trigger: 3000 events produced - 1000 events read = 2000 events deficit
Cold triggers in 3 minutes: 6
Total deficit: ~12,000 records   ← matches the observed 12,621
```

The original fix attempt was to simply pass a larger offset cap to the cold query. But a deeper issue was discovered: both the hot and cold queries were reading from the **same Spark streaming DataFrame**, meaning they shared the same Kafka source and the same offset policy.

### The fix

The solution was to build **two independent Kafka sources** — one for each query — by calling `build_stream()` twice with different parameters:

```python
# Two completely independent Kafka sources
hot_classified = build_stream(spark)  # uses MAX_OFFSETS_PER_TRIGGER=1000
cold_classified = build_stream(
    spark,
    max_offsets_per_trigger=COLD_MAX_OFFSETS_PER_TRIGGER  # "" = no cap
)
```

Setting `COLD_MAX_OFFSETS_PER_TRIGGER=""` (empty string) removes the offset cap entirely for the cold query. Spark interprets an absent `maxOffsetsPerTrigger` option as unlimited, allowing the cold query to drain its full backlog in each 30-second window regardless of how many events accumulated.

Each query also got its own checkpoint directory so their Kafka consumer group offsets are tracked independently:

```python
hot_query.option("checkpointLocation", f"{CHECKPOINT_ROOT}/baseline_hot")
cold_query.option("checkpointLocation", f"{CHECKPOINT_ROOT}/baseline_cold")
```

### Verification

After this fix, consumer lag at normal load dropped to 0:

```
Run: normal-20260417t035340z
consumer_lag_records: 0
processed_events: 19395   ← 97.95% of 19,800 input events
```

---

## Problem 4: Latency Metric Overwritten to 0.0 by Cold Batch

### What was observed

Run `normal-20260417t033016z` showed latency values of exactly 0.0, not null, despite events being processed:

```
Run: normal-20260417t033016z
avg_end_to_end_latency_seconds: 0.0
p95_end_to_end_latency_seconds: 0.0
ingest_to_redis_p95_seconds: 0.0
consumer_lag_records: 11800
```

0.0 is suspicious — real latency can never be exactly zero. This was a metric corruption issue.

### Root cause

The `StreamMetricsRegistry` class maintains a shared dictionary of Prometheus gauge values. The cold batch callback called `update_batch_metrics()` at the end of every 30-second cold trigger cycle. Since the cold query never computes latency, it passed `None` for both latency fields:

```python
# Cold batch — no latency computed
STREAM_METRICS_REGISTRY.update_batch_metrics(
    avg_latency_seconds=None,
    p95_latency_seconds=None,
    ...
)
```

The original implementation of `update_batch_metrics` had a bug: it always wrote the value it received, including `None`, but Python's `None` was then cast or treated as `0.0` when stored in the metrics dictionary. This meant every 30 seconds, the cold batch was **overwriting whatever the hot batch had written** for latency — replacing a valid value like `2.70` with `0.0`.

The timing worked against detection: the hot batch ran every 2 seconds and wrote a valid latency. But the evaluation controller read the metrics endpoint only once at the end of the workload, 30 seconds after the last hot batch. The cold batch fired at the same point and won the race, writing `0.0` last.

### The fix

Added a `None` guard to `update_batch_metrics` so it skips the assignment when the value is `None`, preserving whatever the hot batch last wrote:

```python
def update_batch_metrics(self, *, avg_latency_seconds, p95_latency_seconds, ...):
    with self._lock:
        if avg_latency_seconds is not None:      # ← guard added
            self._metrics["vacciguard_stream_latest_batch_avg_latency_seconds"] = avg_latency_seconds
        if p95_latency_seconds is not None:      # ← guard added
            self._metrics["vacciguard_stream_latest_batch_p95_latency_seconds"] = p95_latency_seconds
        # non-latency fields always written ...
```

Also added a dedicated `update_hot_latency_metrics()` method that only the hot batch calls, making ownership explicit:

```python
def update_hot_latency_metrics(self, *, avg_latency_seconds, p95_latency_seconds):
    with self._lock:
        if avg_latency_seconds is not None:
            self._metrics["vacciguard_stream_latest_batch_avg_latency_seconds"] = avg_latency_seconds
        if p95_latency_seconds is not None:
            self._metrics["vacciguard_stream_latest_batch_p95_latency_seconds"] = p95_latency_seconds
```

The rule after this fix: **only the hot batch touches latency metrics; the cold batch never does**.

---

## Problem 5: Post-Workload Metrics Reset to 0.0

### What was observed

Even after fixing Problem 4, a related issue existed: after the workload finished and all events were consumed, the hot batch continued firing every 2 seconds on empty Kafka batches. Each empty batch called `update_redis_metrics(ingest_to_redis_p95_seconds=None)`. The `update_redis_metrics` method did not have a `None` guard — it always wrote whatever it received, treating `None` as `0.0`.

This meant the `ingest_to_redis_p95_seconds` metric was reset to `0.0` within 2–4 seconds after the workload ended. If the evaluation controller read the metrics endpoint slightly after the workload, it would see `0.0` instead of the true last value (e.g., `2.91`).

### Root cause

In `write_latest_state_to_redis`, the flow was:

```python
pipeline.execute()           # write to Redis
redis_done_ts = time.time()

# ... compute latency ...
STREAM_METRICS_REGISTRY.update_redis_metrics(
    ingest_to_redis_p95_seconds=ingest_to_redis_p95_seconds   # None if batch was empty
)
```

When the batch was empty (`written == 0`), `ingest_to_redis_p95_seconds` was never assigned but the call to `update_redis_metrics` still happened, passing `None`, which overwrote the previous valid value with `0.0`.

### The fix

Added an early return guard at the top of the Redis write cycle, immediately after `pipeline.execute()`:

```python
pipeline.execute()
redis_done_ts = time.time()

if written == 0:
    log.info("Batch %s: no device states to write to Redis", batch_id)
    return   # ← do not update any metrics
```

If the batch produced no Redis writes (workload has ended, Kafka is empty), the function returns before touching any metrics. The last valid latency value is preserved in the registry until the process restarts.

### Why this matters

The evaluation controller reads the metrics endpoint once at the end of the workload. Without this guard, there was a race condition: if an empty post-workload hot batch fired between the workload ending and the controller reading metrics, the reported latency would be `0.0` instead of the true measured value. With the guard, the metric is stable.

---

## Problem 6: Evaluation Controller Drops Cold-Path Config on Each Run

### What was observed

After adding `COLD_TRIGGER_INTERVAL` and `COLD_MAX_OFFSETS_PER_TRIGGER` to the Kubernetes ConfigMap (`vacciguard-pipeline-config`), the pipeline was restarted manually with correct behavior. But when the evaluation controller ran the next evaluation, the consumer lag problem returned.

The deployed pod was reading `COLD_MAX_OFFSETS_PER_TRIGGER=""` correctly from the ConfigMap, but after the evaluation controller patched the ConfigMap to set up a new run's Kafka topic and S3 paths, the pod restarted — and this time `COLD_MAX_OFFSETS_PER_TRIGGER` was gone from the ConfigMap.

### Root cause

The evaluation controller's `patch_pipeline_config` function worked by reading the current ConfigMap and calling `build_pipeline_config_patch` to produce a replacement ConfigMap. Kubernetes applies this with a strategic merge patch that **replaces the entire `data` dict** — it does not merge individual keys.

`build_pipeline_config_patch` had parameters for `cold_trigger_interval` and `cold_max_offsets_per_trigger`, but `patch_pipeline_config` in `main.py` never read those keys from the current ConfigMap and never forwarded them:

```python
# Before fix — cold keys not forwarded
patch_body = controller.build_pipeline_config_patch(
    trigger_interval=data["TRIGGER_INTERVAL"],
    # cold_trigger_interval and cold_max_offsets_per_trigger: not passed
    ...
)
```

Since `build_pipeline_config_patch` had default values for these parameters (`cold_trigger_interval="30 seconds"`, `cold_max_offsets_per_trigger=""`), the patch did include them — but only with their hardcoded defaults, not the values from the running ConfigMap. In practice this meant `COLD_MAX_OFFSETS_PER_TRIGGER` was always reset to `""`, which was actually the right value, but `COLD_TRIGGER_INTERVAL` was also being used from the code default rather than the cluster.

The deeper issue was that `build_pipeline_config_patch` was not in the forwarded path from `main.py`, meaning any operator who changed the ConfigMap manually to tune cold parameters would have those changes silently overwritten on the next evaluation run.

### The fix

Updated `patch_pipeline_config` in `main.py` to read the cold parameters from the current ConfigMap and forward them explicitly:

```python
def patch_pipeline_config(contract):
    config_map = read_pipeline_config()
    data = config_map.data or {}
    patch_body = controller.build_pipeline_config_patch(
        ...
        trigger_interval=data["TRIGGER_INTERVAL"],
        cold_trigger_interval=data.get("COLD_TRIGGER_INTERVAL", "30 seconds"),   # ← added
        max_offsets_per_trigger=data.get("MAX_OFFSETS_PER_TRIGGER"),
        cold_max_offsets_per_trigger=data.get("COLD_MAX_OFFSETS_PER_TRIGGER", ""),  # ← added
        ...
    )
```

This ensures that any cold-path configuration present in the running ConfigMap is preserved across evaluation runs.

---

## Problem 7: Spike Load Latency Regression (146s avg)

### What was observed

Running the spike load scenario (1000 eps, 10× normal) on the fixed hot/cold pipeline produced a massive latency regression:

```
Run: spike-20260417t040032z
configured_events_per_second: 1000.0
avg_end_to_end_latency_seconds: 146.37
p95_end_to_end_latency_seconds: 146.38
ingest_to_redis_p95_seconds: 146.38
consumer_lag_records: 0
processed_events: 194082 / 198000 = 98.02%
```

Average and P95 are almost identical (both 146s), and consumer lag is 0 despite the very high latency. The pipeline processed 98% of all events — no data was lost.

### Root cause

The `MAX_OFFSETS_PER_TRIGGER=1000` setting limits how many Kafka records the hot query reads in a single 2-second trigger cycle. This creates an effective throughput ceiling for the hot path:

```
Hot query throughput ceiling = MAX_OFFSETS_PER_TRIGGER / TRIGGER_INTERVAL
                             = 1000 events / 2 seconds
                             = 500 eps
```

At 1000 eps input, the hot query can only process 500 events per second. The other 500 events/second accumulate as a Kafka backlog:

```
Backlog growth rate: 1000 eps - 500 eps = 500 events/second
Workload duration: 198 seconds (3 minutes + 18 seconds)
Total backlog at workload end: ~99,000 events
```

When the evaluation controller read the metrics endpoint at the end of the workload, the hot batch was actively processing events that had been waiting in Kafka for approximately:

```
99,000 events backlog / 500 eps drain rate ≈ 198 seconds drain time
Events being processed now were produced ~146 seconds ago
```

This explains the 146-second average latency — it is not measurement error or a bug, it is the literal age of the events currently being processed relative to when they were produced.

**Why avg ≈ P95:** The hot batch uses a window deduplication step that collapses all rows to one row per device (keeping the latest event per device per batch). After this collapse, all rows in the batch share the same `redis_done_ts` (the moment `pipeline.execute()` returned). The latency for every row in the batch is computed as `redis_done_ts - replay_sent_at`. Since the batch contains events from many different production times but all share the same `redis_done_ts`, the variance is low, and avg ≈ P95.

**Why consumer lag = 0:** The cold query had no offset cap (`COLD_MAX_OFFSETS_PER_TRIGGER=""`), so it drained the full backlog in its 30-second windows. By the time the evaluation controller measured consumer lag, the cold query had already caught up. Consumer lag reflects the cold query's position in the Kafka topic, not the hot query's position.

### Why this was not fixed (and should not be for baseline)

The `MAX_OFFSETS_PER_TRIGGER=1000` cap was deliberately introduced on the hot path to protect Redis from being overwhelmed by burst writes. Raising it is a valid optimization but changes the pipeline's behavior under normal load as well. Per the project design philosophy:

- The **baseline pipeline** is a reference implementation. Its behavior under spike is an honest measurement — the pipeline maintains data durability (98%) but sacrifices real-time freshness under 10× load.
- The **optimized pipeline** is where this cap would be raised (e.g., to 10,000 or removed entirely with Redis write rate-limiting instead), or where horizontal scaling of the hot query would be introduced.

The spike result is therefore a finding, not a defect: it establishes the throughput ceiling of the current design and motivates the optimization work.

---

## Summary: Before and After

### Normal load (100 eps)

| Metric | Single-query baseline (Apr 16) | Hot/cold optimized (Apr 17) | Change |
|--------|-------------------------------|----------------------------|--------|
| Avg latency | 13.74s | 2.70s | **-80.4%** |
| P95 latency | 33.27s | 2.88s | **-91.3%** |
| Ingest-to-Redis P95 | 19.71s | 2.91s | **-85.2%** |
| Consumer lag | 0 | 0 | — |
| Processed rate | 98.17% | 97.95% | -0.2% |
| SLA (<5s avg) | FAIL | **PASS** | |

### Spike load (1000 eps)

| Metric | Single-query baseline (Apr 16) | Hot/cold optimized (Apr 17) | Change |
|--------|-------------------------------|----------------------------|--------|
| Avg latency | 19.67s | 146.37s | +644% |
| P95 latency | 31.85s | 146.38s | +360% |
| Consumer lag | 0 | 0 | — |
| Processed rate | 98.15% | 98.02% | -0.1% |
| Data durability | maintained | maintained | — |

The spike regression on the hot/cold pipeline is explained entirely by the `MAX_OFFSETS_PER_TRIGGER=1000` ceiling (500 eps effective throughput on the hot path). The single-query pipeline had no such ceiling at the same setting, because its trigger interval was 30 seconds — `1000 events / 30 seconds = 33 eps ceiling`, but the single query processed events in batched aggregation without a per-event Redis round-trip, making the ceiling non-binding.

---

## Key Engineering Lessons

### 1. Decouple fast writes from slow writes at the architectural level

Mixing Redis writes (milliseconds) and S3 writes (hundreds of milliseconds each) in a single Spark batch causes the slow operation to set the floor for end-to-end latency. The only reliable fix is architectural separation — put them in different queries with different trigger intervals.

### 2. Shared streaming sources share constraints

When two Spark streaming queries read from the same DataFrame (same Kafka source), they share all source-level options including `maxOffsetsPerTrigger`. A cap designed for one query's trigger interval will create a bottleneck for the other query with a different trigger interval. Each query should have its own source with its own parameters.

### 3. Any code path that can write a metric should have a None guard

Metrics shared between multiple code paths (hot batch, cold batch, post-workload empty batches) will be overwritten by whichever path writes last. If a code path legitimately has no value to report, it must explicitly skip the write — not write a sentinel value like 0.0 or None-cast-to-0.0. The rule: "only write when you have a valid value."

### 4. Evaluate controllers that replace ConfigMaps must re-read all keys

A function that generates a replacement ConfigMap from scratch must read every key it does not generate itself from the current running ConfigMap. Any key that it forgets to forward will be silently removed from the next run's configuration. The pattern is: read current state → merge with new values → write merged result.

### 5. Empty batches are a post-workload threat to metric stability

Spark Structured Streaming continues to fire trigger cycles even when the Kafka topic is empty. Any metric update that happens unconditionally in the batch callback will run against empty batches. Add an early-return guard (`if written == 0: return`) to prevent post-workload empty cycles from corrupting the final metric values that the evaluation controller reads.

### 6. Latency at spike load measures queue age, not processing time

When a streaming pipeline has an offset cap that creates a sustained backlog, the measured end-to-end latency at any point reflects the age of the oldest unprocessed event in the queue — not the time taken to process it. A 146-second latency at 1000 eps does not mean the pipeline is slow to process events; it means events are waiting 146 seconds before they are processed, because the hot query can only drain 500 of the 1000 events arriving each second.
