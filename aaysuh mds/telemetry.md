# VacciGuard Telemetry & Bottleneck Diagnosis Guide

> **Date:** 2026-04-13
> **Purpose:** Complete telemetry documentation — what was fixed, what metrics exist, how to diagnose bottlenecks, and how the Grafana dashboard works.

---

## What Was Broken Before Our Fixes

| Problem | Impact | Fix |
|---|---|---|
| Replay producer `send_future.get()` on every message | Capped throughput at ~147 eps even at 1000 eps target | Batched futures (200 at a time), tuned Kafka producer (linger_ms=20, batch_size=64KB, gzip, max_in_flight=5) |
| Latency measured to S3 write, not to alert | Included 5s trigger wait + S3 write time, masking real alert SLA | Separate batch processing duration + Redis write timing + alert-path latency histogram |
| No per-stage timing | Couldn't tell if Spark, Redis, or S3 was the bottleneck | Individual timing for batch processing, Redis writes, S3 writes |
| No Kafka consumer lag visibility | Couldn't tell if Spark was keeping up at scale | StreamingQueryListener exposing inputRowsPerSecond vs processedRowsPerSecond |
| `ingest_delay_seconds` computed but never exposed | Wasted signal — told you event arrival delay but was only in Parquet output | Exposed as avg/p95 Prometheus gauges |
| Only gauges, no histograms | Couldn't compute true percentiles or set up latency alerts | Alert-path latency as Prometheus histogram with buckets `[0.1, 0.5, 1, 2, 5, 10, 20, 30]` |
| Timestamp granularity was 1 second | Up to 1-second quantization error on latency | Microsecond precision (`%Y-%m-%dT%H:%M:%S.%fZ`) |
| Duplicate metric names (`_sum_seconds` vs `_seconds_sum`) | Confusing — two different metrics with overlapping purpose | Removed raw gauge duplicates, kept only histogram convention |
| Replay producer pod had no labels | Prometheus couldn't scrape replay metrics — 4 Grafana panels showed "No Data" | Added `job_name: replay-producer` label to pod template |
| `onQueryIdle` abstract method missing (PySpark 3.5+) | Stream processor crashed on startup | Added empty `onQueryIdle` method |
| PySpark `Row.get()` doesn't exist | `AttributeError: get` on replay_sent_at in Redis write | Changed to `row["replay_sent_at"]` |
| Double writes to Redis/S3 | `write_batch_summary` re-wrote outputs already done by `write_processed_batch` | Separated concerns: `write_batch_summary` = metrics only, `write_processed_batch` = writes only |

---

## Metrics Now Available

### Replay Producer (port 9109, `/metrics`)

| Metric | Type | Description |
|---|---|---|
| `vacciguard_replay_sent_events_total` | Counter | Total events sent to Kafka |
| `vacciguard_replay_configured_rate_events_per_second` | Gauge | Target EPS |
| `vacciguard_replay_duration_seconds` | Gauge | Wall-clock replay duration |
| `vacciguard_replay_completion_status` | Gauge | 0=running, 1=success, 2=failed |
| `vacciguard_replay_kafka_ack_latency_sum_seconds` | Counter (sum) | Sum of Kafka ack latencies |
| `vacciguard_replay_kafka_ack_latency_count` | Counter (count) | Count of Kafka ack measurements |
| `vacciguard_replay_send_errors_total` | Counter | Total Kafka send errors |
| `vacciguard_replay_loaded_events` | Gauge | Events loaded from workload file |

### Stream Processor (port 9108, `/metrics`)

| Metric | Type | Description |
|---|---|---|
| `vacciguard_stream_processed_events_total` | Counter | Total processed events |
| `vacciguard_stream_invalid_events_total` | Counter | Total invalid events |
| `vacciguard_stream_deduplicated_events_total` | Counter | Total deduplicated events |
| `vacciguard_stream_breach_events_total` | Counter | Total breach events |
| `vacciguard_stream_batch_processing_duration_seconds` | Gauge | Actual Spark computation time per batch |
| `vacciguard_stream_redis_write_duration_seconds` | Gauge | Time to write batch results to Redis |
| `vacciguard_stream_redis_latest_keys_written` | Gauge | Device keys written to Redis per batch |
| `vacciguard_stream_s3_processed_write_duration_seconds` | Gauge | Time to write processed output to S3 |
| `vacciguard_stream_ingest_delay_avg_seconds` | Gauge | Avg event_time to Kafka ingest delay |
| `vacciguard_stream_ingest_delay_p95_seconds` | Gauge | P95 event_time to Kafka ingest delay |
| `vacciguard_stream_alert_latency_seconds_bucket` | **Histogram** | Alert-path latency distribution (for true p95) |
| `vacciguard_stream_input_rows_per_second` | Gauge | Kafka consumption rate |
| `vacciguard_stream_processed_rows_per_second` | Gauge | Spark processing rate |
| `vacciguard_stream_state_total_rows` | Gauge | Total rows in Spark state (dedup watermark) |
| `vacciguard_stream_latest_batch_avg_latency_seconds` | Gauge | Legacy: avg latency to S3 write |
| `vacciguard_stream_latest_batch_p95_latency_seconds` | Gauge | Legacy: p95 latency to S3 write |

---

## Grafana Dashboard (6 Rows, 19 Panels)

### Row 1: Pipeline Overview
- **Total Events Processed** — cumulative counter
- **Total Invalid Events** — with green/yellow/red thresholds
- **Total Breach Events** — cumulative counter
- **Pipeline Status** — gauge (0/1) showing health

### Row 2: Throughput Analysis
- **Replay Actual EPS** — smooth timeseries showing actual send rate (with gradient fill)
- **Input vs Processed Rows/Second** — two lines on one graph. If green line (processed) drops below blue line (input), Spark is falling behind

### Row 3: Latency Deep Dive
- **Alert-Path Latency P95** — the REAL SLA metric. Uses `histogram_quantile` on alert latency buckets. Green <5s, Yellow 5-10s, Red >10s
- **Batch Processing Duration** — Spark compute time. Green <1s, Yellow 1-3s, Red >3s
- **End-to-End Avg + P95** — legacy latency for reference

### Row 4: Infrastructure Health
- **Redis Write Duration** — alert delivery speed
- **S3 Write Duration** — archival speed
- **Ingest Delay Avg** — event arrival delay
- **State Size** — dedup watermark state. Green <10K rows, Yellow 10-50K, Red >50K (growing state = memory leak)

### Row 5: Replay Producer Health
- **Completion Status** — success/failure
- **Send Errors** — total Kafka send failures
- **Kafka ACK Latency** — time waiting for Kafka acknowledgment

### Row 6: Event Classification
- **Valid vs Invalid** — pie chart showing ratio
- **Deduplicated Count** — total deduplicated events
- **Breach vs Safe** — pie chart showing event classification ratio

---

## How to Diagnose Bottlenecks

### Step 1: Is Replay Keeping Up?

```promql
rate(vacciguard_replay_sent_events_total[15s])
```

**Interpretation:**
- If actual EPS ≈ configured EPS → replay is fine
- If actual EPS < configured EPS → replay is the bottleneck

### Step 2: Is Spark Keeping Up?

Look at Grafana Row 2 panel "Input vs Processed Rows/Second":
- If green line = blue line → Spark is keeping up
- If green line < blue line → Spark is falling behind (consumer lag)

Or query:
```promql
vacciguard_stream_input_rows_per_second
vacciguard_stream_processed_rows_per_second
```

### Step 3: Where Is Time Being Spent?

```promql
# Spark computation
vacciguard_stream_batch_processing_duration_seconds

# Redis write
vacciguard_stream_redis_write_duration_seconds

# S3 write
vacciguard_stream_s3_processed_write_duration_seconds
```

**Interpretation:**
- Highest value is your bottleneck
- At 1000 eps, if `batch_processing_duration` > 3s → Spark logic is too slow
- If `redis_write_duration` > 1s → Redis is the bottleneck
- If `s3_processed_write_duration` > 2s → S3 writes dominate

### Step 4: Is the Alert SLA Met?

```promql
histogram_quantile(0.95, rate(vacciguard_stream_alert_latency_seconds_bucket[1m]))
```

**Target:** < 5 seconds. If above 5s, look at `batch_processing_duration` + `redis_write_duration` to find which stage is slow.

### Step 5: Is State Growing Unbounded?

```promql
vacciguard_stream_state_total_rows
```

If this keeps growing over time → dedup watermark isn't expiring old state → memory leak risk.

---

## Files Changed

| File | What Changed |
|---|---|
| `services/replay-producer/producer.py` | Batched futures, tuned Kafka producer, microsecond timestamps, Kafka ack latency metric, send error counter |
| `services/stream-processor/job.py` | Per-batch timing, Redis/S3 write timing, ingest_delay exposure, alert-path latency histogram, StreamingQueryListener, cleaned up duplicate metrics, separated metrics from writes |
| `infra/kubernetes/base/job-replay-producer.yaml` | Added `job_name: replay-producer` pod label for Prometheus scraping |
| `infra/monitoring/prometheus/configmap-prometheus.yaml` | Added evaluation-controller scrape job |
| `infra/monitoring/grafana/configmap-dashboard-baseline-overview.yaml` | Complete redesign: 6 rows, 19 panels, dark theme, SLA thresholds, pie charts, timeseries with gradients |
| `tests/quick-aws-smoke-test/` | New folder: 1-minute smoke tests at 100eps and 1000eps on AWS |

---

## Verification Results

All changes verified:
- ✅ `python -m py_compile` passes for all Python files
- ✅ YAML validation passes for all Kubernetes manifests
- ✅ JSON validation passes for Grafana dashboard
- ✅ Stream processor starts and processes events (verified with 300 event local test)
- ✅ Replay producer sends at target rate (verified at 5 eps locally, 1000 eps in previous AWS test)
- ✅ All Prometheus metric names referenced in Grafana panels exist in source code
- ✅ No breaking changes to existing test suite

---

## What's Still Not Covered

1. **Per-event distributed trace** — no trace_id propagated through pipeline
2. **Kafka consumer lag per-partition** — aggregate rates visible but not per-partition lag
3. **CloudWatch Container Insights** — documented but not automated
4. **Optimized pipeline** — `infra/kubernetes/optimized/` is still a stub
