# Normal Load Evaluation Plan

**Scenario:** `normal`
**Load:** 100 events/second
**Duration:** 5 minutes
**Total events:** ~33,000
**Applies to:** Baseline Pipeline + Optimized Pipeline (run separately)

> **Status:** Baseline runs completed. Optimized pipeline not yet built — Optimized Actual columns are intentionally blank and will be filled in once the optimized pipeline is deployed.

---

## What This Scenario Tests

The normal load scenario is the baseline health check for the pipeline. It runs the pipeline at its designed operating load — 100 events per second — and verifies that every core requirement is met under non-stress conditions.

This scenario answers the professor's fundamental question: **does the pipeline work correctly at its intended operating load?**

If this scenario fails or shows degraded metrics, every other scenario is meaningless. This must pass cleanly for both pipelines.

---

## Professor Requirements This Scenario Satisfies

| Requirement (from PDF) | What this run measures |
|------------------------|------------------------|
| End-to-end latency < 5s | `avg_end_to_end_latency_seconds` and `p95_end_to_end_latency_seconds` |
| ≥ 99.9% pipeline uptime | `consumer_lag = 0` — all events processed |
| Data quality (valid/invalid classification) | `invalid_rate_pct` — must be ~1.82% |
| Throughput measurement | `throughput_eps` — must match configured 100 eps |
| Pipeline success | `pipeline_success = true` |
| Deduplication | `deduplicated_events` — correctly identifies duplicates |
| Breach detection | `breach_events` — correctly triggers on threshold breaches |
| S3 cold storage writes | `processed_output_objects` in evaluation report |
| Redis hot path | `ingest_to_redis_p95_seconds` — confirms Redis writes complete within SLA |

---

## Pre-Run Checklist

Complete all of these before starting the run. A skipped step is the most common cause of a failed evaluation.

- [ ] AWS credentials are valid: `aws sts get-caller-identity`
- [ ] EKS cluster is reachable: `kubectl -n vacciguard get pods` — all pods Running
- [ ] Stream processor is up and showing "2 active queries" in logs: `kubectl -n vacciguard logs -f deployment/stream-processor | grep "active queries"`
- [ ] No leftover evaluation jobs from a previous run: `kubectl -n vacciguard get jobs | grep evaluation` — should be empty or Completed
- [ ] No consumer lag from a previous run: check stream processor logs are showing normal batches
- [ ] Kafka topic from the previous run is not bleeding events into this run (the controller creates a new topic per run — this is automatic)

---

## Terminal Setup

Open two terminals before starting.

**Terminal 1 — watch stream processor live:**
```bash
kubectl -n vacciguard logs -f deployment/stream-processor
```
Keep this open throughout the run. You will read latency values and batch counts from here.

**Terminal 2 — run the evaluation:**

For **baseline pipeline**:
```bash
WORKLOAD_DURATION_MINUTES=5 \
bash scripts/run-aws-baseline-evaluation.sh \
  baseline \
  normal \
  normal-$(date -u +%Y%m%dT%H%M%SZ)
```

For **optimized pipeline** (run separately, after the baseline run completes):
```bash
WORKLOAD_DURATION_MINUTES=5 \
bash scripts/run-aws-evaluation-controller.sh \
  optimized \
  normal \
  normal-optimized-$(date -u +%Y%m%dT%H%M%SZ)
```

> **Important:** Run baseline first, let it fully complete, then run optimized. Do not run them concurrently — they share the same stream processor deployment.

---

## What to Observe Live During the Run

### In Terminal 1 (stream processor logs):

**Every 2 seconds — hot batch:**
```
Batch N: wrote X latest device states to Redis (avg_latency=2.46s, p95=2.90s)
```
- `avg_latency` should be between 2.0s and 3.5s
- `p95` should be below 5.0s
- Both values should be **stable** — not increasing over time

**Every 30 seconds — cold batch:**
```
Batch N summary: valid=NNN processed=NNN invalid=N duplicates=N
```
- `invalid` count should be roughly 2% of total events seen so far
- `duplicates` should be small (< 1%)

**What to write down:**
```
First hot batch latency (avg): _______s
First hot batch latency (P95): _______s
Last hot batch latency (avg):  _______s
Last hot batch latency (P95):  _______s
Any error messages seen:       yes / no
```

Stability is the key signal here: if the first batch shows 2.5s and the last batch shows 2.5s, the pipeline is stable. If latency is climbing over time, the pipeline is falling behind.

---

## Metrics to Capture After the Run

The evaluation controller will write a JSON report to `artifacts/aws-baseline-evaluations/<run-id>.json`.

Fill in the table below for each pipeline run:

| Metric | Expected (Baseline) | Baseline Actual | Optimized Actual |
|--------|---------------------|-----------------|------------------|
| `status` | `succeeded` | | |
| `pipeline_success` | `true` | | |
| `avg_end_to_end_latency_seconds` | 2.3 – 2.7s | | |
| `p95_end_to_end_latency_seconds` | 2.8 – 3.1s | | |
| `ingest_to_redis_p95_seconds` | 2.9 – 3.1s | | |
| `throughput_eps` | 100.0 | | |
| `consumer_lag_records` | 0 | | |
| `watermark_delay_seconds` | 0.0 | | |
| `input_events` | 33,000 | | |
| `processed_events` | ~32,388 | | |
| `processed_rate_pct` | ~98.15% | | |
| `invalid_events` | 600 | | |
| `invalid_rate_pct` | 1.82% | | |
| `deduplicated_events` | ~12 | | |
| `deduplication_rate_pct` | ~0.04% | | |
| `breach_events` | ~520 | | |
| `processed_output_objects` | ~48 | | |
| `cost_per_run` | "Not run" (calculate below) | | |
| `cost_per_gb_processed` | "Not run" (calculate below) | | |
| `run_id` | — | | |

---

## Cost Estimation (Analytical)

The evaluation report always shows `cost_per_run: "Not run"` and `cost_per_gb_processed: "Not run"` — the pipeline does not compute these automatically. Calculate them analytically after each run using the formula below.

### Infrastructure Cost (per run)

| Component | SKU | Rate | Notes |
|-----------|-----|------|-------|
| EKS nodes | 2× t3.medium | $0.0416/hr each = $0.0832/hr | Always-on during eval |
| ElastiCache | cache.t4g.micro | $0.0128/hr | Always-on |
| EKS control plane | Managed control plane | $0.1000/hr | Fixed per cluster |
| **Total infrastructure** | | **$0.1960/hr** | |

**For a 5-minute run:**
```
Infrastructure cost = $0.1960/hr × (5 min / 60 min) = $0.0163
```

### S3 Cost (per run)

| Component | Rate | Normal Run Estimate |
|-----------|------|---------------------|
| PUT requests (~50 objects) | $0.005 per 1,000 = $0.00025 | ~$0.0003 |
| Data storage (~6.6 MB) | $0.023/GB/month → ~$0.00015 for 5 min | negligible |
| GET requests (report reads) | ~$0.0004 per 1,000 | negligible |
| **Total S3** | | **~$0.0003** |

### Total Cost Per Run

```
Total = Infrastructure + S3
      = $0.0163 + $0.0003
      = ~$0.017 per 5-minute normal run
```

### Cost Per GB Processed

First calculate data volume processed:
```
Events:         ~32,388 processed events
Avg event size: ~200 bytes (JSON with device ID, timestamp, temperature, facility ID)
Total data:     32,388 × 200 bytes = ~6.48 MB = ~0.0065 GB

Cost per GB = Total cost / GB processed
            = $0.017 / 0.0065 GB
            = ~$2.62 / GB
```

### Fill In After Each Run

| | Baseline | Optimized |
|-|----------|-----------|
| Run duration (min) | 5 | 5 |
| Infrastructure cost | $0.0163 | $0.0163 |
| S3 cost | ~$0.0003 | ~$0.0003 |
| **Total cost per run** | **~$0.017** | **~$0.017** |
| Processed events | | |
| Data volume (MB) | ~6.48 MB | |
| **Cost per GB** | **~$2.62/GB** | |

> Note: Both pipelines run on the same infrastructure, so the per-run infrastructure cost is identical. The only difference would be S3 write volume if the optimized pipeline writes more output objects.

---

## S3 Verification

After the run, count the processed S3 objects to confirm cold storage is working:

```bash
# Replace <run-id> with the actual run ID from the report
aws s3 ls s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/baseline/normal/<run-id>/processed/ \
  --recursive | wc -l
```

**Expected: 40–55 objects.**

Basis: 5-minute run at 30-second cold trigger = 10 trigger cycles. Each trigger can produce 4–5 parquet part files. Expected range is 40–55 objects.

Record the actual count: `S3 processed object count: _______`

---

## Analysis Guide: What Each Metric Means

### Latency (avg_end_to_end_latency_seconds)
This is the time from when an event is published to Kafka to when the hot query writes it to Redis. It is computed as a weighted average across all hot batches.

- **2.0–3.0s** = excellent. Hot query is keeping up with 100 eps without backlog.
- **3.0–5.0s** = acceptable. Minor processing overhead, still within SLA.
- **> 5.0s** = SLA breach. Pipeline is falling behind.

The SLA target is **< 5 seconds end-to-end**.

### P95 Latency
The 95th percentile of latency across all events in the run. This is more meaningful than average because it captures the tail — the 5% of events that took longest.

A P95 close to the average means latency is consistent. A P95 much higher than average means there are occasional spikes (usually at cold trigger boundaries when the JVM pauses briefly).

### Ingest-to-Redis P95
The time from Kafka consumption to Redis write, at P95. This is a sub-component of end-to-end latency. If this is close to the total P95, it means Kafka ingestion itself is fast and the bottleneck is in Spark processing + Redis write.

### Consumer Lag = 0
This is the single most important metric. `consumer_lag = 0` means the cold query consumed every single message in the Kafka topic before the run ended. No events were lost, skipped, or left unprocessed.

If consumer_lag > 0 after the run ends, it means the pipeline did not finish processing all events — something went wrong.

### Invalid Rate ~1.82%
The workload generator injects exactly 2% invalid events. After deduplication, the measured rate lands at ~1.82%. This is the expected value. If invalid_rate is significantly different, something changed in the event classification logic.

### Processed Rate ~98.15%
This is `processed_events / input_events`. The ~1.85% gap is accounted for by invalid events (1.82%) and deduplicated events (0.04%). A processed rate of 98% + invalid rate of 1.82% + dedup rate of 0.04% ≈ 100% — all events are accounted for.

### Breach Events
The workload generator creates events that periodically trigger temperature threshold breaches. ~520 breach events in a 5-minute run at 100 eps is the expected count. A significantly different count could indicate a breach detection logic issue.

---

## Comparing Baseline vs Optimized

After both runs are complete, compute the improvement:

| Metric | Baseline | Optimized | Change |
|--------|----------|-----------|--------|
| Avg latency | | | |
| P95 latency | | | |
| Ingest-to-Redis P95 | | | |
| Processed rate | | | |
| Consumer lag | | | |

**What to look for:**
- Latency should be lower in the optimized pipeline (hot/cold split reduces contention on the hot path)
- Processed rate should be the same (both pipelines process all events)
- Consumer lag should be 0 in both (neither pipeline loses events at normal load)
- Invalid rate must be exactly the same (1.82%) — this is data classification, not throughput

If the optimized pipeline shows higher latency than baseline at normal load, that is a regression and must be investigated before the report is written.

---

## Key Findings to Document in the Report

After completing both runs, write up the following findings:

### Finding 1: Latency Under Normal Load
State the measured avg and P95 latency for both pipelines. Compare against the 5-second SLA. Confirm both pass.

### Finding 2: Data Completeness
State that consumer_lag = 0 for both runs, meaning 100% of the 33,000 input events were consumed and processed. The ~1.85% gap between input and processed events is fully explained by invalid classification (1.82%) and deduplication (0.04%).

### Finding 3: Data Quality
The pipeline correctly classifies events: 98.15% valid+processed, 1.82% invalid (correctly rejected), 0.04% duplicate (correctly deduplicated). Breach detection correctly identified ~520 threshold violation events.

### Finding 4: Cold Storage
S3 received ≥ 40 processed parquet objects from the 5-minute run, confirming the cold query was writing successfully throughout the workload.

### Finding 5: Pipeline Stability
Latency was stable from first batch to last batch — no drift upward, no recovery events, no error logs. The pipeline ran without human intervention for the full 5-minute window.

---

## Common Issues and How to Handle Them

**Issue: Run says "succeeded" but processed_rate is much lower than 98%**
- Check consumer_lag. If consumer_lag = 0, the low processed_rate is a metric artifact (counter reset), not data loss.
- If consumer_lag > 0, the pipeline did not finish processing. Investigate stream processor logs for errors.

**Issue: Avg latency is higher than expected (> 3.5s)**
- Check if a previous run left consumer lag. If the Kafka topic had leftover messages, the new run's latency measurement includes catch-up time.
- Check stream processor pod CPU usage: `kubectl -n vacciguard top pods`

**Issue: pipeline_success = false**
- Read the `failure_reason` field in the JSON report.
- Check evaluation controller pod logs: `kubectl -n vacciguard logs -l app=evaluation-controller`

**Issue: S3 object count is 0**
- The cold query may not have completed its first trigger cycle before the run ended.
- For a 5-minute run, the cold query fires every 30 seconds — should have fired 10 times.
- Check stream processor logs for "cold batch" messages.

---

## Report Section Template

Use this structure when writing the normal load section in the project report:

```
Normal Load Evaluation (100 eps, 5 minutes)

The pipeline was evaluated under normal operating load: 100 events/second
for 5 minutes, producing 33,000 total input events.

Latency Results:
- Baseline avg: X.XXs | P95: X.XXs
- Optimized avg: X.XXs | P95: X.XXs
- SLA target: < 5s — both pipelines PASS

Data Completeness:
- Consumer lag: 0 (both pipelines consumed all 33,000 events)
- Processed rate: ~98.15% (remaining 1.85% = 1.82% invalid + 0.04% dedup)

Data Quality:
- Invalid rate: 1.82% (expected: ~1.82%) — classification correct
- Breach detection: ~520 events triggered threshold alerts

Key Finding:
[Insert the one insight that matters most from your run]
```
