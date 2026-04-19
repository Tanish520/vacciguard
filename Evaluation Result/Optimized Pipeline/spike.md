# Spike Load - Optimized Pipeline

**Branch:** `baseline-spike-fix`  
**Pipeline:** `optimized`  
**Scenario:** `spike`  
**Load:** `1000 eps`  
**Duration:** `5 minutes`  
**Runs:** `3`

---

## Executive Summary

The optimized split pipeline holds the spike workload comfortably under the 5-second SLA across all three runs. The three spike runs produced average latency between `1.69 s` and `2.77 s`, with P95 between `1.90 s` and `3.00 s`. Throughput stayed at roughly `1000 eps`, and consumer lag remained `0` in every run.

The historical baseline spike run was catastrophic by comparison (`326.33 s` avg, `326.36 s` p95). The optimized split architecture therefore turns a runaway backlog scenario into a stable, low-latency one while preserving correctness.

---

## Run Table

| Trial | Run ID | Avg Latency | P95 | P99 | Throughput | Lag | Hot Batch | Cold Batch | Processed | Dedup | Breach |
|------|--------|-------------|-----|-----|------------|-----|-----------|------------|-----------|-------|--------|
| 1 | `opt-spike-1-20260417t185301z` | `1.84 s` | `1.90 s` | `1.90 s` | `1000.0 eps` | `0` | `1.27 s` | `14.55 s` | `323,330` | `670` | `4,896` |
| 2 | `opt-spike-2-20260417t190044z` | `1.69 s` | `2.07 s` | `2.11 s` | `999.9 eps` | `0` | `1.19 s` | `14.40 s` | `323,603` | `397` | `4,904` |
| 3 | `opt-spike-3-20260417t190824z` | `2.77 s` | `3.00 s` | `3.00 s` | `999.9 eps` | `0` | `0.77 s` | `12.22 s` | `323,641` | `359` | `4,903` |

---

## Aggregate View

| Metric | Min | Max | Mean |
|--------|-----|-----|------|
| Avg end-to-end latency | `1.69 s` | `2.77 s` | `2.10 s` |
| P95 end-to-end latency | `1.90 s` | `3.00 s` | `2.32 s` |
| P99 end-to-end latency | `1.90 s` | `3.00 s` | `2.34 s` |
| Throughput | `999.9 eps` | `1000.0 eps` | `999.93 eps` |
| Hot batch duration | `0.77 s` | `1.27 s` | `1.08 s` |
| Cold batch duration | `12.22 s` | `14.55 s` | `13.72 s` |
| Processed events | `323,330` | `323,641` | `323,525` |

---

## Detailed Metrics

| Metric | Value |
|--------|-------|
| status | `succeeded` |
| pipeline_success | `true` |
| configured_replay_rate_eps | `1000.0` |
| throughput_eps | `999.93` |
| consumer_lag_records | `0` |
| input_events | `330,000` |
| processed_events | `323,525` |
| processed_rate_pct | `98.04%` |
| invalid_events | `6,000` |
| invalid_rate_pct | `1.82%` |
| deduplicated_events | `475` |
| deduplication_rate_pct | `0.14%` |
| breach_events | `4,901` |
| breach_rate_pct | `1.49%` |
| processed_output_objects | `45.3` |
| invalid_output_objects | `0.0` |
| event_time_lag_p95_seconds | `0.0` |
| watermark_delay_seconds | `0.0` |
| pod_restart_count | `2.0` |
| queries_active | `1.0` |
| cost_per_run (calculated) | `~$0.017` |
| cost_per_gb_processed (calculated) | `~$0.25/GB` |

---

## Observations

- All three spike runs stayed under the 5-second SLA.
- Throughput remained essentially perfect at `~1000 eps`, which means the producer did not become the bottleneck.
- Consumer lag stayed at `0`, so the pipeline drained the Kafka topic fully even under heavy load.
- The hot path remained tightly clustered around `1 second` per batch, which is the critical result for real-time SLA protection.
- The cold path was still slower and more variable, but it never contaminated the hot latency metric because the split architecture isolates the responsibilities cleanly.

---

## Key Findings

- The optimized split pipeline completely eliminates the baseline spike collapse.
- The hottest metric for the project is the end-to-end Redis latency, and that remains comfortably under `5s` in every spike trial.
- The third spike trial was slightly slower than the first two, but it still stayed far below the SLA threshold.
- This is the strongest evidence that the SLA-aware split architecture is doing real work, not just shifting numbers around.

---

## Comparison With Baseline

- Baseline spike run: `326.33 s` avg, `326.36 s` p95
- Optimized spike mean across 3 runs: `2.10 s` avg, `2.32 s` p95
- Improvement in average latency: about `155x`
- Improvement in P95 latency: about `141x`

---

## Report Artifacts

- Trial 1: `s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/spike/opt-spike-1-20260417t185301z/report.json`
- Trial 2: `s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/spike/opt-spike-2-20260417t190044z/report.json`
- Trial 3: `s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/spike/opt-spike-3-20260417t190824z/report.json`

---

## S3 Verification

```bash
aws s3 ls s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/spike/opt-spike-3-20260417t190824z/processed/ --recursive | wc -l
```

Object count: `44`

---

## Cost Calculation

- Infrastructure: `5 min × $0.196/hr = ~$0.0163`
- S3: `~$0.0003`
- Total per run: `~$0.0166`
- Data volume: `330,000 events × 200 bytes ≈ 66.0 MB`
- Cost per GB: `~$0.25/GB`

### Why the cost per GB is low here

The spike workload moves much more data through the same fixed infrastructure, so the cluster cost is amortized across a larger processed volume. That makes the cost per GB look significantly better than the normal-load case, even though the absolute runtime cost per run is almost the same.

---

## Raw Report Highlights

```json
{
  "scenario": "spike",
  "pipeline_target": "optimized",
  "avg_end_to_end_latency_seconds_mean": 2.10,
  "p95_end_to_end_latency_seconds_mean": 2.32,
  "processed_events_mean": 323525,
  "consumer_lag_records": 0,
  "throughput_eps_mean": 999.93,
  "input_events_per_run": 330000,
  "workload_duration_minutes": 5,
  "processed_output_objects_mean": 45.3
}
```
