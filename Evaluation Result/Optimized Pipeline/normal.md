# Normal Load - Optimized Pipeline

**Branch:** `baseline-spike-fix`  
**Pipeline:** `optimized`  
**Scenario:** `normal`  
**Load:** `100 eps`  
**Duration:** `5 minutes`  
**Runs:** `3`

---

## Executive Summary

The optimized split pipeline is consistently below the 5-second SLA under normal load. Across three fresh AWS runs, the hot path stayed between `1.37 s` and `1.79 s` average latency, with P95 between `1.64 s` and `1.92 s`. Consumer lag remained `0` in every run, so the pipeline drained Kafka completely each time.

The cold path is slower and more variable, but that is expected because it owns the archival S3 work. The important result is that cold-path variability no longer drags the live Redis SLA below the target.

Compared with the historical baseline normal run (`2.60 s` avg, `3.11 s` p95), the optimized split pipeline reduced average latency to about `1.56 s` on average and P95 to about `1.75 s` on average.

---

## Run Table

| Trial | Run ID | Avg Latency | P95 | P99 | Throughput | Lag | Hot Batch | Cold Batch | Processed | Dedup | Breach |
|------|--------|-------------|-----|-----|------------|-----|-----------|------------|-----------|-------|--------|
| 1 | `opt-normal-1-20260417t183111z` | `1.37 s` | `1.70 s` | `1.75 s` | `100.0 eps` | `0` | `1.20 s` | `11.82 s` | `32,350` | `50` | `519` |
| 2 | `opt-normal-2-20260417t183834z` | `1.79 s` | `1.92 s` | `1.92 s` | `100.0 eps` | `0` | `1.26 s` | `3.48 s` | `32,388` | `12` | `520` |
| 3 | `opt-normal-3-20260417t184552z` | `1.51 s` | `1.64 s` | `1.64 s` | `100.0 eps` | `0` | `1.22 s` | `11.68 s` | `32,336` | `64` | `520` |

---

## Aggregate View

| Metric | Min | Max | Mean |
|--------|-----|-----|------|
| Avg end-to-end latency | `1.37 s` | `1.79 s` | `1.56 s` |
| P95 end-to-end latency | `1.64 s` | `1.92 s` | `1.75 s` |
| P99 end-to-end latency | `1.64 s` | `1.92 s` | `1.77 s` |
| Throughput | `100.0 eps` | `100.0 eps` | `100.0 eps` |
| Hot batch duration | `1.20 s` | `1.26 s` | `1.23 s` |
| Cold batch duration | `3.48 s` | `11.82 s` | `8.99 s` |
| Processed events | `32,336` | `32,388` | `32,358` |

---

## Detailed Metrics

| Metric | Value |
|--------|-------|
| status | `succeeded` |
| pipeline_success | `true` |
| throughput_eps | `100.0` |
| consumer_lag_records | `0` |
| input_events | `33,000` |
| processed_events | `32,358` |
| processed_rate_pct | `98.05%` |
| invalid_events | `600` |
| invalid_rate_pct | `1.82%` |
| deduplicated_events | `42` |
| deduplication_rate_pct | `0.13%` |
| breach_events | `519` |
| breach_rate_pct | `1.57%` |
| processed_output_objects | `46.7` |
| invalid_output_objects | `1.0` |
| event_time_lag_p95_seconds | `0.0` |
| watermark_delay_seconds | `0.0` |
| pod_restart_count | `1.0` |
| queries_active | `1.0` |
| cost_per_run (calculated) | `~$0.017` |
| cost_per_gb_processed (calculated) | `~$2.58/GB` |

---

## Observations

- The optimized hot path remains comfortably under the 5-second SLA in all three runs.
- The cold path shows run-to-run variability, but that variability does not spill into Redis latency because the split architecture isolates the live path.
- Consumer lag stayed at `0` every time, which means the pipeline did not leave Kafka backlog behind.
- Invalid events stayed constant at `600` per run, which matches the workload design for a 5-minute, `100 eps` evaluation.
- Deduplicated counts varied between `12` and `64` because the event ordering and duplicates differ slightly run to run, but this did not affect SLA compliance.

---

## Key Findings

- The optimized split pipeline is now stable for normal load.
- The hot path is the part that matters for SLA, and it stays low-latency even when the cold path takes longer.
- The optimized design is not only faster than the historical optimized results from `2026-04-16`, it is also materially better than the baseline normal run.
- The gap between hot and cold durations is a feature, not a bug: the system is intentionally prioritizing live Redis freshness over archival throughput.

---

## Comparison With Baseline

- Baseline normal run: `2.60 s` avg, `3.11 s` p95
- Optimized normal mean across 3 runs: `1.56 s` avg, `1.75 s` p95
- Improvement in average latency: about `40%`
- Improvement in P95 latency: about `44%`

---

## Report Artifacts

- Trial 1: `s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/opt-normal-1-20260417t183111z/report.json`
- Trial 2: `s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/opt-normal-2-20260417t183834z/report.json`
- Trial 3: `s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/opt-normal-3-20260417t184552z/report.json`

---

## S3 Verification

```bash
aws s3 ls s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/opt-normal-3-20260417t184552z/processed/ --recursive | wc -l
```

Object count: `48`

---

## Cost Calculation

- Infrastructure: `5 min × $0.196/hr = ~$0.0163`
- S3: `~$0.0003`
- Total per run: `~$0.0166`
- Data volume: `33,000 events × 200 bytes ≈ 6.6 MB`
- Cost per GB: `~$2.58/GB`

### Why the cost per GB is still high at normal load

The optimized split pipeline improves latency substantially, but it still keeps the same AWS cluster alive for a relatively small 5-minute, 33k-event workload. That means the cost remains mostly fixed infrastructure cost rather than variable data cost. The pipeline is now latency-efficient, but it is not yet fully amortizing the cluster at this traffic level.

---

## Raw Report Highlights

```json
{
  "scenario": "normal",
  "pipeline_target": "optimized",
  "avg_end_to_end_latency_seconds_mean": 1.56,
  "p95_end_to_end_latency_seconds_mean": 1.75,
  "processed_events_mean": 32358,
  "consumer_lag_records": 0,
  "throughput_eps": 100.0,
  "input_events_per_run": 33000,
  "workload_duration_minutes": 5,
  "processed_output_objects_mean": 46.7
}
```
