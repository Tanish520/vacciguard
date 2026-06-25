# Failure Recovery - Optimized Pipeline

**Branch:** `baseline-spike-fix`  
**Pipeline:** `optimized`  
**Scenario:** `failure-recovery`  
**Load:** `100 eps`  
**Duration:** `10 minutes`  
**Failure injection:** `Kill optimized-hot pod at the 3-minute mark`  
**Runs:** `3`

---

## Executive Summary

The optimized split pipeline recovers quickly and consistently from a forced hot-pod deletion. Across three repeated runs, the hot service came back in roughly `15.7 seconds` on average, and the first post-restart hot batch returned within about `8.7 seconds` on average after readiness. Consumer lag stayed at `0` in every run, which is the strongest evidence that no Kafka data was lost during the restart window.

The main difference from the baseline failure-recovery run is that the optimized architecture keeps the live path isolated. The hot service restarts and resumes Redis updates without being blocked by the cold archival path, so the pipeline returns to normal operation more quickly and more predictably.

---

## Run Table

| Trial | Run ID | Kill Time | Ready Time | Recovery | First Hot Batch | Avg Latency | P95 | Hot Batch | Cold Batch | Lag | Processed |
|------|--------|-----------|------------|----------|-----------------|-------------|-----|-----------|------------|-----|-----------|
| 1 | `opt-fr-1-20260417t191730z` | `19:20:57Z` | `19:21:12Z` | `15 s` | `19:21:21Z` | `1.69 s` | `1.73 s` | `1.26 s` | `3.10 s` | `0` | `64,766` |
| 2 | `opt-fr-2-20260417t193103z` | `19:35:10Z` | `19:35:26Z` | `16 s` | `19:35:35Z` | `1.61 s` | `2.00 s` | `0.70 s` | `3.20 s` | `0` | `64,760` |
| 3 | `opt-fr-3-20260417t194413z` | `19:48:21Z` | `19:48:37Z` | `16 s` | `19:48:45Z` | `2.04 s` | `2.07 s` | `1.23 s` | `11.05 s` | `0` | `64,748` |

---

## Aggregate View

| Metric | Min | Max | Mean |
|--------|-----|-----|------|
| Recovery time | `15 s` | `16 s` | `15.67 s` |
| Avg end-to-end latency | `1.61 s` | `2.04 s` | `1.78 s` |
| P95 end-to-end latency | `1.73 s` | `2.07 s` | `1.93 s` |
| Hot-path gap after readiness | `8 s` | `9 s` | `8.67 s` |
| Hot batch duration | `0.70 s` | `1.26 s` | `1.06 s` |
| Cold batch duration | `3.10 s` | `11.05 s` | `5.78 s` |
| Processed events | `64,748` | `64,766` | `64,758` |

---

## Detailed Metrics

| Metric | Value |
|--------|-------|
| status | `succeeded` |
| pipeline_success | `true` |
| throughput_eps | `100.0` |
| consumer_lag_records | `0` |
| input_events | `66,000` |
| processed_events | `64,758` |
| processed_rate_pct | `98.12%` |
| invalid_events | `1,200` |
| invalid_rate_pct | `1.82%` |
| deduplicated_events | `42` |
| deduplication_rate_pct | `0.06%` |
| breach_events | `1,008` |
| breach_rate_pct | `1.53%` |
| processed_output_objects | `89.3` |
| invalid_output_objects | `0.7` |
| event_time_lag_p95_seconds | `0.0` |
| watermark_delay_seconds | `0.0` |
| pod_restart_count | `1.3` |
| queries_active | `1.0` |
| recovery_time_after_failure | `15.67 s` |
| cost_per_run (calculated) | `~$0.033` |
| cost_per_gb_processed (calculated) | `~$2.58/GB` |

---

## Observations

- The hot service consistently comes back in about a quarter of a minute after a forced delete.
- The first live Redis batch is restored within single-digit seconds after readiness.
- Consumer lag stays at `0`, so the restart does not cause Kafka backlog loss.
- The cold service may continue to vary in runtime, but it does not prevent the hot service from recovering.
- The third run had the longest cold batch, but the hot SLA still recovered normally, which is exactly the behavior this architecture is supposed to provide.

---

## Key Findings

- The optimized pipeline is resilient under abrupt hot-path failure.
- Recovery is repeatable: the three runs are very close to each other in both recovery time and post-restart latency.
- The live SLA path is protected even while the archival path is still doing heavier work.
- The result supports the project claim that the split architecture provides graceful degradation instead of a full pipeline stall.

---

## Comparison With Baseline

- Baseline failure-recovery run: `24 s` recovery, `2.46 s` avg latency, `2.90 s` p95
- Optimized failure-recovery mean across 3 runs: `15.67 s` recovery, `1.78 s` avg latency, `1.93 s` p95
- Improvement in recovery time: about `35%`
- Improvement in average latency: about `28%`
- Improvement in P95 latency: about `34%`

---

## Report Artifacts

- Trial 1: `s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/failure-recovery/opt-fr-1-20260417t191730z/report.json`
- Trial 2: `s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/failure-recovery/opt-fr-2-20260417t193103z/report.json`
- Trial 3: `s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/failure-recovery/opt-fr-3-20260417t194413z/report.json`

---

## S3 Verification

```bash
aws s3 ls s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/failure-recovery/opt-fr-3-20260417t194413z/processed/ --recursive | wc -l
```

Object count: `92`

---

## Cost Calculation

- Infrastructure: `10 min × $0.196/hr = ~$0.0327`
- S3: `~$0.0006`
- Total per run: `~$0.0333`
- Data volume: `66,000 events × 200 bytes ≈ 13.2 MB`
- Cost per GB: `~$2.58/GB`

### Why the cost per GB is similar to normal load here

The failure-recovery run uses the same cluster for a longer 10-minute window, but it also processes proportionally more data. That keeps the cost-per-GB roughly in the same range as the normal run, while the recovery test gives us a better view of resilience and restart behavior.

---

## Raw Report Highlights

```json
{
  "scenario": "failure-recovery",
  "pipeline_target": "optimized",
  "avg_end_to_end_latency_seconds_mean": 1.78,
  "p95_end_to_end_latency_seconds_mean": 1.93,
  "recovery_time_seconds_mean": 15.67,
  "consumer_lag_records": 0,
  "throughput_eps": 100.0,
  "input_events_per_run": 66000,
  "workload_duration_minutes": 10,
  "failure_injection": "optimized-hot pod deletion at 3 minutes",
  "processed_events_mean": 64758
}
```
