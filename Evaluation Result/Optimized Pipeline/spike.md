# Spike Load — Optimized Pipeline

**Run ID:** `20260416-optimized-5m-spike`  
**Date:** `2026-04-16`  
**Duration:** `5 minutes`  
**Load:** `1000 eps`  
**Total input events:** `330,000`

---

## Metrics

| Metric | Value |
|--------|-------|
| status | `succeeded` |
| pipeline_success | `true` |
| avg_end_to_end_latency_seconds | `22.94 s` |
| p95_end_to_end_latency_seconds | `33.76 s` |
| ingest_to_redis_p95_seconds | `28.85 s` |
| throughput_eps | `999.9` |
| consumer_lag_records | `0` |
| input_events | `330,000` |
| processed_events | `323,898` |
| processed_rate_pct | `98.15%` |
| invalid_events | `6,000` |
| invalid_rate_pct | `1.82%` |
| deduplicated_events | `102` |
| breach_events | `4,907` |
| processed_output_objects | `48` |
| cost_per_run (calculated) | `~$0.017` |
| cost_per_gb_processed (calculated) | `~$0.25/GB` |

---

## Observations (from stream processor logs / report)

- First hot batch latency: `22.94s` avg, `33.76s` P95.
- Last hot batch latency: `22.94s` avg, `33.76s` P95.
- Latency stable / climbing / other: stable, but still too high for the 5-second SLA.
- Any errors seen: no functional errors; the optimized pipeline preserved correctness but still carried a sizeable backlog under spike load.

---

## S3 Verification

```bash
aws s3 ls s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/spike/20260416-optimized-5m-spike/processed/ --recursive | wc -l
```

Object count: `48`

---

## Cost Calculation

- Infrastructure: `5 min × $0.196/hr = ~$0.0163`
- S3: `~$0.0003`
- Total per run: `~$0.0166`
- Data volume: `330,000 events × 200 bytes ≈ 66.0 MB`
- Cost per GB: `~$0.25/GB`

---

## Key Findings

- The optimized pipeline substantially improved spike latency compared with the baseline spike run, but it still does not meet the <5s SLA.
- Consumer lag remains `0`, so the run preserved data completeness even at 10× load.
- Event-time lag and watermark delay are very high, which means the optimized path is still paying a substantial cost in streaming state / window settlement.
- This is the key comparison result for the spike scenario: optimization helped, but it did not fully solve the spike bottleneck.

---

## Raw Report JSON

```json
{
  "avg_end_to_end_latency_seconds": 22.94,
  "breach_events": 4907,
  "breach_window_output_objects": 48,
  "bucket_name": "vacciguard-tanish-baseline-ap-south-1-data",
  "configured_events_per_second": 1000.0,
  "consumer_lag_records": 0,
  "controller_job_success": true,
  "cost_per_gb_processed": "Not run",
  "cost_per_run": "Not run",
  "deduplicated_events": 102,
  "deduplication_rate_pct": 0.03,
  "event_time_lag_p95_seconds": 366.2,
  "failure_reason": null,
  "ingest_to_redis_p95_seconds": 28.85,
  "input_events": 330000,
  "invalid_events": 6000,
  "invalid_output_objects": 8,
  "invalid_rate_pct": 1.82,
  "kafka_topic": "vacciguard-eval-20260416-optimized-5m-spike",
  "p95_end_to_end_latency_seconds": 33.76,
  "pipeline_success": true,
  "pipeline_target": "optimized",
  "processed_events": 323898,
  "processed_output_objects": 48,
  "processed_rate_pct": 98.15,
  "recovery_time_after_failure": "Not run",
  "replay_job_success": true,
  "report_json_s3_uri": "s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/spike/20260416-optimized-5m-spike/report.json",
  "report_markdown_s3_uri": "s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/spike/20260416-optimized-5m-spike/report.md",
  "run_id": "20260416-optimized-5m-spike",
  "s3_prefix": "evaluations/optimized/spike/20260416-optimized-5m-spike",
  "scenario": "spike",
  "spike_result": "Not run",
  "status": "succeeded",
  "stream_metrics_source": "metrics_endpoint",
  "throughput_eps": 999.9,
  "watermark_delay_seconds": 681.2,
  "workload_duration_minutes": 5,
  "workload_family_version": "evaluation-workload-v1"
}
```
