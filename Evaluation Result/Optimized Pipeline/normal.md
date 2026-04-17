# Normal Load — Optimized Pipeline

**Run ID:** `20260416-optimized-5m-normal`  
**Date:** `2026-04-16`  
**Duration:** `5 minutes`  
**Load:** `100 eps`  
**Total input events:** `33,000`

---

## Metrics

| Metric | Value |
|--------|-------|
| status | `succeeded` |
| pipeline_success | `true` |
| avg_end_to_end_latency_seconds | `14.19 s` |
| p95_end_to_end_latency_seconds | `35.88 s` |
| ingest_to_redis_p95_seconds | `30.85 s` |
| throughput_eps | `100.0` |
| consumer_lag_records | `0` |
| input_events | `33,000` |
| processed_events | `32,396` |
| processed_rate_pct | `98.17%` |
| invalid_events | `600` |
| invalid_rate_pct | `1.82%` |
| deduplicated_events | `4` |
| breach_events | `520` |
| processed_output_objects | `60` |
| cost_per_run (calculated) | `~$0.017` |
| cost_per_gb_processed (calculated) | `~$2.58/GB` |

---

## Observations (from stream processor logs / report)

- First hot batch latency: `14.19s` avg, `35.88s` P95.
- Last hot batch latency: `14.19s` avg, `35.88s` P95.
- Latency stable / climbing / other: stable but far above the target; the optimized path still needs work.
- Any errors seen: no direct errors, but the event-time lag metrics were very high, which suggests the time windowing path was still backing up.

---

## S3 Verification

```bash
aws s3 ls s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/20260416-optimized-5m-normal/processed/ --recursive | wc -l
```

Object count: `60`

---

## Cost Calculation

- Infrastructure: `5 min × $0.196/hr = ~$0.0163`
- S3: `~$0.0003`
- Total per run: `~$0.0166`
- Data volume: `33,000 events × 200 bytes ≈ 6.6 MB`
- Cost per GB: `~$2.52/GB`

---

## Key Findings

- The optimized pipeline did not yet beat the baseline on normal load in this run: latency is much higher than the baseline normal run (`14.19s` avg, `35.88s` p95).
- Data completeness is still good: consumer lag is `0`, processed rate is `98.17%`, and invalid rate remains the expected `1.82%`.
- The high `event_time_lag_p95_seconds` and `watermark_delay_seconds` indicate the optimized path still has a backlog problem in the event-time handling stage.
- This is an important negative finding: the optimized branch is not yet the final answer for normal-load latency.

---

## Raw Report JSON

```json
{
  "avg_end_to_end_latency_seconds": 14.19,
  "breach_events": 520,
  "breach_window_output_objects": 60,
  "bucket_name": "vacciguard-tanish-baseline-ap-south-1-data",
  "configured_events_per_second": 100.0,
  "consumer_lag_records": 0,
  "controller_job_success": true,
  "cost_per_gb_processed": "Not run",
  "cost_per_run": "Not run",
  "deduplicated_events": 4,
  "deduplication_rate_pct": 0.01,
  "event_time_lag_p95_seconds": 339.39,
  "failure_reason": null,
  "ingest_to_redis_p95_seconds": 30.85,
  "input_events": 33000,
  "invalid_events": 600,
  "invalid_output_objects": 8,
  "invalid_rate_pct": 1.82,
  "kafka_topic": "vacciguard-eval-20260416-optimized-5m-normal",
  "p95_end_to_end_latency_seconds": 35.88,
  "pipeline_success": true,
  "pipeline_target": "optimized",
  "processed_events": 32396,
  "processed_output_objects": 60,
  "processed_rate_pct": 98.17,
  "recovery_time_after_failure": "Not run",
  "replay_job_success": true,
  "report_json_s3_uri": "s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/20260416-optimized-5m-normal/report.json",
  "report_markdown_s3_uri": "s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/20260416-optimized-5m-normal/report.md",
  "run_id": "20260416-optimized-5m-normal",
  "s3_prefix": "evaluations/optimized/normal/20260416-optimized-5m-normal",
  "scenario": "normal",
  "spike_result": "Not run",
  "status": "succeeded",
  "stream_metrics_source": "metrics_endpoint",
  "throughput_eps": 100.0,
  "watermark_delay_seconds": 655.39,
  "workload_duration_minutes": 5,
  "workload_family_version": "evaluation-workload-v1"
}
```
