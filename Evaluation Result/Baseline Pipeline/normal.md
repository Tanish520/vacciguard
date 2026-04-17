# Normal Load — Baseline Pipeline

**Run ID:** `normal-20260417t074819z`  
**Date:** `2026-04-17`  
**Duration:** `5 minutes`  
**Load:** `100 eps`  
**Total input events:** `33,000`

---

## Metrics

| Metric | Value |
|--------|-------|
| status | `succeeded` |
| pipeline_success | `true` |
| avg_end_to_end_latency_seconds | `2.60 s` |
| p95_end_to_end_latency_seconds | `3.11 s` |
| ingest_to_redis_p95_seconds | `3.15 s` |
| throughput_eps | `100.0` |
| consumer_lag_records | `0` |
| input_events | `33,000` |
| processed_events | `32,349` |
| processed_rate_pct | `98.03%` |
| invalid_events | `600` |
| invalid_rate_pct | `1.82%` |
| deduplicated_events | `51` |
| breach_events | `519` |
| processed_output_objects | `52` |
| cost_per_run (calculated) | `~$0.017` |
| cost_per_gb_processed (calculated) | `~$2.58/GB` |

---

## Observations (from stream processor logs / report)

- First hot batch latency: `2.60s` avg, `3.11s` P95.
- Last hot batch latency: `2.60s` avg, `3.11s` P95.
- Latency stable / climbing / other: stable; the run stayed under the 5-second SLA throughout the report window.
- Any errors seen: no.

---

## S3 Verification

```bash
aws s3 ls s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/baseline/normal/normal-20260417t074819z/processed/ --recursive | wc -l
```

Object count: `52`

---

## Cost Calculation

- Infrastructure: `5 min × $0.196/hr = ~$0.0163`
- S3: `~$0.0003`
- Total per run: `~$0.0166`
- Data volume: `33,000 events × 200 bytes ≈ 6.6 MB`
- Cost per GB: `~$2.58/GB`

### Why the cost per GB is relatively high here

The normal-load run keeps the same AWS cluster alive even though it processes a relatively small amount of data. That means most of the expense is fixed infrastructure cost, not variable storage or network cost. Because the run only moves about `6.6 MB`, the same cluster cost is spread across a small data volume, which makes the cost per GB look high. In other words, the pipeline is efficient in latency terms, but it is not fully amortizing the cluster cost at this traffic level.

---

## Key Findings

- The normal-load baseline passes the main SLA: avg latency `2.60s` and p95 latency `3.11s` are both comfortably under the 5-second target.
- Consumer lag is `0`, which means the pipeline drained the Kafka topic completely and did not leave backlog behind.
- Data quality is consistent with the workload design: `1.82%` invalid events, `0.15%` deduplicated events, and `519` breach events.
- Cold storage stayed healthy, with `52` processed S3 objects written during the run.

---

## Raw Report JSON

```json
{
  "avg_end_to_end_latency_seconds": 2.6,
  "breach_events": 519,
  "breach_window_output_objects": 43,
  "bucket_name": "vacciguard-tanish-baseline-ap-south-1-data",
  "configured_events_per_second": 100.0,
  "consumer_lag_records": 0,
  "controller_job_success": true,
  "cost_per_gb_processed": "Not run",
  "cost_per_run": "Not run",
  "deduplicated_events": 51,
  "deduplication_rate_pct": 0.15,
  "event_time_lag_p95_seconds": 0.0,
  "failure_reason": null,
  "ingest_to_redis_p95_seconds": 3.15,
  "input_events": 33000,
  "invalid_events": 600,
  "invalid_output_objects": 1,
  "invalid_rate_pct": 1.82,
  "kafka_topic": "vacciguard-eval-normal-20260417t074819z",
  "p95_end_to_end_latency_seconds": 3.11,
  "pipeline_success": true,
  "pipeline_target": "baseline",
  "processed_events": 32349,
  "processed_output_objects": 52,
  "processed_rate_pct": 98.03,
  "recovery_time_after_failure": "Not run",
  "replay_job_success": true,
  "report_json_s3_uri": "s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/baseline/normal/normal-20260417t074819z/report.json",
  "report_markdown_s3_uri": "s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/baseline/normal/normal-20260417t074819z/report.md",
  "run_id": "normal-20260417t074819z",
  "s3_prefix": "evaluations/baseline/normal/normal-20260417t074819z",
  "scenario": "normal",
  "spike_result": "Not run",
  "status": "succeeded",
  "stream_metrics_source": "metrics_endpoint",
  "throughput_eps": 100.0,
  "watermark_delay_seconds": 0.0,
  "workload_duration_minutes": 5,
  "workload_family_version": "evaluation-workload-v1"
}
```
