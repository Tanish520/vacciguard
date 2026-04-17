# Failure Recovery — Optimized Pipeline

**Run ID:** `Not run yet`  
**Date:** `Not run yet`  
**Duration:** `Not run yet`  
**Load:** `100 eps`  
**Total input events:** `Not run yet`

---

## Metrics

| Metric | Value |
|--------|-------|
| status | `Not run` |
| pipeline_success | `Not run` |
| avg_end_to_end_latency_seconds | `Not run` |
| p95_end_to_end_latency_seconds | `Not run` |
| ingest_to_redis_p95_seconds | `Not run` |
| throughput_eps | `Not run` |
| consumer_lag_records | `Not run` |
| input_events | `Not run` |
| processed_events | `Not run` |
| processed_rate_pct | `Not run` |
| invalid_events | `Not run` |
| invalid_rate_pct | `Not run` |
| deduplicated_events | `Not run` |
| breach_events | `Not run` |
| processed_output_objects | `Not run` |
| cost_per_run (calculated) | `Not run` |
| cost_per_gb_processed (calculated) | `Not run` |

---

## Observations (from stream processor logs)

- First hot batch latency: `Not run`.
- Last hot batch latency: `Not run`.
- Latency stable / climbing / other: `Not run`.
- Any errors seen: `Not run`.

---

## S3 Verification

```bash
aws s3 ls s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/failure-recovery/<run-id>/processed/ --recursive | wc -l
```

Object count: `Not run`

---

## Cost Calculation

- Infrastructure: `Not run`
- S3: `Not run`
- Total per run: `Not run`
- Data volume: `Not run`
- Cost per GB: `Not run`

---

## Key Findings

- This scenario has not been executed yet for the optimized pipeline.
- Once the run exists, this file should be updated with the actual recovery time, consumer lag, and post-recovery latency numbers.
- Keep this file separate from the baseline failure-recovery report so the two pipelines remain easy to compare later.

---

## Raw Report JSON

`Not run yet`
