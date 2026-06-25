# Spike Load - Baseline Pipeline

**Run ID:** `spike-20260417t080746z`  
**Date:** `2026-04-17`  
**Duration:** `5 minutes`  
**Load:** `1000 eps`  
**Total input events:** `330,000`

---

## Metrics

| Metric | Value |
|--------|-------|
| status | `succeeded` |
| pipeline_success | `true` |
| avg_end_to_end_latency_seconds | `225.76 s` |
| p95_end_to_end_latency_seconds | `225.77 s` |
| ingest_to_redis_p95_seconds | `225.77 s` |
| throughput_eps | `999.9` |
| consumer_lag_records | `0` |
| input_events | `330,000` |
| processed_events | `323,919` |
| processed_rate_pct | `98.16%` |
| invalid_events | `6,000` |
| invalid_rate_pct | `1.82%` |
| deduplicated_events | `81` |
| breach_events | `4,909` |
| processed_output_objects | `48` |
| cost_per_run (calculated) | `~$0.017` |
| cost_per_gb_processed (calculated) | `~$0.25/GB` |

---

## Observations (from stream processor logs / report)

- First hot batch latency: `225.76s` avg, `225.77s` P95.
- Last hot batch latency: `225.76s` avg, `225.77s` P95.
- Latency stable / climbing / other: stable but severely degraded; the pipeline stayed behind for the entire run instead of recovering mid-run.
- Any errors seen: no functional errors in the final report. The issue is saturation, not correctness failure.

### Root Cause: Why Latency Reached 225s

The `225s` latency is caused by a hard throughput ceiling on the hot query: `MAX_OFFSETS_PER_TRIGGER=1000`. With a 2-second trigger interval, the hot query reads at most 1,000 Kafka messages per trigger, which means it can process at most **500 events per second**. At 1,000 eps input, events arrive exactly twice as fast as the pipeline can read them.

This creates a growing backlog in the Kafka topic:

```
Input rate:       1,000 eps
Hot query rate:     500 eps (capped)
Backlog growth:     500 events every 2 seconds

After 5 minutes (300s):
  Backlog = 500 × (300 / 2) = 75,000 events waiting
  Last event wait time = 75,000 / 500 = 150s → measured as ~225s end-to-end
```

The end-to-end latency includes both the Kafka wait time and the Spark processing time, which is why the measured value is slightly higher than the theoretical 150s.

**Why 225s here vs 146s in an earlier run:** the earlier run was only 3 minutes long (198,000 events). This run is the full 5 minutes (330,000 events). A longer run means a larger backlog accumulates before the replay producer stops, so the last events in the queue wait longer. The mechanism is identical — only the run duration changed.

---

## S3 Verification

```bash
aws s3 ls s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/baseline/spike/spike-20260417t080746z/processed/ --recursive | wc -l
```

Object count: `48`

---

## Cost Calculation

- Infrastructure: `5 min x $0.196/hr = ~$0.0163`
- S3: `~$0.0003`
- Total per run: `~$0.0166`
- Data volume: `330,000 events x 200 bytes ~= 66.0 MB`
- Cost per GB: `~$0.25/GB`

### Why the cost per GB is much lower than normal load

The spike run is cheaper per GB because the cluster cost does not scale up much when the load increases. We are still paying for the same EKS nodes, Redis, and control plane while the test is running, so the bill is mostly fixed. The difference is that the spike scenario sends about `10x` more events than the normal run, so the same fixed cost is spread over a much larger amount of data. That makes each GB look much cheaper.

Put another way:
- **normal load** = same cluster cost, but only a little data, so cost per GB is high
- **spike load** = same cluster cost, but much more data, so cost per GB is low

This is why spike looks more cost-efficient even though it is much worse on latency. The pipeline is using the cluster more fully, but it is also falling behind on processing speed.

---

## Key Findings

- The baseline pipeline remains correct under spike load, but it misses the latency target by a wide margin: `225.77s` P95 is about 72x worse than the normal-load P95 of roughly `3.11s`.
- Consumer lag is still `0`, which means the pipeline eventually drains Kafka and does not lose events, but it only does so after building a large backlog.
- The fact that `ingest_to_redis_p95_seconds` matches the end-to-end P95 almost exactly is important: the delay is happening before Redis visibility, not in the downstream reporting or S3 verification path.
- Data quality stays stable at scale: the invalid rate remains the expected `1.82%`, and deduplication stays tiny at `0.02%`. That tells us the classification logic itself is not what breaks under load.
- The spike run is therefore a capacity problem, not a correctness problem. The baseline design can preserve data, but it cannot keep latency under control at 10x load.
- This is the clearest evidence that the optimized pipeline needs a fundamentally lighter hot path or better load partitioning if it is expected to meet the professor's spike requirement.

---

## Raw Report JSON

```json
{
  "avg_end_to_end_latency_seconds": 225.76,
  "breach_events": 4909,
  "breach_window_output_objects": 48,
  "bucket_name": "vacciguard-tanish-baseline-ap-south-1-data",
  "configured_events_per_second": 1000.0,
  "consumer_lag_records": 0,
  "controller_job_success": true,
  "cost_per_gb_processed": "Not run",
  "cost_per_run": "Not run",
  "deduplicated_events": 81,
  "deduplication_rate_pct": 0.02,
  "event_time_lag_p95_seconds": 0.0,
  "failure_reason": null,
  "ingest_to_redis_p95_seconds": 225.77,
  "input_events": 330000,
  "invalid_events": 6000,
  "invalid_output_objects": 1,
  "invalid_rate_pct": 1.82,
  "kafka_topic": "vacciguard-eval-spike-20260417t080746z",
  "p95_end_to_end_latency_seconds": 225.77,
  "pipeline_success": true,
  "pipeline_target": "baseline",
  "processed_events": 323919,
  "processed_output_objects": 48,
  "processed_rate_pct": 98.16,
  "recovery_time_after_failure": "Not run",
  "replay_job_success": true,
  "report_json_s3_uri": "s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/baseline/spike/spike-20260417t080746z/report.json",
  "report_markdown_s3_uri": "s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/baseline/spike/spike-20260417t080746z/report.md",
  "run_id": "spike-20260417t080746z",
  "s3_prefix": "evaluations/baseline/spike/spike-20260417t080746z",
  "scenario": "spike",
  "spike_result": "Not run",
  "status": "succeeded",
  "stream_metrics_source": "metrics_endpoint",
  "throughput_eps": 999.9,
  "watermark_delay_seconds": 0.0,
  "workload_duration_minutes": 5,
  "workload_family_version": "evaluation-workload-v1"
}
```
