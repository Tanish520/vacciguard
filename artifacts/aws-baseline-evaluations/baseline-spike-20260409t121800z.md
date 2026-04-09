# AWS Baseline Evaluation baseline-spike-20260409t121800z

- Run ID: `baseline-spike-20260409t121800z`
- Scenario: `spike`
- Workload family version: `evaluation-workload-v1`
- Workload file: `data/workloads/evaluation/v1/spike.events.ndjson`
- Workload source: `s3`
- Workload size bytes: `151766103`
- Declared input events: `792000`
- Configured replay rate: `1000.0`
- Kafka topic: `vacciguard-eval-baseline-spike-20260409t121800z`
- S3 prefix: `s3://vacciguard-baseline-data/evaluations/baseline-spike-20260409t121800z/`
- Status: `aborted`
- Abort reason: `requested 1000 eps replay rate was not achievable by the current replay path`

## Evaluation Summary

| Metric | Baseline |
|---|---:|
| Workload family version | evaluation-workload-v1 |
| Scenario | spike |
| Configured replay rate | 1000.00 events/s |
| Avg end-to-end latency | Not measured |
| P95 latency | Not measured |
| Throughput | 146.90 events/s |
| 10x spike success/failure | Failed - replay bottleneck |
| Recovery time after failure | Not run |
| Input events | 792000 |
| Processed events | Not measured before abort |
| Invalid events | Not measured before abort |
| Deduplicated events | Not measured before abort |
| Breach events | Not measured before abort |
| Cost per run | Not run |
| Cost per GB processed | Not run |

## Important Findings

- The spike workload was staged correctly from S3 and the replay job started on the S3-capable image `baseline-monitoring-s3-20260409`.
- The requested `1000 eps` replay rate was not reached. Live replay logs showed the producer stabilizing around `146.9 eps` after the initial warm-up.
- Because the replay side itself was the bottleneck, the run was intentionally aborted instead of waiting for a long timeout that would not improve the validity of the result.
- Before the abort, the pipeline had already written `29` processed objects and `29` breach-window objects under the spike run prefix.
- This run should be interpreted as a failed high-load evaluation at the requested 10x rate, not as a completed throughput/latency benchmark.

## Replay Evidence

```text
2026-04-09T06:49:38Z INFO Sent 8000/792000  actual 146.6 eps
2026-04-09T06:49:39Z INFO Sent 8150/792000  actual 146.7 eps
2026-04-09T06:49:42Z INFO Sent 8600/792000  actual 146.7 eps
2026-04-09T06:49:45Z INFO Sent 8950/792000  actual 146.9 eps
```

## S3 Evidence Summary

```text
processed_objects=29
invalid_objects=0
breach_objects=29
```
