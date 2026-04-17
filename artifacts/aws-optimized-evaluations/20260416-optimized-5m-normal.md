# Evaluation Report: 20260416-optimized-5m-normal

## Summary
- pipeline target: optimized
- scenario: normal
- status: succeeded
- workload family version: evaluation-workload-v1
- report markdown: s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/20260416-optimized-5m-normal/report.md
- report json: s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/normal/20260416-optimized-5m-normal/report.json

## Metrics
| Metric | Baseline |
|---|---:|
| Workload family version | evaluation-workload-v1 |
| Scenario | normal |
| Configured replay rate | 100.00 events/s |
| Stream metrics source | metrics_endpoint |
| Avg end-to-end latency | 14.19 s |
| P95 latency | 35.88 s |
| Throughput | 100.00 events/s |
| Input events | 33000 |
| Processed events | 32396 |
| Processed rate | 98.17% |
| Invalid events | 600 |
| Invalid rate | 1.82% |
| Deduplicated events | 4 |
| Deduplication rate | 0.01% |
| Breach events | 520 |
| Event-time lag P95 | 339.39 s |
| Ingest-to-Redis P95 | 30.85 s |
| Watermark delay | 655.39 s |
| Consumer lag | 0 records |
| Processed output objects | 60 |
| Invalid output objects | 8 |
| Breach window output objects | 60 |
| Pipeline success | True |
| Controller job success | True |
| Replay job success | True |
| 10x spike success/failure | Not run |
| Recovery time after failure | Not run |
| Cost per run | Not run |
| Cost per GB processed | Not run |
