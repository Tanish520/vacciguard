# Evaluation Report: 20260416-optimized-5m-spike

## Summary
- pipeline target: optimized
- scenario: spike
- status: succeeded
- workload family version: evaluation-workload-v1
- report markdown: s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/spike/20260416-optimized-5m-spike/report.md
- report json: s3://vacciguard-tanish-baseline-ap-south-1-data/evaluations/optimized/spike/20260416-optimized-5m-spike/report.json

## Metrics
| Metric | Baseline |
|---|---:|
| Workload family version | evaluation-workload-v1 |
| Scenario | spike |
| Configured replay rate | 1000.00 events/s |
| Stream metrics source | metrics_endpoint |
| Avg end-to-end latency | 22.94 s |
| P95 latency | 33.76 s |
| Throughput | 999.90 events/s |
| Input events | 330000 |
| Processed events | 323898 |
| Processed rate | 98.15% |
| Invalid events | 6000 |
| Invalid rate | 1.82% |
| Deduplicated events | 102 |
| Deduplication rate | 0.03% |
| Breach events | 4907 |
| Event-time lag P95 | 366.20 s |
| Ingest-to-Redis P95 | 28.85 s |
| Watermark delay | 681.20 s |
| Consumer lag | 0 records |
| Processed output objects | 48 |
| Invalid output objects | 8 |
| Breach window output objects | 48 |
| Pipeline success | True |
| Controller job success | True |
| Replay job success | True |
| 10x spike success/failure | Not run |
| Recovery time after failure | Not run |
| Cost per run | Not run |
| Cost per GB processed | Not run |
