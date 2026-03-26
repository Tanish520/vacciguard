# Baseline Experiment Results

Use this file to record the outcome of each baseline run in a consistent format.

## Experiment Protocol

- Baseline mode: SNS enabled
- Repetitions per workload: 3
- DynamoDB reset before each run: yes
- Measurement window: 5 minutes
- Warm-up window: record separately if used
- CloudWatch namespace: `VacciGuard/BaselinePipeline`

## Workload Matrix

| Workload label | Active fridges | Records/sec | Duration (sec) | Repetitions |
|---|---:|---:|---:|---:|
| W1 | 500 | 500 | 300 | 3 |
| W2 | 1000 | 1000 | 300 | 3 |
| W3 | 5000 | 5000 | 300 | 3 |
| W4 | 10000 | 10000 | 300 | 3 |
| W5 | 20000 | 20000 | 300 | 3 |

## Per-Run Recording Template

For each run, record:

- run label
- start time
- end time
- pipeline image / commit
- DynamoDB latency P50/P90/P99
- alert latency P50/P90/P99
- throughput
- total processed
- total failed
- breach count
- alerts published
- duplicate alerts
- SLA violations
- observations

### Run Template

```text
Run Label:
Workload:
Repetition:
Start Time:
End Time:
Commit / Image:

Metrics
- ThroughputRecordsPerSecond:
- RecordsProcessedTotal:
- RecordsFailedTotal:
- BreachEventsTotal:
- AlertsPublishedTotal:
- DuplicateAlertsTotal:
- SlaViolationsTotal:
- DynamoDbLatencyP50Ms:
- DynamoDbLatencyP90Ms:
- DynamoDbLatencyP99Ms:
- AlertLatencyP50Ms:
- AlertLatencyP90Ms:
- AlertLatencyP99Ms:

Important Findings
- 
- 
- 
```

## Result Summary Table

| Workload | Repetition | Throughput | Processed | Failed | Breaches | Alerts | Duplicates | SLA Violations | DynamoDB P99 (ms) | Alert P99 (ms) | Key finding |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 500/sec | 1 |  |  |  |  |  |  |  |  |  |  |
| 500/sec | 2 |  |  |  |  |  |  |  |  |  |  |
| 500/sec | 3 |  |  |  |  |  |  |  |  |  |  |
| 1000/sec | 1 |  |  |  |  |  |  |  |  |  |  |
| 1000/sec | 2 |  |  |  |  |  |  |  |  |  |  |
| 1000/sec | 3 |  |  |  |  |  |  |  |  |  |  |
| 5000/sec | 1 |  |  |  |  |  |  |  |  |  |  |
| 5000/sec | 2 |  |  |  |  |  |  |  |  |  |  |
| 5000/sec | 3 |  |  |  |  |  |  |  |  |  |  |
| 10000/sec | 1 |  |  |  |  |  |  |  |  |  |  |
| 10000/sec | 2 |  |  |  |  |  |  |  |  |  |  |
| 10000/sec | 3 |  |  |  |  |  |  |  |  |  |  |
| 20000/sec | 1 |  |  |  |  |  |  |  |  |  |  |
| 20000/sec | 2 |  |  |  |  |  |  |  |  |  |  |
| 20000/sec | 3 |  |  |  |  |  |  |  |  |  |  |

## Cross-Run Findings

Use this section to summarize the most important observations after all repetitions:

- At what workload did throughput stop increasing?
- At what workload did `AlertLatencyP99Ms` cross the 5000 ms SLA threshold?
- At what workload did `SlaViolationsTotal` become significant?
- Did duplicate alerts remain suppressed?
- Did failure counts remain near zero?
- Which workload level represents the practical limit of the baseline?
